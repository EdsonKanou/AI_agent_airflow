"""
DAG ETL : S3 → PostgreSQL

Ce DAG télécharge des fichiers CSV depuis S3, les transforme et les charge dans PostgreSQL.

Pattern typique :
- Utilisation de Hooks pour les connexions
- Variables Airflow pour la configuration
- Gestion des erreurs avec retry
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import io


# === Configuration via Airflow Variables ===
S3_BUCKET = Variable.get("s3_bucket", default_var="my-data-bucket")
S3_KEY = Variable.get("s3_key", default_var="raw/sales_data.csv")


def extract_from_s3(**context):
    """
    Extraire les données depuis S3.
    
    Utilise S3Hook pour se connecter (nécessite une connexion 'aws_default' dans Airflow).
    """
    print(f"📥 Téléchargement depuis s3://{S3_BUCKET}/{S3_KEY}")
    
    # Créer le hook S3 (connexion configurée dans Airflow UI)
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Télécharger le fichier
    file_content = s3_hook.read_key(
        key=S3_KEY,
        bucket_name=S3_BUCKET
    )
    
    # Lire le CSV
    df = pd.read_csv(io.StringIO(file_content))
    
    print(f"✅ {len(df)} lignes téléchargées")
    print(f"📊 Colonnes : {df.columns.tolist()}")
    
    # Sauvegarder temporairement
    temp_path = f"/tmp/s3_data_{context['ds_nodash']}.csv"
    df.to_csv(temp_path, index=False)
    
    # Passer le chemin à la tâche suivante via XCom
    context['ti'].xcom_push(key='temp_csv_path', value=temp_path)
    
    return len(df)


def transform_data(**context):
    """
    Transformer les données.
    
    Nettoyage classique :
    - Supprimer les doublons
    - Gérer les valeurs manquantes
    - Valider les types
    """
    # Récupérer le chemin depuis XCom
    ti = context['ti']
    temp_path = ti.xcom_pull(task_ids='extract_from_s3', key='temp_csv_path')
    
    print(f"🔧 Transformation des données depuis {temp_path}")
    
    # Lire le CSV
    df = pd.read_csv(temp_path)
    
    # Supprimer les doublons
    initial_count = len(df)
    df = df.drop_duplicates()
    print(f"🗑️  {initial_count - len(df)} doublons supprimés")
    
    # Supprimer les lignes avec valeurs manquantes critiques
    df = df.dropna(subset=['customer_id', 'amount'])
    
    # Valider que amount > 0
    df = df[df['amount'] > 0]
    
    # Ajouter une colonne de date de traitement
    df['processed_at'] = datetime.now()
    
    print(f"✅ {len(df)} lignes après transformation")
    
    # Sauvegarder
    cleaned_path = temp_path.replace('.csv', '_cleaned.csv')
    df.to_csv(cleaned_path, index=False)
    
    ti.xcom_push(key='cleaned_csv_path', value=cleaned_path)
    
    return len(df)


def load_to_postgres(**context):
    """
    Charger les données dans PostgreSQL.
    
    Utilise PostgresHook et gère les transactions.
    """
    ti = context['ti']
    cleaned_path = ti.xcom_pull(task_ids='transform_data', key='cleaned_csv_path')
    
    print(f"💾 Chargement dans PostgreSQL depuis {cleaned_path}")
    
    # Lire les données nettoyées
    df = pd.read_csv(cleaned_path)
    
    # Créer le hook PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Préparer les données pour l'insertion
    records = df.to_dict('records')
    
    # Insertion par batch de 1000 lignes
    batch_size = 1000
    total_inserted = 0
    
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        
        # Construire la requête INSERT
        # Note : En production, utilisez COPY ou insert_rows() du hook
        insert_sql = """
            INSERT INTO sales_data (customer_id, amount, date, processed_at)
            VALUES (%(customer_id)s, %(amount)s, %(date)s, %(processed_at)s)
            ON CONFLICT (customer_id, date) DO UPDATE
            SET amount = EXCLUDED.amount, processed_at = EXCLUDED.processed_at;
        """
        
        # Exécuter le batch
        pg_hook.run(insert_sql, parameters=batch, autocommit=True)
        
        total_inserted += len(batch)
        print(f"  ✅ {total_inserted}/{len(records)} lignes insérées")
    
    print(f"✅ Chargement terminé : {total_inserted} lignes")
    
    return total_inserted


# === Définition du DAG ===

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email': ['data-alerts@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='s3_to_postgres_etl',
    default_args=default_args,
    description='ETL pipeline: Download from S3, transform, load to PostgreSQL',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 's3', 'postgres', 'production'],
    max_active_runs=1,  # Une seule exécution à la fois
) as dag:
    
    # Tâche 1 : Extraction
    extract = PythonOperator(
        task_id='extract_from_s3',
        python_callable=extract_from_s3,
        provide_context=True,
    )
    
    # Tâche 2 : Transformation
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )
    
    # Tâche 3 : Chargement
    load = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )
    
    # Dépendances
    extract >> transform >> load