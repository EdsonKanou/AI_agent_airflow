"""
DAG de synchronisation PostgreSQL → MySQL.

Ce DAG illustre :
- Connexions à plusieurs bases de données
- Mapping de schémas différents
- Gestion des transactions
- Pattern de synchronisation incrémentale
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.operators.python import PythonOperator
import pandas as pd


def extract_from_postgres(**context):
    """
    Extraire les données depuis PostgreSQL.
    
    Extraction incrémentale basée sur last_updated.
    """
    print("📥 Extraction depuis PostgreSQL...")
    
    # Récupérer la dernière date de sync depuis XCom (ou défaut)
    ti = context['ti']
    last_sync = ti.xcom_pull(
        task_ids='extract_from_postgres',
        key='last_sync_date',
        default='2024-01-01'
    )
    
    print(f"🔍 Dernière sync : {last_sync}")
    
    # Connexion PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_source')
    
    # Requête incrémentale
    query = f"""
        SELECT 
            id,
            user_id,
            product_name,
            quantity,
            price,
            order_date,
            updated_at
        FROM orders
        WHERE updated_at > '{last_sync}'
        ORDER BY updated_at ASC
        LIMIT 10000;
    """
    
    # Exécuter la requête
    df = pg_hook.get_pandas_df(query)
    
    print(f"✅ {len(df)} nouvelles lignes extraites")
    
    if len(df) == 0:
        print("ℹ️  Aucune nouvelle donnée")
        return None
    
    # Sauvegarder temporairement
    temp_path = f"/tmp/pg_data_{context['ds_nodash']}.csv"
    df.to_csv(temp_path, index=False)
    
    # Récupérer la dernière date pour la prochaine sync
    latest_date = df['updated_at'].max()
    
    ti.xcom_push(key='temp_csv_path', value=temp_path)
    ti.xcom_push(key='last_sync_date', value=str(latest_date))
    ti.xcom_push(key='row_count', value=len(df))
    
    return temp_path


def transform_for_mysql(**context):
    """
    Transformer les données pour le schéma MySQL.
    
    Mapping de colonnes et conversion de types.
    """
    ti = context['ti']
    temp_path = ti.xcom_pull(task_ids='extract_from_postgres', key='temp_csv_path')
    
    if not temp_path:
        print("ℹ️  Aucune donnée à transformer")
        return None
    
    print(f"🔧 Transformation pour MySQL depuis {temp_path}")
    
    # Lire les données
    df = pd.read_csv(temp_path)
    
    # Mapping de colonnes (schéma PostgreSQL → MySQL)
    column_mapping = {
        'id': 'order_id',
        'user_id': 'customer_id',
        'product_name': 'product',
        'quantity': 'qty',
        'price': 'unit_price',
        'order_date': 'created_date',
        'updated_at': 'sync_date'
    }
    
    df = df.rename(columns=column_mapping)
    
    # Ajouter une colonne calculée
    df['total_amount'] = df['qty'] * df['unit_price']
    
    # Conversion de dates
    df['created_date'] = pd.to_datetime(df['created_date']).dt.strftime('%Y-%m-%d')
    df['sync_date'] = pd.to_datetime(df['sync_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"✅ {len(df)} lignes transformées")
    print(f"📊 Colonnes finales : {df.columns.tolist()}")
    
    # Sauvegarder
    transformed_path = temp_path.replace('.csv', '_transformed.csv')
    df.to_csv(transformed_path, index=False)
    
    ti.xcom_push(key='transformed_csv_path', value=transformed_path)
    
    return transformed_path


def load_to_mysql(**context):
    """
    Charger les données dans MySQL.
    
    Utilise INSERT ... ON DUPLICATE KEY UPDATE.
    """
    ti = context['ti']
    transformed_path = ti.xcom_pull(task_ids='transform_data', key='transformed_csv_path')
    
    if not transformed_path:
        print("ℹ️  Aucune donnée à charger")
        return 0
    
    print(f"💾 Chargement dans MySQL depuis {transformed_path}")
    
    # Lire les données transformées
    df = pd.read_csv(transformed_path)
    
    # Connexion MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_target')
    
    # Préparer les données pour l'insertion
    records = df.to_dict('records')
    
    # Construire la requête INSERT avec UPSERT
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    update_clause = ', '.join([f"{col}=VALUES({col})" for col in df.columns if col != 'order_id'])
    
    insert_query = f"""
        INSERT INTO orders_sync ({columns})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
    """
    
    # Insertion par batch
    batch_size = 500
    total_inserted = 0
    
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            # Préparer les valeurs
            values = [tuple(record.values()) for record in batch]
            
            # Exécuter le batch
            cursor.executemany(insert_query, values)
            conn.commit()
            
            total_inserted += len(batch)
            print(f"  ✅ {total_inserted}/{len(records)} lignes insérées")
        
        print(f"✅ Chargement terminé : {total_inserted} lignes")
        
        return total_inserted
    
    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur lors du chargement : {e}")
        raise
    
    finally:
        cursor.close()
        conn.close()


def verify_sync(**context):
    """
    Vérifier l'intégrité de la synchronisation.
    
    Compare les comptages entre PostgreSQL et MySQL.
    """
    print("🔍 Vérification de l'intégrité...")
    
    # Comptage PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id='postgres_source')
    pg_count = pg_hook.get_first("SELECT COUNT(*) FROM orders;")[0]
    
    # Comptage MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_target')
    mysql_count = mysql_hook.get_first("SELECT COUNT(*) FROM orders_sync;")[0]
    
    print(f"📊 PostgreSQL : {pg_count} lignes")
    print(f"📊 MySQL : {mysql_count} lignes")
    
    # Tolérance de 1% de différence
    diff_pct = abs(pg_count - mysql_count) / pg_count * 100 if pg_count > 0 else 0
    
    if diff_pct <= 1.0:
        print(f"✅ Synchronisation OK (différence : {diff_pct:.2f}%)")
        return True
    else:
        print(f"⚠️  Écart important : {diff_pct:.2f}%")
        # En production : envoyer une alerte
        return False


# === Définition du DAG ===

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': True,  # Ne pas lancer si la run précédente a échoué
    'email': ['data-eng@company.com'],
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='postgres_to_mysql_sync',
    default_args=default_args,
    description='Incremental sync from PostgreSQL to MySQL with schema mapping',
    schedule_interval='0 */6 * * *',  # Toutes les 6 heures
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sync', 'postgres', 'mysql', 'etl'],
    max_active_runs=1,
) as dag:
    
    # Tâche 1 : Extraction
    extract = PythonOperator(
        task_id='extract_from_postgres',
        python_callable=extract_from_postgres,
    )
    
    # Tâche 2 : Transformation
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_for_mysql,
    )
    
    # Tâche 3 : Chargement
    load = PythonOperator(
        task_id='load_to_mysql',
        python_callable=load_to_mysql,
    )
    
    # Tâche 4 : Vérification
    verify = PythonOperator(
        task_id='verify_sync',
        python_callable=verify_sync,
    )
    
    # Dépendances
    extract >> transform >> load >> verify