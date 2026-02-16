"""
DAG de test pour vérifier que l'installation fonctionne.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def dire_bonjour():
    """Fonction simple qui affiche bonjour."""
    print("🎉 Bonjour depuis Airflow !")
    return "Success"


def dire_au_revoir():
    """Fonction simple qui affiche au revoir."""
    print("👋 Au revoir depuis Airflow !")
    return "Success"


# Arguments par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définition du DAG
with DAG(
    dag_id='test_installation',
    default_args=default_args,
    description='DAG de test pour vérifier l\'installation',
    schedule_interval=None,  # Lancement manuel uniquement
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'jour1'],
) as dag:

    # Tâche 1
    tache_bonjour = PythonOperator(
        task_id='dire_bonjour',
        python_callable=dire_bonjour,
    )

    # Tâche 2
    tache_au_revoir = PythonOperator(
        task_id='dire_au_revoir',
        python_callable=dire_au_revoir,
    )

    # Définir l'ordre : bonjour → au_revoir
    tache_bonjour >> tache_au_revoir