"""
DAG éducatif : Chaque ligne est expliquée.
Ce DAG simule un pipeline ETL (Extract, Transform, Load).
"""

# ========================================
# 1. IMPORTS
# ========================================
from datetime import datetime, timedelta  # Gestion des dates
from airflow import DAG  # Classe principale
from airflow.operators.python import PythonOperator  # Exécuter du Python
from airflow.operators.bash import BashOperator  # Exécuter des commandes shell
from airflow.utils.dates import days_ago  # Utilitaire pour dates relatives


# ========================================
# 2. FONCTIONS MÉTIER (ce que font les tâches)
# ========================================

def extraire_donnees():
    """
    EXTRACT : Simuler l'extraction de données depuis une source.
    Dans la vraie vie : requête API, lecture fichier, connexion BDD.
    """
    print("📥 Extraction des données depuis la source...")
    donnees = {"users": 150, "transactions": 3200}
    print(f"Données extraites : {donnees}")
    return donnees  # ⚠️ Les return sont stockés par Airflow (XCom)


def transformer_donnees(**context):
    """
    TRANSFORM : Nettoyer, formater, agréger les données.
    
    **context : Airflow passe automatiquement des métadonnées
    (task_instance, execution_date, etc.)
    """
    # Récupérer le résultat de la tâche précédente
    ti = context['ti']  # ti = Task Instance
    donnees = ti.xcom_pull(task_ids='extraire')  # Récupérer depuis XCom
    
    print(f"🔧 Transformation des données : {donnees}")
    
    # Simulation de transformation
    donnees_transformees = {
        "users_actifs": donnees["users"] * 0.7,
        "revenu_moyen": donnees["transactions"] / donnees["users"]
    }
    
    print(f"Données transformées : {donnees_transformees}")
    return donnees_transformees


def charger_donnees(**context):
    """
    LOAD : Sauvegarder les données traitées.
    Dans la vraie vie : INSERT dans BDD, écriture fichier, envoi vers data warehouse.
    """
    ti = context['ti']
    donnees = ti.xcom_pull(task_ids='transformer')
    
    print(f"💾 Chargement des données : {donnees}")
    print("✅ Données sauvegardées avec succès !")


def envoyer_rapport():
    """
    Tâche finale : notification/rapport.
    """
    print("📧 Envoi du rapport aux parties prenantes...")
    print("✅ Pipeline ETL terminé avec succès !")


# ========================================
# 3. DEFAULT_ARGS (Configuration par défaut)
# ========================================

default_args = {
    # Propriétaire du DAG (pour le monitoring)
    'owner': 'data_team',
    
    # Si une tâche échoue, ne pas bloquer les tâches futures indépendantes
    'depends_on_past': False,
    
    # Email en cas d'échec (nécessite config SMTP dans Airflow)
    'email': ['admin@exemple.com'],
    'email_on_failure': False,  # Désactivé pour tests locaux
    'email_on_retry': False,
    
    # Nombre de tentatives avant d'abandonner
    'retries': 2,
    
    # Délai entre chaque tentative
    'retry_delay': timedelta(minutes=2),
    
    # Timeout : annuler la tâche si elle prend trop de temps
    'execution_timeout': timedelta(minutes=10),
}


# ========================================
# 4. DÉFINITION DU DAG
# ========================================

with DAG(
    # --- Identifiant unique ---
    dag_id='pipeline_etl_complet',
    
    # --- Arguments par défaut (hérités par toutes les tâches) ---
    default_args=default_args,
    
    # --- Description (visible dans l'UI) ---
    description='Pipeline ETL complet : Extract → Transform → Load',
    
    # --- Date de début ---
    # Le DAG peut s'exécuter à partir de cette date
    start_date=datetime(2024, 1, 1),
    
    # --- Planification ---
    # Cron: "0 9 * * *" = Tous les jours à 9h
    # None = Manuel uniquement
    # @daily, @hourly, @weekly sont des raccourcis
    schedule_interval='0 9 * * *',  # Tous les jours à 9h
    
    # --- Catchup ---
    # False = Ne pas exécuter les runs manqués
    # True = Rattraper toutes les exécutions depuis start_date
    catchup=False,
    
    # --- Tags (pour filtrer dans l'UI) ---
    tags=['etl', 'production', 'jour2'],
    
    # --- Timeout global du DAG ---
    dagrun_timeout=timedelta(hours=1),
    
) as dag:
    
    # ========================================
    # 5. DÉFINITION DES TÂCHES
    # ========================================
    
    # --- Tâche 1 : Extraction ---
    tache_extraction = PythonOperator(
        task_id='extraire',  # ID unique dans le DAG
        python_callable=extraire_donnees,  # Fonction à exécuter
        # provide_context=True,  # Obsolète en Airflow 2.x
    )
    
    # --- Tâche 2 : Transformation ---
    tache_transformation = PythonOperator(
        task_id='transformer',
        python_callable=transformer_donnees,
        # Fournir le contexte Airflow (task_instance, etc.)
        provide_context=True,
    )
    
    # --- Tâche 3 : Chargement ---
    tache_chargement = PythonOperator(
        task_id='charger',
        python_callable=charger_donnees,
        provide_context=True,
    )
    
    # --- Tâche 4 : Nettoyage (commande shell) ---
    tache_nettoyage = BashOperator(
        task_id='nettoyer_fichiers_temp',
        bash_command='echo "🗑️  Nettoyage des fichiers temporaires..." && ls -la',
    )
    
    # --- Tâche 5 : Rapport ---
    tache_rapport = PythonOperator(
        task_id='envoyer_rapport',
        python_callable=envoyer_rapport,
    )
    
    # ========================================
    # 6. DÉPENDANCES (Ordre d'exécution)
    # ========================================
    
    # Syntaxe 1 : Chaînage linéaire
    tache_extraction >> tache_transformation >> tache_chargement
    
    # Syntaxe 2 : Parallélisation
    # Après le chargement, nettoyage ET rapport en parallèle
    tache_chargement >> [tache_nettoyage, tache_rapport]
    
    # Équivalent à :
    # tache_chargement >> tache_nettoyage
    # tache_chargement >> tache_rapport