"""
DAG de Pipeline Machine Learning.

Ce DAG illustre :
- Entraînement de modèle scikit-learn
- Évaluation et validation
- Branchement conditionnel selon performance
- Sauvegarde de modèle avec versioning
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
import pickle
import json
import os


def load_and_prepare_data(**context):
    """
    Charger et préparer les données pour l'entraînement.
    
    Exemple avec le dataset Iris (remplacer par vos vraies données).
    """
    print("📊 Chargement des données...")
    
    # Charger le dataset
    data = load_iris()
    X, y = data.data, data.target
    
    print(f"✅ {len(X)} échantillons chargés")
    print(f"📈 Features : {data.feature_names}")
    
    # Split train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    
    # Sauvegarder temporairement
    data_dir = "/tmp/ml_pipeline"
    os.makedirs(data_dir, exist_ok=True)
    
    with open(f"{data_dir}/train_data.pkl", 'wb') as f:
        pickle.dump((X_train, y_train), f)
    
    with open(f"{data_dir}/test_data.pkl", 'wb') as f:
        pickle.dump((X_test, y_test), f)
    
    context['ti'].xcom_push(key='data_dir', value=data_dir)
    context['ti'].xcom_push(key='train_size', value=len(X_train))
    
    print(f"💾 Données sauvegardées dans {data_dir}")
    
    return data_dir


def train_model(**context):
    """
    Entraîner le modèle de machine learning.
    """
    ti = context['ti']
    data_dir = ti.xcom_pull(task_ids='load_data', key='data_dir')
    
    print("🤖 Entraînement du modèle...")
    
    # Charger les données d'entraînement
    with open(f"{data_dir}/train_data.pkl", 'rb') as f:
        X_train, y_train = pickle.load(f)
    
    # Créer et entraîner le modèle
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=10,
        random_state=42
    )
    
    model.fit(X_train, y_train)
    
    print("✅ Modèle entraîné")
    
    # Sauvegarder le modèle
    model_path = f"{data_dir}/model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    
    ti.xcom_push(key='model_path', value=model_path)
    
    return model_path


def evaluate_model(**context):
    """
    Évaluer les performances du modèle.
    
    Retourne les métriques (accuracy, F1-score).
    """
    ti = context['ti']
    data_dir = ti.xcom_pull(task_ids='load_data', key='data_dir')
    model_path = ti.xcom_pull(task_ids='train_model', key='model_path')
    
    print("📊 Évaluation du modèle...")
    
    # Charger le modèle et les données de test
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    with open(f"{data_dir}/test_data.pkl", 'rb') as f:
        X_test, y_test = pickle.load(f)
    
    # Prédictions
    y_pred = model.predict(X_test)
    
    # Calculer les métriques
    accuracy = accuracy_score(y_test, y_pred)
    f1 = f1_score(y_test, y_pred, average='weighted')
    
    metrics = {
        'accuracy': float(accuracy),
        'f1_score': float(f1),
        'test_samples': len(X_test)
    }
    
    print(f"📈 Accuracy : {accuracy:.4f}")
    print(f"📈 F1-Score : {f1:.4f}")
    
    # Sauvegarder les métriques
    metrics_path = f"{data_dir}/metrics.json"
    with open(metrics_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    ti.xcom_push(key='metrics', value=metrics)
    ti.xcom_push(key='accuracy', value=accuracy)
    
    return metrics


def check_model_performance(**context):
    """
    Décider si le modèle est assez bon pour la production.
    
    Retourne le task_id de la branche à suivre.
    """
    ti = context['ti']
    accuracy = ti.xcom_pull(task_ids='evaluate_model', key='accuracy')
    
    ACCURACY_THRESHOLD = 0.90  # 90%
    
    print(f"🔍 Accuracy : {accuracy:.4f} | Seuil : {ACCURACY_THRESHOLD}")
    
    if accuracy >= ACCURACY_THRESHOLD:
        print("✅ Modèle acceptable → Déploiement")
        return 'deploy_model'
    else:
        print("❌ Modèle insuffisant → Notification")
        return 'send_failure_alert'


def deploy_model(**context):
    """
    Déployer le modèle en production.
    
    Simule le déploiement (copie vers un répertoire de production).
    """
    ti = context['ti']
    model_path = ti.xcom_pull(task_ids='train_model', key='model_path')
    metrics = ti.xcom_pull(task_ids='evaluate_model', key='metrics')
    
    print("🚀 Déploiement du modèle...")
    
    # Créer un nom versionné
    version = datetime.now().strftime('%Y%m%d_%H%M%S')
    prod_dir = "/tmp/models_production"
    os.makedirs(prod_dir, exist_ok=True)
    
    prod_model_path = f"{prod_dir}/model_v{version}.pkl"
    
    # Copier le modèle
    import shutil
    shutil.copy(model_path, prod_model_path)
    
    # Sauvegarder les métadonnées
    metadata = {
        'version': version,
        'deployed_at': datetime.now().isoformat(),
        'metrics': metrics,
        'model_path': prod_model_path
    }
    
    with open(f"{prod_dir}/metadata_v{version}.json", 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✅ Modèle déployé : {prod_model_path}")
    print(f"📊 Accuracy : {metrics['accuracy']:.4f}")
    
    return prod_model_path


def send_failure_alert(**context):
    """
    Envoyer une alerte si le modèle n'est pas assez performant.
    """
    ti = context['ti']
    metrics = ti.xcom_pull(task_ids='evaluate_model', key='metrics')
    
    print("⚠️  ALERTE : Modèle insuffisant")
    print(f"📊 Metrics : {metrics}")
    print("📧 Envoi d'une notification à l'équipe ML...")
    
    # En production : envoyer un vrai email ou Slack
    
    return "Alert sent"


# === Définition du DAG ===

default_args = {
    'owner': 'ml_team',
    'depends_on_past': False,
    'email': ['ml-team@company.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='ml_training_pipeline',
    default_args=default_args,
    description='ML pipeline: train, evaluate, deploy based on performance',
    schedule_interval='0 3 * * 0',  # Tous les dimanches à 3h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'training', 'sklearn', 'production'],
) as dag:
    
    # Tâche 1 : Chargement des données
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_and_prepare_data,
    )
    
    # Tâche 2 : Entraînement
    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )
    
    # Tâche 3 : Évaluation
    evaluate = PythonOperator(
        task_id='evaluate_model',
        python_callable=evaluate_model,
    )
    
    # Tâche 4 : Branchement conditionnel
    check_performance = BranchPythonOperator(
        task_id='check_model_performance',
        python_callable=check_model_performance,
    )
    
    # Tâche 5a : Déploiement (si bon)
    deploy = PythonOperator(
        task_id='deploy_model',
        python_callable=deploy_model,
    )
    
    # Tâche 5b : Alerte (si mauvais)
    alert = PythonOperator(
        task_id='send_failure_alert',
        python_callable=send_failure_alert,
    )
    
    # Tâche 6 : Point de convergence
    end = DummyOperator(
        task_id='end_pipeline',
        trigger_rule='none_failed_min_one_success'  # Continue si au moins une branche réussit
    )
    
    # Dépendances
    load_data >> train >> evaluate >> check_performance
    check_performance >> [deploy, alert] >> end