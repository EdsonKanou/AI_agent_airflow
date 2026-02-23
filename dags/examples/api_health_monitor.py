"""
DAG de monitoring de santé d'API.

Ce DAG illustre :
- Vérification périodique de disponibilité
- Alertes conditionnelles
- Pattern de short-circuit
- Monitoring proactif
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
import requests
import json


def check_api_health():
    """
    Vérifier la santé de l'API en détail.
    
    Retourne False si problème détecté (déclenche l'alerte).
    """
    api_url = "https://api.github.com"
    
    print(f"🔍 Vérification de {api_url}")
    
    try:
        response = requests.get(f"{api_url}/status", timeout=10)
        
        # Vérifier le status code
        if response.status_code != 200:
            print(f"❌ Status code : {response.status_code}")
            return False
        
        # Vérifier le temps de réponse
        response_time = response.elapsed.total_seconds()
        print(f"⏱️  Temps de réponse : {response_time:.2f}s")
        
        if response_time > 2.0:
            print("⚠️  Temps de réponse lent (> 2s)")
            return False
        
        # Vérifier le rate limit
        remaining = response.headers.get('X-RateLimit-Remaining', '0')
        print(f"📊 Rate limit restant : {remaining}")
        
        if int(remaining) < 100:
            print("⚠️  Rate limit faible")
            return False
        
        print("✅ API en bonne santé")
        return True
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur de connexion : {e}")
        return False


def send_slack_alert(**context):
    """
    Envoyer une alerte Slack.
    
    Simule l'envoi (en production, utilisez SlackWebhookOperator).
    """
    print("🚨 ALERTE : Problème détecté sur l'API")
    
    ti = context['ti']
    execution_date = context['execution_date']
    
    # Message d'alerte
    message = {
        "text": "🚨 API Health Alert",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*API Health Check Failed*"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Time:*\n{execution_date}"},
                    {"type": "mrkdwn", "text": "*Severity:*\nHigh"},
                ]
            }
        ]
    }
    
    print(f"📧 Slack message : {json.dumps(message, indent=2)}")
    
    # En production :
    # slack_webhook_url = Variable.get("slack_webhook_url")
    # requests.post(slack_webhook_url, json=message)
    
    return "Alert sent"


def log_successful_check(**context):
    """
    Logger la vérification réussie.
    """
    execution_date = context['execution_date']
    
    print(f"✅ API health check passed at {execution_date}")
    
    # En production : logger dans une base de données
    log_entry = {
        "timestamp": str(execution_date),
        "status": "healthy",
        "api": "GitHub API"
    }
    
    print(f"📝 Log : {log_entry}")
    
    return "Logged"


# === Définition du DAG ===

default_args = {
    'owner': 'monitoring_team',
    'depends_on_past': False,
    'email': ['alerts@company.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='api_health_monitor',
    default_args=default_args,
    description='Monitor API health and send alerts if issues detected',
    schedule_interval='*/15 * * * *',  # Toutes les 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring', 'api', 'alerts'],
) as dag:
    
    # Tâche 1 : Vérification basique (HTTP Sensor)
    basic_check = HttpSensor(
        task_id='basic_api_check',
        http_conn_id='http_default',
        endpoint='https://api.github.com',
        request_params={},
        response_check=lambda response: response.status_code == 200,
        poke_interval=10,
        timeout=30,
        mode='poke',
    )
    
    # Tâche 2 : Vérification détaillée
    # ShortCircuitOperator : arrête le workflow si retourne True (tout va bien)
    detailed_check = ShortCircuitOperator(
        task_id='detailed_health_check',
        python_callable=check_api_health,
    )
    
    # Tâche 3 : Envoyer l'alerte (exécutée seulement si detailed_check retourne False)
    send_alert = PythonOperator(
        task_id='send_slack_alert',
        python_callable=send_slack_alert,
    )
    
    # Tâche 4 : Logger le succès
    log_success = PythonOperator(
        task_id='log_successful_check',
        python_callable=log_successful_check,
        trigger_rule='none_failed',  # S'exécute si aucune tâche n'a échoué
    )
    
    # Dépendances
    basic_check >> detailed_check >> send_alert
    detailed_check >> log_success