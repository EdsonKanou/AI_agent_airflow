"""
DAG de scraping d'API avec notification email.

Ce DAG illustre :
- TaskFlow API (décorateurs @task)
- Gestion des erreurs API avec retry
- Envoi d'email avec résultats
- Pattern de rate limiting
"""

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator
from airflow.models import Variable
import requests
import json
import time
from typing import List, Dict


# === Configuration ===
API_BASE_URL = Variable.get("api_base_url", default_var="https://api.github.com")
MAX_RETRIES = 3
RETRY_DELAY = 5  # secondes


@task(retries=MAX_RETRIES, retry_delay=timedelta(seconds=RETRY_DELAY))
def fetch_api_data() -> List[Dict]:
    """
    Récupérer des données depuis une API REST.
    
    Exemple : Top repositories GitHub Python.
    Gère le rate limiting et les erreurs réseau.
    """
    url = f"{API_BASE_URL}/search/repositories"
    params = {
        'q': 'language:python',
        'sort': 'stars',
        'order': 'desc',
        'per_page': 10
    }
    
    print(f"🌐 Appel API : {url}")
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            
            # Vérifier le rate limit
            remaining = response.headers.get('X-RateLimit-Remaining', 'N/A')
            print(f"📊 Rate limit restant : {remaining}")
            
            response.raise_for_status()  # Lève une exception si erreur HTTP
            
            data = response.json()
            repos = data['items']
            
            print(f"✅ {len(repos)} repositories récupérés")
            
            # Extraire les infos pertinentes
            results = []
            for repo in repos:
                results.append({
                    'name': repo['name'],
                    'stars': repo['stargazers_count'],
                    'url': repo['html_url'],
                    'description': repo['description'][:100] if repo['description'] else 'N/A'
                })
            
            return results
        
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Tentative {attempt}/{MAX_RETRIES} échouée : {e}")
            
            if attempt < MAX_RETRIES:
                wait_time = RETRY_DELAY * (2 ** (attempt - 1))  # Exponential backoff
                print(f"⏳ Attente de {wait_time}s avant retry...")
                time.sleep(wait_time)
            else:
                raise  # Dernière tentative, on propage l'erreur


@task
def process_data(raw_data: List[Dict]) -> Dict:
    """
    Traiter et agréger les données récupérées.
    
    Calcule des statistiques et prépare le rapport.
    """
    print(f"🔧 Traitement de {len(raw_data)} repositories")
    
    # Calculer des stats
    total_stars = sum(repo['stars'] for repo in raw_data)
    avg_stars = total_stars / len(raw_data) if raw_data else 0
    top_repo = max(raw_data, key=lambda x: x['stars']) if raw_data else None
    
    stats = {
        'total_repos': len(raw_data),
        'total_stars': total_stars,
        'average_stars': int(avg_stars),
        'top_repo': top_repo,
        'timestamp': datetime.now().isoformat()
    }
    
    print(f"📊 Stats calculées : {stats}")
    
    return stats


@task
def generate_report(repos: List[Dict], stats: Dict) -> str:
    """
    Générer un rapport HTML.
    
    Crée un fichier HTML avec les résultats pour l'email.
    """
    print("📝 Génération du rapport HTML")
    
    html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #333; }}
            table {{ border-collapse: collapse; width: 100%; margin-top: 20px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #4CAF50; color: white; }}
            .stats {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <h1>📊 Rapport GitHub - Top Python Repositories</h1>
        
        <div class="stats">
            <h2>Statistiques</h2>
            <ul>
                <li><strong>Nombre de repos :</strong> {stats['total_repos']}</li>
                <li><strong>Total d'étoiles :</strong> {stats['total_stars']:,}</li>
                <li><strong>Moyenne d'étoiles :</strong> {stats['average_stars']:,}</li>
                <li><strong>Top repository :</strong> {stats['top_repo']['name']} ({stats['top_repo']['stars']:,} ⭐)</li>
                <li><strong>Date :</strong> {stats['timestamp']}</li>
            </ul>
        </div>
        
        <h2>Top 10 Repositories</h2>
        <table>
            <tr>
                <th>Rang</th>
                <th>Nom</th>
                <th>⭐ Étoiles</th>
                <th>Description</th>
            </tr>
    """
    
    for i, repo in enumerate(repos, 1):
        html += f"""
            <tr>
                <td>{i}</td>
                <td><a href="{repo['url']}">{repo['name']}</a></td>
                <td>{repo['stars']:,}</td>
                <td>{repo['description']}</td>
            </tr>
        """
    
    html += """
        </table>
    </body>
    </html>
    """
    
    # Sauvegarder le rapport
    report_path = f"/tmp/github_report_{datetime.now().strftime('%Y%m%d')}.html"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"✅ Rapport sauvegardé : {report_path}")
    
    return report_path


# === Définition du DAG avec TaskFlow ===

@dag(
    dag_id='api_scraper_with_email',
    description='Scrape GitHub API and send email report',
    schedule_interval='0 9 * * 1',  # Tous les lundis à 9h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['api', 'scraping', 'email', 'github'],
    default_args={
        'owner': 'data_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': True,
        'email': ['team@company.com'],
    }
)
def api_scraper_dag():
    """
    Pipeline de scraping API avec notification.
    
    Workflow :
    1. Fetch data from API
    2. Process and calculate stats
    3. Generate HTML report
    4. Send email with report
    """
    
    # Étape 1 : Récupérer les données
    raw_data = fetch_api_data()
    
    # Étape 2 : Traiter les données
    stats = process_data(raw_data)
    
    # Étape 3 : Générer le rapport
    report_path = generate_report(raw_data, stats)
    
    # Étape 4 : Envoyer l'email
    # Note : EmailOperator nécessite une configuration SMTP dans Airflow
    send_email = EmailOperator(
        task_id='send_email_report',
        to=['team@company.com'],
        subject='GitHub Python Repositories - Weekly Report',
        html_content='<p>Veuillez trouver le rapport hebdomadaire en pièce jointe.</p>',
        files=[report_path],
    )
    
    # Définir les dépendances
    report_path >> send_email


# Instancier le DAG
dag_instance = api_scraper_dag()