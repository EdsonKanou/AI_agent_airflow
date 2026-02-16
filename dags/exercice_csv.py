from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import csv
import json

def generer_csv():
    """Créer un fichier CSV de test."""
    donnees = [
        ["nom", "age", "ville"],
        ["Alice", "25", "Paris"],
        ["Bob", "30", "Lyon"],
        ["Charlie", "35", "Marseille"],
    ]
    
    filepath = "/tmp/donnees.csv"
    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(donnees)
    
    print(f"✅ CSV créé : {filepath}")
    return filepath

def analyser_csv(**context):
    """Lire le CSV et calculer des stats."""
    ti = context['ti']
    filepath = ti.xcom_pull(task_ids='generer_csv')
    
    with open(filepath, 'r') as f:
        reader = csv.DictReader(f)
        donnees = list(reader)
    
    # Calculer l'âge moyen
    ages = [int(row['age']) for row in donnees]
    age_moyen = sum(ages) / len(ages)
    
    stats = {
        "nb_personnes": len(donnees),
        "age_moyen": age_moyen,
        "villes": [row['ville'] for row in donnees]
    }
    
    print(f"📊 Statistiques : {stats}")
    return stats

def generer_rapport(**context):
    """Créer un rapport textuel."""
    ti = context['ti']
    stats = ti.xcom_pull(task_ids='analyser_csv')
    
    rapport = f"""
    📋 RAPPORT D'ANALYSE
    ===================
    Nombre de personnes : {stats['nb_personnes']}
    Âge moyen : {stats['age_moyen']:.1f} ans
    Villes : {', '.join(stats['villes'])}
    """
    
    print(rapport)
    
    # Sauvegarder le rapport
    with open('/tmp/rapport.txt', 'w') as f:
        f.write(rapport)
    
    print("✅ Rapport sauvegardé dans /tmp/rapport.txt")

with DAG(
    dag_id='analyse_csv',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['exercice', 'jour2'],
) as dag:
    
    t1 = PythonOperator(task_id='generer_csv', python_callable=generer_csv)
    t2 = PythonOperator(task_id='analyser_csv', python_callable=analyser_csv)
    t3 = PythonOperator(task_id='generer_rapport', python_callable=generer_rapport)
    
    t1 >> t2 >> t3