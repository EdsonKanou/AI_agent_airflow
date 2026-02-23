"""
Test de l'intégration du code cleaner.
"""
import sys
sys.path.insert(0, '.')

from agent_ia import get_client


def test_cleaning():
    """Tester le nettoyage automatique."""
    
    # Code brut avec markdown (simulé)
    raw_code_with_markdown = """
Here's the Airflow DAG you requested:
```python
from airflow import DAG
from datetime import datetime

with DAG('test', start_date=datetime(2024,1,1)) as dag:
    pass
```

This DAG is simple and does nothing.
"""
    
    print("="*60)
    print("TEST : Nettoyage automatique du code")
    print("="*60)
    
    print("\n📝 Code brut reçu :")
    print("-"*60)
    print(raw_code_with_markdown)
    print("-"*60)
    
    # Simuler le nettoyage (sans appeler Ollama)
    from agent_ia.utils.code_cleaner import clean_and_validate
    
    clean_code, is_valid, error = clean_and_validate(raw_code_with_markdown)
    
    print("\n✨ Code nettoyé :")
    print("-"*60)
    print(clean_code)
    print("-"*60)
    
    print(f"\n✅ Valide : {is_valid}")
    if error:
        print(f"⚠️  Erreur : {error}")
    
    # Vérifier que le markdown est bien enlevé
    assert '```' not in clean_code, "Le markdown n'a pas été enlevé"
    assert 'Here\'s' not in clean_code, "Les explications n'ont pas été enlevées"
    assert 'from airflow' in clean_code, "Le code a été perdu"
    
    print("\n🎉 Test réussi : Le code a été correctement nettoyé !")


if __name__ == "__main__":
    test_cleaning()