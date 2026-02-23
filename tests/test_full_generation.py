"""
Test du workflow complet de génération.
"""
import sys
sys.path.insert(0, '.')

from agent_ia.generator import DAGGenerator


def test_generation_with_rag():
    """Test : Génération avec RAG activé."""
    print("\n" + "="*60)
    print("TEST : Génération complète avec RAG")
    print("="*60)
    
    generator = DAGGenerator(
        model="codellama",
        use_rag=True,
        rag_examples=2,
        temperature=0.2
    )
    
    description = """
    Create an ETL pipeline that:
    1. Downloads CSV files from S3
    2. Cleans the data (remove duplicates)
    3. Loads into PostgreSQL
    4. Runs daily at 3 AM
    """
    
    requirements = {
        'dag_id': 'test_etl_pipeline',
        'schedule': '0 3 * * *',
        'tags': "['etl', 'test']",
        'owner': 'data_team'
    }
    
    code, is_valid, error, metadata = generator.generate_and_save(
        description,
        requirements
    )
    
    print("\n" + "-"*60)
    print("CODE GÉNÉRÉ :")
    print("-"*60)
    print(code[:500])
    print("...")
    print("-"*60)
    
    print(f"\nValidation : {'OK' if is_valid else 'ÉCHEC'}")
    if not is_valid:
        print(f"Erreur : {error}")
    
    print(f"\nMétadonnées :")
    print(f"  - RAG utilisé : {metadata['rag_used']}")
    print(f"  - Taille du code : {metadata.get('code_length', 0)} caractères")
    
    if 'error' in metadata and metadata['error']:
        print(f"  - Erreur détaillée : {metadata['error']}")
    
    return is_valid


def test_generation_without_rag():
    """Test : Génération sans RAG."""
    print("\n" + "="*60)
    print("TEST : Génération sans RAG")
    print("="*60)
    
    generator = DAGGenerator(
        model="codellama",
        use_rag=False,
        temperature=0.2
    )
    
    description = "Create a simple DAG that prints hello world"
    
    code, is_valid, error, metadata = generator.generate_and_save(description)
    
    print(f"\nValidation : {'OK' if is_valid else 'ÉCHEC'}")
    print(f"RAG utilisé : {metadata['rag_used']}")
    
    return True  # Succès même si code invalide (test de workflow)


def main():
    """Lancer les tests."""
    print("\n" + "="*60)
    print("TESTS DU GÉNÉRATEUR COMPLET")
    print("="*60)
    
    tests = [
        test_generation_with_rag,
        test_generation_without_rag,
    ]
    
    results = []
    for test in tests:
        result = test()
        results.append(result)
    
    print("\n" + "="*60)
    print(f"RÉSULTAT : {sum(results)}/{len(tests)} tests réussis")
    print("="*60)


if __name__ == "__main__":
    main()