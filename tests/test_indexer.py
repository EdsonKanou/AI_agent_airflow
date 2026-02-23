"""
Tests de l'indexeur de DAG.
"""
import sys
sys.path.insert(0, '.')

from agent_ia.rag.indexer import DAGIndexer
import os


def test_extract_metadata():
    """Test 1 : Extraire les métadonnées d'un DAG."""
    print("\n" + "="*60)
    print("TEST 1 : Extraction de métadonnées")
    print("="*60)
    
    indexer = DAGIndexer()
    
    # Tester sur un DAG d'exemple
    dag_path = "dags/examples/s3_to_postgres_etl.py"
    
    if not os.path.exists(dag_path):
        print(f"⚠️  {dag_path} n'existe pas")
        return False
    
    metadata = indexer.extract_dag_metadata(dag_path)
    
    if metadata:
        print("\n📊 Métadonnées extraites :")
        print(f"   DAG ID : {metadata['dag_id']}")
        print(f"   Description : {metadata['description'][:100]}...")
        print(f"   Schedule : {metadata['schedule_interval']}")
        print(f"   Tasks : {metadata['task_ids']}")
        print(f"   Operators : {metadata['operators']}")
        print(f"   Tags : {metadata['tags']}")
        print(f"   TaskFlow : {metadata['has_taskflow']}")
        
        print("\n✅ Extraction réussie !")
        return True
    else:
        print("❌ Extraction échouée")
        return False


def test_index_single_dag():
    """Test 2 : Indexer un seul DAG."""
    print("\n" + "="*60)
    print("TEST 2 : Indexation d'un DAG")
    print("="*60)
    
    indexer = DAGIndexer(db_path="test_rag_db")
    
    dag_path = "dags/examples/s3_to_postgres_etl.py"
    
    success = indexer.index_dag(dag_path, overwrite=True)
    
    if success:
        print("\n✅ Indexation réussie !")
        
        # Vérifier les stats
        stats = indexer.get_stats()
        print(f"\n📊 Stats : {stats['total_dags']} DAG dans la base")
        
        return True
    else:
        print("❌ Indexation échouée")
        return False


def test_index_directory():
    """Test 3 : Indexer un répertoire complet."""
    print("\n" + "="*60)
    print("TEST 3 : Indexation d'un répertoire")
    print("="*60)
    
    indexer = DAGIndexer(db_path="test_rag_db")
    
    stats = indexer.index_directory("dags/examples", overwrite=True)
    
    print(f"\n📊 Statistiques :")
    print(f"   ✅ Succès : {stats['success']}")
    print(f"   ❌ Échecs : {stats['failed']}")
    
    # Stats détaillées
    detailed_stats = indexer.get_stats()
    print(f"\n📈 Stats détaillées :")
    print(f"   Total DAG : {detailed_stats['total_dags']}")
    if 'top_operators' in detailed_stats:
        print(f"   Top operators : {detailed_stats['top_operators']}")
    
    return stats['success'] > 0


def main():
    """Lancer tous les tests."""
    print("\n" + "🧪 " + "="*58)
    print("🧪  TESTS DE L'INDEXER - JOUR 4B")
    print("🧪 " + "="*58)
    
    tests = [
        test_extract_metadata,
        test_index_single_dag,
        test_index_directory,
    ]
    
    results = []
    
    for test in tests:
        result = test()
        results.append(result)
        
        if not result:
            print(f"\n⚠️  Le test {test.__name__} a échoué.")
            break
    
    print("\n" + "="*60)
    print(f"📊 RÉSULTAT : {sum(results)}/{len(tests)} tests réussis")
    print("="*60)
    
    if all(results):
        print("\n🎉 Tous les tests sont passés !")
        print("✅ L'Indexer est opérationnel !")
    else:
        print("\n⚠️  Certains tests ont échoué.")


if __name__ == "__main__":
    main()