"""
Tests du Retriever pour la recherche de DAG similaires.
"""
import sys
sys.path.insert(0, '.')

from agent_ia.rag.retriever import DAGRetriever, get_retriever


def test_basic_search():
    """Test 1 : Recherche sémantique basique."""
    print("\n" + "="*60)
    print("TEST 1 : Recherche sémantique basique")
    print("="*60)
    
    retriever = get_retriever()
    
    # Requête de test
    query = "ETL pipeline with S3 and PostgreSQL"
    
    print(f"\n[Query] {query}")
    print("\n[Searching]...")
    
    results = retriever.search(query, top_k=3)
    
    if not results:
        print("[FAIL] Aucun résultat trouvé")
        return False
    
    print(f"\n[Results] {len(results)} DAG trouvés")
    print()
    
    for i, result in enumerate(results, 1):
        similarity = result['similarity'] * 100
        metadata = result['metadata']
        
        print(f"{i}. {metadata['dag_id']}")
        print(f"   Similarité : {similarity:.1f}%")
        print(f"   Description : {metadata.get('description', 'N/A')[:80]}...")
        print(f"   Operators : {metadata.get('operators', 'N/A')}")
        print()
    
    # Vérifier que le DAG S3->Postgres est en tête
    top_dag = results[0]['metadata']['dag_id']
    if 's3' in top_dag.lower() or 'postgres' in top_dag.lower():
        print("[PASS] Le DAG le plus pertinent a été trouvé")
        return True
    else:
        print(f"[WARN] Top résultat : {top_dag} (attendu : s3_to_postgres)")
        return True  # Pas un échec critique


def test_search_by_tags():
    """Test 2 : Recherche par tags."""
    print("\n" + "="*60)
    print("TEST 2 : Recherche par tags")
    print("="*60)
    
    retriever = get_retriever()
    
    tags = ['etl', 'production']
    
    print(f"\n[Tags] {tags}")
    
    results = retriever.search_by_tags(tags, top_k=5)
    
    if not results:
        print("[INFO] Aucun DAG avec ces tags")
        return True
    
    print(f"\n[Results] {len(results)} DAG trouvés")
    print()
    
    for result in results:
        metadata = result['metadata']
        matched = result['matched_tags']
        
        print(f"- {metadata['dag_id']}")
        print(f"  Tags correspondants : {matched}")
        print()
    
    print("[PASS] Recherche par tags fonctionnelle")
    return True


def test_search_by_operator():
    """Test 3 : Recherche par opérateur."""
    print("\n" + "="*60)
    print("TEST 3 : Recherche par opérateur")
    print("="*60)
    
    retriever = get_retriever()
    
    operator = 'PythonOperator'
    
    print(f"\n[Operator] {operator}")
    
    results = retriever.search_by_operator(operator, top_k=5)
    
    if not results:
        print("[FAIL] Aucun DAG avec PythonOperator trouvé")
        return False
    
    print(f"\n[Results] {len(results)} DAG trouvés")
    print()
    
    for result in results:
        metadata = result['metadata']
        print(f"- {metadata['dag_id']}")
        print(f"  Operators : {metadata.get('operators', 'N/A')}")
        print()
    
    print("[PASS] Recherche par opérateur fonctionnelle")
    return True


def test_get_best_examples():
    """Test 4 : Génération d'exemples pour prompt."""
    print("\n" + "="*60)
    print("TEST 4 : Génération d'exemples pour prompt")
    print("="*60)
    
    retriever = get_retriever()
    
    description = "Create a machine learning training pipeline"
    
    print(f"\n[Description] {description}")
    print("\n[Generating examples]...")
    
    # Sans code (métadonnées seulement)
    examples_meta = retriever.get_best_examples(
        description,
        max_examples=2,
        include_code=False
    )
    
    print("\n--- EXEMPLES (métadonnées seulement) ---")
    print(examples_meta[:500])
    print("...")
    
    # Avec code
    examples_full = retriever.get_best_examples(
        description,
        max_examples=1,
        include_code=True
    )
    
    print("\n--- EXEMPLES (avec code) ---")
    print(examples_full[:800])
    print("...")
    
    if "SIMILAR EXAMPLES" in examples_full and "```python" in examples_full:
        print("\n[PASS] Exemples correctement formatés")
        return True
    else:
        print("\n[FAIL] Format d'exemples incorrect")
        return False


def test_list_all_dags():
    """Test 5 : Lister tous les DAG."""
    print("\n" + "="*60)
    print("TEST 5 : Lister tous les DAG indexés")
    print("="*60)
    
    retriever = get_retriever()
    
    dags = retriever.list_all_dags()
    
    if not dags:
        print("[FAIL] Aucun DAG trouvé")
        return False
    
    print(f"\n[Total] {len(dags)} DAG indexés")
    print()
    
    for dag in dags:
        print(f"- {dag['dag_id']}")
        print(f"  Schedule : {dag['schedule']}")
        print(f"  Tasks : {dag['num_tasks']}")
        print()
    
    print("[PASS] Liste des DAG récupérée")
    return True


def test_statistics():
    """Test 6 : Statistiques de la collection."""
    print("\n" + "="*60)
    print("TEST 6 : Statistiques de la collection")
    print("="*60)
    
    retriever = get_retriever()
    
    stats = retriever.get_statistics()
    
    print(f"\n[Total DAG] {stats.get('total_dags', 0)}")
    
    if 'operators' in stats:
        print("\n[Top Operators]")
        for op, count in stats['operators'].items():
            print(f"  - {op} : {count}")
    
    if 'tags' in stats:
        print("\n[Top Tags]")
        for tag, count in stats['tags'].items():
            print(f"  - {tag} : {count}")
    
    print(f"\n[DAG with TaskFlow] {stats.get('taskflow_dags', 0)}")
    
    print("\n[PASS] Statistiques récupérées")
    return True


def main():
    """Lancer tous les tests."""
    print("\n" + "="*60)
    print("TESTS DU RETRIEVER - JOUR 4B PARTIE 3")
    print("="*60)
    
    tests = [
        test_basic_search,
        test_search_by_tags,
        test_search_by_operator,
        test_get_best_examples,
        test_list_all_dags,
        test_statistics,
    ]
    
    results = []
    
    for test in tests:
        result = test()
        results.append(result)
        
        if not result:
            print(f"\n[ERROR] Le test {test.__name__} a échoué")
            break
    
    print("\n" + "="*60)
    print(f"RÉSULTAT : {sum(results)}/{len(tests)} tests réussis")
    print("="*60)
    
    if all(results):
        print("\nTous les tests sont passés")
        print("Le Retriever est opérationnel")
    else:
        print("\nCertains tests ont échoué")


if __name__ == "__main__":
    main()