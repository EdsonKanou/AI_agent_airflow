"""
Script d'initialisation complète du système RAG.
"""
import sys
sys.path.insert(0, '.')

from agent_ia.rag.indexer import DAGIndexer
from agent_ia.rag.retriever import DAGRetriever


def setup_rag():
    """Initialiser le système RAG complet."""
    
    print("\n" + "="*60)
    print("INITIALISATION DU SYSTÈME RAG")
    print("="*60)
    
    # Étape 1 : Indexation
    print("\n[1/2] Indexation des DAG...")
    indexer = DAGIndexer(db_path="rag_database")
    
    results = indexer.index_directory(
        directory="dags/examples",
        recursive=False,
        overwrite=True
    )
    
    if results['success'] == 0:
        print("[ERREUR] Aucun DAG indexé")
        return False
    
    print(f"[OK] {results['success']} DAG indexés")
    
    # Étape 2 : Vérification
    print("\n[2/2] Vérification...")
    retriever = DAGRetriever(db_path="rag_database")
    
    # Test de recherche
    test_results = retriever.search("ETL pipeline", top_k=1)
    
    if test_results:
        print(f"[OK] Recherche fonctionnelle")
        print(f"[OK] Exemple trouvé : {test_results[0]['metadata']['dag_id']}")
    else:
        print("[ERREUR] Recherche échouée")
        return False
    
    # Stats finales
    stats = retriever.get_statistics()
    
    print("\n" + "="*60)
    print("SYSTÈME RAG PRÊT")
    print("="*60)
    print(f"Total DAG : {stats['total_dags']}")
    
    if 'operators' in stats:
        print("\nOpérateurs disponibles :")
        for op, count in list(stats['operators'].items())[:5]:
            print(f"  - {op} : {count}")
    
    return True


if __name__ == "__main__":
    success = setup_rag()
    
    if success:
        print("\n[SUCCÈS] Vous pouvez maintenant :")
        print("  - Lancer les tests : python tests/test_retriever.py")
        print("  - Générer des DAG : python tests/test_full_generation.py")
    else:
        print("\n[ÉCHEC] Vérifiez les erreurs ci-dessus")
    
    sys.exit(0 if success else 1)