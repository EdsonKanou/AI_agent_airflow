"""
Script pour indexer les DAG d'exemples dans la base principale.
"""
import sys
sys.path.insert(0, '.')

from agent_ia.rag.indexer import DAGIndexer


def main():
    """Indexer tous les DAG d'exemples."""
    
    print("="*60)
    print("INDEXATION DES DAG D'EXEMPLES")
    print("="*60)
    
    # Créer l'indexer (base par défaut : rag_database)
    indexer = DAGIndexer(db_path="rag_database")
    
    # Vérifier l'état actuel
    stats_before = indexer.get_stats()
    print(f"\n[Avant] {stats_before['total_dags']} DAG dans la base")
    
    # Indexer le dossier d'exemples
    print("\n[Indexation] Lecture de dags/examples/")
    results = indexer.index_directory(
        directory="dags/examples",
        recursive=False,
        overwrite=True  # Écraser si déjà présents
    )
    
    # Afficher les résultats
    print("\n" + "="*60)
    print("RÉSULTAT")
    print("="*60)
    print(f"Succès : {results['success']}")
    print(f"Échecs : {results['failed']}")
    
    # Vérifier l'état final
    stats_after = indexer.get_stats()
    print(f"\n[Après] {stats_after['total_dags']} DAG dans la base")
    
    if stats_after['total_dags'] > 0:
        print("\n[OK] Base RAG prête à l'emploi")
        
        # Afficher des stats détaillées
        if 'top_operators' in stats_after:
            print("\n[Stats] Top opérateurs :")
            for op, count in list(stats_after['top_operators'].items())[:5]:
                print(f"  - {op} : {count}")
        
        return True
    else:
        print("\n[ERREUR] La base est toujours vide")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)