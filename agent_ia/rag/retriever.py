"""
Récupération de DAG similaires depuis ChromaDB.

Le Retriever effectue la recherche sémantique pour trouver
les DAG existants les plus similaires à une requête.
"""

import chromadb
from chromadb.config import Settings
from typing import List, Dict, Any, Optional

from .embedder import get_embedder


class DAGRetriever:
    """
    Recherche de DAG similaires dans la base vectorielle.
    
    Utilise la similarité cosinus entre embeddings pour trouver
    les DAG les plus pertinents.
    """
    
    def __init__(self, db_path: str = "rag_database"):
        """
        Initialiser le retriever.
        
        Args:
            db_path: Chemin vers la base ChromaDB
        """
        self.db_path = db_path
        self.embedder = get_embedder()
        
        # Connexion à ChromaDB
        self.client = chromadb.PersistentClient(
            path=db_path,
            settings=Settings(
                anonymized_telemetry=False,
                allow_reset=True
            )
        )
        
        # Récupérer la collection
        try:
            self.collection = self.client.get_collection(name="airflow_dags")
            count = self.collection.count()
            print(f"[Retriever] Collection chargée : {count} DAG disponibles")
        except Exception as e:
            print(f"[Retriever] Erreur : Collection non trouvée")
            print(f"[Retriever] Indexez d'abord vos DAG avec DAGIndexer")
            self.collection = None
    
    def search(
        self, 
        query: str, 
        top_k: int = 3,
        min_similarity: float = 0.0
    ) -> List[Dict[str, Any]]:
        """
        Rechercher les DAG les plus similaires à une requête.
        
        Args:
            query: Description du DAG recherché
            top_k: Nombre de résultats à retourner (défaut: 3)
            min_similarity: Seuil minimum de similarité (0-1, défaut: 0)
        
        Returns:
            list: Liste de DAG avec code, métadonnées et score de similarité
        
        Example:
            >>> retriever = DAGRetriever()
            >>> results = retriever.search("ETL pipeline with S3", top_k=2)
            >>> print(results[0]['metadata']['dag_id'])
            s3_to_postgres_etl
        """
        if not self.collection:
            print("[Search] Aucune collection disponible")
            return []
        
        # Générer l'embedding de la requête
        query_embedding = self.embedder.encode(query)
        
        # Rechercher dans ChromaDB
        try:
            results = self.collection.query(
                query_embeddings=[query_embedding.tolist()],
                n_results=top_k,
                include=["documents", "metadatas", "distances"]
            )
        except Exception as e:
            print(f"[Search] Erreur lors de la recherche : {e}")
            return []
        
        # Formater les résultats
        formatted_results = []
        
        if results['ids'] and len(results['ids'][0]) > 0:
            for i in range(len(results['ids'][0])):
                # Calculer la similarité (1 - distance)
                # ChromaDB utilise la distance L2, on convertit en similarité
                distance = results['distances'][0][i]
                similarity = 1 / (1 + distance)  # Normalisation
                
                # Filtrer par seuil minimum
                if similarity < min_similarity:
                    continue
                
                formatted_results.append({
                    'id': results['ids'][0][i],
                    'code': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i],
                    'distance': distance,
                    'similarity': similarity
                })
        
        return formatted_results
    
    def search_by_tags(
        self,
        tags: List[str],
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Rechercher des DAG par tags.
        
        Args:
            tags: Liste de tags à rechercher
            top_k: Nombre de résultats maximum
        
        Returns:
            list: DAG contenant au moins un des tags
        """
        if not self.collection:
            return []
        
        # Construire le filtre pour ChromaDB
        # Note : ChromaDB supporte les filtres sur métadonnées
        results = []
        
        try:
            all_data = self.collection.get()
            
            for i, metadata in enumerate(all_data['metadatas']):
                dag_tags = metadata.get('tags', '').split(',')
                dag_tags = [t.strip() for t in dag_tags if t.strip()]
                
                # Vérifier si au moins un tag correspond
                if any(tag in dag_tags for tag in tags):
                    results.append({
                        'id': all_data['ids'][i],
                        'code': all_data['documents'][i],
                        'metadata': metadata,
                        'matched_tags': [t for t in tags if t in dag_tags]
                    })
                
                if len(results) >= top_k:
                    break
            
            return results
        
        except Exception as e:
            print(f"[SearchByTags] Erreur : {e}")
            return []
    
    def search_by_operator(
        self,
        operator: str,
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Rechercher des DAG utilisant un opérateur spécifique.
        
        Args:
            operator: Nom de l'opérateur (ex: 'PythonOperator')
            top_k: Nombre de résultats maximum
        
        Returns:
            list: DAG utilisant cet opérateur
        """
        if not self.collection:
            return []
        
        results = []
        
        try:
            all_data = self.collection.get()
            
            for i, metadata in enumerate(all_data['metadatas']):
                operators = metadata.get('operators', '').split(',')
                operators = [op.strip() for op in operators if op.strip()]
                
                if operator in operators:
                    results.append({
                        'id': all_data['ids'][i],
                        'code': all_data['documents'][i],
                        'metadata': metadata
                    })
                
                if len(results) >= top_k:
                    break
            
            return results
        
        except Exception as e:
            print(f"[SearchByOperator] Erreur : {e}")
            return []
    
    def get_best_examples(
    self,
    description: str,
    max_examples: int = 2,
    include_code: bool = True,
    code_preview_lines: int = 30
    ) -> str:
        """
        Récupérer les meilleurs exemples formatés pour injection dans un prompt.
        
        Cette fonction est utilisée par le générateur pour enrichir le prompt
        avec des exemples pertinents de votre codebase.
        
        Args:
            description: Description du DAG à générer
            max_examples: Nombre maximum d'exemples à inclure
            include_code: Inclure du code (True) ou juste les métadonnées (False)
            code_preview_lines: Nombre de lignes de code à inclure (défaut: 30)
        
        Returns:
            str: Texte formaté avec les exemples
        """
        results = self.search(description, top_k=max_examples)
        
        if not results:
            return "No similar examples found in the database."
        
        examples_text = "SIMILAR EXAMPLES FROM YOUR CODEBASE:\n\n"
        
        for i, result in enumerate(results, 1):
            similarity_pct = result['similarity'] * 100
            metadata = result['metadata']
            
            examples_text += f"--- EXAMPLE {i} (Similarity: {similarity_pct:.0f}%) ---\n"
            examples_text += f"DAG ID: {metadata.get('dag_id', 'unknown')}\n"
            
            if metadata.get('description'):
                desc = metadata['description'][:150]
                examples_text += f"Description: {desc}\n"
            
            examples_text += f"Schedule: {metadata.get('schedule', 'N/A')}\n"
            examples_text += f"Tasks: {metadata.get('num_tasks', 0)}\n"
            
            if metadata.get('operators'):
                examples_text += f"Operators: {metadata['operators']}\n"
            
            if metadata.get('tags'):
                examples_text += f"Tags: {metadata['tags']}\n"
            
            if include_code:
                # Inclure seulement un aperçu du code, pas tout
                code_lines = result['code'].split('\n')
                
                # Prendre les N premières lignes significatives
                preview_lines = []
                line_count = 0
                
                for line in code_lines:
                    # Ignorer les lignes vides et commentaires au début
                    if line_count == 0 and (not line.strip() or line.strip().startswith('#')):
                        continue
                    
                    preview_lines.append(line)
                    line_count += 1
                    
                    if line_count >= code_preview_lines:
                        break
                
                preview_code = '\n'.join(preview_lines)
                
                examples_text += f"\nCode Preview (first {code_preview_lines} lines):\n```python\n{preview_code}\n```\n"
                examples_text += f"... (full code has {len(code_lines)} lines)\n"
            
            examples_text += "\n"
        
        return examples_text

    
    def get_dag_by_id(self, dag_id: str) -> Optional[Dict[str, Any]]:
        """
        Récupérer un DAG spécifique par son ID.
        
        Args:
            dag_id: ID du DAG à récupérer
        
        Returns:
            dict | None: DAG trouvé ou None
        """
        if not self.collection:
            return None
        
        try:
            all_data = self.collection.get()
            
            for i, metadata in enumerate(all_data['metadatas']):
                if metadata.get('dag_id') == dag_id:
                    return {
                        'id': all_data['ids'][i],
                        'code': all_data['documents'][i],
                        'metadata': metadata
                    }
            
            return None
        
        except Exception as e:
            print(f"[GetByID] Erreur : {e}")
            return None
    
    def list_all_dags(self) -> List[Dict[str, str]]:
        """
        Lister tous les DAG indexés (métadonnées seulement).
        
        Returns:
            list: Liste de dictionnaires avec dag_id, description, tags
        """
        if not self.collection:
            return []
        
        try:
            all_data = self.collection.get()
            
            dags = []
            for metadata in all_data['metadatas']:
                dags.append({
                    'dag_id': metadata.get('dag_id', 'unknown'),
                    'description': metadata.get('description', 'N/A')[:100],
                    'schedule': metadata.get('schedule', 'N/A'),
                    'num_tasks': metadata.get('num_tasks', 0),
                    'tags': metadata.get('tags', ''),
                })
            
            return dags
        
        except Exception as e:
            print(f"[ListAll] Erreur : {e}")
            return []
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Obtenir des statistiques sur la collection.
        
        Returns:
            dict: Statistiques diverses
        """
        if not self.collection:
            return {'error': 'Collection non disponible'}
        
        stats = {
            'total_dags': self.collection.count(),
            'collection_name': self.collection.name,
            'db_path': self.db_path,
        }
        
        try:
            all_data = self.collection.get()
            
            # Statistiques sur les opérateurs
            operator_counts = {}
            for metadata in all_data['metadatas']:
                operators = metadata.get('operators', '').split(',')
                for op in operators:
                    op = op.strip()
                    if op:
                        operator_counts[op] = operator_counts.get(op, 0) + 1
            
            stats['operators'] = dict(sorted(
                operator_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10])
            
            # Statistiques sur les tags
            tag_counts = {}
            for metadata in all_data['metadatas']:
                tags = metadata.get('tags', '').split(',')
                for tag in tags:
                    tag = tag.strip()
                    if tag:
                        tag_counts[tag] = tag_counts.get(tag, 0) + 1
            
            stats['tags'] = dict(sorted(
                tag_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10])
            
            # Statistiques TaskFlow
            stats['taskflow_dags'] = sum(
                1 for m in all_data['metadatas'] if m.get('has_taskflow')
            )
            
        except Exception as e:
            stats['error'] = str(e)
        
        return stats


# === Fonction utilitaire ===

def get_retriever(db_path: str = "rag_database") -> DAGRetriever:
    """
    Factory pour créer un retriever.
    
    Args:
        db_path: Chemin vers la base ChromaDB
    
    Returns:
        DAGRetriever: Instance du retriever
    """
    return DAGRetriever(db_path=db_path)