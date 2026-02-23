"""
Indexation des DAG Airflow existants dans ChromaDB.

Ce module :
1. Lit les fichiers .py des DAG
2. Extrait les métadonnées avec AST (dag_id, tasks, imports, etc.)
3. Crée des embeddings du code et de la description
4. Stocke dans ChromaDB pour la recherche sémantique
"""

import os
import ast
from pathlib import Path
from typing import Dict, List, Optional, Any
import chromadb
from chromadb.config import Settings

from .embedder import get_embedder


class DAGIndexer:
    """
    Indexeur de DAG Airflow pour le RAG.
    
    Permet de créer une base de connaissance searchable
    à partir de DAG existants.
    """
    
    def __init__(self, db_path: str = "rag_database"):
        """
        Initialiser l'indexeur.
        
        Args:
            db_path: Chemin vers la base de données ChromaDB
        """
        self.db_path = db_path
        self.embedder = get_embedder()
        
        print(f"📊 Initialisation de la base RAG : {db_path}")
        
        # Créer le client ChromaDB
        self.client = chromadb.PersistentClient(
            path=db_path,
            settings=Settings(
                anonymized_telemetry=False,  # Désactiver la télémétrie
                allow_reset=True
            )
        )
        
        # Créer ou récupérer la collection
        self.collection = self.client.get_or_create_collection(
            name="airflow_dags",
            metadata={
                "description": "Collection of Airflow DAG examples for RAG",
                "embedding_model": "all-MiniLM-L6-v2"
            }
        )
        
        print(f"✅ Collection initialisée : {self.collection.count()} DAG déjà indexés")
    
    def extract_dag_metadata(self, filepath: str) -> Optional[Dict[str, Any]]:
        """
        Extraire les métadonnées d'un fichier DAG Python.
        
        Utilise l'AST pour analyser le code sans l'exécuter.
        
        Args:
            filepath: Chemin vers le fichier .py
        
        Returns:
            dict | None: Métadonnées extraites ou None si erreur
        """
        try:
            # Lire le fichier
            with open(filepath, 'r', encoding='utf-8') as f:
                code = f.read()
            
            # Parser avec AST
            tree = ast.parse(code, filename=filepath)
            
            # Structure de métadonnées
            metadata = {
                'filepath': filepath,
                'filename': Path(filepath).name,
                'code': code,
                'code_length': len(code),
                'dag_id': None,
                'description': None,
                'schedule_interval': None,
                'tags': [],
                'task_ids': [],
                'imports': [],
                'operators': [],
                'functions': [],
                'has_taskflow': False,
            }
            
            # Extraire le docstring du module
            docstring = ast.get_docstring(tree)
            if docstring:
                # Garder seulement les 500 premiers caractères
                metadata['description'] = docstring[:500].strip()
            
            # Parcourir l'arbre AST
            for node in ast.walk(tree):
                
                # === IMPORTS ===
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        metadata['imports'].append(alias.name)
                
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        metadata['imports'].append(node.module)
                        
                        # Détecter les opérateurs utilisés
                        for alias in node.names:
                            if 'Operator' in alias.name:
                                metadata['operators'].append(alias.name)
                
                # === FONCTIONS ===
                elif isinstance(node, ast.FunctionDef):
                    metadata['functions'].append(node.name)
                    
                    # Détecter TaskFlow API (décorateurs @task)
                    for decorator in node.decorator_list:
                        if isinstance(decorator, ast.Name) and decorator.id == 'task':
                            metadata['has_taskflow'] = True
                        elif isinstance(decorator, ast.Attribute) and decorator.attr == 'task':
                            metadata['has_taskflow'] = True
                
                # === ARGUMENTS KEYWORD (dag_id, task_id, etc.) ===
                elif isinstance(node, ast.keyword):
                    
                    # dag_id
                    if node.arg == 'dag_id':
                        if isinstance(node.value, ast.Constant):
                            metadata['dag_id'] = node.value.value
                    
                    # task_id
                    elif node.arg == 'task_id':
                        if isinstance(node.value, ast.Constant):
                            metadata['task_ids'].append(node.value.value)
                    
                    # schedule_interval
                    elif node.arg == 'schedule_interval' or node.arg == 'schedule':
                        if isinstance(node.value, ast.Constant):
                            metadata['schedule_interval'] = node.value.value
                    
                    # tags
                    elif node.arg == 'tags':
                        if isinstance(node.value, ast.List):
                            for elt in node.value.elts:
                                if isinstance(elt, ast.Constant):
                                    metadata['tags'].append(elt.value)
            
            # Si pas de dag_id trouvé, utiliser le nom du fichier
            if not metadata['dag_id']:
                metadata['dag_id'] = Path(filepath).stem
            
            # Dédupliquer les listes
            metadata['imports'] = list(set(metadata['imports']))
            metadata['operators'] = list(set(metadata['operators']))
            metadata['task_ids'] = list(set(metadata['task_ids']))
            
            return metadata
        
        except SyntaxError as e:
            print(f"⚠️  Erreur de syntaxe dans {filepath}: {e}")
            return None
        
        except Exception as e:
            print(f"⚠️  Erreur lors de l'analyse de {filepath}: {e}")
            return None
    
    def create_searchable_text(self, metadata: Dict[str, Any]) -> str:
        """
        Créer un texte optimisé pour la recherche sémantique.
        
        Ce texte sera converti en embedding et stocké dans ChromaDB.
        
        Args:
            metadata: Métadonnées extraites du DAG
        
        Returns:
            str: Texte descriptif du DAG
        """
        parts = []
        
        # DAG ID
        if metadata['dag_id']:
            parts.append(f"DAG ID: {metadata['dag_id']}")
        
        # Description
        if metadata['description']:
            parts.append(f"Description: {metadata['description']}")
        
        # Schedule
        if metadata['schedule_interval']:
            parts.append(f"Schedule: {metadata['schedule_interval']}")
        
        # Tags
        if metadata['tags']:
            parts.append(f"Tags: {', '.join(metadata['tags'])}")
        
        # Tasks
        if metadata['task_ids']:
            parts.append(f"Tasks: {', '.join(metadata['task_ids'][:10])}")  # Max 10
        
        # Operators utilisés
        if metadata['operators']:
            parts.append(f"Operators: {', '.join(metadata['operators'])}")
        
        # Pattern TaskFlow
        if metadata['has_taskflow']:
            parts.append("Uses TaskFlow API with @task decorators")
        
        # Imports importants (filtrer le bruit)
        relevant_imports = [
            imp for imp in metadata['imports']
            if any(keyword in imp for keyword in ['airflow', 'aws', 's3', 'postgres', 'mysql', 'http', 'email'])
        ]
        if relevant_imports:
            parts.append(f"Imports: {', '.join(relevant_imports[:5])}")
        
        return "\n".join(parts)
    
    def index_dag(self, filepath: str, overwrite: bool = False) -> bool:
        """
        Indexer un seul fichier DAG.
        
        Args:
            filepath: Chemin vers le fichier .py
            overwrite: Réindexer même si déjà présent
        
        Returns:
            bool: True si succès, False sinon
        """
        filename = Path(filepath).name
        
        # Vérifier si déjà indexé
        if not overwrite:
            existing = self.collection.get(ids=[filename])
            if existing['ids']:
                print(f"⏭️  {filename} déjà indexé (utilisez overwrite=True pour réindexer)")
                return True
        
        # Extraire les métadonnées
        metadata = self.extract_dag_metadata(filepath)
        
        if not metadata:
            print(f"❌ Impossible d'extraire les métadonnées de {filename}")
            return False
        
        # Créer le texte searchable
        searchable_text = self.create_searchable_text(metadata)
        
        # Générer l'embedding
        embedding = self.embedder.encode(searchable_text)
        
        # Préparer les métadonnées pour ChromaDB (seulement des types simples)
        chroma_metadata = {
            'filename': metadata['filename'],
            'dag_id': metadata['dag_id'] or 'unknown',
            'description': (metadata['description'] or '')[:200],  # Limiter la taille
            'schedule': str(metadata['schedule_interval'] or 'None'),
            'num_tasks': len(metadata['task_ids']),
            'has_taskflow': metadata['has_taskflow'],
            'tags': ','.join(metadata['tags'][:5]),  # Max 5 tags
            'operators': ','.join(metadata['operators'][:5]),
        }
        
        # Ajouter ou mettre à jour dans ChromaDB
        try:
            self.collection.upsert(
                ids=[filename],
                embeddings=[embedding.tolist()],
                documents=[metadata['code']],  # Code complet
                metadatas=[chroma_metadata]
            )
            
            print(f"✅ Indexé : {filename} (DAG: {metadata['dag_id']}, {len(metadata['task_ids'])} tâches)")
            return True
        
        except Exception as e:
            print(f"❌ Erreur lors de l'indexation de {filename}: {e}")
            return False
    
    def index_directory(self, directory: str, recursive: bool = False, overwrite: bool = False) -> Dict[str, int]:
        """
        Indexer tous les DAG d'un répertoire.
        
        Args:
            directory: Chemin vers le dossier
            recursive: Inclure les sous-dossiers
            overwrite: Réindexer les DAG déjà présents
        
        Returns:
            dict: Statistiques (success, failed, skipped)
        """
        if not os.path.exists(directory):
            print(f"⚠️  Le dossier {directory} n'existe pas")
            return {'success': 0, 'failed': 0, 'skipped': 0}
        
        print(f"\n📁 Indexation des DAG dans : {directory}")
        print(f"   Récursif : {recursive}")
        print(f"   Écraser existants : {overwrite}")
        print()
        
        stats = {'success': 0, 'failed': 0, 'skipped': 0}
        
        # Trouver tous les fichiers .py
        if recursive:
            py_files = list(Path(directory).rglob('*.py'))
        else:
            py_files = list(Path(directory).glob('*.py'))
        
        # Filtrer les fichiers spéciaux
        py_files = [
            f for f in py_files
            if not f.name.startswith(('_', '.'))
            and f.name != '__init__.py'
        ]
        
        print(f"🔍 {len(py_files)} fichiers Python trouvés")
        print()
        
        # Indexer chaque fichier
        for filepath in py_files:
            result = self.index_dag(str(filepath), overwrite=overwrite)
            
            if result:
                stats['success'] += 1
            else:
                stats['failed'] += 1
        
        # Afficher le résumé
        print()
        print("="*60)
        print(f"📊 RÉSUMÉ DE L'INDEXATION")
        print("="*60)
        print(f"✅ Succès : {stats['success']}")
        print(f"❌ Échecs : {stats['failed']}")
        print(f"📦 Total dans la base : {self.collection.count()} DAG")
        print("="*60)
        
        return stats
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Obtenir des statistiques sur la base RAG.
        
        Returns:
            dict: Statistiques détaillées
        """
        count = self.collection.count()
        
        stats = {
            'total_dags': count,
            'collection_name': self.collection.name,
            'db_path': self.db_path,
        }
        
        # Récupérer tous les DAG pour des stats détaillées
        if count > 0:
            all_data = self.collection.get()
            
            # Compter les opérateurs utilisés
            operators = {}
            for metadata in all_data['metadatas']:
                for op in metadata.get('operators', '').split(','):
                    op = op.strip()
                    if op:
                        operators[op] = operators.get(op, 0) + 1
            
            stats['top_operators'] = dict(sorted(operators.items(), key=lambda x: x[1], reverse=True)[:5])
            stats['has_taskflow_count'] = sum(1 for m in all_data['metadatas'] if m.get('has_taskflow'))
        
        return stats
    
    def reset_collection(self):
        """
        Réinitialiser complètement la collection (DANGER : supprime tout).
        """
        print("⚠️  ATTENTION : Suppression de toute la collection...")
        self.client.delete_collection(name="airflow_dags")
        self.collection = self.client.create_collection(name="airflow_dags")
        print("✅ Collection réinitialisée")