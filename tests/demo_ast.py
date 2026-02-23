"""
Démonstration de l'analyse AST pour extraire des métadonnées d'un DAG.
"""
import ast
import sys
sys.path.insert(0, '.')


# === Code DAG exemple ===
dag_code = '''
"""
Mon DAG de test.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator

dag_id = 'mon_dag_test'

def task_fonction():
    print("Hello")

with DAG(dag_id='exemple', schedule='@daily') as dag:
    task1 = PythonOperator(task_id='tache_1', python_callable=task_fonction)
'''


def analyser_dag(code: str):
    """Analyser un code DAG avec AST."""
    
    print("="*60)
    print("ANALYSE AST D'UN DAG")
    print("="*60)
    
    # Parser le code
    tree = ast.parse(code)
    
    metadata = {
        'imports': [],
        'dag_id': None,
        'task_ids': [],
        'functions': [],
        'docstring': None
    }
    
    # Extraire le docstring du module
    metadata['docstring'] = ast.get_docstring(tree)
    
    # Parcourir tous les nœuds
    for node in ast.walk(tree):
        
        # Extraire les imports
        if isinstance(node, ast.Import):
            for alias in node.names:
                metadata['imports'].append(alias.name)
        
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                metadata['imports'].append(node.module)
        
        # Extraire les définitions de fonctions
        elif isinstance(node, ast.FunctionDef):
            metadata['functions'].append(node.name)
        
        # Extraire les arguments keyword (dag_id, task_id, etc.)
        elif isinstance(node, ast.keyword):
            if node.arg == 'dag_id':
                if isinstance(node.value, ast.Constant):
                    metadata['dag_id'] = node.value.value
            
            elif node.arg == 'task_id':
                if isinstance(node.value, ast.Constant):
                    metadata['task_ids'].append(node.value.value)
    
    # Afficher les résultats
    print(f"\n📄 Docstring : {metadata['docstring']}")
    print(f"\n📦 Imports : {metadata['imports']}")
    print(f"\n🏷️  DAG ID : {metadata['dag_id']}")
    print(f"\n📋 Task IDs : {metadata['task_ids']}")
    print(f"\n⚙️  Fonctions : {metadata['functions']}")
    
    return metadata


if __name__ == "__main__":
    analyser_dag(dag_code)