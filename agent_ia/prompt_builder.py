"""
Construction de prompts pour la génération de DAG Airflow.
"""
from typing import Dict, Any


class PromptBuilder:
    """
    Construit des prompts optimisés pour la génération de DAG.
    """
    
    # Template de base pour générer un DAG
    DAG_GENERATION_TEMPLATE = """You are an expert Apache Airflow developer. Generate a valid Airflow DAG based on the following description.

IMPORTANT RULES:
1. Output ONLY Python code, no markdown, no explanations
2. Use apache-airflow 2.7+ syntax
3. Import all necessary modules
4. Include docstrings
5. Use meaningful variable names
6. Add error handling where appropriate
7. Use PythonOperator for Python tasks, BashOperator for shell commands

DESCRIPTION:
{description}

REQUIREMENTS:
{requirements}

Generate the complete DAG code now:"""
    
    @staticmethod
    def build_dag_prompt(description: str, requirements: Dict[str, Any] = None) -> str:
        """
        Construire un prompt pour générer un DAG.
        
        Args:
            description: Description en langage naturel du pipeline
            requirements: Exigences supplémentaires (optionnel)
        
        Returns:
            str: Le prompt formaté
        
        Example:
            >>> builder = PromptBuilder()
            >>> prompt = builder.build_dag_prompt(
            ...     "Create a daily ETL pipeline that downloads data from an API"
            ... )
        """
        # Formater les requirements
        req_text = ""
        if requirements:
            req_text = "\n".join([f"- {k}: {v}" for k, v in requirements.items()])
        else:
            req_text = "- No specific requirements"
        
        return PromptBuilder.DAG_GENERATION_TEMPLATE.format(
            description=description,
            requirements=req_text
        )
    
    @staticmethod
    def build_correction_prompt(code: str, error: str) -> str:
        """
        Construire un prompt pour corriger du code erroné.
        
        Args:
            code: Code Python avec erreurs
            error: Message d'erreur
        
        Returns:
            str: Le prompt de correction
        """
        return f"""The following Airflow DAG code has an error. Fix it.

ERROR:
{error}

ORIGINAL CODE:
```python
{code}
```

CORRECTED CODE (output only the fixed code, no explanations):"""
    
    @staticmethod
    def build_explanation_prompt(code: str) -> str:
        """
        Construire un prompt pour expliquer un DAG.
        
        Args:
            code: Code Python du DAG
        
        Returns:
            str: Le prompt d'explication
        """
        return f"""Explain the following Airflow DAG in simple terms:
```python
{code}
```

Provide:
1. What this DAG does (1-2 sentences)
2. List of tasks and their purpose
3. Execution order
4. Schedule information"""