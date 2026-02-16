"""
Agent IA pour générer des DAG Airflow.
"""
from .ollama_client import OllamaClient, get_client
from .prompt_builder import PromptBuilder
from .config import OLLAMA_MODEL, DAGS_OUTPUT_DIR

__all__ = [
    'OllamaClient',
    'get_client',
    'PromptBuilder',
    'OLLAMA_MODEL',
    'DAGS_OUTPUT_DIR'
]