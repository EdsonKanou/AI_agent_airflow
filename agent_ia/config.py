"""
Configuration de l'agent IA.
"""

# === Ollama Configuration ===
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_MODEL = "codellama"  # Modèle par défaut
OLLAMA_TIMEOUT = 220  # Timeout en secondes

# === Paramètres de génération ===
GENERATION_CONFIG = {
    "temperature": 0.2,  # Faible = plus déterministe (0.0 à 1.0)
    "top_p": 0.9,        # Nucleus sampling
    "top_k": 40,         # Nombre de tokens à considérer
    "num_predict": 1000, # Nombre max de tokens générés
}

# === Chemins ===
DAGS_OUTPUT_DIR = "dags/generated"  # Où sauvegarder les DAG générés