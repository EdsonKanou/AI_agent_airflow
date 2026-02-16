"""
Test de connexion à Ollama via Python.
"""
import requests
import json

def test_ollama():
    """Envoyer une requête simple à Ollama."""
    
    url = "http://localhost:11434/api/generate"
    
    payload = {
        "model": "codellama",
        "prompt": "Write a Python function that adds two numbers. Only output the code, no explanation.",
        "stream": False  # Réponse complète d'un coup
    }
    
    print("📤 Envoi de la requête à Ollama...")
    
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        result = response.json()
        code_genere = result['response']
        
        print("\n✅ Réponse reçue :")
        print("="*50)
        print(code_genere)
        print("="*50)
    else:
        print(f"❌ Erreur : {response.status_code}")
        print(response.text)

if __name__ == "__main__":
    test_ollama()