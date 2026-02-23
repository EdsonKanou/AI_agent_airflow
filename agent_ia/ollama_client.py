"""
Client pour communiquer avec Ollama.
"""
import requests
import json
from typing import Dict, Any, Optional
from .utils.code_cleaner import clean_and_validate
from .config import OLLAMA_BASE_URL, OLLAMA_MODEL, OLLAMA_TIMEOUT, GENERATION_CONFIG


class OllamaClient:
    """
    Client pour interagir avec le serveur Ollama local.
    """
    
    def __init__(self, model: str = OLLAMA_MODEL):
        """
        Initialiser le client.
        
        Args:
            model: Nom du modèle Ollama à utiliser
        """
        self.base_url = OLLAMA_BASE_URL
        self.model = model
        self.timeout = OLLAMA_TIMEOUT
    
    def generate(self, prompt: str, **kwargs) -> str:
        """
        Générer du texte à partir d'un prompt.
        
        Args:
            prompt: Le prompt à envoyer au modèle
            **kwargs: Paramètres supplémentaires (temperature, top_p, etc.)
        
        Returns:
            str: Le texte généré
        
        Raises:
            ConnectionError: Si Ollama n'est pas accessible
            ValueError: Si la réponse est invalide
        """
        # Fusionner les paramètres par défaut avec ceux fournis
        config = {**GENERATION_CONFIG, **kwargs}
        
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,  # Réponse complète
            "options": config
        }
        
        try:
            print(f"🤖 Génération avec {self.model}...")
            
            response = requests.post(
                f"{self.base_url}/api/generate",
                json=payload,
                timeout=self.timeout
            )
            
            response.raise_for_status()  # Lève une exception si erreur HTTP
            
            result = response.json()
            generated_text = result.get('response', '')
            
            if not generated_text:
                raise ValueError("Réponse vide du modèle")
            
            print(f"✅ Génération terminée ({len(generated_text)} caractères)")
            
            return generated_text
        
        except requests.exceptions.ConnectionError:
            raise ConnectionError(
                f"❌ Impossible de se connecter à Ollama sur {self.base_url}. "
                "Vérifiez qu'Ollama est bien lancé (commande: ollama serve)"
            )
        
        except requests.exceptions.Timeout:
            raise TimeoutError(
                f"⏱️  La génération a pris plus de {self.timeout}s. "
                "Essayez de simplifier le prompt ou d'augmenter le timeout."
            )
        
        except requests.exceptions.HTTPError as e:
            raise ValueError(
                f"❌ Erreur HTTP {e.response.status_code}: {e.response.text}"
            )
            
    def generate_dag_code(self, prompt: str, **kwargs) -> tuple[str, bool, str]:
        """
        Générer du code DAG et le nettoyer automatiquement.
        
        Version spécialisée de generate() qui :
        - Génère le code
        - Nettoie le markdown et les explications
        - Valide la structure
        
        Args:
            prompt: Prompt de génération
            **kwargs: Paramètres de génération
        
        Returns:
            tuple[str, bool, str]: (code_nettoyé, is_valid, error_message)
        
        Example:
            >>> client = OllamaClient()
            >>> code, valid, error = client.generate_dag_code(prompt)
            >>> if valid:
            ...     print("Code prêt à sauvegarder")
        """
        # Générer le code brut
        raw_code = self.generate(prompt, **kwargs)
        
        # Nettoyer et valider
        clean_code, is_valid, error_msg = clean_and_validate(raw_code)
        
        if not is_valid:
            print(f"⚠️  Code généré invalide : {error_msg}")
            print("💡 Le code sera quand même retourné pour correction manuelle")
        
        return clean_code, is_valid, error_msg

    
    def chat(self, messages: list, **kwargs) -> str:
        """
        Converser avec le modèle (mode chat).
        
        Args:
            messages: Liste de messages [{"role": "user", "content": "..."}]
            **kwargs: Paramètres supplémentaires
        
        Returns:
            str: La réponse du modèle
        """
        config = {**GENERATION_CONFIG, **kwargs}
        
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "options": config
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/api/chat",
                json=payload,
                timeout=self.timeout
            )
            
            response.raise_for_status()
            result = response.json()
            
            return result['message']['content']
        
        except Exception as e:
            raise Exception(f"Erreur lors du chat: {str(e)}")
    
    def list_models(self) -> list:
        """
        Lister les modèles disponibles.
        
        Returns:
            list: Liste des modèles installés
        """
        try:
            response = requests.get(f"{self.base_url}/api/tags")
            response.raise_for_status()
            
            models = response.json().get('models', [])
            return [model['name'] for model in models]
        
        except Exception as e:
            raise Exception(f"Erreur lors de la récupération des modèles: {str(e)}")
    
    def is_available(self) -> bool:
        """
        Vérifier si Ollama est accessible.
        
        Returns:
            bool: True si Ollama répond
        """
        try:
            response = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False


# === Fonction utilitaire ===

def get_client(model: Optional[str] = None) -> OllamaClient:
    """
    Factory pour créer un client Ollama.
    
    Args:
        model: Modèle à utiliser (optionnel)
    
    Returns:
        OllamaClient: Instance du client
    """
    return OllamaClient(model=model or OLLAMA_MODEL)