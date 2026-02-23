"""
Module pour créer des embeddings (représentations vectorielles de texte).

Un embedding transforme du texte en vecteur numérique qui capture le sens sémantique.
Exemple : "chat" et "cat" auront des vecteurs très proches.
"""

from sentence_transformers import SentenceTransformer
from typing import List, Union
import numpy as np


class Embedder:
    """
    Classe pour gérer la création d'embeddings.
    
    Utilise le modèle 'all-MiniLM-L6-v2' qui :
    - Produit des vecteurs de 384 dimensions
    - Est optimisé pour la recherche sémantique
    - Fonctionne bien en anglais
    - Est léger (80 MB) et rapide
    """
    
    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        """
        Initialiser l'embedder avec un modèle spécifique.
        
        Args:
            model_name (str): Nom du modèle SentenceTransformer.
                             Par défaut : 'all-MiniLM-L6-v2'
        
        Note:
            Au premier lancement, le modèle sera téléchargé depuis
            Hugging Face (~80 MB). Les fois suivantes, il utilisera
            la version en cache.
        """
        print(f"📦 Chargement du modèle d'embedding : {model_name}")
        print("   (Peut prendre 30s au premier lancement...)")
        
        # Charger le modèle
        # device=None → Utilise GPU si disponible, sinon CPU
        self.model = SentenceTransformer(model_name, device=None)
        self.model_name = model_name
        self.embedding_dimension = self.model.get_sentence_embedding_dimension()
        
        print(f"✅ Modèle chargé : {self.embedding_dimension} dimensions")
    
    def encode(self, texts: Union[str, List[str]], 
               show_progress: bool = False) -> np.ndarray:
        """
        Encoder un ou plusieurs textes en embeddings.
        
        Args:
            texts: Un texte (str) ou une liste de textes (List[str])
            show_progress: Afficher une barre de progression (défaut: False)
        
        Returns:
            np.ndarray: 
                - Si input = str  → shape (embedding_dim,)
                - Si input = list → shape (n_texts, embedding_dim)
        
        Example:
            >>> embedder = Embedder()
            >>> 
            >>> # Un seul texte
            >>> emb = embedder.encode("Hello world")
            >>> emb.shape
            (384,)
            >>> 
            >>> # Plusieurs textes
            >>> embs = embedder.encode(["Hello", "World"])
            >>> embs.shape
            (2, 384)
        """
        # Convertir str en list si nécessaire
        was_single_string = isinstance(texts, str)
        if was_single_string:
            texts = [texts]
        
        # Encoder les textes
        embeddings = self.model.encode(
            texts,
            show_progress_bar=show_progress,
            convert_to_numpy=True,  # Retourner des numpy arrays
            normalize_embeddings=True  # Normaliser pour similarité cosinus
        )
        
        # Si input était un seul string, retourner un seul vecteur
        if was_single_string:
            return embeddings[0]
        
        return embeddings
    
    def similarity(self, text1: str, text2: str) -> float:
        """
        Calculer la similarité sémantique entre deux textes.
        
        Args:
            text1: Premier texte
            text2: Deuxième texte
        
        Returns:
            float: Score de similarité entre -1 et 1
                  (1 = identique, 0 = non corrélé, -1 = opposé)
        
        Note:
            Utilise la similarité cosinus :
            cos(θ) = (A · B) / (||A|| × ||B||)
        
        Example:
            >>> embedder = Embedder()
            >>> 
            >>> sim1 = embedder.similarity("cat", "chat")
            >>> print(f"cat vs chat: {sim1:.2f}")
            cat vs chat: 0.65  # Assez similaire
            >>> 
            >>> sim2 = embedder.similarity("cat", "computer")
            >>> print(f"cat vs computer: {sim2:.2f}")
            cat vs computer: 0.05  # Très différent
        """
        # Encoder les deux textes
        emb1 = self.encode(text1)
        emb2 = self.encode(text2)
        
        # Calculer la similarité cosinus
        # Note : Les embeddings sont déjà normalisés, donc c'est juste un dot product
        similarity_score = np.dot(emb1, emb2)
        
        return float(similarity_score)
    
    def batch_similarity(self, query: str, candidates: List[str]) -> List[float]:
        """
        Calculer la similarité entre une requête et plusieurs candidats.
        
        Args:
            query: Texte de référence
            candidates: Liste de textes à comparer
        
        Returns:
            List[float]: Scores de similarité pour chaque candidat
        
        Example:
            >>> embedder = Embedder()
            >>> scores = embedder.batch_similarity(
            ...     "DAG for S3",
            ...     ["S3 pipeline", "API scraper", "Database backup"]
            ... )
            >>> print(scores)
            [0.85, 0.32, 0.41]  # S3 pipeline est le plus similaire
        """
        # Encoder la requête
        query_emb = self.encode(query)
        
        # Encoder tous les candidats
        candidate_embs = self.encode(candidates)
        
        # Calculer les similarités (dot product car normalisé)
        similarities = np.dot(candidate_embs, query_emb)
        
        return similarities.tolist()
    
    def get_info(self) -> dict:
        """
        Obtenir des informations sur le modèle.
        
        Returns:
            dict: Informations (nom, dimensions, etc.)
        """
        return {
            "model_name": self.model_name,
            "embedding_dimension": self.embedding_dimension,
            "max_seq_length": self.model.max_seq_length,
        }


# === Fonction utilitaire pour créer un embedder global ===

_global_embedder = None

def get_embedder() -> Embedder:
    """
    Singleton pour obtenir une instance unique d'Embedder.
    
    Évite de recharger le modèle plusieurs fois (économie de RAM).
    
    Returns:
        Embedder: Instance partagée de l'embedder
    
    Example:
        >>> from agent_ia.rag.embedder import get_embedder
        >>> embedder = get_embedder()  # Charge le modèle
        >>> embedder2 = get_embedder()  # Réutilise la même instance
        >>> embedder is embedder2
        True
    """
    global _global_embedder
    
    if _global_embedder is None:
        _global_embedder = Embedder()
    
    return _global_embedder