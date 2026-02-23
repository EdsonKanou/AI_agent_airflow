"""
Générateur de DAG Airflow avec support RAG.

Ce module orchestre :
1. Récupération d'exemples via RAG (si activé)
2. Construction du prompt enrichi
3. Génération du code avec Ollama
4. Nettoyage et validation du code
"""

import os
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

from .ollama_client import get_client
from .prompt_builder import PromptBuilder
from .rag.retriever import get_retriever
from .utils.code_cleaner import clean_and_validate
from .config import DAGS_OUTPUT_DIR


class DAGGenerator:
    """
    Générateur de DAG Airflow avec RAG.
    
    Orchestration complète du processus de génération.
    """
    
    def __init__(
        self,
        model: str = "codellama",
        use_rag: bool = True,
        rag_examples: int = 2,
        temperature: float = 0.2
    ):
        """
        Initialiser le générateur.
        
        Args:
            model: Modèle Ollama à utiliser
            use_rag: Utiliser le RAG pour enrichir le prompt
            rag_examples: Nombre d'exemples RAG à inclure
            temperature: Température de génération (0-1)
        """
        self.model = model
        self.use_rag = use_rag
        self.rag_examples = rag_examples
        self.temperature = temperature
        
        # Initialiser les composants
        self.ollama_client = get_client(model=model)
        self.prompt_builder = PromptBuilder()
        
        if use_rag:
            self.retriever = get_retriever()
            print(f"[Generator] RAG activé ({rag_examples} exemples)")
        else:
            self.retriever = None
            print("[Generator] RAG désactivé")
    
    def generate(
        self,
        description: str,
        requirements: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, bool, str, Dict[str, Any]]:
        """
        Générer un DAG complet.
        
        Args:
            description: Description en langage naturel du pipeline
            requirements: Exigences optionnelles (dag_id, schedule, etc.)
        
        Returns:
            tuple: (code, is_valid, error_message, metadata)
        
        Example:
            >>> generator = DAGGenerator()
            >>> code, valid, err, meta = generator.generate(
            ...     "Create an ETL pipeline",
            ...     {"dag_id": "my_etl", "schedule": "@daily"}
            ... )
        """
        print("\n" + "="*60)
        print("GÉNÉRATION DE DAG")
        print("="*60)
        
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'description': description,
            'requirements': requirements or {},
            'use_rag': self.use_rag,
            'model': self.model,
        }
        
        # Étape 1 : Récupérer des exemples RAG
        rag_context = ""
        if self.use_rag and self.retriever:
            print("\n[Step 1/4] Recherche d'exemples similaires via RAG...")
            rag_context = self.retriever.get_best_examples(
                description,
                max_examples=self.rag_examples,
                include_code=True,
                code_preview_lines=30
            )
            
            if "SIMILAR EXAMPLES" in rag_context:
                print("[Step 1/4] Exemples RAG récupérés")
            else:
                print("[Step 1/4] Aucun exemple pertinent trouvé")
                rag_context = ""
        else:
            print("\n[Step 1/4] RAG désactivé, pas d'exemples")
        
        metadata['rag_used'] = bool(rag_context)
        
        # Étape 2 : Construire le prompt
        print("\n[Step 2/4] Construction du prompt...")
        prompt = self.prompt_builder.build_dag_prompt(description, requirements)
        
        # Injecter le contexte RAG au début du prompt
        if rag_context:
            prompt = rag_context + "\n\n" + prompt
        
        print(f"[Step 2/4] Prompt construit ({len(prompt)} caractères)")
        
        # Étape 3 : Générer le code avec Ollama
        print("\n[Step 3/4] Génération du code avec Ollama...")
        try:
            raw_code = self.ollama_client.generate(
                prompt,
                temperature=self.temperature,
                num_predict=2000
            )
            print(f"[Step 3/4] Code généré ({len(raw_code)} caractères)")
        except Exception as e:
            error_msg = f"Erreur lors de la génération : {str(e)}"
            print(f"[Step 3/4] ERREUR : {error_msg}")
            return "", False, error_msg, metadata
        
        # Étape 4 : Nettoyer et valider
        print("\n[Step 4/4] Nettoyage et validation du code...")
        clean_code, is_valid, error_msg = clean_and_validate(raw_code)
        
        if is_valid:
            print("[Step 4/4] Code valide")
        else:
            print(f"[Step 4/4] Code invalide : {error_msg}")
        
        metadata['is_valid'] = is_valid
        metadata['code_length'] = len(clean_code)
        
        print("\n" + "="*60)
        print(f"RÉSULTAT : {'SUCCÈS' if is_valid else 'ÉCHEC'}")
        print("="*60)
        
        return clean_code, is_valid, error_msg, metadata
    
    
    def generate_and_save(
        self,
        description: str,
        requirements: Optional[Dict[str, Any]] = None,
        filename: Optional[str] = None
    ) -> Tuple[str, bool, str, Dict[str, Any]]:
        """
        Générer un DAG complet.
        
        Args:
            description: Description en langage naturel du pipeline
            requirements: Exigences optionnelles (dag_id, schedule, etc.)
        
        Returns:
            tuple: (code, is_valid, error_message, metadata)
        
        Example:
            >>> generator = DAGGenerator()
            >>> code, valid, err, meta = generator.generate(
            ...     "Create an ETL pipeline",
            ...     {"dag_id": "my_etl", "schedule": "@daily"}
            ... )
        """
        print("\n" + "="*60)
        print("GÉNÉRATION DE DAG")
        print("="*60)
        
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'description': description,
            'requirements': requirements or {},
            'use_rag': self.use_rag,
            'model': self.model,
        }
        
        # Étape 1 : Récupérer des exemples RAG
        rag_context = ""
        if self.use_rag and self.retriever:
            print("\n[Step 1/4] Recherche d'exemples similaires via RAG...")
            rag_context = self.retriever.get_best_examples(
                description,
                max_examples=self.rag_examples,
                include_code=True,
                code_preview_lines=30
            )
            
            if "SIMILAR EXAMPLES" in rag_context:
                print("[Step 1/4] Exemples RAG récupérés")
            else:
                print("[Step 1/4] Aucun exemple pertinent trouvé")
                rag_context = ""
        else:
            print("\n[Step 1/4] RAG désactivé, pas d'exemples")
        
        metadata['rag_used'] = bool(rag_context)
        
        # Étape 2 : Construire le prompt
        print("\n[Step 2/4] Construction du prompt...")
        prompt = self.prompt_builder.build_dag_prompt(description, requirements)
        
        # Injecter le contexte RAG au début du prompt
        if rag_context:
            prompt = rag_context + "\n\n" + prompt
        
        print(f"[Step 2/4] Prompt construit ({len(prompt)} caractères)")
        
        # Étape 3 : Générer le code avec Ollama
        print("\n[Step 3/4] Génération du code avec Ollama...")
        try:
            raw_code = self.ollama_client.generate(
                prompt,
                temperature=self.temperature,
                num_predict=2000
            )
            print(f"[Step 3/4] Code généré ({len(raw_code)} caractères)")
        except Exception as e:
            error_msg = f"Erreur lors de la génération : {str(e)}"
            print(f"[Step 3/4] ERREUR : {error_msg}")
            return "", False, error_msg, metadata
        
        # Étape 4 : Nettoyer et valider
        print("\n[Step 4/4] Nettoyage et validation du code...")
        clean_code, is_valid, error_msg = clean_and_validate(raw_code)
        
        if is_valid:
            print("[Step 4/4] Code valide")
        else:
            print(f"[Step 4/4] Code invalide : {error_msg}")
        
        metadata['is_valid'] = is_valid
        metadata['code_length'] = len(clean_code)
        
        print("\n" + "="*60)
        print(f"RÉSULTAT : {'SUCCÈS' if is_valid else 'ÉCHEC'}")
        print("="*60)
        
        # Créer le nom de fichier
        if not filename:
            dag_id = requirements.get('dag_id', 'generated_dag') if requirements else 'generated_dag'
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{dag_id}_{timestamp}.py"
        
        # Créer le dossier si nécessaire
        os.makedirs(DAGS_OUTPUT_DIR, exist_ok=True)
        
        # Sauvegarder
        filepath = os.path.join(DAGS_OUTPUT_DIR, filename)

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(clean_code)

            print(f"\n[Save] Fichier sauvegardé : {filepath}")

            metadata['filepath'] = filepath
            return clean_code, is_valid, "", metadata

        except Exception as e:
            error = f"Erreur lors de la sauvegarde : {str(e)}"
            print(f"\n[Save] ERREUR : {error}")

            metadata['filepath'] = None
            return clean_code, False, error, metadata

        
        #return clean_code, is_valid, error_msg, metadata
    
        
        
        
    def generate_and_save_last(
        self,
        description: str,
        requirements: Optional[Dict[str, Any]] = None,
        filename: Optional[str] = None
    ) -> Tuple[bool, str]:
        """
        Générer un DAG et le sauvegarder automatiquement.
        
        Args:
            description: Description du pipeline
            requirements: Exigences optionnelles
            filename: Nom du fichier (auto-généré si non fourni)
        
        Returns:
            tuple: (success, filepath_or_error_message)
        """
        # Générer le code
        code, is_valid, error_msg, metadata = self.generate(description, requirements)
        
        if not is_valid:
            return False, f"Génération échouée : {error_msg}"
        
        # Créer le nom de fichier
        if not filename:
            dag_id = requirements.get('dag_id', 'generated_dag') if requirements else 'generated_dag'
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{dag_id}_{timestamp}.py"
        
        # Créer le dossier si nécessaire
        os.makedirs(DAGS_OUTPUT_DIR, exist_ok=True)
        
        # Sauvegarder
        filepath = os.path.join(DAGS_OUTPUT_DIR, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(code)
            
            print(f"\n[Save] Fichier sauvegardé : {filepath}")
            return True, filepath
        
        except Exception as e:
            error = f"Erreur lors de la sauvegarde : {str(e)}"
            print(f"\n[Save] ERREUR : {error}")
            return False, error