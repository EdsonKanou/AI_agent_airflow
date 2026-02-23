"""
Utilitaires pour nettoyer le code généré par les LLM.

Les modèles génèrent souvent du markdown (```python) qu'il faut enlever.
"""

import re
from typing import Optional


def clean_generated_code(raw_code: str) -> str:
    """
    Nettoyer le code Python généré par un LLM.
    
    Supprime :
    - Les blocs markdown (```python, ```)
    - Les espaces superflus au début/fin
    - Les commentaires explicatifs avant le code
    
    Args:
        raw_code: Code brut généré par le LLM
    
    Returns:
        str: Code Python nettoyé
    
    Example:
        >>> raw = "```python\\nprint('hello')\\n```"
        >>> clean = clean_generated_code(raw)
        >>> print(clean)
        print('hello')
    """
    code = raw_code.strip()
    
    # Pattern 1 : ```python ... ```
    if '```python' in code:
        # Extraire le contenu entre ```python et ```
        match = re.search(r'```python\s*\n(.*?)\n```', code, re.DOTALL)
        if match:
            code = match.group(1)
    
    # Pattern 2 : ``` ... ``` (sans langage)
    elif code.startswith('```') and code.endswith('```'):
        code = code[3:-3].strip()
        # Si la première ligne est un nom de langage, l'enlever
        lines = code.split('\n')
        if lines[0].strip() in ['python', 'py', 'python3']:
            code = '\n'.join(lines[1:])
    
    # Enlever les explications avant le code (détection heuristique)
    # Si le code commence par des phrases explicatives avant "from" ou "import"
    lines = code.split('\n')
    first_code_line = None
    
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith(('from ', 'import ', 'def ', 'class ', '@', 'if ', 'for ', 'while ')):
            first_code_line = i
            break
        # Accepter aussi les docstrings au début
        if stripped.startswith('"""') or stripped.startswith("'''"):
            first_code_line = i
            break
    
    if first_code_line is not None and first_code_line > 0:
        # Vérifier que les lignes avant ne sont pas des commentaires Python
        potential_explanations = lines[:first_code_line]
        if not all(line.strip().startswith('#') or not line.strip() for line in potential_explanations):
            # Ce sont des explications en langage naturel, les supprimer
            code = '\n'.join(lines[first_code_line:])
    
    return code.strip()


def extract_python_code(text: str) -> Optional[str]:
    """
    Extraire le premier bloc de code Python d'un texte.
    
    Utile quand le LLM génère du texte + code mélangés.
    
    Args:
        text: Texte contenant potentiellement du code
    
    Returns:
        str | None: Code Python extrait, ou None si aucun trouvé
    """
    # Chercher des blocs ```python
    match = re.search(r'```python\s*\n(.*?)\n```', text, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    # Chercher des blocs ``` génériques
    match = re.search(r'```\s*\n(.*?)\n```', text, re.DOTALL)
    if match:
        potential_code = match.group(1).strip()
        # Vérifier si ça ressemble à du Python
        if any(keyword in potential_code for keyword in ['import ', 'def ', 'class ', 'from ']):
            return potential_code
    
    return None


# def validate_code_structure(code: str) -> tuple[bool, str]:
#     """
#     Valider que le code a une structure minimale correcte.
    
#     Vérifie :
#     - Au moins un import
#     - Pas de syntax markdown résiduelle
#     - Pas de texte explicatif
    
#     Args:
#         code: Code Python à valider
    
#     Returns:
#         tuple[bool, str]: (is_valid, error_message)
#     """
#     lines = code.split('\n')
    
#     # Vérifier qu'il n'y a pas de markdown résiduel
#     if '```' in code:
#         return False, "Code contient encore des marqueurs markdown (```)"
    
#     # Vérifier qu'il y a au moins un import ou def
#     has_import = any('import ' in line for line in lines)
#     has_def = any('def ' in line or 'class ' in line for line in lines)
    
#     if not (has_import or has_def):
#         return False, "Code ne contient ni import ni définition de fonction/classe"
    
#     # Vérifier qu'il n'y a pas de texte explicatif non-Python
#     # (lignes qui ne sont ni code, ni commentaires, ni docstrings)
#     in_docstring = False
#     for line in lines:
#         stripped = line.strip()
        
#         # Gérer les docstrings
#         if '"""' in stripped or "'''" in stripped:
#             in_docstring = not in_docstring
#             continue
        
#         if in_docstring:
#             continue
        
#         # Ligne vide ou commentaire = OK
#         if not stripped or stripped.startswith('#'):
#             continue
        
#         # Vérifier si c'est du Python valide (heuristique)
#         if not any([
#             stripped.startswith(('from ', 'import ', 'def ', 'class ', '@', 'if ', 'for ', 'while ', 'with ', 'try:', 'except', 'finally:', 'return', 'yield', 'raise', 'assert')),
#             '=' in stripped,
#             stripped.endswith((':',)),
#             stripped in ['pass', 'continue', 'break'],
#             stripped.startswith(('print(', 'self.', 'super(')),
#         ]):
#             # Pourrait être du texte explicatif
#             if len(stripped.split()) > 3 and not '(' in stripped:
#                 return False, f"Ligne suspecte (texte non-Python?) : {stripped[:50]}"
    
#     return True, ""


import ast

def validate_code_structure(code: str) -> tuple[bool, str]:
    """
    Valide que le code est du Python syntaxiquement correct
    + vérifications basiques de structure
    """

    # 1️⃣ Pas de markdown
    if '```' in code:
        return False, "Code contient encore des marqueurs markdown (```)"

    # 2️⃣ Vérifier que ça compile en Python
    try:
        ast.parse(code)
    except SyntaxError as e:
        return False, f"Erreur de syntaxe Python : {e}"

    lines = code.split('\n')

    # 3️⃣ Vérifier qu'il y a au moins un import ou def/class
    has_import = any(line.strip().startswith(('import ', 'from ')) for line in lines)
    has_def = any(line.strip().startswith(('def ', 'class ')) for line in lines)

    if not (has_import or has_def):
        return False, "Code ne contient ni import ni définition de fonction/classe"

    return True, ""


# === Fonction principale ===

def clean_and_validate(raw_code: str) -> tuple[str, bool, str]:
    """
    Nettoyer et valider du code généré en une seule étape.
    
    Args:
        raw_code: Code brut du LLM
    
    Returns:
        tuple[str, bool, str]: (cleaned_code, is_valid, error_message)
    
    Example:
        >>> raw = "Here's the code:\\n```python\\nprint('hi')\\n```"
        >>> code, valid, error = clean_and_validate(raw)
        >>> print(valid)
        True
    """
    # Étape 1 : Nettoyer
    cleaned = clean_generated_code(raw_code)
    
    # Étape 2 : Valider
    is_valid, error = validate_code_structure(cleaned)
    
    return cleaned, is_valid, error