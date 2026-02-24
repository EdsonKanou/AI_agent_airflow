"""
Interface Streamlit pour le générateur de DAG Airflow.

Features:
- Génération avec/sans RAG
- Comparaison côte à côte
- Historique des générations
- Gestion de la base RAG
"""

import streamlit as st
import os
import sys
from datetime import datetime
from pathlib import Path

# Ajouter le path pour les imports
sys.path.insert(0, '.')

from agent_ia.generator import DAGGenerator
from agent_ia.rag.indexer import DAGIndexer
from agent_ia.rag.retriever import DAGRetriever


# Configuration de la page
st.set_page_config(
    page_title="Générateur de DAG Airflow",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)


# Styles CSS personnalisés
st.markdown("""
<style>
    .stTextArea textarea {
        font-family: 'Courier New', monospace;
        font-size: 14px;
    }
    .dag-code {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #28a745;
    }
    .error-box {
        background-color: #f8d7da;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #dc3545;
    }
</style>
""", unsafe_allow_html=True)


# === Sidebar : Configuration ===
with st.sidebar:
    st.title("Configuration")
    
    st.divider()
    
    # Modèle Ollama
    model = st.selectbox(
        "Modèle IA",
        ["codellama", "mistral", "deepseek-coder"],
        index=0,
        help="Modèle Ollama pour la génération"
    )
    
    # Température
    temperature = st.slider(
        "Température",
        min_value=0.0,
        max_value=1.0,
        value=0.2,
        step=0.1,
        help="0 = déterministe, 1 = créatif"
    )
    
    st.divider()
    
    # Section RAG
    st.subheader("RAG (Retrieval-Augmented Generation)")
    
    # Vérifier l'état de la base RAG
    try:
        retriever = DAGRetriever()
        if retriever.collection:
            rag_count = retriever.collection.count()
        else:
            rag_count = 0
    except:
        rag_count = 0
    
    st.metric("DAG indexés", rag_count)
    
    if rag_count == 0:
        st.warning("Aucun DAG indexé. Le RAG ne sera pas efficace.")
    
    # Bouton d'indexation
    if st.button("Indexer les DAG d'exemples", type="secondary"):
        with st.spinner("Indexation en cours..."):
            indexer = DAGIndexer()
            results = indexer.index_directory("dags/examples", overwrite=True)
            
            if results['success'] > 0:
                st.success(f"{results['success']} DAG indexés avec succès")
                st.rerun()
            else:
                st.error("Échec de l'indexation")
    
    st.divider()
    
    # Informations
    st.caption("Version : 1.0.0")
    st.caption("Powered by Ollama + RAG")


# === Tabs principales ===
tab1, tab2, tab3, tab4 = st.tabs([
    "Génération Simple",
    "Comparaison avec/sans RAG",
    "Historique",
    "Documentation"
])


# ============================================================
# TAB 1 : GÉNÉRATION SIMPLE
# ============================================================
with tab1:
    st.header("Générer un DAG Airflow")
    
    # Formulaire
    with st.form("generation_form"):
        
        # Description
        description = st.text_area(
            "Description du pipeline",
            height=200,
            placeholder="""Exemple :
Create a DAG that:
1. Downloads CSV files from S3 bucket
2. Cleans the data (remove duplicates)
3. Loads into PostgreSQL
4. Sends email notification
5. Runs daily at 3 AM""",
            help="Décrivez votre pipeline en langage naturel"
        )
        
        # Colonnes pour les paramètres
        col1, col2 = st.columns(2)
        
        with col1:
            dag_id = st.text_input(
                "DAG ID",
                value="my_pipeline",
                help="Identifiant unique du DAG"
            )
            
            schedule = st.selectbox(
                "Planification",
                ["@daily", "@hourly", "@weekly", "None", "Personnalisé"],
                help="Fréquence d'exécution"
            )
            
            if schedule == "Personnalisé":
                cron_expr = st.text_input(
                    "Expression cron",
                    placeholder="0 9 * * *"
                )
            else:
                cron_expr = schedule
        
        with col2:
            tags = st.text_input(
                "Tags (séparés par des virgules)",
                value="production, etl",
                help="Tags pour organiser vos DAG"
            )
            
            owner = st.text_input(
                "Propriétaire",
                value="data_team"
            )
        
        st.divider()
        
        # Option RAG (IMPORTANTE)
        use_rag = st.checkbox(
            "Utiliser le RAG (exemples de votre codebase)",
            value=True,
            help="Injecter des exemples similaires pour améliorer la génération"
        )
        
        if use_rag:
            rag_examples = st.slider(
                "Nombre d'exemples RAG",
                min_value=1,
                max_value=3,
                value=2,
                help="Plus d'exemples = meilleure qualité mais génération plus lente"
            )
        else:
            rag_examples = 0
        
        # Bouton de génération
        submitted = st.form_submit_button(
            "Générer le DAG",
            type="primary",
            use_container_width=True
        )
    
    # Traitement de la soumission
    if submitted:
        if not description.strip():
            st.error("Veuillez fournir une description du pipeline")
        else:
            tags_list = [f"'{t.strip()}'" for t in tags.split(',')]
            # Préparer les requirements
            requirements = {
                'dag_id': dag_id,
                'schedule': cron_expr,
                'tags': f"[{', '.join(tags_list)}]",
                'owner': owner
            }
            
            # Générer
            with st.spinner("Génération en cours..."):
                generator = DAGGenerator(
                    model=model,
                    use_rag=use_rag,
                    rag_examples=rag_examples,
                    temperature=temperature
                )
                
                code, is_valid, error, metadata = generator.generate(
                    description,
                    requirements
                )
            
            # Afficher le résultat
            if is_valid:
                st.markdown('<div class="success-box">', unsafe_allow_html=True)
                st.success("DAG généré avec succès")
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Informations
                col1, col2, col3 = st.columns(3)
                col1.metric("Taille", f"{metadata['code_length']} caractères")
                col2.metric("RAG utilisé", "Oui" if metadata['rag_used'] else "Non")
                col3.metric("Modèle", metadata['model'])
                
                # Code
                st.divider()
                st.subheader("Code généré")
                st.code(code, language="python", line_numbers=True)
                
                # Actions
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("Sauvegarder dans dags/generated/", type="primary"):
                        os.makedirs("dags/generated", exist_ok=True)
                        filename = f"{dag_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.py"
                        filepath = os.path.join("dags/generated", filename)
                        
                        with open(filepath, 'w', encoding='utf-8') as f:
                            f.write(code)
                        
                        st.success(f"Sauvegardé : {filepath}")
                
                with col2:
                    st.download_button(
                        label="Télécharger",
                        data=code,
                        file_name=f"{dag_id}.py",
                        mime="text/x-python"
                    )
                
                with col3:
                    if st.button("Régénérer"):
                        st.rerun()
            
            else:
                st.markdown('<div class="error-box">', unsafe_allow_html=True)
                st.error("Échec de la génération")
                st.markdown('</div>', unsafe_allow_html=True)
                
                st.error(error)
                
                if 'timeout' in error.lower():
                    st.info("Le timeout a été atteint. Essayez de :")
                    st.markdown("""
                    - Simplifier la description
                    - Désactiver le RAG
                    - Utiliser un modèle plus rapide (mistral)
                    """)


# ============================================================
# TAB 2 : COMPARAISON AVEC/SANS RAG
# ============================================================
with tab2:
    st.header("Comparer les générations avec/sans RAG")
    
    st.info("Cette fonctionnalité permet de voir l'impact du RAG sur la qualité du code généré.")
    
    with st.form("comparison_form"):
        description_comp = st.text_area(
            "Description du pipeline",
            height=150,
            placeholder="Décrivez votre pipeline...",
            key="comp_desc"
        )
        
        dag_id_comp = st.text_input("DAG ID", value="comparison_dag", key="comp_id")
        
        submitted_comp = st.form_submit_button("Générer les 2 versions", type="primary")
    
    if submitted_comp:
        if not description_comp.strip():
            st.error("Veuillez fournir une description")
        else:
            requirements_comp = {'dag_id': dag_id_comp, 'schedule': '@daily'}
            
            col1, col2 = st.columns(2)
            
            # Génération SANS RAG
            with col1:
                st.subheader("SANS RAG")
                
                with st.spinner("Génération sans RAG..."):
                    gen_no_rag = DAGGenerator(
                        model=model,
                        use_rag=False,
                        temperature=temperature
                    )
                    
                    code_no_rag, valid_no_rag, err_no_rag, meta_no_rag = gen_no_rag.generate(
                        description_comp,
                        requirements_comp
                    )
                
                if valid_no_rag:
                    st.success("Génération réussie")
                    st.metric("Taille", f"{meta_no_rag['code_length']} caractères")
                    st.code(code_no_rag, language="python", line_numbers=True)
                else:
                    st.error(f"Échec : {err_no_rag}")
            
            # Génération AVEC RAG
            with col2:
                st.subheader("AVEC RAG")
                
                with st.spinner("Génération avec RAG..."):
                    gen_with_rag = DAGGenerator(
                        model=model,
                        use_rag=True,
                        rag_examples=2,
                        temperature=temperature
                    )
                    
                    code_with_rag, valid_with_rag, err_with_rag, meta_with_rag = gen_with_rag.generate(
                        description_comp,
                        requirements_comp
                    )
                
                if valid_with_rag:
                    st.success("Génération réussie")
                    st.metric("Taille", f"{meta_with_rag['code_length']} caractères")
                    st.code(code_with_rag, language="python", line_numbers=True)
                else:
                    st.error(f"Échec : {err_with_rag}")
            
            # Analyse comparative
            if valid_no_rag and valid_with_rag:
                st.divider()
                st.subheader("Analyse comparative")
                
                col1, col2, col3 = st.columns(3)
                
                col1.metric(
                    "Différence de taille",
                    f"{meta_with_rag['code_length'] - meta_no_rag['code_length']} caractères"
                )
                
                col2.metric(
                    "RAG utilisé",
                    "Oui" if meta_with_rag['rag_used'] else "Non"
                )
                
                # Compter les imports
                imports_no_rag = code_no_rag.count('import ')
                imports_with_rag = code_with_rag.count('import ')
                
                col3.metric(
                    "Différence d'imports",
                    f"{imports_with_rag - imports_no_rag}"
                )


# ============================================================
# TAB 3 : HISTORIQUE
# ============================================================
with tab3:
    st.header("Historique des DAG générés")
    
    generated_dir = "dags/generated"
    
    if os.path.exists(generated_dir):
        files = sorted(
            [f for f in os.listdir(generated_dir) if f.endswith('.py')],
            key=lambda x: os.path.getmtime(os.path.join(generated_dir, x)),
            reverse=True
        )
        
        if files:
            st.info(f"{len(files)} DAG générés")
            
            # Afficher les fichiers
            for filename in files:
                filepath = os.path.join(generated_dir, filename)
                
                # Informations sur le fichier
                file_stat = os.stat(filepath)
                file_size = file_stat.st_size
                file_time = datetime.fromtimestamp(file_stat.st_mtime)
                
                with st.expander(f"{filename} ({file_size} octets) - {file_time.strftime('%Y-%m-%d %H:%M')}"):
                    # Lire le fichier
                    with open(filepath, 'r', encoding='utf-8') as f:
                        code = f.read()
                    
                    st.code(code, language="python", line_numbers=True)
                    
                    # Actions
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        st.download_button(
                            label="Télécharger",
                            data=code,
                            file_name=filename,
                            mime="text/x-python",
                            key=f"download_{filename}"
                        )
                    
                    with col2:
                        if st.button("Copier dans le presse-papier", key=f"copy_{filename}"):
                            st.info("Code copié (fonctionnalité à implémenter)")
                    
                    with col3:
                        if st.button("Supprimer", key=f"delete_{filename}", type="secondary"):
                            os.remove(filepath)
                            st.success(f"{filename} supprimé")
                            st.rerun()
        else:
            st.warning("Aucun DAG généré pour l'instant")
    else:
        st.warning("Le dossier dags/generated/ n'existe pas encore")


# ============================================================
# TAB 4 : DOCUMENTATION
# ============================================================
with tab4:
    st.header("Documentation")
    
    st.markdown("""
    ## Guide d'utilisation
    
    ### 1. Préparation
    
    **Avant de générer des DAG**, indexez vos DAG existants :
    1. Allez dans la sidebar
    2. Cliquez sur "Indexer les DAG d'exemples"
    3. Attendez la confirmation
    
    ### 2. Générer un DAG
    
    **Onglet "Génération Simple"** :
    1. Décrivez votre pipeline en détail
    2. Configurez les paramètres (dag_id, schedule, etc.)
    3. Choisissez d'activer ou non le RAG
    4. Cliquez sur "Générer le DAG"
    
    **Conseils pour une bonne description** :
    - Listez les tâches étape par étape
    - Mentionnez les technologies (S3, PostgreSQL, etc.)
    - Spécifiez la fréquence d'exécution
    - Indiquez les dépendances entre tâches
    
    ### 3. Comprendre le RAG
    
    **RAG = Retrieval-Augmented Generation**
    
    Quand activé, le système :
    1. Cherche des DAG similaires dans votre codebase
    2. Utilise ces exemples pour guider la génération
    3. Produit du code cohérent avec vos pratiques
    
    **Avantages** :
    - Code plus cohérent avec votre style
    - Meilleure utilisation des opérateurs
    - Respect de vos conventions
    
    **Inconvénients** :
    - Génération plus lente
    - Nécessite des DAG indexés
    
    ### 4. Comparer avec/sans RAG
    
    **Onglet "Comparaison"** :
    - Générez 2 versions du même DAG
    - Comparez la qualité du code
    - Choisissez la meilleure version
    
    ### 5. Planification (Schedule)
    
    - `@daily` : Tous les jours à minuit
    - `@hourly` : Toutes les heures
    - `@weekly` : Tous les lundis
    - `None` : Manuel uniquement
    - Personnalisé : Expression cron (ex: `0 9 * * 1`)
    
    ### 6. Bonnes pratiques
    
    - Testez le DAG dans Airflow avant de le mettre en production
    - Utilisez des tags pour organiser vos DAG
    - Indexez régulièrement vos nouveaux DAG
    - Comparez les versions avec/sans RAG
    
    ### 7. Résolution de problèmes
    
    **Timeout lors de la génération** :
    - Simplifiez la description
    - Désactivez le RAG
    - Utilisez un modèle plus rapide (mistral)
    
    **Code invalide** :
    - Vérifiez la description (pas trop vague)
    - Réessayez avec une température plus basse
    - Activez le RAG pour plus de cohérence
    
    **RAG inefficace** :
    - Indexez plus de DAG d'exemples
    - Vérifiez que vos exemples sont pertinents
    
    ## Ressources
    
    - [Documentation Airflow](https://airflow.apache.org/docs/)
    - [Documentation Ollama](https://ollama.ai/docs)
    - [Cron Expression Generator](https://crontab.guru/)
    """)


# === Footer ===
st.divider()
st.markdown("""
<div style='text-align: center; color: gray; padding: 1rem;'>
    Générateur de DAG Airflow v1.0.0 | Propulsé par Ollama + RAG
</div>
""", unsafe_allow_html=True)