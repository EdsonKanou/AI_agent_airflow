"""
Tests complets de l'agent IA.
"""
import sys
sys.path.insert(0, '.')  # Pour importer agent_ia

from agent_ia import get_client, PromptBuilder


def test_connexion_ollama():
    """Test 1 : Vérifier qu'Ollama est accessible."""
    print("\n" + "="*60)
    print("TEST 1 : Connexion à Ollama")
    print("="*60)
    
    client = get_client()
    
    if client.is_available():
        print("✅ Ollama est accessible")
    else:
        print("❌ Ollama n'est pas accessible")
        print("💡 Lancez 'ollama serve' dans un autre terminal")
        return False
    
    return True


def test_lister_modeles():
    """Test 2 : Lister les modèles disponibles."""
    print("\n" + "="*60)
    print("TEST 2 : Modèles disponibles")
    print("="*60)
    
    client = get_client()
    
    try:
        models = client.list_models()
        print(f"📦 Modèles installés : {len(models)}")
        for model in models:
            print(f"   - {model}")
        
        if 'codellama' not in [m.split(':')[0] for m in models]:
            print("⚠️  CodeLlama n'est pas installé")
            print("💡 Lancez : ollama pull codellama")
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def test_generation_simple():
    """Test 3 : Générer du code simple."""
    print("\n" + "="*60)
    print("TEST 3 : Génération de code simple")
    print("="*60)
    
    client = get_client()
    
    prompt = "Write a Python function that calculates the factorial of a number. Only output the code. NO comments outside code,NO explanations,ONLY valid Python. Just the code. "
    
    try:
        code = client.generate(prompt, temperature=0.1, num_predict=200)
        
        print("\n📝 Code généré :")
        print("-" * 60)
        print(code)
        print("-" * 60)
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def test_generation_dag():
    """Test 4 : Générer un DAG Airflow simple."""
    print("\n" + "="*60)
    print("TEST 4 : Génération d'un DAG Airflow")
    print("="*60)
    
    client = get_client()
    builder = PromptBuilder()
    
    description = """
    Create a simple Airflow DAG that:
    1. Prints 'Hello' in task 1
    2. Prints 'World' in task 2
    3. Task 2 runs after task 1
    4. Runs daily at 9 AM
    """
    
    requirements = {
        "dag_id": "hello_world_dag",
        "schedule": "@daily",
        "tags": "['test', 'hello']"
    }
    
    prompt = builder.build_dag_prompt(description, requirements)
    
    print("📤 Prompt envoyé :")
    print("-" * 60)
    print(prompt[:300] + "...")  # Afficher les 300 premiers caractères
    print("-" * 60)
    
    try:
        code = client.generate(prompt, temperature=0.2, num_predict=800)
        
        print("\n✅ DAG généré :")
        print("=" * 60)
        print(code)
        print("=" * 60)
        
        # Sauvegarder pour inspection manuelle
        output_file = "dags/generated/test_generated_dag.py"
        
        import os
        os.makedirs("dags/generated", exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(code)
        
        print(f"\n💾 DAG sauvegardé dans : {output_file}")
        print("💡 Vérifiez-le dans l'interface Airflow (http://localhost:8081)")
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def main():
    """Lancer tous les tests."""
    print("\n" + "🧪 " + "="*58)
    print("🧪  TESTS DE L'AGENT IA - JOUR 3")
    print("🧪 " + "="*58)
    
    tests = [
        test_connexion_ollama,
        test_lister_modeles,
        test_generation_simple,
        test_generation_dag,
    ]
    
    results = []
    
    for test in tests:
        result = test()
        results.append(result)
        
        if not result:
            print(f"\n⚠️  Le test {test.__name__} a échoué.")
            print("💡 Corrigez le problème avant de continuer.\n")
            break
    
    print("\n" + "="*60)
    print(f"📊 RÉSULTAT : {sum(results)}/{len(tests)} tests réussis")
    print("="*60)
    
    if all(results):
        print("\n🎉 Tous les tests sont passés !")
        print("✅ L'agent IA est opérationnel !")
    else:
        print("\n⚠️  Certains tests ont échoué.")


if __name__ == "__main__":
    main()