"""
Tests pour vérifier que l'Embedder fonctionne correctement.
"""
import sys
sys.path.insert(0, '.')

from agent_ia.rag.embedder import Embedder, get_embedder
import numpy as np


def test_1_initialisation():
    """Test 1 : Vérifier que l'embedder se charge correctement."""
    print("\n" + "="*60)
    print("TEST 1 : Initialisation de l'Embedder")
    print("="*60)
    
    try:
        embedder = Embedder()
        
        info = embedder.get_info()
        print(f"✅ Modèle chargé : {info['model_name']}")
        print(f"✅ Dimensions : {info['embedding_dimension']}")
        print(f"✅ Longueur max : {info['max_seq_length']} tokens")
        
        assert info['embedding_dimension'] == 384, "Dimension incorrecte"
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def test_2_encode_simple():
    """Test 2 : Encoder un texte simple."""
    print("\n" + "="*60)
    print("TEST 2 : Encodage d'un texte simple")
    print("="*60)
    
    try:
        embedder = get_embedder()
        
        text = "Create an ETL pipeline"
        embedding = embedder.encode(text)
        
        print(f"📝 Texte : '{text}'")
        print(f"📊 Embedding shape : {embedding.shape}")
        print(f"📈 Premiers éléments : {embedding[:5]}")
        
        # Vérifications
        assert embedding.shape == (384,), "Shape incorrecte"
        assert isinstance(embedding, np.ndarray), "Type incorrect"
        assert -1 <= embedding[0] <= 1, "Valeurs non normalisées"
        
        print("✅ Encodage réussi !")
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def test_3_encode_batch():
    """Test 3 : Encoder plusieurs textes d'un coup."""
    print("\n" + "="*60)
    print("TEST 3 : Encodage batch (plusieurs textes)")
    print("="*60)
    
    try:
        embedder = get_embedder()
        
        texts = [
            "Download data from S3",
            "Process CSV files",
            "Load into PostgreSQL"
        ]
        
        embeddings = embedder.encode(texts, show_progress=True)
        
        print(f"📝 Nombre de textes : {len(texts)}")
        print(f"📊 Embeddings shape : {embeddings.shape}")
        
        # Vérifications
        assert embeddings.shape == (3, 384), "Shape incorrecte"
        
        print("✅ Encodage batch réussi !")
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def test_4_similarity():
    """Test 4 : Calculer la similarité entre textes."""
    print("\n" + "="*60)
    print("TEST 4 : Calcul de similarité sémantique")
    print("="*60)
    
    try:
        embedder = get_embedder()
        
        # Paires de textes à comparer
        pairs = [
            ("Download from S3", "Retrieve files from AWS S3"),  # Très similaire
            ("Download from S3", "Train ML model"),             # Différent
            ("cat", "chat"),                                     # Synonymes
            ("Python", "Java"),                                  # Langages (moyennement similaire)
        ]
        
        print("\n📊 Calcul des similarités :\n")
        
        for text1, text2 in pairs:
            sim = embedder.similarity(text1, text2)
            
            # Interprétation
            if sim > 0.7:
                interpretation = "🟢 Très similaire"
            elif sim > 0.4:
                interpretation = "🟡 Moyennement similaire"
            else:
                interpretation = "🔴 Peu similaire"
            
            print(f"  '{text1}' vs '{text2}'")
            print(f"  → Similarité : {sim:.3f} {interpretation}\n")
        
        print("✅ Test de similarité réussi !")
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def test_5_batch_similarity():
    """Test 5 : Comparer une requête à plusieurs candidats."""
    print("\n" + "="*60)
    print("TEST 5 : Similarité batch (1 vs N)")
    print("="*60)
    
    try:
        embedder = get_embedder()
        
        query = "ETL pipeline for S3 data"
        
        candidates = [
            "Download files from AWS S3 bucket",
            "Scrape data from website API",
            "Backup PostgreSQL database",
            "Process CSV and load to S3",
            "Train machine learning model"
        ]
        
        scores = embedder.batch_similarity(query, candidates)
        
        print(f"🔍 Requête : '{query}'\n")
        print("📊 Scores de similarité :\n")
        
        # Trier par score décroissant
        ranked = sorted(zip(candidates, scores), key=lambda x: x[1], reverse=True)
        
        for i, (candidate, score) in enumerate(ranked, 1):
            bar = "█" * int(score * 30)
            print(f"  {i}. [{score:.3f}] {bar}")
            print(f"     {candidate}\n")
        
        print("✅ Batch similarity réussi !")
        
        return True
    
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return False


def main():
    """Lancer tous les tests."""
    print("\n" + "🧪 " + "="*58)
    print("🧪  TESTS DE L'EMBEDDER - JOUR 4A")
    print("🧪 " + "="*58)
    
    tests = [
        test_1_initialisation,
        test_2_encode_simple,
        test_3_encode_batch,
        test_4_similarity,
        test_5_batch_similarity,
    ]
    
    results = []
    
    for test in tests:
        result = test()
        results.append(result)
        
        if not result:
            print(f"\n⚠️  Le test {test.__name__} a échoué.")
            print("💡 Vérifiez l'installation : pip install sentence-transformers\n")
            break
    
    print("\n" + "="*60)
    print(f"📊 RÉSULTAT : {sum(results)}/{len(tests)} tests réussis")
    print("="*60)
    
    if all(results):
        print("\n🎉 Tous les tests sont passés !")
        print("✅ L'Embedder est opérationnel !")
        print("\n💡 Prochaine étape : Implémenter l'Indexer")
    else:
        print("\n⚠️  Certains tests ont échoué.")
        print("💡 Résolvez les erreurs avant de continuer.")


if __name__ == "__main__":
    main()