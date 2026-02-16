"""
Tester l'impact de la température sur la génération.
"""
import sys
sys.path.insert(0, '.')

from agent_ia import get_client

def test_temperatures():
    """Comparer les générations avec différentes températures."""
    
    client = get_client()
    prompt = "Write a Python function to reverse a string. Only code."
    
    temperatures = [0.0, 0.5, 1.0]
    
    for temp in temperatures:
        print(f"\n{'='*60}")
        print(f"Temperature = {temp}")
        print('='*60)
        
        code = client.generate(prompt, temperature=temp, num_predict=150)
        print(code)

if __name__ == "__main__":
    test_temperatures()