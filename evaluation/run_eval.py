import json
import requests
import sys

def run_eval():
    try:
        with open('evaluation/gold_corpus.json', 'r') as f:
            corpus = json.load(f)
    except FileNotFoundError:
        print("Corpus not found.")
        return

    results = []
    
    print(f"Running evaluation on {len(corpus)} samples...")
    
    for sample in corpus:
        text = sample['text']
        print(f"Processing: {text}")
        
        try:
            # Call local API
            res = requests.post('http://127.0.0.1:5000/interpret', json={"text": text})
            if res.status_code != 200:
                print(f"Error: {res.text}")
                results.append({"id": sample['id'], "status": "error", "error": res.text})
                continue
                
            plan = res.json()
            
            # Check intent
            intent_match = plan.get('intent') == sample['expected_intent']
            
            # Check tables (simplified)
            schema_refs = plan.get('schema_refs', [])
            referenced_tables = set([r.split('.')[0] for r in schema_refs if '.' in r])
            expected_tables = set(sample['expected_tables'])
            tables_match = expected_tables.issubset(referenced_tables) # At least expected are there
            
            results.append({
                "id": sample['id'],
                "status": "success",
                "intent_match": intent_match,
                "tables_match": tables_match,
                "plan": plan
            })
            
        except Exception as e:
            print(f"Exception: {e}")
            results.append({"id": sample['id'], "status": "exception", "error": str(e)})

    # Summary
    total = len(results)
    intent_correct = sum(1 for r in results if r.get('intent_match'))
    print(f"\nResults: {intent_correct}/{total} intent accuracy.")
    
    with open('evaluation/results.json', 'w') as f:
        json.dump(results, f, indent=2)

if __name__ == '__main__':
    run_eval()
