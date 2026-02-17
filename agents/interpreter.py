import json
import os
from openai import OpenAI
from agents.prompts import INTERPRETER_SYSTEM_PROMPT, INTERPRETER_FEW_SHOT
from utils import get_schema_info

class QueryInterpreterAgent:
    def __init__(self):
        api_key = os.environ.get("OPENAI_API_KEY")
        if api_key:
            self.client = OpenAI(api_key=api_key)
            self.mock_mode = False
        else:
            self.client = None
            self.mock_mode = True
            print("Warning: OPENAI_API_KEY not found. Running in MOCK mode.")
        
        self.schema_info = get_schema_info()

    def interpret(self, user_text):
        """
        Converts natural language to a structured plan with SQL.
        """
        if self.mock_mode:
            return self._mock_interpret(user_text)

        schema_json = json.dumps(self.schema_info, indent=2)
        # ... (rest of real implementation) ...
        # For brevity, I'll just return the mock if key is missing, 
        # but I should keep the real code if I had the key. 
        # Since I'm replacing the whole method or class, I need to be careful.
        
        # Let's just implement the real call here too for completeness if I were to paste it all back
        # but for this specific tool call I'll just handle the mock logic.
        
        system_prompt = INTERPRETER_SYSTEM_PROMPT.format(schema_json=schema_json)
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": INTERPRETER_FEW_SHOT},
            {"role": "user", "content": f"User request: {user_text}"}
        ]

        try:
            response = self.client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=messages,
                temperature=0,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content
            plan = json.loads(content)
            self._validate_schema_refs(plan.get("schema_refs", []))
            return plan
        except Exception as e:
            return {"error": str(e)}

    def _mock_interpret(self, text):
        # Simple rule-based mock for demo purposes
        text = text.lower()
        if "potassium" in text:
            return {
                "intent": "SELECT",
                "sql_template": "SELECT l.test_name, l.value, l.units FROM labs l JOIN encounters e ON l.encounter_id = e.id JOIN patients p ON e.patient_id = p.id WHERE p.mrn = :mrn AND l.test_name = 'Potassium'",
                "params": {"mrn": "MRN001"},
                "explanation": "Mock: Retrieving potassium labs.",
                "schema_refs": ["labs.test_name", "labs.value", "patients.mrn"]
            }
        elif "add aspirin" in text:
            return {
                "intent": "INSERT",
                "sql_template": "INSERT INTO meds (patient_id, drug_name, dose, start_date) VALUES (:patient_id, 'Aspirin', '75mg', :start_date)",
                "params": {"patient_id": 1, "start_date": "2023-10-01"},
                "explanation": "Mock: Adding aspirin order.",
                "schema_refs": ["meds.drug_name"]
            }
        elif "delete" in text:
             return {
                "intent": "DELETE",
                "sql_template": "DELETE FROM patients",
                "params": {},
                "explanation": "Mock: Deleting patients.",
                "schema_refs": ["patients.id"]
            }
        return {
            "intent": "SELECT",
            "sql_template": "SELECT * FROM patients LIMIT 5",
            "params": {},
            "explanation": "Mock: Default select.",
            "schema_refs": ["patients.id"]
        }

    def _validate_schema_refs(self, refs):
        # TODO: Implement strict checking against self.schema_info
        pass
