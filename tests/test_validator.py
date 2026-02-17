import unittest
from agents.validator import ValidationRulesAgent
import os

class TestValidationRulesAgent(unittest.TestCase):
    def setUp(self):
        # Create a temporary policy file
        self.test_policy = """
roles:
  clinician:
    allow:
      select: ["patients"]
  admin:
    allow:
      delete: ["patients"]
pii_fields:
  patients: ["mrn"]
high_risk:
  - op: "DELETE"
    condition: "rows_affected > 0"
    require_approval: true
"""
        with open('test_policies.yaml', 'w') as f:
            f.write(self.test_policy)
        self.agent = ValidationRulesAgent('test_policies.yaml')

    def tearDown(self):
        if os.path.exists('test_policies.yaml'):
            os.remove('test_policies.yaml')

    def test_validate_allowed(self):
        plan = {
            "intent": "SELECT",
            "schema_refs": ["patients.id"],
            "sql_template": "SELECT * FROM patients"
        }
        result = self.agent.validate("clinician", plan)
        self.assertTrue(result['approved'])

    def test_validate_denied_rbac(self):
        plan = {
            "intent": "SELECT",
            "schema_refs": ["meds.id"], # Meds not allowed for clinician in test policy
            "sql_template": "SELECT * FROM meds"
        }
        result = self.agent.validate("clinician", plan)
        self.assertFalse(result['approved'])
        self.assertIn("Role 'clinician' is not authorized", result['messages'][0])

    def test_validate_pii_warning(self):
        plan = {
            "intent": "SELECT",
            "schema_refs": ["patients.mrn"],
            "sql_template": "SELECT mrn FROM patients"
        }
        result = self.agent.validate("clinician", plan)
        self.assertTrue(result['approved']) # Soft fail
        self.assertTrue(any("PII field" in msg for msg in result['messages']))

    def test_validate_unsafe_delete(self):
        plan = {
            "intent": "DELETE",
            "schema_refs": ["patients.id"],
            "sql_template": "DELETE FROM patients" # No WHERE
        }
        # Assuming admin allows delete, but safety rule blocks no-where
        # But wait, my test policy doesn't have safety_rules defined, so it defaults to empty?
        # The agent code checks hardcoded safety rules? No, it checks logic.
        # Let's see validator.py logic.
        # It checks: if intent in ['UPDATE', 'DELETE'] and 'WHERE' not in sql_template:
        
        result = self.agent.validate("admin", plan)
        self.assertFalse(result['approved'])
        self.assertIn("must have a WHERE clause", result['messages'][0])

if __name__ == '__main__':
    unittest.main()
