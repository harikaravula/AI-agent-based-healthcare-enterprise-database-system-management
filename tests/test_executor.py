import unittest
from unittest.mock import MagicMock, patch
from agents.executor import DatabaseUpdateAgent

class TestDatabaseUpdateAgent(unittest.TestCase):
    def setUp(self):
        self.mock_interpreter = MagicMock()
        self.mock_validator = MagicMock()
        self.agent = DatabaseUpdateAgent(self.mock_interpreter, self.mock_validator)

    def test_execute_dry_run(self):
        self.mock_interpreter.interpret.return_value = {
            "intent": "UPDATE",
            "sql_template": "UPDATE meds SET dose='100mg' WHERE id=:id",
            "params": {"id": 1}
        }
        self.mock_validator.validate.return_value = {"approved": True}
        
        result = self.agent.execute(1, "clinician", "Update med 1", dry_run=True)
        
        self.assertEqual(result['status'], 'dry_run')
        self.assertIn("Review SQL", result['message'])

    def test_execute_write_success(self):
        self.mock_interpreter.interpret.return_value = {
            "intent": "UPDATE",
            "sql_template": "UPDATE meds SET dose='100mg' WHERE id=:id",
            "params": {"id": 1},
            "schema_refs": ["meds.dose"]
        }
        self.mock_validator.validate.return_value = {"approved": True}
        
        with patch('agents.executor.db.session') as mock_session:
            mock_result = MagicMock()
            mock_result.rowcount = 1
            mock_session.execute.return_value = mock_result
            
            # Mock AuditLog to avoid actual DB model creation issues if any
            with patch('agents.executor.AuditLog') as mock_audit_log:
                mock_entry = MagicMock()
                mock_entry.audit_id = 100
                mock_audit_log.return_value = mock_entry
                
                result = self.agent.execute(1, "clinician", "Update med 1", dry_run=False)
                
                self.assertEqual(result['status'], 'success')
                self.assertEqual(result['rows_affected'], 1)
                mock_session.add.assert_called() # Audit log added

    def test_execute_blocked_validation(self):
        self.mock_interpreter.interpret.return_value = {"intent": "DELETE"}
        self.mock_validator.validate.return_value = {"approved": False}
        
        result = self.agent.execute(1, "clinician", "Delete everything", dry_run=False)
        
        self.assertEqual(result['status'], 'blocked')

if __name__ == '__main__':
    unittest.main()
