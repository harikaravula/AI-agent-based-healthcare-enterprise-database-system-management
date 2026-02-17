import unittest
from unittest.mock import MagicMock, patch
from agents.analytics import AnalyticsAgent

class TestAnalyticsAgent(unittest.TestCase):
    def setUp(self):
        self.mock_interpreter = MagicMock()
        self.mock_validator = MagicMock()
        self.agent = AnalyticsAgent(self.mock_interpreter, self.mock_validator)

    def test_run_analytics_autonomous_success(self):
        # Mock interpreter
        self.mock_interpreter.interpret.return_value = {
            "intent": "SELECT",
            "sql_template": "SELECT * FROM patients",
            "params": {}
        }
        
        # Mock validator
        self.mock_validator.validate.return_value = {"approved": True}
        
        # Mock DB execution
        with patch('agents.analytics.db') as mock_db:
            mock_conn = MagicMock()
            mock_db.engine.connect.return_value.__enter__.return_value = mock_conn
            
            with patch('agents.analytics.pd.read_sql') as mock_read_sql:
                mock_df = MagicMock()
                mock_df.to_dict.return_value = [{"id": 1}]
                mock_df.__len__.return_value = 1
                mock_read_sql.return_value = mock_df
                
                result = self.agent.run_analytics("analyst", "Show patients")
                
                self.assertEqual(result['status'], 'success')
                self.assertEqual(len(result['data']), 1)

    def test_run_analytics_blocked_validation(self):
        self.mock_interpreter.interpret.return_value = {"intent": "SELECT"}
        self.mock_validator.validate.return_value = {"approved": False, "messages": ["Blocked"]}
        
        result = self.agent.run_analytics("analyst", "Show patients")
        
        self.assertEqual(result['status'], 'blocked')
        self.assertIn("Blocked", result['validation']['messages'])

    def test_run_analytics_blocked_unsafe_intent(self):
        self.mock_interpreter.interpret.return_value = {"intent": "DELETE"} # Not allowed in analytics
        self.mock_validator.validate.return_value = {"approved": True}
        
        result = self.agent.run_analytics("analyst", "Delete patients")
        
        self.assertEqual(result['status'], 'blocked')
        self.assertIn("read-only operations", result['message'])

if __name__ == '__main__':
    unittest.main()
