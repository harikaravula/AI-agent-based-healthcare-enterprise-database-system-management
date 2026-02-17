import unittest
from unittest.mock import MagicMock, patch
from agents.interpreter import QueryInterpreterAgent

class TestQueryInterpreterAgent(unittest.TestCase):
    @patch('agents.interpreter.OpenAI')
    @patch('agents.interpreter.get_schema_info')
    def test_interpret_success(self, mock_get_schema, mock_openai):
        # Mock schema
        mock_get_schema.return_value = {"tables": {"patients": {"columns": {"mrn": "text"}}}}
        
        # Mock OpenAI response
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '''
        {
            "intent": "SELECT",
            "sql_template": "SELECT * FROM patients WHERE mrn = :mrn",
            "params_schema": {"mrn": "text"},
            "params": {"mrn": "123"},
            "explanation": "Test explanation",
            "schema_refs": ["patients.mrn"]
        }
        '''
        mock_client.chat.completions.create.return_value = mock_response
        
        agent = QueryInterpreterAgent()
        result = agent.interpret("Show patient 123")
        
        self.assertEqual(result['intent'], 'SELECT')
        self.assertEqual(result['params']['mrn'], '123')
        self.assertIn('patients.mrn', result['schema_refs'])

    @patch('agents.interpreter.OpenAI')
    def test_interpret_error(self, mock_openai):
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_client.chat.completions.create.side_effect = Exception("API Error")
        
        agent = QueryInterpreterAgent()
        result = agent.interpret("Show patient 123")
        
        self.assertIn('error', result)
        self.assertEqual(result['error'], 'API Error')

if __name__ == '__main__':
    unittest.main()
