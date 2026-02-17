import pandas as pd
from database import db
from sqlalchemy import text

class AnalyticsAgent:
    def __init__(self, interpreter_agent, validator_agent):
        self.interpreter = interpreter_agent
        self.validator = validator_agent

    def run_analytics(self, user_role, user_text, mode='autonomous'):
        """
        Runs analytics based on user text.
        Mode: 'autonomous' (executes if safe) or 'assisted' (returns candidates).
        """
        # 1. Interpret
        plan = self.interpreter.interpret(user_text)
        if 'error' in plan:
            return {"error": plan['error']}

        # 2. Validate
        validation = self.validator.validate(user_role, plan)
        if not validation['approved']:
            return {
                "status": "blocked",
                "validation": validation,
                "plan": plan
            }

        # 3. Execute (if autonomous and read-only)
        intent = plan.get('intent', '').upper()
        if mode == 'autonomous':
            if intent not in ['SELECT', 'AGGREGATE']:
                 return {
                    "status": "blocked",
                    "message": "Analytics agent only supports read-only operations in autonomous mode.",
                    "plan": plan
                }
            
            try:
                sql = plan.get('sql_template')
                params = plan.get('params', {})
                
                # Execute using pandas for dataframe result
                # Note: params handling with pandas read_sql might vary, using sqlalchemy text
                with db.engine.connect() as conn:
                    df = pd.read_sql(text(sql), conn, params=params)
                
                return {
                    "status": "success",
                    "data": df.to_dict(orient='records'),
                    "summary": f"Retrieved {len(df)} rows.",
                    "sql_executed": sql
                }
            except Exception as e:
                return {"error": str(e)}
        
        else: # Assisted mode
            return {
                "status": "candidates",
                "candidates": [plan], # In real system, generate multiple
                "message": "Please review the candidate query."
            }
