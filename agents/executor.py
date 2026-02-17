from database import db, AuditLog
from sqlalchemy import text
import json
from datetime import datetime

class DatabaseUpdateAgent:
    def __init__(self, interpreter_agent, validator_agent):
        self.interpreter = interpreter_agent
        self.validator = validator_agent

    def execute(self, user_id, user_role, user_text, dry_run=True, justification=None):
        """
        Executes a write operation.
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

        intent = plan.get('intent', '').upper()
        sql_template = plan.get('sql_template')
        params = plan.get('params', {})
        schema_refs = plan.get('schema_refs', [])

        # 3. Dry Run
        if dry_run:
            return self._dry_run(intent, sql_template, params)

        # 4. Execute (Write)
        if intent not in ['INSERT', 'UPDATE', 'DELETE']:
            return {"error": "Only write operations are supported by this agent."}

        try:
            with db.session.begin():
                # Capture pre-state (optional, simplified)
                pre_state = None 
                
                # Execute
                result = db.session.execute(text(sql_template), params)
                rows_affected = result.rowcount
                
                # Audit Log
                audit_entry = AuditLog(
                    user_id=user_id, # Assuming user_id is passed or resolved
                    action_type=intent,
                    affected_table=self._extract_table(schema_refs),
                    affected_row_ids=str(rows_affected), # Simplified
                    sql_executed=sql_template,
                    pre_state=json.dumps(pre_state) if pre_state else None,
                    validation_summary=json.dumps(validation),
                    justification=justification
                )
                db.session.add(audit_entry)
            
            return {
                "status": "success",
                "rows_affected": rows_affected,
                "audit_id": audit_entry.audit_id
            }
            
        except Exception as e:
            return {"error": str(e)}

    def _dry_run(self, intent, sql, params):
        # For dry run, we can try to estimate impact.
        # For DELETE/UPDATE, we can run a SELECT COUNT(*) with the same WHERE clause.
        # This requires parsing the WHERE clause which is hard without a parser.
        # Simplified: Just return the plan and say it's a dry run.
        
        return {
            "status": "dry_run",
            "sql_planned": sql,
            "params": params,
            "message": "Dry run successful. Review SQL before execution."
        }

    def _extract_table(self, schema_refs):
        if not schema_refs: return "unknown"
        # Return the first table referenced
        return schema_refs[0].split('.')[0] if '.' in schema_refs[0] else "unknown"

    def get_audit_logs(self, limit=50):
        logs = AuditLog.query.order_by(AuditLog.ts.desc()).limit(limit).all()
        return [{
            "audit_id": l.audit_id,
            "ts": l.ts,
            "action": l.action_type,
            "table": l.affected_table,
            "sql": l.sql_executed,
            "justification": l.justification
        } for l in logs]
