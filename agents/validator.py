import yaml
import os

class ValidationRulesAgent:
    def __init__(self, policy_path='policies.yaml'):
        self.policy_path = policy_path
        self.policies = self._load_policies()

    def _load_policies(self):
        if not os.path.exists(self.policy_path):
            return {}
        with open(self.policy_path, 'r') as f:
            return yaml.safe_load(f)

    def validate(self, user_role, plan):
        """
        Validates the plan against policies.
        """
        intent = plan.get('intent', '').upper()
        schema_refs = plan.get('schema_refs', [])
        sql_template = plan.get('sql_template', '').upper()
        
        messages = []
        approved = True
        risk_level = "low"
        suggested_fixes = []

        # 1. RBAC Check
        if not self._check_rbac(user_role, intent, schema_refs):
            approved = False
            messages.append(f"Role '{user_role}' is not authorized to perform '{intent}' on referenced tables.")

        # 2. Safety Rules (WHERE clause check)
        if intent in ['UPDATE', 'DELETE'] and 'WHERE' not in sql_template:
            approved = False
            messages.append("Hard Rule Violation: DELETE/UPDATE must have a WHERE clause.")
            suggested_fixes.append("Add a WHERE clause to restrict the operation.")

        # 3. PII Check (Warning only for now, or masking instruction)
        pii_warnings = self._check_pii(schema_refs)
        if pii_warnings:
            messages.extend(pii_warnings)
            # Could escalate risk level
        
        # 4. High Risk Operations (Heuristic)
        if intent == 'DELETE':
            risk_level = "high"
            messages.append("DELETE operations are high risk and require approval.")
        
        return {
            "approved": approved,
            "messages": messages,
            "risk_level": risk_level,
            "suggested_fixes": suggested_fixes
        }

    def _check_rbac(self, role, intent, schema_refs):
        role_policy = self.policies.get('roles', {}).get(role, {})
        allowed_ops = role_policy.get('allow', {})
        
        # Map intent to op type (select, insert, update, delete)
        op_type = intent.lower()
        if op_type == 'aggregate': op_type = 'select' # Treat aggregate as select for now
        
        allowed_tables = allowed_ops.get(op_type, [])
        
        if "*" in allowed_tables:
            return True
            
        # Check if all referenced tables are in allowed_tables
        # schema_refs are like "table.column"
        referenced_tables = set()
        for ref in schema_refs:
            if "." in ref:
                table = ref.split(".")[0]
                referenced_tables.add(table)
        
        for table in referenced_tables:
            if table not in allowed_tables:
                return False
                
        return True

    def _check_pii(self, schema_refs):
        warnings = []
        pii_policy = self.policies.get('pii_fields', {})
        
        for ref in schema_refs:
            if "." in ref:
                table, col = ref.split(".")
                if table in pii_policy and col in pii_policy[table]:
                    warnings.append(f"Warning: Accessing PII field '{ref}'. Ensure this is necessary.")
        return warnings

    def update_policies(self, new_policies_yaml):
        try:
            # Validate YAML format
            yaml.safe_load(new_policies_yaml)
            with open(self.policy_path, 'w') as f:
                f.write(new_policies_yaml)
            self.policies = self._load_policies()
            return True, "Policies updated successfully."
        except Exception as e:
            return False, str(e)
