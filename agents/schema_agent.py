"""
Schema Generation Agent

Analyzes parsed data and requirements to generate optimal database schema.
Implements DS-STAR's iterative planning approach with:
- Planner: Create high-level schema design
- Coder: Generate SQLAlchemy model code
- Verifier: Validate schema design
- Router: Determine if refinement is needed
"""

import os
import openai
from typing import Dict, List, Any, Optional, Callable
import json

class SchemaAgent:
    """
    Agent that generates database schemas from file analysis and requirements.
    Uses iterative refinement with LLM-based verification.
    """
    
    def __init__(self, model: str = "gpt-4o-mini", max_rounds: int = 10):
        """
        Initialize the schema agent.
        
        Args:
            model: OpenAI model to use (default: gpt-4o-mini)
            max_rounds: Maximum refinement rounds (default: 10)
        """
        self.client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = model
        self.max_rounds = max_rounds
        self.temperature = 0.1  # Low temperature for consistent output
    
    def generate_schema(
        self,
        file_analysis: Dict[str, Any],
        requirements: str,
        existing_schema: Optional[Dict] = None,
        progress_callback: Optional[Callable[[Dict], None]] = None
    ) -> Dict[str, Any]:
        """
        Generate database schema using iterative refinement.
        
        Args:
            file_analysis: Output from FileAnalyzer
            requirements: User's natural language requirements
            existing_schema: Optional existing schema for merging
            progress_callback: Callback function for progress updates
            
        Returns:
            Dictionary containing schema and verification details
        """
        refinement_history = []
        current_plan = None
        
        # Initial planning
        for round_num in range(1, self.max_rounds + 1):
            print(f"\n=== Schema Generation Round {round_num}/{self.max_rounds} ===")
            
            # Notify progress
            if progress_callback:
                progress_dict = {
                    "round": round_num,
                    "max_rounds": self.max_rounds,
                    "stage": "planning" if round_num == 1 else "refining",
                    "message": f"Round {round_num}/{self.max_rounds}: Analyzing and planning..."
                }
                progress_callback(progress_dict)
            
            # Step 1: Planner - Create or refine plan
            if current_plan is None:
                # Initial plan
                current_plan = self._create_initial_plan(file_analysis, requirements)
            else:
                # Refine based on verification feedback
                last_verification = refinement_history[-1]["verification"]
                current_plan = self._refine_plan(
                    current_plan,
                    last_verification,
                    file_analysis,
                    requirements
                )
            
            refinement_history.append({
                "round": round_num,
                "plan": current_plan,
                "verification": None,
                "action": None
            })
            
            # Step 2: Coder - Generate SQLAlchemy code
            if progress_callback:
                progress_callback({
                    "round": round_num,
                    "max_rounds": self.max_rounds,
                    "stage": "generating_code",
                    "message": f"Round {round_num}/{self.max_rounds}: Generating SQLAlchemy code..."
                })
                
            schema_code = self._generate_code(current_plan, file_analysis)
            
            # Step 3: Verifier - Validate schema
            if progress_callback:
                progress_callback({
                    "round": round_num,
                    "max_rounds": self.max_rounds,
                    "stage": "verifying",
                    "message": f"Round {round_num}/{self.max_rounds}: Verifying schema validity..."
                })
                
            verification = self._verify_schema(schema_code, current_plan, file_analysis, requirements)
            refinement_history[-1]["verification"] = verification
            
            # Step 4: Router - Decide next action
            if verification["is_sufficient"]:
                print(f"✓ Schema verified successfully in round {round_num}")
                break
            
            action = self._route_next_action(verification)
            refinement_history[-1]["action"] = action
            print(f"→ Action: {action['type']} - {action['reason']}")
            
            # Report failure action for next iteration display
            if progress_callback:
                short_reason = action['reason'][:50] + "..." if len(action['reason']) > 50 else action['reason']
                progress_callback({
                    "round": round_num,
                    "max_rounds": self.max_rounds,
                    "stage": "refining",
                    "message": f"Refining: {short_reason}"
                })
            
            if round_num == self.max_rounds:
                print(f"⚠ Maximum rounds reached, using current schema")
        
        # Generate final results
        final_schema = self._generate_code(current_plan, file_analysis)
        description = self._generate_description(current_plan, file_analysis)
        
        return {
            "schema_code": final_schema,
            "schema_description": description,
            "verification_status": verification["is_sufficient"],
            "warnings": verification.get("warnings", []),
            "relationships_detected": current_plan.get("relationships", []),
            "refinement_history": refinement_history,
            "rounds_taken": len(refinement_history)
        }
    
    def _create_initial_plan(self, file_analysis: Dict, requirements: str) -> Dict[str, Any]:
        """Create initial high-level schema plan."""
        
        prompt = f"""You are a database schema design expert. Based on the file analysis and requirements below, create a high-level database schema plan.

FILE ANALYSIS:
{file_analysis['natural_language_summary']}

Suggested Primary Keys:
{json.dumps(file_analysis.get('suggested_primary_keys', {}), indent=2)}

Potential Relationships:
{json.dumps(file_analysis.get('potential_relationships', []), indent=2)}

USER REQUIREMENTS:
{requirements}

Create a schema plan with:
1. List of tables (entities) with their purpose
2. Columns for each table with data types
3. Primary keys
4. Foreign key relationships
5. Constraints (unique, not null, etc.)
6. Indexes for optimization

Return your plan as a JSON object with this structure:
{{
    "tables": [
        {{
            "name": "table_name",
            "purpose": "description",
            "columns": [
                {{
                    "name": "column_name",
                    "type": "sqlalchemy_type",
                    "nullable": true/false,
                    "unique": true/false,
                    "primary_key": true/false,
                    "foreign_key": "referenced_table.column" or null
                }}
            ],
            "indexes": ["column_name"]
        }}
    ],
    "relationships": [
        {{
            "from_table": "table1",
            "from_column": "column",
            "to_table": "table2",
            "to_column": "column",
            "relationship_type": "one-to-many"|"many-to-many"|"one-to-one"
        }}
    ]
}}

Use SQLAlchemy types: Integer, String(length), Float, Boolean, Date, DateTime, Text"""

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a database schema design expert. Always return valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.temperature,
            response_format={"type": "json_object"}
        )
        
        plan = json.loads(response.choices[0].message.content)
        return plan
    
    def _refine_plan(
        self,
        current_plan: Dict,
        verification_feedback: Dict,
        file_analysis: Dict,
        requirements: str
    ) -> Dict[str, Any]:
        """Refine the plan based on verification feedback."""
        
        prompt = f"""You are refining a database schema plan based on verification feedback.

CURRENT PLAN:
{json.dumps(current_plan, indent=2)}

VERIFICATION FEEDBACK:
{json.dumps(verification_feedback, indent=2)}

ORIGINAL FILE ANALYSIS:
{file_analysis['natural_language_summary']}

USER REQUIREMENTS:
{requirements}

Based on the issues identified, refine the schema plan. Address:
- Missing primary keys
- Invalid foreign key relationships
- Data type mismatches
- Missing constraints
- Any other issues mentioned in the feedback

Return the refined plan using the same JSON structure as before."""

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a database schema design expert. Always return valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.temperature,
            response_format={"type": "json_object"}
        )
        
        refined_plan = json.loads(response.choices[0].message.content)
        return refined_plan
    
    def _generate_code(self, plan: Dict, file_analysis: Dict) -> str:
        """Generate SQLAlchemy model code from plan."""
        
        prompt = f"""Generate clean, production-ready SQLAlchemy model code based on this schema plan.

SCHEMA PLAN:
{json.dumps(plan, indent=2)}

Requirements:
1. Import necessary modules (from flask_sqlalchemy import SQLAlchemy, from datetime import datetime, etc.)
2. Assume db = SQLAlchemy() exists
3. Create model classes that inherit from db.Model
4. Include __tablename__ for each model
5. Define all columns with appropriate types and constraints
6. Define relationships using db.relationship() with proper backrefs
7. Add helpful __repr__ methods
8. Include comments explaining complex relationships
9. Follow SQLAlchemy best practices

Return ONLY the Python code, no markdown formatting or explanations."""

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a Python and SQLAlchemy expert. Return only valid Python code."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.temperature
        )
        
        code = response.choices[0].message.content
        # Remove markdown code blocks if present
        if code.startswith("```python"):
            code = code.split("```python")[1].split("```")[0].strip()
        elif code.startswith("```"):
            code = code.split("```")[1].split("```")[0].strip()
        
        return code
    
    def _verify_schema(
        self,
        schema_code: str,
        plan: Dict,
        file_analysis: Dict,
        requirements: str
    ) -> Dict[str, Any]:
        """Verify the schema design using LLM as judge."""
        
        prompt = f"""You are a database schema verification expert. Verify if this schema is sufficient and correct.

GENERATED CODE:
```python
{schema_code}
```

ORIGINAL PLAN:
{json.dumps(plan, indent=2)}

FILE ANALYSIS:
{file_analysis['natural_language_summary']}

USER REQUIREMENTS:
{requirements}

Verify the following:
1. All tables have primary keys
2. Foreign key relationships are valid
3. Data types match the source data
4. Constraints are appropriate
5. Requirements are satisfied
6. No obvious design flaws

Return a JSON object:
{{
    "is_sufficient": true/false,
    "issues": [
        {{
            "severity": "critical"|"warning"|"info",
            "category": "primary_key"|"foreign_key"|"data_type"|"constraint"|"requirement"|"design",
            "description": "detailed issue description",
            "suggestion": "how to fix this"
        }}
    ],
    "warnings": ["warning messages"],
    "passed_checks": ["list of checks that passed"]
}}"""

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a database schema verification expert. Always return valid JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=self.temperature,
            response_format={"type": "json_object"}
        )
        
        verification = json.loads(response.choices[0].message.content)
        
        # Schema is sufficient if no critical issues
        if not verification.get("is_sufficient"):
            critical_issues = [i for i in verification.get("issues", []) if i.get("severity") == "critical"]
            verification["is_sufficient"] = len(critical_issues) == 0
        
        return verification
    
    def _route_next_action(self, verification: Dict) -> Dict[str, str]:
        """Determine the next action based on verification results."""
        
        issues = verification.get("issues", [])
        
        if not issues:
            return {"type": "complete", "reason": "No issues found"}
        
        # Prioritize critical issues
        critical = [i for i in issues if i.get("severity") == "critical"]
        if critical:
            return {
                "type": "refine",
                "reason": f"Fix critical issue: {critical[0]['description']}"
            }
        
        # Then warnings
        warnings = [i for i in issues if i.get("severity") == "warning"]
        if warnings:
            return {
                "type": "refine",
                "reason": f"Address warning: {warnings[0]['description']}"
            }
        
        return {"type": "complete", "reason": "Only minor issues remain"}
    
    def _generate_description(self, plan: Dict, file_analysis: Dict) -> str:
        """Generate natural language description of the schema."""
        
        lines = ["# Database Schema Description\n"]
        
        lines.append(f"This schema contains {len(plan['tables'])} table(s):\n")
        
        for table in plan['tables']:
            lines.append(f"\n## {table['name']}")
            lines.append(f"{table.get('purpose', 'No description')}\n")
            lines.append(f"**Columns ({len(table['columns'])}):**")
            
            for col in table['columns']:
                flags = []
                if col.get('primary_key'):
                    flags.append("PK")
                if col.get('unique'):
                    flags.append("UNIQUE")
                if not col.get('nullable', True):
                    flags.append("NOT NULL")
                if col.get('foreign_key'):
                    flags.append(f"FK → {col['foreign_key']}")
                
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                lines.append(f"- `{col['name']}` ({col['type']}){flag_str}")
        
        if plan.get('relationships'):
            lines.append("\n## Relationships\n")
            for rel in plan['relationships']:
                lines.append(
                    f"- {rel['from_table']}.{rel['from_column']} → "
                    f"{rel['to_table']}.{rel['to_column']} "
                    f"({rel.get('relationship_type', 'unknown')})"
                )
        
        return "\n".join(lines)
