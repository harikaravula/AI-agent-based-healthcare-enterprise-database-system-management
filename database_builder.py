"""
Database Builder Module

Executes schema generation and data population.
Handles dynamic model creation, validation, and bulk insertion.
"""

import os
import sqlite3
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, date
import json
from pathlib import Path

class DatabaseBuilder:
    """Builds databases from dynamically generated schemas."""
    
    def __init__(self, db_directory: str = "./instance"):
        """
        Initialize the database builder.
        
        Args:
            db_directory: Directory to store database files
        """
        self.db_directory = Path(db_directory)
        self.db_directory.mkdir(parents=True, exist_ok=True)
    
    def create_database_from_schema(
        self,
        schema_code: str,
        db_name: str,
        parsed_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Create database from schema code and populate with data.
        
        Args:
            schema_code: Python code defining SQLAlchemy models
            db_name: Name for the database file
            parsed_data: List of parsed file data
            
        Returns:
            Dictionary with status, counts, and errors
        """
        result = {
            "success": False,
            "db_path": None,
            "tables_created": 0,
            "rows_inserted": {},
            "errors": [],
            "warnings": []
        }
        
        db_path = self.db_directory / f"{db_name}.db"
        result["db_path"] = str(db_path)
        
        try:
            # Extract model definitions from schema code
            models = self._extract_models_from_code(schema_code)
            
            # Create database and tables using raw SQL
            # (Since we can't dynamically execute Python to create SQLAlchemy models safely)
            create_table_statements = self._generate_create_table_sql(schema_code)
            
            # Create database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Create tables
            for table_sql in create_table_statements:
                try:
                    cursor.execute(table_sql)
                    result["tables_created"] += 1
                except sqlite3.Error as e:
                    result["errors"].append(f"Error creating table: {str(e)}")
            
            conn.commit()
            
            # Populate tables with data
            for file_data in parsed_data:
                for table_info in file_data["tables"]:
                    table_name = table_info["name"]
                    
                    # Insert data
                    inserted, errors = self._insert_data(
                        conn,
                        table_name,
                        table_info["data"],
                        table_info["columns"]
                    )
                    
                    result["rows_inserted"][table_name] = inserted
                    if errors:
                        result["warnings"].extend(errors)
            
            conn.commit()
            conn.close()
            
            result["success"] = True
            
        except Exception as e:
            result["errors"].append(f"Database creation failed: {str(e)}")
        
        return result
    
    def _extract_models_from_code(self, schema_code: str) -> List[str]:
        """Extract model class names from schema code."""
        models = []
        for line in schema_code.split('\n'):
            line = line.strip()
            if line.startswith('class ') and '(db.Model)' in line:
                model_name = line.split('class ')[1].split('(')[0].strip()
                models.append(model_name)
        return models
    
    def _generate_create_table_sql(self, schema_code: str) -> List[str]:
        """
        Generate CREATE TABLE SQL statements from SQLAlchemy model code.
        This is a simplified parser - for production, consider using SQLAlchemy properly.
        """
        statements = []
        
        # Parse schema code to extract table definitions
        lines = schema_code.split('\n')
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith('class ') and '(db.Model)' in line:
                # Found a model class
                model_name = line.split('class ')[1].split('(')[0].strip()
                table_name = None
                columns = []
                
                # Look for __tablename__ and columns
                i += 1
                while i < len(lines):
                    line = lines[i].strip()
                    
                    if line.startswith('__tablename__'):
                        table_name = line.split('=')[1].strip().strip("'\"")
                    
                    elif '= db.Column(' in line:
                        # Parse column definition
                        col_name = line.split('=')[0].strip()
                        col_def = self._parse_column_definition(line)
                        if col_def:
                            columns.append((col_name, col_def))
                    
                    elif line.startswith('class ') or (line and not line.startswith('#') and not '=' in line and not line.startswith('def')):
                        # End of  current class
                        break
                    
                    i += 1
                
                if table_name and columns:
                    sql = self._build_create_table_sql(table_name, columns)
                    statements.append(sql)
                
                continue
            
            i += 1
        
        return statements
    
    def _parse_column_definition(self, line: str) -> Optional[str]:
        """Parse a SQLAlchemy column definition to SQL."""
        try:
            # Extract column type and constraints
            if 'db.Integer' in line:
                col_type = 'INTEGER'
            elif 'db.String' in line:
                col_type = 'TEXT'
            elif 'db.Float' in line:
                col_type = 'REAL'
            elif 'db.Boolean' in line:
                col_type = 'INTEGER'  # SQLite uses INTEGER for boolean
            elif 'db.Date' in line:
                col_type = 'DATE'
            elif 'db.DateTime' in line:
                col_type = 'DATETIME'
            elif 'db.Text' in line:
                col_type = 'TEXT'
            else:
                col_type = 'TEXT'  # Default
            
            constraints = []
            
            if 'primary_key=True' in line:
                constraints.append('PRIMARY KEY')
            if 'unique=True' in line:
                constraints.append('UNIQUE')
            if 'nullable=False' in line:
                constraints.append('NOT NULL')
            if 'autoincrement=True' in line or ('primary_key=True' in line and 'Integer' in line):
                constraints.append('AUTOINCREMENT')
            
            sql_def = col_type
            if constraints:
                sql_def += ' ' + ' '.join(constraints)
            
            return sql_def
        
        except Exception:
            return None
    
    def _build_create_table_sql(self, table_name: str, columns: List[Tuple[str, str]]) -> str:
        """Build CREATE TABLE SQL statement."""
        col_defs = [f"{name} {definition}" for name, definition in columns]
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n  "
        sql += ",\n  ".join(col_defs)
        sql += "\n);"
        return sql
    
    def _insert_data(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        data: List[Dict],
        columns: List[Dict]
    ) -> Tuple[int, List[str]]:
        """
        Insert data into a table.
        
        Returns:
            Tuple of (rows_inserted, error_messages)
        """
        if not data:
            return 0, []
        
        cursor = conn.cursor()
        errors = []
        inserted = 0
        
        # Get column names from data (first row)
        if not data:
            return 0, []
        
        col_names = list(data[0].keys())
        
        # Prepare INSERT statement
        placeholders = ','.join(['?' for _ in col_names])
        insert_sql = f"INSERT INTO {table_name} ({','.join(col_names)}) VALUES ({placeholders})"
        
        for row in data:
            try:
                values = [self._convert_value(row.get(col)) for col in col_names]
                cursor.execute(insert_sql, values)
                inserted += 1
            except sqlite3.Error as e:
                errors.append(f"Error inserting row into {table_name}: {str(e)}")
        
        return inserted, errors
    
    def _convert_value(self, value: Any) -> Any:
        """Convert Python values to SQLite-compatible format."""
        if value is None:
            return None
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, bool):
            return 1 if value else 0
        elif isinstance(value, (dict, list)):
            return json.dumps(value)
        else:
            return value
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """List all databases in the directory."""
        databases = []
        
        for db_file in self.db_directory.glob("*.db"):
            databases.append({
                "name": db_file.stem,
                "path": str(db_file),
                "size_bytes": db_file.stat().st_size,
                "created": datetime.fromtimestamp(db_file.stat().st_ctime).isoformat()
            })
        
        return databases
    
    def get_database_info(self, db_name: str) -> Dict[str, Any]:
        """Get information about a database."""
        db_path = self.db_directory / f"{db_name}.db"
        
        if not db_path.exists():
            return {"error": "Database not found"}
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        info = {
            "name": db_name,
            "path": str(db_path),
            "tables": []
        }
        
        for (table_name,) in tables:
            # Get table info
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            info["tables"].append({
                "name": table_name,
                "columns": [
                    {"name": col[1], "type": col[2], "nullable": not col[3], "primary_key": bool(col[5])}
                    for col in columns
                ],
                "row_count": row_count
            })
        
        conn.close()
        return info
