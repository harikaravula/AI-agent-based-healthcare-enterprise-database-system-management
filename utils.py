from database import db, Patient, Encounter, Lab, Medication, User, AuditLog
from sqlalchemy import inspect

def get_schema_info():
    """
    Introspects the SQLAlchemy models to return a JSON-serializable schema representation.
    """
    models = [Patient, Encounter, Lab, Medication, User, AuditLog]
    schema_info = {"tables": {}}

    for model in models:
        table_name = model.__tablename__
        columns = {}
        pks = []
        
        mapper = inspect(model)
        
        for column in mapper.columns:
            # Simplified type mapping for LLM context
            col_type = str(column.type).lower()
            if 'integer' in col_type: col_type = 'int'
            elif 'string' in col_type or 'text' in col_type: col_type = 'text'
            elif 'date' in col_type or 'time' in col_type: col_type = 'date'
            elif 'float' in col_type or 'real' in col_type: col_type = 'float'
            
            columns[column.name] = col_type
            if column.primary_key:
                pks.append(column.name)
        
        schema_info["tables"][table_name] = {
            "columns": columns,
            "pks": pks
        }
        
    return schema_info
