from flask import Flask, request, jsonify, render_template
from dotenv import load_dotenv
import os
import threading

load_dotenv()
from config import Config
from database import db, init_db
from utils import get_schema_info
from agents.interpreter import QueryInterpreterAgent
from agents.validator import ValidationRulesAgent
from agents.analytics import AnalyticsAgent

app = Flask(__name__)
app.config.from_object(Config)

init_db(app)

@app.route('/')
def index():
    return render_template('index.html')

# Initialize Agents
interpreter_agent = QueryInterpreterAgent()
validator_agent = ValidationRulesAgent()
analytics_agent = AnalyticsAgent(interpreter_agent, validator_agent)

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200

@app.route('/schema', methods=['GET'])
def schema_introspection():
    """
    Helper endpoint to view the schema introspection result.
    """
    schema_info = get_schema_info()
    return jsonify(schema_info)

@app.route('/interpret', methods=['POST'])
def interpret():
    data = request.json
    user_text = data.get('text')
    if not user_text:
        return jsonify({"error": "Missing 'text' field"}), 400
    
    result = interpreter_agent.interpret(user_text)
    return jsonify(result)

@app.route('/validate', methods=['POST'])
def validate():
    data = request.json
    user_role = data.get('user', {}).get('role', 'clinician') # Default to clinician if not specified
    plan = data.get('plan')
    
    if not plan:
        return jsonify({"error": "Missing 'plan' field"}), 400
        
    result = validator_agent.validate(user_role, plan)
    return jsonify(result)

@app.route('/analytics', methods=['POST'])
def analytics():
    data = request.json
    user_role = data.get('user', {}).get('role', 'analyst')
    user_text = data.get('text')
    mode = data.get('mode', 'autonomous')
    
    if not user_text:
        return jsonify({"error": "Missing 'text' field"}), 400
        
    result = analytics_agent.run_analytics(user_role, user_text, mode)
    return jsonify(result)

from agents.executor import DatabaseUpdateAgent

executor_agent = DatabaseUpdateAgent(interpreter_agent, validator_agent)

@app.route('/execute', methods=['POST'])
def execute():
    data = request.json
    user_id = data.get('user', {}).get('id', 0)
    user_role = data.get('user', {}).get('role', 'clinician')
    user_text = data.get('text')
    dry_run = data.get('dry_run', True)
    justification = data.get('justification')
    
    if not user_text:
        return jsonify({"error": "Missing 'text' field"}), 400
        
    result = executor_agent.execute(user_id, user_role, user_text, dry_run, justification)
    return jsonify(result)

@app.route('/audit', methods=['GET'])
def audit():
    logs = executor_agent.get_audit_logs()
    return jsonify(logs)

@app.route('/admin/policies', methods=['POST'])
def admin_policies():
    # In a real app, check for admin role here
    new_policies = request.data.decode('utf-8')
    success, message = validator_agent.update_policies(new_policies)
    if success:
        return jsonify({"message": message}), 200
    else:
        return jsonify({"error": message}), 400

# ============= Data Ingestion Endpoints =============

from werkzeug.utils import secure_filename
from agents.ingestion_orchestrator import IngestionOrchestrator

@app.route('/ingestion')
def ingestion_ui():
    return render_template('ingestion.html')

ALLOWED_EXTENSIONS = {'csv', 'json', 'jsonl', 'xlsx', 'xls', 'txt', 'tsv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

orchestrator = IngestionOrchestrator()

@app.route('/upload-data', methods=['POST'])
def upload_data():
    """
    Upload multiple data files for ingestion.
    Accepts: multipart/form-data with files and requirements
    Returns: Upload ID and file processing status
    """
    if 'files[]' not in request.files:
        return jsonify({"error": "No files provided"}), 400
    
    files = request.files.getlist('files[]')
    requirements = request.form.get('requirements', '')
    
    if not requirements:
        return jsonify({"error": "Requirements specification is required"}), 400
    
    uploaded_files = []
    upload_errors = []
    
    # Save uploaded files
    upload_id = None
    for file in files:
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            
            if not upload_id:
                # Start ingestion to get upload_id
                upload_id = orchestrator.start_ingestion([], requirements)
            
            upload_path = orchestrator.upload_dir / upload_id
            file_path = upload_path / filename
            file.save(file_path)
            
            uploaded_files.append({
                "name": filename,
                "path": str(file_path)
            })
        else:
            upload_errors.append(f"Invalid file: {file.filename}")
    
    if not uploaded_files:
        return jsonify({"error": "No valid files uploaded", "details": upload_errors}), 400
    
    # Update state with file information
    state = orchestrator._get_state(upload_id)
    state.files = uploaded_files
    orchestrator._save_state(state)
    
    return jsonify({
        "success": True,
        "upload_id": upload_id,
        "files_uploaded": len(uploaded_files),
        "errors": upload_errors
    }), 200

@app.route('/process-files/<upload_id>', methods=['POST'])
def process_files(upload_id):
    """
    Process uploaded files: parse and analyze.
    Returns: File analysis summary
    """
    result = orchestrator.process_files(upload_id)
    
    if "error" in result:
        return jsonify(result), 400
    
    return jsonify(result), 200

@app.route('/generate-schema/<upload_id>', methods=['POST'])
def generate_schema_endpoint(upload_id):
    """
    Generate database schema from analyzed files.
    Returns: Schema generation result
    """
    is_async = request.args.get('async_mode') == 'true'
    
    if is_async:
        thread = threading.Thread(target=orchestrator.generate_schema, args=(upload_id,))
        thread.start()
        return jsonify({"status": "processing", "message": "Schema generation started in background"}), 202
        
    result = orchestrator.generate_schema(upload_id)
    
    if "error" in result:
        return jsonify(result), 400
    
    return jsonify(result), 200

@app.route('/schema/<upload_id>', methods=['GET'])
def review_schema(upload_id):
    """
    Retrieve generated schema for review.
    Returns: Schema code, description, and warnings
    """
    result = orchestrator.get_schema(upload_id)
    
    if "error" in result:
        return jsonify(result), 404
    
    return jsonify(result), 200

@app.route('/create-database/<upload_id>', methods=['POST'])
def create_database_endpoint(upload_id):
    """
    Execute schema creation and data population.
    Accepts: {"db_name": str (optional), "approved": bool}
    Returns: Database creation status
    """
    data = request.json or {}
    db_name = data.get('db_name')
    approved = data.get('approved', True)
    
    result = orchestrator.finalize_ingestion(
        upload_id=upload_id,
        db_name=db_name,
        approved=approved
    )
    
    if "error" in result:
        return jsonify(result), 400
    
    return jsonify(result), 200

@app.route('/ingestion-status/<upload_id>', methods=['GET'])
def ingestion_status(upload_id):
    """
    Check status of data ingestion process.
    Returns: Current stage, progress, and errors
    """
    result = orchestrator.get_status(upload_id)
    
    if "error" in result:
        return jsonify(result), 404
    
    return jsonify(result), 200

@app.route('/databases', methods=['GET'])
def list_databases():
    """
    List all created databases.
    Returns: List of databases with metadata
    """
    databases = orchestrator.db_builder.list_databases()
    return jsonify({"databases": databases}), 200

@app.route('/database/<db_name>/info', methods=['GET'])
def database_info(db_name):
    """
    Get detailed information about a database.
    Returns: Tables, columns, and row counts
    """
    info = orchestrator.db_builder.get_database_info(db_name)
    
    if "error" in info:
        return jsonify(info), 404
    
    return jsonify(info), 200

if __name__ == '__main__':
    app.run(debug=True, port=5001)
