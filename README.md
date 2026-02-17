# Multi-Agent Healthcare DBMS

A reproducible, safety-first Multi-Agent Database Management System built with Flask and SQLite. This system leverages multiple AI agents to convert natural language requests into schema-aware SQL, validate them against strict governance rules, and execute them with a full audit trail.

## Features

- **Natural Language to SQL**: Converts user queries into SQL using the `QueryInterpreterAgent`.
- **Safety & Governance**: Validates all queries against role-based policies and safety rules using the `ValidationRulesAgent`.
- **Role-Based Access Control (RBAC)**: Supports different roles (e.g., clinician, analyst, admin) with specific permissions defined in `policies.yaml`.
- **Audit Logging**: Tracks all executed queries, including user ID, role, timestamp, and justification.
- **Analytics**: Provides insights and data analysis capabilities via the `AnalyticsAgent`.
- **Schema Introspection**: Dynamically understands the database schema.

## Prerequisites

- Python 3.8+
- OpenAI API Key (for the AI agents)

## Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd Agents
    ```

2.  **Create and activate a virtual environment (optional but recommended):**
    ```bash
    python -m venv venv
    # On Windows:
    venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

## Configuration

1.  **Environment Variables:**
    Create a `.env` file in the root directory and add your OpenAI API key and any other necessary configuration:
    ```env
    OPENAI_API_KEY=your_openai_api_key_here
    FLASK_SECRET_KEY=your_secret_key
    DATABASE_URL=sqlite:///library.db
    ```

2.  **Policies:**
    Review and modify `policies.yaml` to define roles, permissions, and safety rules as needed.

## Usage

1.  **Run the application:**
    ```bash
    python app.py
    ```
    The server will start at `http://localhost:5001`.

2.  **API Endpoints:**

    -   **`GET /`**: Home page.
    -   **`GET /health`**: Health check.
    -   **`GET /schema`**: View the database schema information.
    -   **`POST /interpret`**: Convert natural language to SQL.
        -   Body: `{"text": "Show me all patients"}`
    -   **`POST /validate`**: Validate a generated plan/SQL.
        -   Body: `{"user": {"role": "clinician"}, "plan": {...}}`
    -   **`POST /execute`**: Execute a query with audit logging.
        -   Body: `{"user": {"id": 1, "role": "clinician"}, "text": "UPDATE ...", "dry_run": false, "justification": "Medical necessity"}`
    -   **`POST /analytics`**: Run analytics on data.
        -   Body: `{"user": {"role": "analyst"}, "text": "Analyze patient trends", "mode": "autonomous"}`
    -   **`GET /audit`**: Retrieve audit logs.
    -   **`POST /admin/policies`**: Update governance policies (Admin only).

## Project Structure

-   `app.py`: Main Flask application entry point.
-   `agents/`: Contains the implementation of various agents (Interpreter, Validator, Analytics, Executor, Schema, Ingestion Orchestrator).
-   `data_ingestion/`: File parsing and analysis modules for automated database creation.
-   `database_builder.py`: Dynamic database creation from generated schemas.
-   `policies.yaml`: Configuration file for RBAC and safety rules.
-   `database.py`: Database initialization and models.
-   `utils.py`: Utility functions.
-   `requirements.txt`: Python dependencies.
-   `requirements_spec_template.md`: Template for data ingestion requirements.
-   `demo_ingestion.py`: Demo script showing automated database creation.

## Automated Database Ingestion (New!)

### Overview

The system now supports **automated database creation and data insertion** from raw files (CSV, JSON, XLSX, TXT) using an AI agent-driven approach inspired by:
- **DS-STAR** (Google Research): Iterative planning with Planner → Coder → Verifier → Router loop
- **Spec-Driven Development** (GitHub): Markdown-based requirements specifications

### Supported File Formats

- **CSV/TSV**: Comma or tab-separated values
- **JSON/JSONL**: JSON arrays, objects, or JSON Lines
- **XLSX/XLS**: Excel spreadsheets (multi-sheet support)
- **TXT**: Text files with automatic delimiter detection

### Workflow

```
1. Upload Files + Requirements
   ↓
2. Parse & Analyze Data (automatic encoding detection, type inference)
   ↓
3. Generate Schema (AI agent with iterative refinement, up to 10 rounds)
   ↓
4. Review Schema (human approval optional)
   ↓
5. Create Database & Populate Data
   ↓
6. Query via SQL (using existing /execute endpoint)
```

### Quick Start (Web Interface) - Recommended 🌟

1. Start the server:
   ```bash
   python app.py
   ```
2. Open your browser to `http://localhost:5001`
3. Click **"Automated Database Ingestion"**
4. Follow the 4-step wizard:
   - **Upload**: Drag & drop files and enter requirements
   - **Analyze**: View data insights and relationships
   - **Generate**: Watch the AI refine the schema
   - **Create**: Build your database with one click

### Quick Start (CLI)

1. **Create a requirements specification** (see `requirements_spec_template.md`)
2. **Run the demo**:
   ```bash
   # Terminal 1: Start server
   python app.py
   
   # Terminal 2: Run demo
   python demo_ingestion.py
   ```

### API Endpoints (Data Ingestion)

-   **`POST /upload-data`**: Upload files with requirements specification
    -   Body: `multipart/form-data` with `files[]` and `requirements`
    -   Returns: `upload_id` for tracking

-   **`POST /process-files/<upload_id>`**: Parse and analyze uploaded files
    -   Returns: File analysis summary with detected structure

-   **`POST /generate-schema/<upload_id>`**: Generate database schema using AI
    -   Returns: Schema description and verification status
    -   *Note: May take 30-60 seconds due to iterative refinement*

-   **`GET /schema/<upload_id>`**: Review generated schema
    -   Returns: SQLAlchemy code, description, warnings, relationships

-   **`POST /create-database/<upload_id>`**: Create and populate database
    -   Body: `{"db_name": "optional", "approved": true}`
    -   Returns: Database path, tables created, rows inserted

-   **`GET /ingestion-status/<upload_id>`**: Check ingestion progress
    -   Returns: Current stage, errors, status

-   **`GET /databases`**: List all created databases

-   **`GET /database/<db_name>/info`**: Get database structure and row counts

### Usage Example (Python)

```python
import requests

BASE_URL = "http://localhost:5000"

# Step 1: Upload files
files = {
    'files[]': [
        open('patients.csv', 'rb'),
        open('encounters.json', 'rb')
    ]
}
requirements = open('requirements_spec.md').read()

response = requests.post(
    f"{BASE_URL}/upload-data",
    files=files,
    data={'requirements': requirements}
)
upload_id = response.json()['upload_id']

# Step 2: Process files
requests.post(f"{BASE_URL}/process-files/{upload_id}")

# Step 3: Generate schema (may take 30-60s)
requests.post(f"{BASE_URL}/generate-schema/{upload_id}")

# Step 4: Review schema
schema = requests.get(f"{BASE_URL}/schema/{upload_id}").json()
print(schema['schema_description'])

# Step 5: Create database
result = requests.post(
    f"{BASE_URL}/create-database/{upload_id}",
    json={"db_name": "my_clinic", "approved": True}
)
print(f"Database created: {result.json()['database_path']}")
```

### Usage Example (cURL)

```bash
# Upload files
curl -X POST http://localhost:5000/upload-data \
  -F 'files[]=@patients.csv' \
  -F 'files[]=@encounters.json' \
  -F 'requirements=@requirements_spec.md'

# Process files (use upload_id from above)
curl -X POST http://localhost:5000/process-files/{upload_id}

# Generate schema
curl -X POST http://localhost:5000/generate-schema/{upload_id}

# Review schema
curl http://localhost:5000/schema/{upload_id}

# Create database
curl -X POST http://localhost:5000/create-database/{upload_id} \
  -H "Content-Type: application/json" \
  -d '{"db_name": "my_clinic", "approved": true}'
```

### Requirements Specification Format

Use the provided `requirements_spec_template.md` as a starting point. Key sections:

```markdown
# Data Ingestion Requirements

## Purpose
Describe what this database is for

## Data Sources
List all files and their contents

## Schema Requirements
- Entities (tables) with primary keys
- Relationships between entities
- Constraints (unique, not null, etc.)
- Special instructions (de-identification, data cleaning)
```

### Features

- **Automatic Schema Inference**: AI analyzes data structure and suggests optimal schema
- **Relationship Detection**: Automatically detects foreign key relationships
- **Primary Key Suggestion**: Identifies best primary key candidates
- **Data Quality Checks**: Warns about high null percentages, duplicates, etc.
- **Iterative Refinement**: Uses DS-STAR's verification loop (up to 10 rounds)
- **Natural Language Summaries**: Human-readable descriptions of data and schema
- **State Persistence**: Resume interrupted ingestion processes
- **Multi-File Support**: Combine data from multiple sources

### Testing

Run unit tests:
```bash
pytest tests/test_data_ingestion.py -v
```

Run demo script:
```bash
python demo_ingestion.py
```
