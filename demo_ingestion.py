"""
Test script to demonstrate the automated database ingestion workflow.

This script shows how the system:
1. Uploads files with requirements
2. Parses and analyzes data
3. Generates database schema using AI
4. Creates and populates the database

Run this after starting the Flask server with: python app.py
"""

import requests
import json
import time
from pathlib import Path

BASE_URL = "http://localhost:5001"

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}\n")

def demo_ingestion():
    """Demonstrate the complete ingestion workflow."""
    
    print_section("AUTOMATED DATABASE INGESTION DEMO")
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=2)
        if response.status_code != 200:
            print("✗ Flask server is not responding correctly.")
            print("Please start the server first with: python3 app.py")
            return
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to Flask server.")
        print("Please start the server first with: python3 app.py")
        print("\nIn a separate terminal, run:")
        print("  cd /Users/abbyswag/Downloads/DIS2555")
        print("  python3 app.py")
        return
    except requests.exceptions.Timeout:
        print("✗ Server connection timed out.")
        return
    
    print("✓ Connected to Flask server\n")
    
    # Step 1: Prepare test data files
    print("Step 1: Creating test data files...")
    
    test_data_dir = Path("./test_data")
    test_data_dir.mkdir(exist_ok=True)
    
    # Create sample patients.csv
    patients_csv = test_data_dir / "patients.csv"
    with open(patients_csv, 'w') as f:
        f.write("mrn,dob,gender,city\n")
        f.write("MRN001,1980-05-12,M,Boston\n")
        f.write("MRN002,1992-08-23,F,New York\n")
        f.write("MRN003,1975-03-15,M,Chicago\n")
    
    # Create sample encounters.json
    encounters_json = test_data_dir / "encounters.json"
    with open(encounters_json, 'w') as f:
        json.dump([
            {"encounter_id": 1, "patient_mrn": "MRN001", "date": "2023-10-01", "department": "Cardiology"},
            {"encounter_id": 2, "patient_mrn": "MRN002", "date": "2023-10-02", "department": "Emergency"},
            {"encounter_id": 3, "patient_mrn": "MRN001", "date": "2023-10-15", "department": "Cardiology"}
        ], f, indent=2)
    
    print(f"✓ Created test files in {test_data_dir}")
    
    # Step 2: Upload files with requirements
    print_section("Step 2: Uploading files with requirements")
    
    requirements = """
# Healthcare Database Requirements

## Purpose
Store patient medical records for a small clinic.

## Data Sources
- patients.csv: Patient demographics
- encounters.json: Medical encounter records

## Schema Requirements

### Entities

1. **Patient**
   - Primary Key: MRN (Medical Record Number)
   - Attributes: DOB, Gender, City
   - Relationships: One Patient → Many Encounters

2. **Encounter**
   - Primary Key: encounter_id
   - Attributes: Date, Department
   - Foreign Key: patient_mrn → Patient.mrn

### Constraints
- Patient.mrn: Unique, Not Null
- Encounter.date: Not Null
- Encounter.patient_mrn: Foreign key to Patient.mrn

### Special Instructions
- Convert all dates to ISO format
- Remove city information (for privacy)
    """
    
    # Open files for upload
    files = [
        ('files[]', (patients_csv.name, open(patients_csv, 'rb'), 'text/csv')),
        ('files[]', (encounters_json.name, open(encounters_json, 'rb'), 'application/json'))
    ]
    
    data = {'requirements': requirements}
    
    response = requests.post(f"{BASE_URL}/upload-data", files=files, data=data)
    
    if response.status_code != 200:
        print(f"✗ Upload failed: {response.json()}")
        return
    
    result = response.json()
    upload_id = result['upload_id']
    print(f"✓ Files uploaded successfully!")
    print(f"  Upload ID: {upload_id}")
    print(f"  Files: {result['files_uploaded']}")
    
    # Step 3: Process files
    print_section("Step 3: Processing and analyzing files")
    
    response = requests.post(f"{BASE_URL}/process-files/{upload_id}")
    
    if response.status_code != 200:
        print(f"✗ Processing failed: {response.json()}")
        return
    
    result = response.json()
    print(f"✓ Files processed successfully!")
    print(f"  Files parsed: {result['files_parsed']}")
    print(f"\n  Analysis Summary:")
    print(f"  {result['analysis']['natural_language_summary'][:500]}...")
    
    # Step 4: Generate schema
    print_section("Step 4: Generating database schema with AI")
    
    print("This may take 30-60 seconds as the AI agent iteratively refines the schema...")
    
    response = requests.post(f"{BASE_URL}/generate-schema/{upload_id}")
    
    if response.status_code != 200:
        print(f"✗ Schema generation failed: {response.json()}")
        return
    
    result = response.json()
    print(f"✓ Schema generated successfully!")
    print(f"  Verification Status: {'✓ Passed' if result['verification_status'] else '⚠ Has warnings'}")
    print(f"  Refinement Rounds: {result['rounds_taken']}")
    
    if result['warnings']:
        print(f"  Warnings: {len(result['warnings'])}")
        for warning in result['warnings'][:3]:
            print(f"    - {warning}")
    
    # Step 5: Review schema
    print_section("Step 5: Reviewing generated schema")
    
    response = requests.get(f"{BASE_URL}/schema/{upload_id}")
    schema_info = response.json()
    
    print("Generated Schema Description:")
    print(schema_info['schema_description'])
    
    print("\n\nGenerated SQLAlchemy Code (first 1000 chars):")
    print(schema_info['schema_code'][:1000])
    print("...\n")
    
    # Step 6: Create database
    print_section("Step 6: Creating database and populating data")
    
    db_name = "demo_clinic"
    response = requests.post(
        f"{BASE_URL}/create-database/{upload_id}",
        json={"db_name": db_name, "approved": True}
    )
    
    if response.status_code != 200:
        print(f"✗ Database creation failed: {response.json()}")
        return
    
    result = response.json()
    print(f"✓ Database created successfully!")
    print(f"  Database: {result['database_name']}")
    print(f"  Path: {result['database_path']}")
    print(f"  Tables created: {result['tables_created']}")
    print(f"  Rows inserted:")
    for table, count in result['rows_inserted'].items():
        print(f"    - {table}: {count} rows")
    
    if result['warnings']:
        print(f"\n  Warnings:")
        for warning in result['warnings']:
            print(f"    - {warning}")
    
    # Step 7: Verify database
    print_section("Step 7: Verifying created database")
    
    response = requests.get(f"{BASE_URL}/database/{db_name}/info")
    db_info = response.json()
    
    print(f"Database Information:")
    print(f"  Name: {db_info['name']}")
    print(f"  Tables: {len(db_info['tables'])}")
    
    for table in db_info['tables']:
        print(f"\n  Table: {table['name']}")
        print(f"    Rows: {table['row_count']}")
        print(f"    Columns:")
        for col in table['columns']:
            flags = []
            if col['primary_key']:
                flags.append('PK')
            if not col['nullable']:
                flags.append('NOT NULL')
            flag_str = f" [{', '.join(flags)}]" if flags else ""
            print(f"      - {col['name']} ({col['type']}){flag_str}")
    
    print_section("DEMO COMPLETED SUCCESSFULLY! 🎉")
    print(f"\nYou can now query the database at: {result['database_path']}")
    print(f"Or use the existing /execute endpoint to run SQL queries on it.")
    
    return upload_id

if __name__ == "__main__":
    try:
        demo_ingestion()
    except requests.exceptions.ConnectionError:
        print("\n✗ Error: Could not connect to Flask server.")
        print("Please start the server first with: python app.py")
    except Exception as e:
        print(f"\n✗ Error: {str(e)}")
        import traceback
        traceback.print_exc()
