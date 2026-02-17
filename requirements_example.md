# Healthcare Database Requirements

## Purpose
Create a database to store patient demographic information and their medical encounter history.

## Data Sources
The following files are being uploaded:
- `patients.csv`: Contains patient details (MRN, DOB, Gender, etc.)
- `encounters.json`: List of patient visits and departments

## Schema Definition
Please create a database with the following entities:

### 1. Patient
- **Primary Key**: `mrn` (Medical Record Number) - must be unique.
- **Attributes**:
  - `dob` (Date of Birth)
  - `gender`
- **Constraints**:
  - `mrn` cannot be null.

### 2. Encounter
- **Primary Key**: `encounter_id` (Integer)
- **Foreign Key**: `patient_mrn` links to `Patient.mrn`.
- **Attributes**:
  - `date` (Visit date)
  - `department` (e.g., Cardiology, ER)

## Data Processing Rules
1. **Privacy**: Do not include any patient names or addresses (City/State) in the database.
2. **Dates**: Ensure all dates are stored in standard ISO format (YYYY-MM-DD).
3. **Relationships**: Establish a one-to-many relationship where one Patient behaves many Encounters.
