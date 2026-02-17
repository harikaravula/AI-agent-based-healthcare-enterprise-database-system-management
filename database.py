from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

class Patient(db.Model):
    __tablename__ = 'patients'
    id = db.Column(db.Integer, primary_key=True)
    mrn = db.Column(db.String(50), unique=True, nullable=False)
    dob = db.Column(db.Date)
    gender = db.Column(db.String(10))
    deidentified = db.Column(db.Integer, default=0)
    
    encounters = db.relationship('Encounter', backref='patient', lazy=True)
    meds = db.relationship('Medication', backref='patient', lazy=True)

class Encounter(db.Model):
    __tablename__ = 'encounters'
    id = db.Column(db.Integer, primary_key=True)
    patient_id = db.Column(db.Integer, db.ForeignKey('patients.id'), nullable=False)
    encounter_date = db.Column(db.Date, nullable=False)
    department = db.Column(db.String(100))
    clinician_id = db.Column(db.Integer) # Refers to User.id conceptually
    
    labs = db.relationship('Lab', backref='encounter', lazy=True)

class Lab(db.Model):
    __tablename__ = 'labs'
    id = db.Column(db.Integer, primary_key=True)
    encounter_id = db.Column(db.Integer, db.ForeignKey('encounters.id'), nullable=False)
    test_name = db.Column(db.String(100), nullable=False)
    value = db.Column(db.Float)
    units = db.Column(db.String(20))
    reference_range = db.Column(db.String(50))
    collected_at = db.Column(db.DateTime, default=datetime.utcnow)

class Medication(db.Model):
    __tablename__ = 'meds'
    id = db.Column(db.Integer, primary_key=True)
    patient_id = db.Column(db.Integer, db.ForeignKey('patients.id'), nullable=False)
    drug_name = db.Column(db.String(100), nullable=False)
    dose = db.Column(db.String(50))
    start_date = db.Column(db.Date)
    end_date = db.Column(db.Date)
    prescriber_id = db.Column(db.Integer) # Refers to User.id conceptually

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    role = db.Column(db.String(20), nullable=False) # clinician, analyst, admin

class AuditLog(db.Model):
    __tablename__ = 'audit_log'
    audit_id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer)
    ts = db.Column(db.DateTime, default=datetime.utcnow)
    action_type = db.Column(db.String(20)) # INSERT, UPDATE, DELETE
    affected_table = db.Column(db.String(50))
    affected_row_ids = db.Column(db.String) # CSV of IDs
    sql_executed = db.Column(db.Text)
    pre_state = db.Column(db.Text) # JSON snapshot
    post_state = db.Column(db.Text) # JSON snapshot
    validation_summary = db.Column(db.Text)
    justification = db.Column(db.Text)

def init_db(app):
    db.init_app(app)
    with app.app_context():
        db.create_all()
