from app import app
from database import db, Patient, Encounter, Lab, Medication, User
from datetime import date, datetime

def seed_data():
    with app.app_context():
        print("Dropping all tables...")
        db.drop_all()
        print("Creating all tables...")
        db.create_all()

        print("Seeding data...")
        
        # Users
        u1 = User(username='dr_house', role='clinician')
        u2 = User(username='nurse_joy', role='clinician')
        u3 = User(username='admin_alice', role='admin')
        u4 = User(username='analyst_bob', role='analyst')
        db.session.add_all([u1, u2, u3, u4])
        db.session.commit()

        # Patients
        p1 = Patient(mrn='MRN001', dob=date(1980, 5, 12), gender='M')
        p2 = Patient(mrn='MRN002', dob=date(1992, 8, 23), gender='F')
        db.session.add_all([p1, p2])
        db.session.commit()

        # Encounters
        e1 = Encounter(patient_id=p1.id, encounter_date=date(2023, 10, 1), department='Cardiology', clinician_id=u1.id)
        e2 = Encounter(patient_id=p2.id, encounter_date=date(2023, 10, 2), department='Emergency', clinician_id=u2.id)
        db.session.add_all([e1, e2])
        db.session.commit()

        # Labs
        l1 = Lab(encounter_id=e1.id, test_name='Potassium', value=4.5, units='mmol/L', reference_range='3.5-5.0')
        l2 = Lab(encounter_id=e1.id, test_name='Sodium', value=140, units='mmol/L', reference_range='135-145')
        l3 = Lab(encounter_id=e2.id, test_name='Glucose', value=5.5, units='mmol/L', reference_range='3.9-5.6')
        db.session.add_all([l1, l2, l3])
        db.session.commit()

        # Meds
        m1 = Medication(patient_id=p1.id, drug_name='Aspirin', dose='75mg', start_date=date(2023, 10, 1), prescriber_id=u1.id)
        m2 = Medication(patient_id=p2.id, drug_name='Ibuprofen', dose='400mg', start_date=date(2023, 10, 2), end_date=date(2023, 10, 5), prescriber_id=u2.id)
        db.session.add_all([m1, m2])
        db.session.commit()

        print("Data seeded successfully.")

if __name__ == '__main__':
    seed_data()
