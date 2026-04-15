import json
import os
import sys
from datetime import datetime
import subprocess




# -------------------------------
# Config (Environment-Aware)
# -------------------------------
def is_databricks():
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

# -------------------------------
# Ensure Faker is Installed (Databricks Serverless)
# -------------------------------
def ensure_faker():
    try:
        from faker import Faker
    except ImportError:
        if is_databricks():
            print("Installing faker in Databricks environment...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", "faker"])
        else:
            raise

ensure_faker()
# Now safe to import
from faker import Faker


if is_databricks():
    BASE_PATH = "/Volumes/ai_dev_db/dev_msingh_bronze/raw"
else:
    BASE_PATH = "/Users/mukeshsingh/spark/ai_db/data/raw"


fake = Faker()


# -------------------------------
# Data Generators
# -------------------------------
def generate_member_data(n=100):
    return [
        {
            "member_id": f"M{i:03}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "state": fake.state(),
            "phone": fake.phone_number(),
            "ssn": fake.ssn()
        }
        for i in range(1, n + 1)
    ]


def generate_plan_data():
    return [
        {"plan_id": "P001", "plan_name": "Basic Plan", "plan_type": "HMO"},
        {"plan_id": "P002", "plan_name": "Premium Plan", "plan_type": "PPO"},
        {"plan_id": "P003", "plan_name": "Gold Plan", "plan_type": "EPO"},
    ]


def generate_drug_data():
    return [
        {"drug_code": "D001", "drug_name": "Atorvastatin", "drug_class": "Cardio"},
        {"drug_code": "D002", "drug_name": "Metformin", "drug_class": "Diabetes"},
        {"drug_code": "D003", "drug_name": "Lisinopril", "drug_class": "BP"},
        {"drug_code": "D004", "drug_name": "Amlodipine", "drug_class": "BP"},
        {"drug_code": "D005", "drug_name": "Omeprazole", "drug_class": "GI"},
    ]


def generate_prescriber_data(n=20):
    specialties = [
        "Cardiology",
        "Internal Medicine",
        "Family Medicine",
        "Endocrinology",
        "Neurology",
        "Oncology",
        "Dermatology",
        "Pediatrics",
    ]

    return [
        {
            "prescriber_id": f"DR{i:03}",  # DR001 → DR020
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "specialty": fake.random_element(specialties),
            "npi": fake.random_number(digits=10, fix_len=True),
            "state": fake.state(),
            "phone": fake.phone_number(),
        }
        for i in range(1, n + 1)
    ]


# -------------------------------
# File Writer
# -------------------------------
def write_json_with_timestamp(base_dir, dataset_name, data):
    os.makedirs(base_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{base_dir}/{dataset_name}_{timestamp}.json"

    with open(file_path, "w") as f:
        for row in data:
            f.write(json.dumps(row) + "\n")

    print(f"{dataset_name.upper()} file created: {file_path}")


# -------------------------------
# Dataset Dispatcher
# -------------------------------
def run_one(dataset: str):
    dataset = dataset.lower()

    dataset_map = {
        "member": (f"{BASE_PATH}/member_raw", generate_member_data),
        "plan": (f"{BASE_PATH}/plan_raw", generate_plan_data),
        "drug": (f"{BASE_PATH}/drug_raw", generate_drug_data),
        "prescriber": (f"{BASE_PATH}/prescriber_raw", generate_prescriber_data),
    }

    if dataset not in dataset_map:
        print("Invalid dataset. Choose from: member, plan, drug, prescriber")
        return

    base_dir, generator = dataset_map[dataset]
    data = generator()

    write_json_with_timestamp(base_dir, dataset, data)


def run_all():
    for dataset in ["member", "plan", "drug", "prescriber"]:
        run_one(dataset)


# -------------------------------
# Main Entry
# -------------------------------
if __name__ == "__main__":
    if len(sys.argv) == 1 or len(sys.argv) == 3:
        # No argument → generate all datasets
        run_all()

    elif len(sys.argv) == 2 or len(sys.argv) == 4:
        # One argument → generate specific dataset
        run_one(sys.argv[1])

    else:
        print("Usage:")
        print("  python dim_data_generator.py              # run all")
        print("  python dim_data_generator.py member")
        print("  python dim_data_generator.py plan")
        print("  python dim_data_generator.py drug")
        print("  python dim_data_generator.py prescriber")
