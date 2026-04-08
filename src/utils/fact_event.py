import json
import os
import random
from datetime import datetime, timedelta
from faker import Faker
import uuid

base_path = "/Users/mukesh.singh/spark/ai_db/data/raw/claim_event_raw"
fake = Faker()


# -------------------------------
# Predefined Dimension Ranges
# -------------------------------
MEMBER_IDS = [f"M{i:03}" for i in range(1, 101)]
PLAN_IDS = ["P001", "P002", "P003"]
DRUG_CODES = ["D001", "D002", "D003", "D004", "D005"]


# -------------------------------
# Generate Claim Events
# -------------------------------
def generate_claim_events(n=200):
    data = []

    for _ in range(n):
        event_time = fake.date_time_between(start_date="-30d", end_date="now")

        record = {
            # Event metadata
            "event_id": str(uuid.uuid4()),
            "event_type": random.choice(["INSERT", "UPDATE", "DELETE"]),
            "event_timestamp": event_time.isoformat(),
            "ingestion_timestamp": datetime.now().isoformat(),

            # Business keys (consistent with dimensions)
            "member_id": random.choice(MEMBER_IDS),
            "plan_id": random.choice(PLAN_IDS),
            "drug_code": random.choice(DRUG_CODES),
            "pharmacy_id": f"PH{random.randint(1, 20):03}",

            # Fact attributes
            "fill_date": fake.date_between(start_date="-60d", end_date="today").isoformat(),
            "quantity": random.randint(1, 90),
            "cost": round(random.uniform(10, 500), 2),
            "status": random.choice(["APPROVED", "REJECTED", "PENDING"]),

            # Additional columns
            "prescriber_id": f"DR{random.randint(1, 50):03}",
            "refill_flag": random.choice([True, False]),
            "days_supply": random.choice([30, 60, 90]),
            "diagnosis_code": f"D{random.randint(100, 999)}",

            # Raw payload (simulate original event)
            "raw_payload": "{}",

            # ingestion metadata
            "_source_file": "faker_generated",
            "_ingest_ts": datetime.now().isoformat()
        }

        data.append(record)

    return data


# -------------------------------
# Write File with Timestamp
# -------------------------------
def write_json_with_timestamp(data):
    os.makedirs(base_path, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_path = f"{base_path}/claim_event_{timestamp}.json"

    with open(file_path, "w") as f:
        for row in data:
            f.write(json.dumps(row) + "\n")

    print(f"Claim event file created: {file_path}")


# -------------------------------
# Main Execution
# -------------------------------
if __name__ == "__main__":
    data = generate_claim_events(200)
    write_json_with_timestamp(data)