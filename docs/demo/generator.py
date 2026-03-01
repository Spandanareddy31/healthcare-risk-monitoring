import json
import os
import random
import time
from datetime import datetime

OUT_DIR = os.path.join("data", "raw_stream")
os.makedirs(OUT_DIR, exist_ok=True)

def generate_event():
    return {
        "patient_id": random.randint(1000, 1100),
        "heart_rate": random.randint(55, 160),
        "spo2": random.randint(80, 100),
        "bp_systolic": random.randint(85, 190),
        "bp_diastolic": random.randint(50, 120),
        "temp_c": round(random.uniform(35.5, 40.5), 1),
        "timestamp": datetime.utcnow().isoformat()
    }

if __name__ == "__main__":
    print("Starting simulated vitals stream...")
    counter = 0
    while True:
        event = generate_event()
        filename = f"vitals_{int(time.time()*1000)}_{counter}.json"
        filepath = os.path.join(OUT_DIR, filename)

        with open(filepath, "w") as f:
            json.dump(event, f)

        counter += 1
        time.sleep(1)
