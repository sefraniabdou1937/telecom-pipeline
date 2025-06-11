import random
import json
import time
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
# === Configuration Kafka ===
KAFKA_TOPIC = "raw-cdr"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
# Plage de numéros "valides"
VALID_PHONE_START = 212660000001
VALID_PHONE_END = 212660000200

# Paramètres
ERROR_RATIO = 0.05
SERVICE_DISTRIBUTION = {
    "voice": 0.6,
    "data": 0.3,
    "sms": 0.1
}

TECHNOLOGIES = ["2G", "3G", "4G", "5G"]
CELL_IDS = ["ALHOCEIMA_23", "CASABLANCA_12", "RABAT_05"]

OUTPUT_PATH = r"C:\Users\ABDOU\Desktop\telecom-pipeline\synthetic_cdr_data.json"
all_records = []  # Pour sauvegarde plus tard

def random_phone_number(valid=True):
    if valid:
        num = random.randint(VALID_PHONE_START, VALID_PHONE_END)
    else:
        return "ABC123XYZ" if random.random() < 0.5 else str(random.randint(212660000201, 212660010000))

    return str(num)

def random_timestamp(start_time, max_seconds=3600):
    delta = timedelta(seconds=random.randint(0, max_seconds))
    return (start_time + delta).strftime("%Y-%m-%dT%H:%M:%SZ")

def generate_voice_record(start_time, corrupt=False):
    rec = {
        "record_type": "voice",
        "timestamp": random_timestamp(start_time),
        "caller_id": random_phone_number(valid=not corrupt),
        "callee_id": random_phone_number(valid=not corrupt),
        "duration_sec": random.randint(1, 3600),
        "cell_id": random.choice(CELL_IDS),
        "technology": random.choice(TECHNOLOGIES)
    }
    if corrupt:
        if random.random() < 0.5:
            rec["duration_sec"] = -random.randint(1, 3600)
        else:
            del rec["callee_id"]
    return rec

def generate_sms_record(start_time, corrupt=False):
    rec = {
        "record_type": "sms",
        "timestamp": random_timestamp(start_time),
        "sender_id": random_phone_number(valid=not corrupt),
        "receiver_id": random_phone_number(valid=not corrupt),
        "cell_id": random.choice(CELL_IDS),
        "technology": random.choice(TECHNOLOGIES)
    }
    if corrupt:
        if random.random() < 0.5:
            rec["sender_id"] = "INVALID"
        else:
            del rec["receiver_id"]
    return rec

def generate_data_record(start_time, corrupt=False):
    rec = {
        "record_type": "data",
        "timestamp": random_timestamp(start_time),
        "user_id": random_phone_number(valid=not corrupt),
        "data_volume_mb": round(random.uniform(0.1, 500.0), 2),
        "session_duration_sec": random.randint(10, 7200),
        "cell_id": random.choice(CELL_IDS),
        "technology": random.choice(TECHNOLOGIES)
    }
    if corrupt:
        if random.random() < 0.5:
            rec["data_volume_mb"] = -round(random.uniform(0.1, 500.0), 2)
        else:
            del rec["user_id"]
    return rec

def generate_record(start_time):
    is_corrupt = random.random() < ERROR_RATIO
    r = random.random()
    cumulative = 0
    for service_type, ratio in SERVICE_DISTRIBUTION.items():
        cumulative += ratio
        if r <= cumulative:
            break

    if service_type == "voice":
        return generate_voice_record(start_time, corrupt=is_corrupt)
    elif service_type == "sms":
        return generate_sms_record(start_time, corrupt=is_corrupt)
    elif service_type == "data":
        return generate_data_record(start_time, corrupt=is_corrupt)
    else:
        return {
            "record_type": "unknown",
            "timestamp": random_timestamp(start_time),
            "details": "unrecognized service"
        }
if __name__ == "__main__":
    print("Streaming synthetic telecom records to Kafka topic 'raw-cdr'. Press Ctrl+C to stop...")
    try:
        while True:
            now = datetime.utcnow()
            record = generate_record(now)
            all_records.append(record)
            print(json.dumps(record, indent=2))

            # ✅ ENVOI AU TOPIC KAFKA
            producer.send(KAFKA_TOPIC, value=record)

            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nArrêt détecté. Sauvegarde des enregistrements...")

        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

        with open(OUTPUT_PATH, "w") as f:
            json.dump(all_records, f, indent=2)

        print(f"{len(all_records)} enregistrements sauvegardés dans : {OUTPUT_PATH}")