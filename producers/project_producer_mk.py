import json
import os
import random
import time
import pathlib
from datetime import datetime
from dotenv import load_dotenv

# Try to import Kafka
try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False

# Logger (optional)
logger = None
try:
    from utils.utils_logger import logger
except ImportError:
    def log_stub(*args, **kwargs): pass
    logger = type("Stub", (), {"info": print, "error": print, "warning": print})()

# Load environment variables
load_dotenv()

# Constants
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
DATA_FOLDER = PROJECT_ROOT.joinpath("data")
DATA_FOLDER.mkdir(exist_ok=True)
DATA_FILE = DATA_FOLDER.joinpath("expenses_live.json")

# Sample data pools
HOSPITALS = ["Green Valley Hospital", "Sunrise Clinic"]
DEPARTMENTS = {
    "Green Valley Hospital": ["Radiology", "Emergency", "Orthopedics"],
    "Sunrise Clinic": ["Cardiology", "Surgery", "Pediatrics"]
}
EXPENSES = {
    "Radiology": [("MRI", 1000, 1500), ("X-ray", 100, 300)],
    "Emergency": [("ER Visit", 700, 1000)],
    "Orthopedics": [("Fracture Treatment", 500, 2000)],
    "Cardiology": [("ECG", 200, 400), ("Stress Test", 300, 600)],
    "Surgery": [("Appendectomy", 4000, 6000), ("Knee Replacement", 8000, 12000)],
    "Pediatrics": [("Vaccination", 50, 150)]
}

PATIENT_IDS = [f"P{str(i).zfill(3)}" for i in range(1, 101)]

# Generator for expense messages
def generate_medical_expenses():
    while True:
        hospital = random.choice(HOSPITALS)
        department = random.choice(DEPARTMENTS[hospital])
        expense_type, min_cost, max_cost = random.choice(EXPENSES[department])
        cost = round(random.uniform(min_cost, max_cost), 2)
        patient_id = random.choice(PATIENT_IDS)
        timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        message = {
            "patient_id": patient_id,
            "hospital": hospital,
            "department": department,
            "expense_type": expense_type,
            "cost": cost,
            "timestamp": timestamp
        }

        yield message

# Environment helpers
def get_kafka_server():
    return os.getenv("KAFKA_SERVER", "localhost:9092")

def get_kafka_topic():
    return os.getenv("PROJECT_TOPIC", "medical_expenses")

def get_message_interval():
    return int(os.getenv("PROJECT_INTERVAL_SECONDS", 2))

# Main producer logic
def main():
    logger.info("🟢 Starting Medical Expenses Producer...")
    interval = get_message_interval()
    topic = get_kafka_topic()
    kafka_server = get_kafka_server()

    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            logger.info(f"✅ Kafka connected at {kafka_server}")
        except Exception as e:
            logger.error(f"❌ Kafka connection failed: {e}")
            producer = None

    try:
        for message in generate_medical_expenses():
            logger.info(message)

            # Write to file
            with DATA_FILE.open("a") as f:
                f.write(json.dumps(message) + "\n")

            # Send to Kafka
            if producer:
                producer.send(topic, value=message)
                logger.info(f"📤 Sent to topic '{topic}'")

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.warning("🛑 Interrupted by user.")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    finally:
        if producer:
            producer.close()
        logger.info("Producer shut down.")

if __name__ == "__main__":
    main()
