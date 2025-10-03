import json
import os
import time
from collections import defaultdict, deque
from dotenv import load_dotenv
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
import seaborn as sns

# Load environment variables from .env
load_dotenv()

# Setup Seaborn styling for the plot
sns.set(style="darkgrid")

# Get Kafka configuration from environment
def get_kafka_server():
    return os.getenv("KAFKA_SERVER", "localhost:9092")

def get_kafka_topic():
    return os.getenv("PROJECT_TOPIC", "medical_expenses")

# Chart setup
plt.ion()  # Turn on interactive mode
fig, ax = plt.subplots()
bars = None

# Data tracking
expense_totals = defaultdict(float)
department_counts = defaultdict(int)  # To track how many expenses we have for each department
history = deque(maxlen=20)  # Not used yet, could be useful for time-based tracking

# Function to update chart
def update_chart():
    global bars
    ax.clear()

    departments = list(expense_totals.keys())
    totals = [expense_totals[dept] for dept in departments]

    # Create bars with Seaborn color palette
    bars = ax.bar(departments, totals, color=sns.color_palette("husl", len(departments)))
    ax.set_title("Live Medical Expenses by Department")
    ax.set_ylabel("Total Cost ($)")
    ax.set_xlabel("Department")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.pause(0.1)  # Ensure that the chart updates

# Main consumer loop
def main():
    kafka_server = get_kafka_server()
    topic = get_kafka_topic()

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=kafka_server,
        auto_offset_reset='earliest',  # Start from the earliest available message
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        consumer_timeout_ms=1000  # Timeout in ms, consumer waits for messages for 1 second
    )

    print(f"✅ Listening to Kafka topic '{topic}' on {kafka_server} ...")
    
    try:
        while True:
            new_data = False

            # Check if consumer is successfully polling messages
            print("Polling for new messages...")
            for message in consumer:
                print(f"Received message: {message.value}")  # Log the message to see what's coming in
                data = message.value
                department = data.get("department")
                cost = data.get("cost")
                expense_type = data.get("expense_type")
                timestamp = data.get("timestamp")

                # Validate message data
                if department and isinstance(cost, (int, float)):
                    expense_totals[department] += cost
                    department_counts[department] += 1  # Track number of expenses for each department
                    print(f"Processed message: Department={department}, Expense Type={expense_type}, Cost={cost}, Timestamp={timestamp}")
                    new_data = True
                else:
                    print(f"Invalid data: {data}")  # Log invalid message

            if new_data:
                update_chart()

            # Add a small delay to avoid a tight loop, ensuring responsiveness and chart update
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted by user.")
    finally:
        consumer.close()
        print("Consumer closed.")
        plt.ioff()  # Disable interactive mode before showing the final plot
        plt.show()  # Ensure final plot display

# Entry point
if __name__ == "__main__":
    main()
