from kafka import KafkaConsumer
import sqlite3
import json

# Kafka Consumer Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "structured-data-topic"

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Connect to SQLite database
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

print(f"Listening for messages on {TOPIC}...")

# Consume messages from Kafka
for message in consumer:
    data = message.value  # Extract JSON message

    file_name = data["file_name"]
    structured_data = json.dumps(data["data"])  # Store as JSON string

    # Insert into structured_data table
    cursor.execute("INSERT INTO structured_data (file_name, data) VALUES (?, ?)",
                   (file_name, structured_data))

    conn.commit()
    print(f"Stored data for {file_name} in database.")

conn.close()
