from kafka import KafkaConsumer
import sqlite3
import json

# Kafka Consumer Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "unstructured-data-topic"

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
    file_path = data["file_path"]
    file_size = data["size"]
    creation_timestamp = data["creation_timestamp"]
    last_modified_timestamp = data["last_modified_timestamp"]

    # Insert into unstructured_data table
    cursor.execute("INSERT INTO unstructured_data (file_name, file_path, size, creation_timestamp, last_modified_timestamp) VALUES (?, ?, ?, ?, ?)",
                   (file_name, file_path, file_size, creation_timestamp, last_modified_timestamp))

    conn.commit()
    print(f"Stored file path and metadata for {file_name} in database.")

conn.close()
