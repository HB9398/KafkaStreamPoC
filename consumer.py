from kafka import KafkaConsumer
import sqlite3
import json
import datetime
import binascii

# Kafka Consumer Configuration
KAFKA_BROKER = "localhost:9092" # Change if running inside Docker
TOPIC = "trial-topic"

# Create Kafka consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),  # Decode JSON messages
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Connect to SQLite
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

print(f"Listening for messages on {TOPIC}...")

# Consume messages from Kafka
for message in consumer:
    kafka_message = message.value

    # Extract metadata
    file_name = kafka_message["file_name"]
    file_size = kafka_message["size"]
    creation_timestamp = kafka_message["creation_timestamp"]
    last_modified_timestamp = kafka_message["last_modified_timestamp"]
    file_content_hex = kafka_message["file_content"]

    # Convert hex string back to binary
    file_data = binascii.unhexlify(file_content_hex)

    # Insert into 'files' table
    cursor.execute("INSERT INTO files (file_name, topic, message) VALUES (?, ?, ?)",
                   (file_name, TOPIC, sqlite3.Binary(file_data)))
    file_id = cursor.lastrowid  # Get the ID of the inserted file

    # Insert into 'file_metadata' table
    cursor.execute("INSERT INTO file_metadata (file_id, file_name, size, creation_timestamp, last_modified_timestamp) VALUES (?, ?, ?, ?, ?)",
                   (file_id, file_name, file_size, creation_timestamp, last_modified_timestamp))
    
    conn.commit()

    print(f"Received and stored '{file_name}' with metadata.")

conn.close()