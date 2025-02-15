from kafka import KafkaConsumer
import sqlite3
import json

# Kafka Consumer Configuration
KAFKA_BROKER = "localhost:9092"  # Change if running inside Docker
TOPIC = "file-transfer-topic"

# Connect to Kafka
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: m.decode("utf-8"),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

# Connect to SQLite
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

print(f"Listening for messages on {TOPIC}...")

# Consume messages
for message in consumer:
    msg_value = message.value
    print(f"Received: {msg_value}")

    # Insert into SQLite
    cursor.execute("INSERT INTO messages (topic, message) VALUES (?, ?)", (TOPIC, msg_value))
    conn.commit()

# Close connection (never reached if running continuously)
conn.close()
