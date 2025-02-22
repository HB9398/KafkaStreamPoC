from kafka import KafkaProducer
import os
import datetime
import json

# Kafka producer setup
KAFKA_BROKER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))
#172.18.0.3

# Define the path to the Docs
folder_path = os.path.expanduser('~/Documents/University of Washington/Coursework/Capstone/Capstone_Q5/KakfaStreamPOC/Trial files')

# Ensure the folder exists
if not os.path.exists(folder_path):
    print(f"Error: Folder '{folder_path}' not found.")
    exit()

# Iterate through all files in the folder
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    # Ensure it's a file (not a folder)
    if os.path.isfile(file_path):
        # Read file in binary mode
        with open(file_path, 'rb') as file:
            file_data = file.read()

        # Get file metadata
        file_size = os.path.getsize(file_path)
        creation_timestamp = datetime.datetime.fromtimestamp(os.path.getctime(file_path)).isoformat()
        last_modified_timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()

        # Create structured Kafka message (JSON metadata + binary content)
        kafka_message = {
            "file_name": file_name,
            "size": file_size,
            "creation_timestamp": creation_timestamp,
            "last_modified_timestamp": last_modified_timestamp,
            "file_content": file_data.hex()  # Convert binary to hex string for transmission
        }

        # Send file to Kafka
        producer.send('trial-topic', value=kafka_message)
        producer.flush()

        print(f"Sent '{file_name}' to Kakfa!")

    else:
        print(f"File not found: {file_path}")


