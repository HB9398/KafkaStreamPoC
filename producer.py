from kafka import KafkaProducer
import os

# Kafka producer setup
producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092')
#172.18.0.3

# Define the path to the Word file
file_path = os.path.expanduser('~/Downloads/IMT565CL.docx')  

# Check if the file exists
if os.path.exists(file_path):
    # Open and read the file in binary mode
    with open(file_path, 'rb') as file:
        file_data = file.read()

    # Send the binary file content to Kafka
    producer.send('file-transfer-topic', value=file_data)
    producer.flush()
    print(f"Sent file content from: {file_path}")
else:
    print(f"File not found at: {file_path}")
