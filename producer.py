from kafka import KafkaProducer
import os
import datetime
import json
from pyspark.sql import SparkSession


# Kafka producer setup
KAFKA_BROKER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda m: json.dumps(m).encode('utf-8'))
#172.18.0.3

# Kafka Topics
STRUCTURED_TOPIC = 'structured-data-topic'  # For JSON/XML
UNSTRUCTURED_TOPIC = 'unstructured-data-topic'  # For Word/PDF

# Define the path to the Docs
folder_path = os.path.expanduser('~/Documents/University of Washington/Coursework/Capstone/Capstone_Q5/KakfaStreamPOC/Trial files')

# Ensure the folder exists
if not os.path.exists(folder_path):
    print(f"Error: Folder '{folder_path}' not found.")
    exit()

# Initialize PySpark for JSON/XML batch processing
spark = SparkSession.builder \
    .appName("BatchProcessing") \
    .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.15.0") \
    .getOrCreate()


# Iterate through all files in the folder
for file_name in os.listdir(folder_path):
    file_path = os.path.join(folder_path, file_name)

    # Skip directories, process only files
    if os.path.isfile(file_path):
        file_extension = os.path.splitext(file_name)[1].lower()

        # Process JSON/XML with PySpark
        if file_extension in ['.json', '.xml']:

            if file_extension == ".json":
                try:
                    df = spark.read.option("multiline", "true").json(file_path)
                    if df.isEmpty():
                        continue

                    structured_data = df.toJSON().collect()
                    kafka_message = {"file_name": file_name, "data": structured_data}

                    producer.send("structured-data-topic", value=kafka_message)
                    producer.flush()
                    print(f"Sent structured data from {file_name} to Kafka.")

                except Exception as e:
                    print(f"Error processing {file_name}: {e}")

            elif file_extension == ".xml":
                try:
                    df = spark.read.format("com.databricks.spark.xml").option("rowTag", "employee").load(file_path)
                    if df.isEmpty():
                        continue

                    structured_data = df.toJSON().collect()
                    kafka_message = {"file_name": file_name, "data": structured_data}

                    producer.send("structured-data-topic", value=kafka_message)
                    producer.flush()
                    print(f"Sent structured data from {file_name} to Kafka.")

                except Exception as e:
                    print(f"Error processing {file_name}: {e}")


        # For Word/PDF, send file path & metadata to Kafka
        elif file_extension in ['.docx', '.pdf']:

            file_size = os.path.getsize(file_path)
            creation_timestamp = datetime.datetime.fromtimestamp(os.path.getctime(file_path)).isoformat()
            last_modified_timestamp = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).isoformat()

            # Prepare Kafka message
            kafka_message = {
                "file_name": file_name,
                "file_path": file_path,
                "size": file_size,
                "creation_timestamp": creation_timestamp,
                "last_modified_timestamp": last_modified_timestamp
            }

            # Send message to Kafka
            producer.send(UNSTRUCTURED_TOPIC, value=kafka_message)
            producer.flush()

            print(f"Sent {file_name} metadata to `{UNSTRUCTURED_TOPIC}`")

        
    else:
        print(f"File not found: {file_path}")

print("All files processed successfully!")
