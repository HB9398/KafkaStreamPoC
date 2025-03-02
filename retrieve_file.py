import sqlite3
import os
import json
import shutil
import datetime

# Define output folders
retrieved_folder = os.path.expanduser("~/Documents/University of Washington/Coursework/Capstone/Capstone_Q5/KakfaStreamPOC/Retrieved files")
structured_folder = os.path.join(retrieved_folder, "Structured Data Files")
unstructured_folder = os.path.join(retrieved_folder, "Unstructured Data Files")

# Ensure the folders exist
for folder in [retrieved_folder, structured_folder, unstructured_folder]:
    if not os.path.exists(folder):
        os.makedirs(folder)

# Connect to SQLite database
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# Retrieve JSON/XML Data (Structured)
cursor.execute("SELECT id, file_name, data, received_timestamp FROM structured_data")
structured_files_retrieved = 0

for row in cursor.fetchall():
    file_id, file_name, data, received_timestamp = row
    file_path = os.path.join(structured_folder, file_name)

    # Save structured data as JSON
    with open(f"{file_path}.json", "w") as file:
        json.dump(json.loads(data), file, indent=4)

    print(f"Retrieved structured data: {file_name}.json (Saved at: {file_path}.json)")
    structured_files_retrieved += 1

# Retrieve Word/PDF Files (Unstructured)
cursor.execute("SELECT file_name, file_path, size, creation_timestamp, last_modified_timestamp FROM unstructured_data")
unstructured_files_retrieved = 0

for row in cursor.fetchall():
    file_name, file_path, size, creation_timestamp, last_modified_timestamp = row

    # Ensure the file exists in the original location
    if os.path.exists(file_path):
        destination_path = os.path.join(unstructured_folder, file_name)

        # Copy the file to retrieved folder
        shutil.copy2(file_path, destination_path)

        # Convert timestamps to datetime format
        creation_time = datetime.datetime.fromisoformat(creation_timestamp)
        modified_time = datetime.datetime.fromisoformat(last_modified_timestamp)

        # Preserve original timestamps
        os.utime(destination_path, (creation_time.timestamp(), modified_time.timestamp()))

        print(f"Retrieved: {file_name} (Copied to: {destination_path})")
        unstructured_files_retrieved += 1
    else:
        print(f"Warning: Original file '{file_path}' not found. Skipping...")

conn.close()

if structured_files_retrieved == 0 and unstructured_files_retrieved == 0:
    print("No files found in the database.")
