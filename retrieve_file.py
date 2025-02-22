import sqlite3
import os
import datetime

# Define output folder
output_folder = os.path.expanduser("~/Documents/University of Washington/Coursework/Capstone/Capstone_Q5/KakfaStreamPOC/Retrieved files")

# Ensure the output folder exists
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Connect to SQLite database
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# Query to fetch all files and their metadata
cursor.execute('''
    SELECT f.id, f.file_name, f.message, m.creation_timestamp, m.last_modified_timestamp
    FROM files f
    JOIN file_metadata m ON f.id = m.file_id
''')

# Retrieve and save files
files_retrieved = 0
for row in cursor.fetchall():
    file_id, file_name, file_data, creation_timestamp, last_modified_timestamp = row

    # Define file save path
    file_path = os.path.join(output_folder, file_name)

    # Save the binary file
    with open(file_path, "wb") as file:
        file.write(file_data)

    # Convert timestamps to datetime format
    creation_time = datetime.datetime.fromisoformat(creation_timestamp)
    modified_time = datetime.datetime.fromisoformat(last_modified_timestamp)

    # Set file metadata (creation & modified timestamps)
    os.utime(file_path, (creation_time.timestamp(), modified_time.timestamp()))

    print(f"Retrieved: {file_name} (Saved at: {file_path})")
    files_retrieved += 1

conn.close()

if files_retrieved == 0:
    print("No files found in the database.")
else:
    print(f"Successfully retrieved {files_retrieved} files!")
