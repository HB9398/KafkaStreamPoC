import sqlite3

# Connect to SQLite database
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# Create 'structured_data' table (for JSON/XML)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS structured_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT NOT NULL,
        data TEXT NOT NULL,
        received_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# Create 'file_metadata' table (for Word/PDF)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS unstructured_data (
        metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT NOT NULL,
        file_path TEXT NOT NULL,
        size INTEGER,
        creation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_modified_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

conn.commit()
conn.close()

print("Database tables created successfully!")