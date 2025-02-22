import sqlite3

# Connect to SQLite database (creates file if it doesn't exist)
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# Create a table for storing Kafka messages
cursor.execute('''
    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT NOT NULL,
        topic TEXT NOT NULL,
        message BLOB,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

# Create file_metadata table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS file_metadata (
        metadata_id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER UNIQUE NOT NULL,
        file_name TEXT NOT NULL,
        size INTEGER,
        creation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        last_modified_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
    )
''')

conn.commit()
conn.close()

print("Database tables created successfully!")

