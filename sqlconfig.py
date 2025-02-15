import sqlite3

# Connect to SQLite database (creates file if it doesn't exist)
conn = sqlite3.connect("kafka_messages.db")
cursor = conn.cursor()

# Create a table for storing Kafka messages
cursor.execute('''
    CREATE TABLE IF NOT EXISTS messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        topic TEXT,
        message TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    )
''')

conn.commit()
conn.close()
