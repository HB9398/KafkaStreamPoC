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

# Create 'unstructured_data' table (for Word/PDF)
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

# Create 'vector_store' table (Persistent FAISS storage)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS vector_store (
        chunk_id TEXT PRIMARY KEY,
        original_doc_id TEXT NOT NULL,
        section_title TEXT,
        chunk_text TEXT NOT NULL,
        embedding_vector BLOB NOT NULL  -- Stores FAISS vector
    )
''')

conn.commit()
conn.close()

print("Database tables created successfully!")