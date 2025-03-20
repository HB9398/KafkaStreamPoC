import sqlite3
import faiss
import numpy as np
from sentence_transformers import SentenceTransformer

# Load Hugging Face SentenceTransformer Model
model = SentenceTransformer('all-MiniLM-L6-v2')  # Matches encoding in data_chunking.py

# Load FAISS Index from SQLite
def load_faiss_from_sqlite():
    conn = sqlite3.connect("kafka_messages.db")
    cursor = conn.cursor()

    cursor.execute("SELECT rowid, embedding_vector FROM vector_store")
    rows = cursor.fetchall()

    if not rows:
        print("No vectors found in the database!")
        return None, []

    index = faiss.IndexFlatL2(384)  # Ensure dimension matches MiniLM
    row_mapping = []  # To map FAISS index -> SQLite row ID

    for row_id, vector in rows:
        index.add(np.frombuffer(vector, dtype=np.float32).reshape(1, -1))
        row_mapping.append(row_id)  # Store row ID for later retrieval

    conn.close()
    return index, row_mapping

# Function to search for similar text chunks
def search_similar_chunks(query, top_k=3):
    index, row_mapping = load_faiss_from_sqlite()
    if index is None:
        print("No FAISS index found!")
        return []

    query_vector = model.encode(query)  # Encode query
    print(f"Query Embedding Generated: {query_vector.shape}")

    distances, indices = index.search(np.array([query_vector], dtype=np.float32), top_k)
    print(f"FAISS Search Results: Indices = {indices}, Distances = {distances}")

    conn = sqlite3.connect("kafka_messages.db")
    cursor = conn.cursor()

    results = []
    for idx in indices[0]:
        if idx == -1:
            continue  # No match found

        row_id = row_mapping[idx]  # Retrieve actual SQLite row ID

        cursor.execute("SELECT original_doc_id, chunk_text FROM vector_store WHERE rowid = ?", (row_id,))
        row = cursor.fetchone()
        if row:
            results.append({"original_doc_id": row[0], "text": row[1]})

    conn.close()

    if not results:
        print("No similar chunks found.")
    
    return results

# Example Query
query_text = "What are consequences of contract breach and the team values for the capstone project?"
search_results = search_similar_chunks(query_text)

if search_results:
    for res in search_results:
        print(f"\nFound in Document ID: {res['original_doc_id']}\n{res['text']}\n{'-'*40}")
else:
    print("No matching results found!")