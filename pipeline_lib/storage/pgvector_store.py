# pipeline_lib/storage/pgvector_store.py
import json
import logging

class PGVectorStore:
    def __init__(self, db_connection):
        self.conn = db_connection
        logging.info("PGVectorStore Adapter initialized.")

    def add(self, chunks_data: list):
        """Adds a list of chunks to the PostgreSQL database."""
        logging.info(f"Adding {len(chunks_data)} chunks to PostgreSQL...")
        
        with self.conn.cursor() as cur:
            for item_id, chunk_text, seq, embedding, metadata in chunks_data:
                cur.execute(
                    """
                    INSERT INTO knowledge_chunks (knowledge_item_id, chunk_text, chunk_sequence, embedding, metadata)
                    VALUES (%s, %s, %s, %s, %s);
                    """,
                    (item_id, chunk_text, seq, embedding, json.dumps(metadata, ensure_ascii=False))
                )
        self.conn.commit()
        logging.info("Successfully added chunks to PostgreSQL.")

    def persist(self):
        # For PostgreSQL, data is persisted on commit, so this does nothing.
        logging.info("PostgreSQL data is already persisted. Nothing to do.")