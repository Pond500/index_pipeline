# pipeline_lib/storage/faiss_store.py
import faiss
import numpy as np
import json
import logging
import os

class FaissStore:
    def __init__(self, config, embedding_dim=1024):
        self.index_path = config['index_path']
        self.metadata_path = config['metadata_path']
        self.embedding_dim = embedding_dim
        self.vectors = []
        self.metadata_list = []
        logging.info("FaissStore Adapter initialized.")

    def add(self, chunks_data: list):
        """Adds a list of chunks to the in-memory store."""
        logging.info(f"Adding {len(chunks_data)} chunks to Faiss in-memory store...")
        for _, chunk_text, _, embedding, metadata in chunks_data:
            self.vectors.append(embedding)
            self.metadata_list.append({
                "chunk_text": chunk_text,
                "metadata": metadata
            })

    def persist(self):
        """Builds and saves the Faiss index and metadata to files."""
        if not self.vectors:
            logging.warning("No vectors to persist for Faiss index.")
            return

        logging.info(f"Persisting Faiss index to {self.index_path}...")
        
        # สร้าง Directory ถ้ายังไม่มี
        os.makedirs(os.path.dirname(self.index_path), exist_ok=True)

        # 1. สร้างและบันทึก Faiss Index
        embeddings_np = np.array(self.vectors).astype('float32')
        index = faiss.IndexFlatIP(self.embedding_dim) # IP (Inner Product) for BGE-m3
        index.add(embeddings_np)
        faiss.write_index(index, self.index_path)

        # 2. บันทึก Metadata ที่คู่กัน
        with open(self.metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self.metadata_list, f, ensure_ascii=False, indent=4)
        
        logging.info("Successfully persisted Faiss index and metadata.")