# main_index.py
import json
import logging
import time
import uuid
from datetime import datetime, timezone
from sentence_transformers import SentenceTransformer

from pipeline_lib.config_loader import load_config
from pipeline_lib.db_handler import get_db_connection
from pipeline_lib.utils import setup_logging
from pipeline_lib.parsers import structured_parser, recursive_parser, cinematic_parser
from pipeline_lib.storage import STORAGE_REGISTRY
from llama_index.embeddings.huggingface import HuggingFaceEmbedding # <-- เพิ่ม import นี้

def main():
    """
    Main function to run the indexing pipeline.
    This script fetches items from 'knowledge_items' that have not yet been chunked,
    processes them according to the chunking strategy defined in config.yaml,
    creates vector embeddings, and stores the final chunks in the configured vector store.
    """
    # 1. Setup and Configuration Loading
    setup_logging()
    config = load_config()
    if not config: return

    # Load chunking settings and the global strategy
    chunk_config = config.get('chunking', {})
    CHUNK_SIZE = chunk_config.get('size', 1000)
    CHUNK_OVERLAP = chunk_config.get('overlap', 200)
    GLOBAL_STRATEGY = chunk_config.get('strategy', 'RECURSIVE')

    # Load parser-specific settings
    parser_config = config.get('parser_settings', {})
    DEFAULT_HEADERS = parser_config.get('default_headers', [])
    CINEMATIC_THRESHOLD = parser_config.get('cinematic_parser', {}).get('breakpoint_percentile_threshold', 95)
    
    logging.info(f"Global chunking strategy set to: '{GLOBAL_STRATEGY}'")

    # 2. Load Embedding Model
    logging.info(f"Loading embedding model: {config['embedding']['model_name']}")
    model = SentenceTransformer(
        config['embedding']['model_name'], 
        device=config['embedding']['device']
    )
    logging.info("Model loaded successfully.")

    # --- สร้าง LlamaIndex Adapter เพียงครั้งเดียว ---
    logging.info("Creating LlamaIndex embedding adapter...")
    llama_embed_adapter = HuggingFaceEmbedding(model_name=config['embedding']['model_name'])
    logging.info("Adapter created successfully.")

    # 3. Initialize Storage Adapter
    store_config = config.get('vector_store', {})
    store_type = store_config.get('type', 'PGVECTOR')
    storage_adapter = None
    conn = get_db_connection(config['database'])
    if not conn: return
    
    logging.info(f"Initializing vector store adapter: {store_type}")
    storage_class = STORAGE_REGISTRY.get(store_type)
    
    if store_type == 'PGVECTOR':
        storage_adapter = storage_class(conn)
    elif store_type == 'FAISS':
        storage_adapter = storage_class(config['vector_store']['faiss'])
    else:
        logging.error(f"Unknown vector store type: {store_type}")
        if conn: conn.close()
        return

    try:
        with conn.cursor() as cur:
            # 4. Fetch Items to Process from PostgreSQL
            cur.execute("""
                SELECT ki.id, ki.full_content, ki.metadata
                FROM knowledge_items ki
                LEFT JOIN knowledge_chunks kc ON ki.id = kc.knowledge_item_id
                WHERE ki.status = 'active' AND kc.id IS NULL;
            """)
            items_to_process = cur.fetchall()

            if not items_to_process:
                logging.info("No new items to index. System is up-to-date.")
                return

            logging.info(f"Found {len(items_to_process)} items to process.")

            all_chunks_to_store = []

            # 5. Process Each Item
            for item_id, full_content, parent_metadata in items_to_process:
                if not full_content or not full_content.strip():
                    logging.warning(f"Skipping item ID {item_id} due to empty content.")
                    continue
                
                base_metadata = parent_metadata.copy()
                base_metadata['document_id'] = item_id
                base_metadata['chunking_strategy'] = GLOBAL_STRATEGY
                
                chunks = []
                
                if GLOBAL_STRATEGY == 'STRUCTURE_AWARE':
                    logging.info(f"  > Using 'STRUCTURE_AWARE' strategy for item ID {item_id}.")
                    headers_for_this_item = parent_metadata.get("custom_headers", DEFAULT_HEADERS)
                    if "custom_headers" in parent_metadata:
                        logging.info("    > Found and using custom headers from metadata.")
                    chunks = structured_parser.parse_document(full_content, base_metadata, headers_for_this_item)

                elif GLOBAL_STRATEGY == 'RECURSIVE':
                    logging.info(f"  > Using 'RECURSIVE' strategy for item ID {item_id}.")
                    chunks = recursive_parser.parse_document(full_content, base_metadata, CHUNK_SIZE, CHUNK_OVERLAP)
                
                elif GLOBAL_STRATEGY == 'CINEMATIC':
                    logging.info(f"  > Using 'CINEMATIC' strategy for item ID {item_id}.")
                    # --- ส่ง Adapter ที่สร้างไว้แล้วเข้าไป ---
                    chunks = cinematic_parser.parse_document(full_content, base_metadata, llama_embed_adapter, CINEMATIC_THRESHOLD)

                else:
                    logging.warning(f"  > Unknown strategy '{GLOBAL_STRATEGY}'. Defaulting to RECURSIVE.")
                    chunks = recursive_parser.parse_document(full_content, base_metadata, CHUNK_SIZE, CHUNK_OVERLAP)

                if not chunks:
                    logging.warning(f"  > No chunks were created for item ID {item_id}.")
                    continue

                # 6. Generate Embeddings in Batches
                texts_to_embed = [chunk_text for chunk_text, meta in chunks]
                logging.info(f"  > Created {len(chunks)} chunks. Generating embeddings...")
                
                embeddings = model.encode(texts_to_embed, normalize_embeddings=True)
                
                # 7. Accumulate chunks to be stored
                for i, (chunk_text, chunk_meta) in enumerate(chunks):
                    chunk_meta['chunk_id'] = str(uuid.uuid4())
                    chunk_meta['chunk_sequence'] = i + 1
                    chunk_meta['indexing_timestamp'] = datetime.now(timezone.utc).isoformat()
                    chunk_meta['schema_version'] = "2.2" # Version with performance fix
                    
                    embedding_vector = embeddings[i].tolist()
                    
                    all_chunks_to_store.append(
                        (item_id, chunk_text, i + 1, embedding_vector, chunk_meta)
                    )

        # 8. Save all accumulated chunks at once using the adapter
        if all_chunks_to_store:
            storage_adapter.add(all_chunks_to_store)
            storage_adapter.persist()
        else:
            logging.info("No new chunks were created to be stored.")

    except Exception as e:
        logging.error(f"An unexpected error occurred during indexing: {e}", exc_info=True)
        if store_type == 'PGVECTOR' and conn: conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()