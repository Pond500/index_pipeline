# main_ingest.py
import os
import json
import logging
from datetime import datetime, timezone

from pipeline_lib.config_loader import load_config
from pipeline_lib.db_handler import get_db_connection
from pipeline_lib.llm_handler import MetadataExtractor
from pipeline_lib.utils import setup_logging
from pipeline_lib.metadata_generator import METADATA_GENERATOR_REGISTRY

def find_instruction_file(file_path, base_path):
    """
    Searches for the correct metadata instruction file using a hierarchical approach.
    1. Checks for a file-specific .meta.json (e.g., my_doc.meta.json).
    2. If not found, checks for a folder-default _folder.meta.json in the current directory.
    3. If not found, walks up to parent directories looking for _folder.meta.json until the base_path is reached.
    """
    # 1. Check for file-specific override (e.g., 'document.meta.json')
    specific_meta_path = file_path.replace(".txt", ".meta.json")
    if os.path.exists(specific_meta_path):
        return specific_meta_path

    # 2. Check for folder-level defaults ('_folder.meta.json') up the hierarchy
    current_dir = os.path.dirname(file_path)
    # Normalize paths to ensure correct comparison
    norm_base_path = os.path.normpath(base_path)

    while True:
        folder_meta_path = os.path.join(current_dir, "_folder.meta.json")
        if os.path.exists(folder_meta_path):
            return folder_meta_path
        
        # Stop if we've reached or gone above the base path
        norm_current_dir = os.path.normpath(current_dir)
        if norm_current_dir == norm_base_path:
            break
        
        # Move up one directory
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir: # Reached the root of the filesystem
            break
        current_dir = parent_dir
        
    return None

def process_source_folder(conn, config, llm_extractor):
    """
    Processes source files (.txt) using a hierarchical on-demand metadata system.
    """
    base_path = config['paths']['docs_root']
    logging.info(f"--- Starting Hierarchical On-Demand processing in root folder: {base_path} ---")
    items_added = 0

    for root, _, files in os.walk(base_path):
        for filename in files:
            if not filename.endswith(".txt"):
                continue

            file_full_path = os.path.join(root, filename)
            
            # --- START: New Hierarchical Logic ---
            instruction_file_path = find_instruction_file(file_full_path, base_path)

            if not instruction_file_path:
                logging.debug(f"Skipping '{filename}': No instruction file (.meta.json or _folder.meta.json) found in hierarchy.")
                continue
            # --- END: New Hierarchical Logic ---

            try:
                with open(instruction_file_path, 'r', encoding='utf-8') as f:
                    sidecar_data = json.load(f)
                active_fields = sidecar_data.get("active_fields")
                if not active_fields or not isinstance(active_fields, list):
                    logging.warning(f"Skipping '{filename}': Instruction file '{instruction_file_path}' is missing a valid 'active_fields' list.")
                    continue
            except Exception as e:
                logging.error(f"Could not read or parse instruction file '{instruction_file_path}' for '{filename}': {e}")
                continue
            
            logging.info(f"Processing '{filename}' using instructions from '{os.path.basename(instruction_file_path)}'")
            
            try:
                with open(file_full_path, 'r', encoding='utf-8') as f:
                    full_content = f.read()

                source_path_for_check = os.path.relpath(file_full_path, base_path).replace(os.path.sep, '/')
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM knowledge_items WHERE (metadata->>'source_path') = %s", (source_path_for_check,))
                    if cur.fetchone():
                        logging.info(f"Skipping '{filename}': Item already exists in database.")
                        continue

                final_metadata = {}
                for field_name in active_fields:
                    generator_func = METADATA_GENERATOR_REGISTRY.get(field_name)
                    if generator_func:
                        func_args = {
                            "llm_extractor": llm_extractor,
                            "content": full_content,
                            "filename": filename,
                            "file_full_path": file_full_path,
                            "base_path": base_path,
                            "sidecar_data": sidecar_data
                        }
                        value = generator_func(**func_args)
                        if value is not None:
                            final_metadata[field_name] = value
                
                final_metadata["ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
               
            
                with conn.cursor() as cur:
                    cur.execute(
                        # --- แก้ไข SQL ให้มี 5 fields และ 5 placeholders ---
                        """INSERT INTO knowledge_items (source_type, status, title, full_content, metadata) VALUES (%s, %s, %s, %s, %s)""",
                        # --- ข้อมูลใน tuple ก็มี 5 ตัว ตรงกันพอดี ---
                        ('RAG', 'active', final_metadata.get('document_title', filename), full_content, json.dumps(final_metadata, ensure_ascii=False))
                    )
                items_added += 1
                logging.info(f"Successfully ingested '{filename}'.")

            except Exception as e:
                logging.error(f"Failed to process '{filename}': {e}", exc_info=True)
                conn.rollback()

    conn.commit()
    logging.info(f"--- File processing finished. Added {items_added} new items. ---")


def main():
    setup_logging()
    config = load_config()
    if not config: return

    conn = get_db_connection(config['database'])
    if not conn: return
        
    llm_extractor = MetadataExtractor(config['llm'])

    try:
        process_source_folder(conn, config, llm_extractor)
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")


if __name__ == "__main__":
    main()