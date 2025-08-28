# main_ingest.py
import os
import json
import logging
from datetime import datetime, timezone
import docx

from pipeline_lib.config_loader import load_config
from pipeline_lib.db_handler import get_db_connection
from pipeline_lib.llm_handler import MetadataExtractor
from pipeline_lib.utils import setup_logging
from pipeline_lib.metadata_generator import METADATA_GENERATOR_REGISTRY

def find_instruction_file(file_path, base_path):
    """
    Busca el archivo de instrucciones de metadatos correcto utilizando un enfoque jerárquico.
    Ahora maneja tanto .txt como .docx.
    """
    # 1. Cambiar la búsqueda de .meta.json para que sea dinámica
    # Separa el nombre del archivo de su extensión y luego añade .meta.json
    base_name = os.path.splitext(file_path)[0]
    specific_meta_path = base_name + ".meta.json"
    
    if os.path.exists(specific_meta_path):
        return specific_meta_path

    # (El resto de esta función es exactamente el mismo)
    current_dir = os.path.dirname(file_path)
    norm_base_path = os.path.normpath(base_path)
    while True:
        folder_meta_path = os.path.join(current_dir, "_folder.meta.json")
        if os.path.exists(folder_meta_path):
            return folder_meta_path
        norm_current_dir = os.path.normpath(current_dir)
        if norm_current_dir == norm_base_path:
            break
        parent_dir = os.path.dirname(current_dir)
        if parent_dir == current_dir:
            break
        current_dir = parent_dir
    return None

def process_source_folder(conn, config, llm_extractor):
    base_path = config['paths']['docs_root']
    logging.info(f"--- Iniciando el procesamiento jerárquico bajo demanda en la carpeta raíz: {base_path} ---")
    items_added = 0

    for root, _, files in os.walk(base_path):
        for filename in files:
            # --- 2. Cambiar para buscar archivos .txt y .docx ---
            if not filename.endswith((".txt", ".docx")):
                continue

            file_full_path = os.path.join(root, filename)
            instruction_file_path = find_instruction_file(file_full_path, base_path)

            if not instruction_file_path:
                logging.debug(f"Omitiendo '{filename}': No se encontró un archivo de instrucciones en la jerarquía.")
                continue
            
            try:
                with open(instruction_file_path, 'r', encoding='utf-8') as f:
                    sidecar_data = json.load(f)
                active_fields = sidecar_data.get("active_fields")
                if not active_fields or not isinstance(active_fields, list):
                    logging.warning(f"Omitiendo '{filename}': El archivo de instrucciones '{instruction_file_path}' no contiene una lista válida de 'active_fields'.")
                    continue
            except Exception as e:
                logging.error(f"No se pudo leer o analizar el archivo de instrucciones '{instruction_file_path}' para '{filename}': {e}")
                continue

            logging.info(f"Procesando '{filename}' usando las instrucciones de '{os.path.basename(instruction_file_path)}'")
            
            try:
                # --- 3. Añadir una condición para elegir el método de lectura de archivos según la extensión ---
                full_content = ""
                if filename.endswith(".txt"):
                    with open(file_full_path, 'r', encoding='utf-8') as f:
                        full_content = f.read()
                elif filename.endswith(".docx"):
                    document = docx.Document(file_full_path)
                    full_content = "\n".join([p.text for p in document.paragraphs])

                source_path_for_check = os.path.relpath(file_full_path, base_path).replace(os.path.sep, '/')
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM knowledge_items WHERE (metadata->>'source_path') = %s", (source_path_for_check,))
                    if cur.fetchone():
                        logging.info(f"Omitiendo '{filename}': El elemento ya existe en la base de datos.")
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
                
                final_metadata["source_type"] = "RAG"
                final_metadata["ingest_timestamp"] = datetime.now(timezone.utc).isoformat()
                final_metadata["chunking_strategy"] = sidecar_data.get("chunking_strategy", "STRUCTURE_AWARE")
                
                with conn.cursor() as cur:
                    cur.execute(
                        """INSERT INTO knowledge_items (source_type, status, title, full_content, metadata) VALUES (%s, %s, %s, %s, %s)""",
                        ('RAG', 'active', final_metadata.get('document_title', filename), full_content, json.dumps(final_metadata, ensure_ascii=False))
                    )
                items_added += 1
                logging.info(f"Se ha ingerido '{filename}' exitosamente.")

            except Exception as e:
                logging.error(f"Fallo al procesar '{filename}': {e}", exc_info=True)
                conn.rollback()

    conn.commit()
    logging.info(f"--- Procesamiento de archivos finalizado. Se añadieron {items_added} nuevos elementos. ---")


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
            logging.info("Conexión a la base de datos cerrada.")


if __name__ == "__main__":
    main()