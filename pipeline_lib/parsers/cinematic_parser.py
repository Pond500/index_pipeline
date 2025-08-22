# pipeline_lib/parsers/cinematic_parser.py
from semantic_text_splitter import TextSplitter # <--- 1. เปลี่ยนชื่อ Library ที่ import
import logging

def parse_document(content: str, metadata: dict, embedding_model) -> list:
    """
    Splits document text based on semantic similarity (thematic breaks).
    This is the CINEMATIC strategy.
    It requires the embedding model to be passed in.
    """
    if not content or not content.strip():
        return []

    logging.info("    > Applying 'CINEMATIC' (Semantic) chunking...")
    
    # --- 2. เปลี่ยนวิธีการสร้าง Chunker ---
    # ใช้ BAAI/bge-m3 ที่เราโหลดมาเป็นตัววัดความหมาย
    # max_tokens ถูกคำนวณจาก chunk_size ที่เราคุ้นเคย
    chunker = TextSplitter.from_huggingface_hub(
        "BAAI/bge-m3", 
        max_tokens=256, # เทียบเท่า chunk_size ประมาณ 1024 ตัวอักษร
        trim_chunks=False
    )
    
    # --- 3. เปลี่ยนชื่อฟังก์ชันที่เรียกใช้ ---
    raw_chunks = chunker.chunks(content)

    # จัดรูปแบบผลลัพธ์ให้ตรงกับระบบของเรา
    chunks_with_meta = []
    document_main_title = metadata.get("document_title", "")
    for chunk_text in raw_chunks:
        enriched_content = f"from topic: {document_main_title}\n\n{chunk_text}"
        meta = metadata.copy()
        meta.update({"source_section": "cinematic_segment"})
        chunks_with_meta.append((enriched_content, meta))

    return chunks_with_meta