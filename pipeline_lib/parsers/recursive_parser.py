# pipeline_lib/parsers/recursive_parser.py

def parse_document(content: str, metadata: dict, chunk_size: int, chunk_overlap: int) -> list:
    """
    Splits document text recursively with size and overlap awareness.
    This is the upgraded RECURSIVE strategy.
    """
    if not content or not content.strip():
        return []

    # 1. เริ่มต้นด้วยการแบ่งตามย่อหน้าเป็นหลัก
    splits = content.split('\n\n')
    
    # 2. รวมประโยคเล็กๆ เข้าด้วยกัน
    merged_splits = []
    current_chunk = ""
    for split in splits:
        if len(current_chunk) + len(split) < chunk_size:
            current_chunk += "\n\n" + split
        else:
            if current_chunk:
                merged_splits.append(current_chunk.strip())
            current_chunk = split
    if current_chunk:
        merged_splits.append(current_chunk.strip())

    # 3. สร้าง Final Chunks พร้อม Overlap
    final_chunks = []
    for text in merged_splits:
        # ถ้า Chunk ที่รวมแล้วยังใหญ่ไป ก็แบ่งย่อยอีก
        if len(text) > chunk_size:
            start = 0
            while start < len(text):
                end = start + chunk_size
                chunk_text = text[start:end]
                final_chunks.append(chunk_text)
                start += chunk_size - chunk_overlap
        else:
            final_chunks.append(text)
    
    # 4. สร้าง enriched_content และ metadata สำหรับแต่ละ Chunk
    chunks_with_meta = []
    document_main_title = metadata.get("document_title", "")
    for chunk_text in final_chunks:
        enriched_content = f"จากหัวข้อ: {document_main_title}\n\n{chunk_text}"
        meta = metadata.copy()
        meta.update({"source_section": "เนื้อหาทั่วไป"})
        chunks_with_meta.append((enriched_content, meta))

    return chunks_with_meta