# pipeline_lib/parsers/structured_parser.py
import re
import logging

def parse_document(content: str, metadata: dict, headers_to_use: list) -> list:
    """
    Splits document text based on a provided list of headers.
    The list of headers is passed in from the main script.
    """
    # ตรวจสอบว่ามีรายการ Header ส่งมาหรือไม่
    if not headers_to_use:
        logging.warning("Structured parser called but no headers were provided. Returning empty list.")
        return []

    chunks = []
    main_headers = headers_to_use
    
    # สร้าง Regex Pattern จากรายการ Header ที่ได้รับมา
    header_pattern = "|".join(f"(?m:^\\s*{re.escape(h.strip())}\\s*:?)" for h in main_headers)

    raw_sections = re.split(header_pattern, content)
    found_headers = re.findall(header_pattern, content)

    # จัดการกับเนื้อหาส่วนแรก (ก่อนเจอ Header แรก)
    if raw_sections and raw_sections[0].strip():
        title_content = raw_sections[0].strip()
        meta = metadata.copy()
        meta.update({"source_section": "เรื่องหลัก"})
        chunks.append((title_content, meta))

    document_main_title = metadata.get("document_title", "")

    # วนลูปสร้าง Chunk จากแต่ละ Section ที่หาเจอ
    for i, header_str in enumerate(found_headers):
        current_header = next((h for h in main_headers if header_str.strip().lower().startswith(h.strip().lower())), "unknown")
        section_content = raw_sections[i + 1].strip()

        if section_content:
            enriched_content = f"from topic: {document_main_title}\n\nContent in section \"{current_header}\":\n{section_content}"
            meta = metadata.copy()
            meta.update({"source_section": current_header})
            chunks.append((enriched_content, meta))
            
    return chunks