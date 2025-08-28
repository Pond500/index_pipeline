# pipeline_lib/parsers/cinematic_parser.py
import logging
from llama_index.core.node_parser import SemanticSplitterNodeParser
from llama_index.core.schema import Document
# ไม่จำเป็นต้อง import SentenceTransformer หรือ HuggingFaceEmbedding ในไฟล์นี้

def parse_document(content: str, metadata: dict, llama_embed_adapter, breakpoint_threshold: int) -> list:
    """
    Splits document text based on semantic similarity using a pre-initialized
    LlamaIndex embedding model adapter.
    """
    if not content or not content.strip():
        return []

    logging.info(f"    > Applying 'CINEMATIC' (LlamaIndex) chunking with threshold: {breakpoint_threshold}")

    try:
        # สร้าง Splitter โดยใช้ Adapter ที่รับเข้ามาโดยตรง
        splitter = SemanticSplitterNodeParser(
            embed_model=llama_embed_adapter,
            breakpoint_percentile_threshold=breakpoint_threshold
        )

        document = Document(text=content)
        nodes = splitter.get_nodes_from_documents([document])

        # แปลงผลลัพธ์กลับเป็นรูปแบบที่เราใช้ (ส่วนนี้เหมือนเดิม)
        chunks_with_meta = []
        document_main_title = metadata.get("document_title", "")
        for node in nodes:
            chunk_text = node.get_content()
            enriched_content = f"from topic: {document_main_title}\n\n{chunk_text}"
            meta = metadata.copy()
            meta.update({"source_section": "cinematic_segment"})
            chunks_with_meta.append((enriched_content, meta))

        return chunks_with_meta
        
    except Exception as e:
        logging.error(f"Cinematic parsing with LlamaIndex failed: {e}", exc_info=True)
        return []