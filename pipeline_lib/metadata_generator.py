# pipeline_lib/metadata_generator.py
import os
import re

# แต่ละฟังก์ชันจะรับผิดชอบการสร้าง Metadata 1 ชนิด
def get_document_title_from_llm(llm_extractor, content, filename, **kwargs): # <--- เพิ่ม , **kwargs
    """ใช้ LLM สร้าง Title"""
    data = llm_extractor.generate_metadata(content, filename)
    return data.get("document_title", filename)

def get_tags_from_llm(llm_extractor, content, filename, **kwargs): # <--- เพิ่ม , **kwargs
    """ใช้ LLM สร้าง Tags"""
    data = llm_extractor.generate_metadata(content, filename)
    return data.get("tags", [])

def get_category_from_path(file_full_path, base_path, **kwargs):
    """สร้าง Category แบบหลายระดับจากโครงสร้างโฟลเดอร์"""
    relative_path = os.path.relpath(file_full_path, base_path)
    dir_path = os.path.dirname(relative_path)
    if dir_path:
        return dir_path.split(os.path.sep)
    return []

def get_source_path(file_full_path, base_path, **kwargs):
    """สร้าง Relative Path ของไฟล์"""
    return os.path.relpath(file_full_path, base_path).replace(os.path.sep, '/')
    
def get_page_count_from_content(content, **kwargs):
    """ค้นหาเลขหน้าที่สูงสุดจาก marker '========== PAGE X ==========' ในเนื้อหา"""
    page_numbers = re.findall(r"========== PAGE (\d+) ==========", content)
    if page_numbers:
        return max(int(num) for num in page_numbers)
    return 1

def get_custom_field_from_sidecar(field_name, sidecar_data, **kwargs):
    """ฟังก์ชันกลางสำหรับดึงข้อมูลใดๆ จาก sidecar file"""
    return sidecar_data.get(field_name)

def get_document_type_from_path(file_full_path, base_path, **kwargs):
    """ดึงประเภทเอกสาร (เช่น ประกาศ, พ.ร.บ., ระเบียบ) จากชื่อโฟลเดอร์"""
    relative_path = os.path.relpath(file_full_path, base_path)
    path_parts = os.path.dirname(relative_path).split(os.path.sep)
    if len(path_parts) > 2:
        return path_parts[2]
    return "ไม่ระบุ"

# --- The Generator Registry ---
METADATA_GENERATOR_REGISTRY = {
    "document_title": get_document_title_from_llm,
    "tags": get_tags_from_llm,
    "category": get_category_from_path,
    "source_path": get_source_path,
    "page_number": get_page_count_from_content,
    "document_type": get_document_type_from_path,
    "case_number": lambda sidecar_data, **kwargs: get_custom_field_from_sidecar("case_number", sidecar_data),
    "effective_date": lambda sidecar_data, **kwargs: get_custom_field_from_sidecar("effective_date", sidecar_data),
    "department": lambda sidecar_data, **kwargs: get_custom_field_from_sidecar("department", sidecar_data),
    "version": lambda sidecar_data, **kwargs: get_custom_field_from_sidecar("version", sidecar_data),
}