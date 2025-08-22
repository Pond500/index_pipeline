
# post_process_metadata.py
import json
import logging

# --- 1. การตั้งค่า ---
INPUT_FILE = "/home/ai-intern02/index_pipeline/storage/metadata.json"  # <-- ไฟล์ผลลัพธ์จาก main_index.py
OUTPUT_FILE = "/home/ai-intern02/index_pipeline/storage/metadata_final.json" # <-- ไฟล์ใหม่ที่เราจะสร้าง

# --- 2. กำหนดค่า: เลือกฟิลด์ที่คุณต้องการเก็บไว้ในผลลัพธ์สุดท้าย ---
# คุณสามารถเพิ่มหรือลบฟิลด์ใน list นี้ได้ตามต้องการ
KEYS_TO_KEEP = [
    "category",
    "page_number",
    "source_path",
    "document_title"
]

def filter_metadata_fields(records: list) -> list:
    """
    ฟังก์ชันสำหรับกรอง metadata object ให้เหลือเฉพาะ key ที่กำหนดใน KEYS_TO_KEEP
    """
    processed_records = []
    for record in records:
        original_metadata = record.get('metadata', {})
        
        # สร้าง metadata object ใหม่ที่จะเก็บเฉพาะฟิลด์ที่ต้องการ
        filtered_metadata = {}
        
        # วนลูปตามรายการฟิลด์ที่ต้องการเก็บ
        for key in KEYS_TO_KEEP:
            # ถ้าเจอ key นี้ในข้อมูลเดิม ให้คัดลอกมาใส่ใน object ใหม่
            if key in original_metadata:
                filtered_metadata[key] = original_metadata[key]
        
        # แทนที่ metadata เดิมด้วย metadata ที่กรองแล้ว
        record['metadata'] = filtered_metadata
        processed_records.append(record)
        
    return processed_records


def main():
    """
    สคริปต์หลักสำหรับอ่าน, กรอง, และบันทึก metadata
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
    
    try:
        logging.info(f"Reading data from {INPUT_FILE}...")
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logging.info(f"Found {len(data)} records. Filtering metadata to keep only specified keys...")
        filtered_data = filter_metadata_fields(data)
        
        logging.info(f"Filtering complete. Saving results to {OUTPUT_FILE}...")
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(filtered_data, f, ensure_ascii=False, indent=4)
            
        logging.info(f"Successfully created the final filtered metadata file: {OUTPUT_FILE}")

    except FileNotFoundError:
        logging.error(f"Error: Input file not found at '{INPUT_FILE}'")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main()