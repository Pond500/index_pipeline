# pipeline_lib/llm_handler.py
from llama_index.llms.openai_like import OpenAILike
from llama_index.core.llms import ChatMessage, MessageRole
import json
import logging

class MetadataExtractor:
    def __init__(self, llm_config):
        self.llm = OpenAILike(
            model=llm_config['model'],
            api_base=llm_config['api_base'],
            api_key=llm_config['api_key'],
            temperature=llm_config['temperature'],
            is_chat_model=True,
            timeout=llm_config['timeout']
        )
        self.limit = llm_config['context_char_limit']
        logging.info(f"LLM Metadata Extractor initialized with model: {llm_config['model']}")

    def generate_metadata(self, content, filename):
        prompt = self._create_prompt(content[:self.limit], filename)
        messages = [ChatMessage(role=MessageRole.USER, content=prompt)]
        try:
            response = self.llm.chat(messages)
            return self._extract_json(response.message.content)
        except Exception as e:
            logging.error(f"LLM API call failed: {e}")
            return {}

    def _create_prompt(self, file_content, filename):
     return f"""คุณคือผู้เชี่ยวชาญด้านการวิเคราะห์และจัดหมวดหมู่เอกสารราชการของกรมการปกครอง (DOPA)
วิเคราะห์เนื้อหาของเอกสารต่อไปนี้อย่างละเอียด:
--- DOCUMENT CONTENT ---
{file_content}
--- END DOCUMENT CONTENT ---

**ภารกิจของคุณ:**
จากเนื้อหาข้างต้น จงสร้างข้อมูลสรุปในรูปแบบ JSON object ที่สมบูรณ์ โดยมีโครงสร้างดังนี้:

{{
  "document_title": "สร้างชื่อเอกสารที่เป็นทางการและสื่อความหมายชัดเจนที่สุดจากเนื้อหา ไม่ใช่จากชื่อไฟล์ ตัวอย่าง: 'แนวทางการปฏิบัติในการขอใบอนุญาตให้มีและใช้อาวุธปืน (แบบ ป.4)'",
  "summary": "สรุปใจความสำคัญของเอกสารนี้ภายใน 1-2 ประโยคสั้นๆ",
  "tags": [
    "คำค้นหาที่ 1",
    "คำค้นหาที่ 2",
    "คำค้นหาที่ 3",
    "คำค้นหาที่ 4",
    "คำค้นหาที่ 5"
  ]
}}

**ข้อกำหนด:**
- `document_title` ต้องมาจากเนื้อหา ไม่ใช่ชื่อไฟล์ `{filename}`
- `summary` ต้องกระชับและจับใจความสำคัญ
- `tags` ต้องเป็นคำค้นหา (Keywords) ที่ประชาชนทั่วไปน่าจะใช้ค้นหาเรื่องนี้
- ตอบกลับเป็น JSON object ที่ถูกต้องเท่านั้น ห้ามมีข้อความอื่นใดๆ นอกเหนือจากนี้
"""

    def _extract_json(self, text: str):
        try:
            start_index = text.find('{')
            end_index = text.rfind('}')
            if start_index != -1 and end_index != -1:
                json_str = text[start_index:end_index+1]
                return json.loads(json_str)
            return {}
        except Exception:
            logging.warning(f"Could not extract JSON from LLM response: {text}")
            return {}