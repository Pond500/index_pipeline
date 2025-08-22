# inspector_app.py
import streamlit as st
import json
import os
import pandas as pd
from collections import defaultdict

# Import library ของโปรเจกต์เรา
from pipeline_lib.config_loader import load_config
from pipeline_lib.db_handler import get_db_connection

# --- การตั้งค่า ---
CONFIG_PATH = "config.yaml"

# --- ฟังก์ชันเสริม ---

@st.cache_data # Cache a data loading to improve performance
def load_faiss_metadata(filepath):
    """Loads the Faiss metadata JSON file."""
    if not os.path.exists(filepath):
        return None
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

@st.cache_resource # Cache DB connection
def get_cached_db_connection(db_config):
    """Gets a cached database connection."""
    return get_db_connection(db_config)

def execute_query(conn, query, params=None, fetch="all"):
    """Executes a SQL query and returns the result."""
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch == "one":
                return cur.fetchone()
            else:
                return cur.fetchall()
    except Exception as e:
        st.error(f"Database query failed: {e}")
        return None

def display_chunks(title, chunks_data):
    """A reusable function to display chunks in expanders with length info."""
    if title and chunks_data:
        st.success(f"**เอกสาร:** {title}")
        st.write(f"พบ **{len(chunks_data)}** Chunks:")
        
        for i, chunk in enumerate(chunks_data):
            seq = chunk.get('chunk_sequence') if isinstance(chunk, dict) else chunk[0]
            text = chunk.get('chunk_text') if isinstance(chunk, dict) else chunk[1]
            
            # --- START: ส่วนที่แก้ไข ---
            # 1. คำนวณความยาวของ chunk_text
            chunk_length = len(text)
            
            # 2. สร้างหัวข้อใหม่ที่มีข้อมูลความยาว
            expander_title = f"Chunk #{seq} (ความยาว: {chunk_length} ตัวอักษร)"
            
            # 3. ใช้หัวข้อใหม่กับ st.expander
            with st.expander(expander_title):
            # --- END: ส่วนที่แก้ไข ---
                st.text_area(f"Chunk Content {i+1}", text, height=200, disabled=True)
                if isinstance(chunk, dict) and 'metadata' in chunk:
                    st.json(chunk['metadata'])

    elif title:
        st.warning(f"**เอกสาร:** {title}\n\nไม่พบ Chunks สำหรับเอกสารนี้")
    else:
        st.error("ไม่พบเอกสารสำหรับ ID ที่ระบุ")

# --- หน้าหลักของแอป ---

st.set_page_config(layout="wide", page_title="Index Inspector")
st.title("🔬 Universal Index Inspector")
st.markdown("---")

# 1. โหลด Config เพื่อตัดสินใจว่าจะใช้โหมดไหน
config = load_config(CONFIG_PATH)
if not config:
    st.error("ไม่พบไฟล์ config.yaml กรุณาตรวจสอบ")
    st.stop()

store_type = config.get('vector_store', {}).get('type', 'PGVECTOR')
st.info(f"โหมดการทำงานปัจจุบัน: **{store_type}** (อ่านจาก config.yaml)")

# ===================================================================
# โหมดที่ 1: ตรวจสอบจาก PostgreSQL (PGVECTOR)
# ===================================================================
if store_type == 'PGVECTOR':
    st.header("Inspecting from PostgreSQL")
    conn = get_cached_db_connection(config['database'])
    if conn:
        # แสดงตารางภาพรวม
        st.subheader("ภาพรวมเอกสารใน `knowledge_items`")
        items_df = pd.read_sql("SELECT id, title, source_type, status FROM knowledge_items ORDER BY id", conn)
        st.dataframe(items_df, use_container_width=True, hide_index=True)

        # ส่วนสำหรับดู Chunks
        st.divider()
        st.subheader("🔍 ตรวจสอบหน่วยข้อมูลย่อย (Chunk Viewer)")
        
        item_id_to_view = st.number_input(
            "ใส่ ID ของเอกสารที่ต้องการดู", min_value=1, step=1,
            help="ดู ID ได้จากตารางด้านบน"
        )
        
        if st.button("🔬 แสดง Chunks", use_container_width=True):
            if item_id_to_view > 0:
                parent_item = execute_query(conn, "SELECT title FROM knowledge_items WHERE id = %s", (item_id_to_view,), fetch="one")
                chunks = execute_query(conn, "SELECT chunk_sequence, chunk_text FROM knowledge_chunks WHERE knowledge_item_id = %s ORDER BY chunk_sequence", (item_id_to_view,), fetch="all")
                display_chunks(parent_item[0] if parent_item else None, chunks)

# ===================================================================
# โหมดที่ 2: ตรวจสอบจาก Faiss (ไฟล์ .bin และ .json)
# ===================================================================
elif store_type == 'FAISS':
    st.header("Inspecting from Faiss Files")
    metadata_path = config.get('vector_store', {}).get('faiss', {}).get('metadata_path', '')
    
    records = load_faiss_metadata(metadata_path)
    if records:
        # เตรียมข้อมูลสำหรับแสดงผล
        doc_summary = defaultdict(lambda: {'title': 'N/A', 'chunk_count': 0, 'chunks': []})
        for i, record in enumerate(records):
            doc_id = record['metadata'].get('document_id')
            if doc_id:
                doc_summary[doc_id]['title'] = record['metadata'].get('document_title', 'Title not found')
                doc_summary[doc_id]['chunk_count'] += 1
                record_for_display = record.copy()
                record_for_display['chunk_sequence'] = record['metadata'].get('chunk_sequence', i)
                doc_summary[doc_id]['chunks'].append(record_for_display)

        # แสดงตารางภาพรวม
        st.subheader("ภาพรวมเอกสารที่พบใน `metadata.json`")
        summary_df = pd.DataFrame([
            {"id": doc_id, "title": data['title'], "chunk_count": data['chunk_count']}
            for doc_id, data in doc_summary.items()
        ]).sort_values(by="id")
        st.dataframe(summary_df, use_container_width=True, hide_index=True)

        # ส่วนสำหรับดู Chunks
        st.divider()
        st.subheader("🔍 ตรวจสอบหน่วยข้อมูลย่อย (Chunk Viewer)")
        
        item_id_to_view = st.number_input(
            "ใส่ ID ของเอกสารที่ต้องการดู", min_value=1, step=1,
            help="ดู ID ได้จากตารางด้านบน"
        )

        if st.button("🔬 แสดง Chunks", use_container_width=True):
            if item_id_to_view in doc_summary:
                doc_data = doc_summary[item_id_to_view]
                # เรียงลำดับ chunk ตาม sequence ก่อนแสดงผล
                sorted_chunks = sorted(doc_data['chunks'], key=lambda x: x['chunk_sequence'])
                display_chunks(doc_data['title'], sorted_chunks)
            else:
                st.error(f"ไม่พบเอกสารสำหรับ ID: {item_id_to_view} ในไฟล์ metadata")