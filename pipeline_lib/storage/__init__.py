# pipeline_lib/storage/__init__.py
from .pgvector_store import PGVectorStore
from .faiss_store import FaissStore

STORAGE_REGISTRY = {
    'PGVECTOR': PGVectorStore,
    'FAISS': FaissStore
}