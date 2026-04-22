import json
import os
import threading
from typing import List
from .indexers.base_indexer import BaseIndexer
from .indexers.faiss_indexer import FaissIndexer
from .indexers.chroma_indexer import ChromaIndexer
from .indexers.sqllite_indexer import SqlliteIndexer
from .embeddings.base_embedder import BaseEmbedder
from .embeddings.sentence_embeder import SentenceEmbedder

__embedder_cache: dict[str, BaseEmbedder] = {}
__embedder_cache_lock = threading.Lock()

def __get_available_indexes(collection_name, persister):
    manifest_path = f"{collection_name}/manifest.json"
    if not persister.is_path_exists(manifest_path):
        raise ValueError(f"Manifest file not found for collection '{collection_name}'")
    
    manifest_content = persister.read_text_file(manifest_path)
    manifest = json.loads(manifest_content)
    
    indexers = manifest.get("indexers", [])
    if not indexers:
        raise ValueError(f"No indexes found for collection '{collection_name}'")
    
    return [indexer["name"] for indexer in indexers]

def __split_indexer_name(indexer_name):
    parts = indexer_name.split("__")
    if len(parts) == 1:
        return parts[0], None
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Invalid indexer name format: {indexer_name}")

def __create_sentence_embedder(embedding_model) -> BaseEmbedder:
    if embedding_model in __embedder_cache:
        return __embedder_cache[embedding_model]

    with __embedder_cache_lock:
        if embedding_model not in __embedder_cache:
            __embedder_cache[embedding_model] = __create_sentence_embedder_uncached(embedding_model)

    return __embedder_cache[embedding_model]

def __create_sentence_embedder_uncached(embedding_model) -> BaseEmbedder:
    model = __create_sentence_embedder_by_old_embedding_model_name(embedding_model)
    if model is not None:
        return model

    parsed_model_name = embedding_model.replace("embeddings_", "").replace("_slash_", "/")
    return SentenceEmbedder(model_name=parsed_model_name)

def __create_sentence_embedder_by_old_embedding_model_name(embedding_model):
    if embedding_model == "embeddings_all-MiniLM-L6-v2":
        return SentenceEmbedder(model_name="sentence-transformers/all-MiniLM-L6-v2")
    
    if embedding_model == "embeddings_all-mpnet-base-v2":
        return SentenceEmbedder(model_name="sentence-transformers/all-mpnet-base-v2")
    
    if embedding_model == "embeddings_multi-qa-distilbert-cos-v1":
        return SentenceEmbedder(model_name="sentence-transformers/multi-qa-distilbert-cos-v1")

    if embedding_model == "embeddings_bge-m3":
        return SentenceEmbedder(model_name="BAAI/bge-m3")
    
    return None

def create_indexer(indexer_name, collection_name=None, persister=None) -> BaseIndexer:
    indexer_type, embedding_model = __split_indexer_name(indexer_name)

    if indexer_type == "indexer_FAISS_IndexFlatL2":
        return FaissIndexer(indexer_name, __create_sentence_embedder(embedding_model))
    
    if indexer_type == "indexer_ChromaDb":
        storage_path = __build_storage_path(indexer_name, collection_name, persister)
        return ChromaIndexer(indexer_name, __create_sentence_embedder(embedding_model), storage_path)

    if indexer_type == "indexer_SqlLiteBM25":
        return SqlliteIndexer(indexer_name)

    raise ValueError(f"Unknown indexer name: {indexer_name}")

def __build_storage_path(indexer_name, collection_name, persister):
    return persister.get_absolute_path(f"{collection_name}/indexes/{indexer_name}/storage")

def load_indexers(index_names, collection_name, persister) -> List[BaseIndexer]:
    if index_names is None:
        names = __get_available_indexes(collection_name, persister)
    else:
        names = index_names
    return [load_indexer(name, collection_name, persister) for name in names]


def load_indexer(indexer_name, collection_name, persister) -> BaseIndexer:
    if indexer_name is None:
        available_indexes = __get_available_indexes(collection_name, persister)
        
        if len(available_indexes) > 1:
            raise ValueError(
                f"Multiple indexes found for collection '{collection_name}': {', '.join(available_indexes)}. "
                f"Please specify which index to use."
            )
        
        indexer_name = available_indexes[0]
    
    indexer_type, embedding_model = __split_indexer_name(indexer_name)

    if indexer_type == "indexer_FAISS_IndexFlatL2":
        serialized_index = persister.read_bin_file(f"{collection_name}/indexes/{indexer_name}/indexer")
        return FaissIndexer(indexer_name, __create_sentence_embedder(embedding_model), serialized_index)
    
    if indexer_type == "indexer_ChromaDb":
        storage_path = __build_storage_path(indexer_name, collection_name, persister)
        storage_dir_exists = os.path.isdir(storage_path)

        if storage_dir_exists:
            return ChromaIndexer(indexer_name, __create_sentence_embedder(embedding_model), storage_path)

        serialized_data = persister.read_bin_file(f"{collection_name}/indexes/{indexer_name}/indexer")
        return ChromaIndexer(indexer_name, __create_sentence_embedder(embedding_model), storage_path, serialized_data)

    if indexer_type == "indexer_SqlLiteBM25":
        serialized_data = persister.read_bin_file(f"{collection_name}/indexes/{indexer_name}/indexer")
        return SqlliteIndexer(indexer_name, serialized_data)

    raise ValueError(f"Unknown indexer name: {indexer_name}")