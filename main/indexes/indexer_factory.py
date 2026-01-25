import json
from .indexers.faiss_indexer import FaissIndexer
from .indexers.chroma_indexer import ChromaIndexer
from .embeddings.sentence_embeder import SentenceEmbedder

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
    if len(parts) != 2:
        raise ValueError(f"Invalid indexer name format: {indexer_name}")
    indexer_type, embedding_model = parts
    return indexer_type, embedding_model

def __create_sentence_embedder(embedding_model):
    if embedding_model == "embeddings_all-MiniLM-L6-v2":
        return SentenceEmbedder(model_name="sentence-transformers/all-MiniLM-L6-v2")
    
    if embedding_model == "embeddings_all-mpnet-base-v2":
        return SentenceEmbedder(model_name="sentence-transformers/all-mpnet-base-v2")
    
    if embedding_model == "embeddings_multi-qa-distilbert-cos-v1":
        return SentenceEmbedder(model_name="sentence-transformers/multi-qa-distilbert-cos-v1")

    if embedding_model == "embeddings_bge-m3":
        return SentenceEmbedder(model_name="BAAI/bge-m3")

    raise ValueError(f"Unknown embedding model: {embedding_model}")

def create_indexer(indexer_name):
    indexer_type, embedding_model = __split_indexer_name(indexer_name)

    if indexer_type == "indexer_FAISS_IndexFlatL2":
        return FaissIndexer(indexer_name, __create_sentence_embedder(embedding_model))
    
    if indexer_type == "indexer_ChromaDb":
        return ChromaIndexer(indexer_name, __create_sentence_embedder(embedding_model))

    raise ValueError(f"Unknown indexer name: {indexer_name}")

def load_indexer(indexer_name, collection_name, persister):
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
        serialized_data = persister.read_bin_file(f"{collection_name}/indexes/{indexer_name}/indexer")
        return ChromaIndexer(indexer_name, __create_sentence_embedder(embedding_model), serialized_data)

    raise ValueError(f"Unknown indexer name: {indexer_name}")