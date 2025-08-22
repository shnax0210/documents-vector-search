from .indexers.faiss_indexer import FaissIndexer
from .embeddings.sentence_embeder import SentenceEmbedder

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
    
    raise ValueError(f"Unknown embedding model: {embedding_model}")

def create_indexer(indexer_name):
    indexer_type, embedding_model = __split_indexer_name(indexer_name)

    if indexer_type == "indexer_FAISS_IndexFlatL2":
        return FaissIndexer(indexer_name, __create_sentence_embedder(embedding_model))

    raise ValueError(f"Unknown indexer name: {indexer_name}")

def load_indexer(indexer_name, collection_name, persister):
    indexer_type, embedding_model = __split_indexer_name(indexer_name)

    if indexer_type == "indexer_FAISS_IndexFlatL2":
        serialized_index = persister.read_bin_file(f"{collection_name}/indexes/{indexer_name}/indexer")
        return FaissIndexer(indexer_name, __create_sentence_embedder(embedding_model), serialized_index)

    raise ValueError(f"Unknown indexer name: {indexer_name}")