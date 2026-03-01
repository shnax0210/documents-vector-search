import json
from .indexers.faiss_indexer import FaissIndexer
from .indexers.chroma_indexer import ChromaIndexer
from .indexers.sqllite_indexer import SqlliteIndexer
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
    if len(parts) == 1:
        return parts[0], None
    if len(parts) == 2:
        return parts[0], parts[1]
    raise ValueError(f"Invalid indexer name format: {indexer_name}")

def __create_sentence_embedder(embedding_model):
    # Check for old name formats for backward compatibility
    model = __create_sentence_embedder_by_old_embedding_model_name(embedding_model)
    if model is not None:
        return model

    # New format allows any model name, but it should start with "embeddings_" and replace "/" with "_slash_"
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

def create_indexer(indexer_name):
    indexer_type, embedding_model = __split_indexer_name(indexer_name)

    if indexer_type == "indexer_FAISS_IndexFlatL2":
        return FaissIndexer(indexer_name, __create_sentence_embedder(embedding_model))
    
    if indexer_type == "indexer_ChromaDb":
        return ChromaIndexer(indexer_name, __create_sentence_embedder(embedding_model))

    if indexer_type == "indexer_SqlLiteBM25":
        return SqlliteIndexer(indexer_name)

    raise ValueError(f"Unknown indexer name: {indexer_name}")

def load_indexers(index_names, collection_name, persister):
    if index_names is None:
        names = __get_available_indexes(collection_name, persister)
    else:
        names = index_names
    return [load_indexer(name, collection_name, persister) for name in names]


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

    if indexer_type == "indexer_SqlLiteBM25":
        serialized_data = persister.read_bin_file(f"{collection_name}/indexes/{indexer_name}/indexer")
        return SqlliteIndexer(indexer_name, serialized_data)

    raise ValueError(f"Unknown indexer name: {indexer_name}")