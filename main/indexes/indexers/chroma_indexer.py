import json
import chromadb
import numpy as np
import tempfile
import shutil
import os
import pickle
from typing import List, Tuple, Optional


class ChromaIndexer:
    def __init__(self, name: str, embedder, serialized_data: Optional[bytes] = None):
        self.name = name
        self.embedder = embedder
        
        self.__temp_dir = tempfile.mkdtemp()
        self.__client = chromadb.PersistentClient(path=self.__temp_dir)

        self.__collection = self.__client.get_or_create_collection(
            name="documents",
            metadata={"hnsw:space": "l2"}
        )
        
        if serialized_data is not None:
            collection_data = pickle.loads(serialized_data)
            self.__collection.add(
                ids=collection_data["ids"],
                embeddings=collection_data["embeddings"],
                metadatas=collection_data["metadatas"]
            )

    def get_name(self) -> str:
        return self.name

    def index_texts(self, ids: np.ndarray, texts: List[str], metadata: Optional[dict] = None):
        embeddings = self.embedder.embed(texts)
        str_ids = [str(int(id_val)) for id_val in ids]

        if metadata is not None:
            for key, value in metadata.items():
                if not isinstance(value, (str, int, float, bool)):
                    metadata[key] = str(value)

        self.__collection.add(
            ids=str_ids,
            embeddings=embeddings.tolist(),
            metadatas=[metadata] * len(str_ids) if metadata else None
        )

    def remove_ids(self, ids: np.ndarray):
        str_ids = [str(int(id_val)) for id_val in ids]
        self.__collection.delete(ids=str_ids)

    def serialize(self) -> bytes:
        results = self.__collection.get(include=["embeddings", "metadatas"])
        collection_data = {
            "ids": results["ids"],
            "embeddings": results["embeddings"],
            "metadatas": results["metadatas"]
        }
        return pickle.dumps(collection_data)

    def search(self, text: str, number_of_results: int = 10, filter: Optional[str] = None) -> Tuple[np.ndarray, np.ndarray]:
        query_embedding = self.embedder.embed(text)
        
        collection_size = self.get_size()
        if collection_size == 0:
            return np.array([[]]), np.array([[]])
        
        results = self.__collection.query(
            query_embeddings=[query_embedding.tolist()],
            n_results=min(number_of_results, collection_size),
            where=json.loads(filter) if filter else None
        )
        
        if not results["ids"][0]:
            return np.array([[]]), np.array([[]])
        
        distances = np.array([results["distances"][0]])
        ids = np.array([[int(id_val) for id_val in results["ids"][0]]])
        
        return distances, ids
    
    def get_size(self) -> int:
        return self.__collection.count()
    
    def support_metadata(self) -> bool:
        return True
    
    def __del__(self):
        if os.path.exists(self.__temp_dir):
            shutil.rmtree(self.__temp_dir)
