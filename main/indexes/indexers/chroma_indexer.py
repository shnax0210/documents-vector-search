import chromadb
from chromadb.config import Settings
import numpy as np
import tempfile
import shutil
import os
import pickle
from typing import List, Tuple, Optional
from datetime import datetime

from main.indexes.filter_parser import parse_filter, FilterNode, FilterCondition, FilterGroup


class ChromaIndexer:
    def __init__(self, name: str, embedder, serialized_data: Optional[bytes] = None):
        self.name = name
        self.embedder = embedder
        
        self.__temp_dir = tempfile.mkdtemp()
        self.__client = chromadb.PersistentClient(
            path=self.__temp_dir,
            settings=Settings(anonymized_telemetry=False),
        )

        self.__collection = self.__client.get_or_create_collection(
            name="documents",
            metadata={"hnsw:space": "l2"}
        )
        
        if serialized_data is not None:
            collection_data = pickle.loads(serialized_data)
            self.__add_in_batches(
                ids=collection_data["ids"],
                embeddings=collection_data["embeddings"],
                metadatas=collection_data["metadatas"]
            )

    def get_name(self) -> str:
        return self.name

    def index_texts(self, ids: np.ndarray, texts: List[str], items_metadata: list[dict] = None):
        embeddings = self.embedder.embed(texts)
        str_ids = [str(int(id_val)) for id_val in ids]

        self.__add_in_batches(
            ids=str_ids,
            embeddings=embeddings.tolist(),
            metadatas=self.__adjust_metadata(items_metadata)
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
        
        filter_expression = parse_filter(filter)

        results = self.__collection.query(
            query_embeddings=[query_embedding.tolist()],
            n_results=min(number_of_results, collection_size),
            where=self.__convert_filter_to_chroma(filter_expression)
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
    
    def __adjust_metadata(self, items_metadata: List[dict]) -> List[dict]:
        adjusted_metadata = []
        for metadata in items_metadata:
            adjusted = metadata.copy()
            if "lastModifiedAt" in metadata:
                adjusted["lastModifiedAt"] = self.__convert_iso_to_timestamp(metadata["lastModifiedAt"])
            if "createdAt" in metadata:
                adjusted["createdAt"] = self.__convert_iso_to_timestamp(metadata["createdAt"])
            
            for key, value in adjusted.items():
                if value is None:
                    adjusted[key] = "None"

            adjusted_metadata.append(adjusted)

        return adjusted_metadata
    
    def __convert_iso_to_timestamp(self, time_value: str) -> int:
        if time_value.endswith('Z'):
            time_value = time_value[:-1] + '+00:00'
        return int(datetime.fromisoformat(time_value).timestamp())
    
    __DATE_FIELDS = {"createdAt", "lastModifiedAt"}

    __OPERATOR_MAP = {
        "!=": "$ne",
        ">": "$gt",
        ">=": "$gte",
        "<": "$lt",
        "<=": "$lte",
    }

    def __convert_filter_to_chroma(self, node: FilterNode):
        if node is None:
            return None

        if isinstance(node, FilterCondition):
            return self.__condition_to_chroma(node)

        chroma_children = [self.__convert_filter_to_chroma(child) for child in node.children]

        if len(chroma_children) == 1:
            return chroma_children[0]

        logical_key = "$and" if node.logical_operator == "and" else "$or"
        return {logical_key: chroma_children}

    def __condition_to_chroma(self, condition: FilterCondition):
        value = condition.value
        if condition.field in self.__DATE_FIELDS:
            value = self.__convert_iso_to_timestamp(value)

        if condition.operator == "=":
            return {condition.field: value}
        return {condition.field: {self.__OPERATOR_MAP[condition.operator]: value}}

    def __add_in_batches(self, ids: List[str], embeddings: List[List[float]], metadatas: List[dict], batch_size: int = 5000):
        total_items = len(ids)
        for i in range(0, total_items, batch_size):
            end_idx = min(i + batch_size, total_items)
            self.__collection.add(
                ids=ids[i:end_idx],
                embeddings=embeddings[i:end_idx],
                metadatas=metadatas[i:end_idx]
            )
    
    def __del__(self):
        if os.path.exists(self.__temp_dir):
            shutil.rmtree(self.__temp_dir)
