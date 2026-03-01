import faiss
import numpy as np
from typing import Tuple

from main.indexes.indexers.base_indexer import BaseIndexer
from main.indexes.embeddings.base_embedder import BaseEmbedder


class FaissIndexer(BaseIndexer):
    def __init__(self, name, embedder: BaseEmbedder, serialized_index=None):
        self.name = name
        self.embedder = embedder
        if serialized_index is not None:
            self.faiss_index = faiss.deserialize_index(serialized_index)
        else:
            self.faiss_index = faiss.IndexIDMap(faiss.IndexFlatL2(embedder.get_number_of_dimensions()))

    def get_name(self) -> str:
        return self.name

    def index_texts(self, ids, texts, items_metadata: list[dict] = None) -> None:
        self.faiss_index.add_with_ids(self.embedder.embed(texts), np.array(ids, dtype=np.int64))

    def remove_ids(self, ids) -> None:
        self.faiss_index.remove_ids(ids)

    def serialize(self) -> bytes:
        return faiss.serialize_index(self.faiss_index)

    def search(self, text, number_of_results=10, filter=None) -> Tuple[np.ndarray, np.ndarray]:
        return self.faiss_index.search(np.expand_dims(self.embedder.embed(text), axis=0), number_of_results)

    def support_metadata(self) -> bool:
        return False

    def get_size(self) -> int:
        return self.faiss_index.ntotal