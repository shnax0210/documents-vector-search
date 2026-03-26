from abc import ABC, abstractmethod
from typing import List, Tuple, Optional
import numpy as np


class BaseIndexer(ABC):
    @abstractmethod
    def get_name(self) -> str: ...

    @abstractmethod
    def index_texts(self, ids: np.ndarray, texts: List[str], items_metadata: list[dict] = None) -> None: ...

    @abstractmethod
    def remove_ids(self, ids: np.ndarray) -> None: ...

    @abstractmethod
    def serialize(self) -> bytes: ...

    @abstractmethod
    def search(self, text: str, number_of_results: int = 10, filter: Optional[str] = None) -> Tuple[np.ndarray, np.ndarray]: ...

    @abstractmethod
    def get_size(self) -> int: ...

    @abstractmethod
    def support_metadata(self) -> bool: ...

    def is_persistent_storage(self) -> bool:
        return False
