from abc import ABC, abstractmethod
from typing import List


class BaseTextSplitter(ABC):
    @abstractmethod
    def split_text(self, text: str) -> List[str]: ...

    @abstractmethod
    def get_details(self) -> dict: ...
