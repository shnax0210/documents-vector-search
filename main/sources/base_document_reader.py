from abc import ABC, abstractmethod
from typing import Generator


class BaseDocumentReader(ABC):
    @abstractmethod
    def read_all_documents(self) -> Generator: ...

    @abstractmethod
    def get_number_of_documents(self) -> int: ...

    @abstractmethod
    def get_reader_details(self) -> dict: ...
