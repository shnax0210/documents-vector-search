from abc import ABC, abstractmethod


class BaseDocumentConverter(ABC):
    @abstractmethod
    def convert(self, document) -> list[dict]: ...

    @abstractmethod
    def get_details(self) -> dict: ...
