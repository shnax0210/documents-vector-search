from abc import ABC, abstractmethod
from typing import List


class BasePersister(ABC):
    @abstractmethod
    def save_text_file(self, data: str, file_path: str) -> None: ...

    @abstractmethod
    def read_text_file(self, file_path: str) -> str: ...

    @abstractmethod
    def save_bin_file(self, data, file_path: str) -> None: ...

    @abstractmethod
    def read_bin_file(self, file_path: str) -> bytes: ...

    @abstractmethod
    def create_folder(self, folder_name: str) -> None: ...

    @abstractmethod
    def remove_folder(self, folder_name: str) -> None: ...

    @abstractmethod
    def remove_file(self, file_path: str) -> None: ...

    @abstractmethod
    def is_path_exists(self, relative_path: str) -> bool: ...

    @abstractmethod
    def read_folder_files(self, relative_path: str) -> List[str]: ...
