from abc import ABC, abstractmethod
import numpy as np


class BaseEmbedder(ABC):
    @abstractmethod
    def embed(self, text) -> np.ndarray: ...

    @abstractmethod
    def get_number_of_dimensions(self) -> int: ...
