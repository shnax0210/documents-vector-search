import argparse
from abc import ABC, abstractmethod


class BaseCliOperation(ABC):
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def help(self) -> str: ...

    @abstractmethod
    def configure(self, parser: argparse.ArgumentParser) -> None: ...

    @abstractmethod
    def run(self, args: dict) -> None: ...

    def log_to_stderr(self, args: dict) -> bool:
        return False
