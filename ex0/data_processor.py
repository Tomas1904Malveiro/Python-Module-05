from abc import ABC, abstractmethod
from typing import Any

class DataProcessor(ABC):
    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        pass

class NumericProcessor(DataProcessor):
    def __init__(self):
        self.data_save: list[str] = []
        self.count: int = 0

    def validate(self, data: Any) -> bool:
        pass

    def ingest(self, data: int | float | list) -> None:
        pass
        
class TextProcessor(DataProcessor):
    def __init__(self):
        pass

class LogProcessor(DataProcessor):
    def __init__(self):
        pass

