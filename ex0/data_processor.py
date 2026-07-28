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
        if not self.data_save:
            raise IndexError("No data available to output")
        rank = self.count - len(self.data_save)
        value = self.data_save.pop(0)
        return (rank, value)


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data_save: list[str] = []
        self.count: int = 0

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)) and not isinstance(data, bool):
            return True
        if isinstance(data, list):
            return all(
                isinstance(item, (int, float)) and not isinstance(item, bool)
                for item in data
            )
        return False

    def ingest(self, data: int | float | list) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self.data_save.append(str(item))
            self.count += 1


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data_save: list[str] = []
        self.count: int = 0

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            return all(isinstance(item, str) for item in data)
        return False

    def ingest(self, data: str | list) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            self.data_save.append(item)
            self.count += 1


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data_save: list[str] = []
        self.count: int = 0

    def validate(self, data: Any) -> bool:
        def is_log_dict(d: Any) -> bool:
            return (
                isinstance(d, dict)
                and all(
                    isinstance(k, str) and isinstance(v, str)
                    for k, v in d.items()
                )
            )

        if is_log_dict(data):
            return True
        if isinstance(data, list):
            return all(is_log_dict(item) for item in data)
        return False

    def ingest(self, data: dict | list) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")
        items = data if isinstance(data, list) else [data]
        for item in items:
            entry = f"{item['log_level']}: {item['log_message']}"
            self.data_save.append(entry)
            self.count += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")
    print("")
    print("Testing Numeric Processor...")
    numeric = NumericProcessor()
    print(f"Trying to validate input '42': {numeric.validate(42)}")
    print(f"Trying to validate input 'Hello': {numeric.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        numeric.ingest("foo")
    except ValueError as e:
        print(f"Got exception: {e}")

    print("Processing data: [1, 2, 3, 4, 5]")
    numeric.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    for i in range(3):
        rank, value = numeric.output()
        print(f"Numeric value {rank}: {value}")

    print("")
    print("Testing Text Processor...")
    text = TextProcessor()
    print(f"Trying to validate input '42': {text.validate(42)}")

    print("Processing data: ['Hello', 'Nexus', 'World']")
    text.ingest(['Hello', 'Nexus', 'World'])
    print("Extracting 1 value...")
    for i in range(1):
            rank, value = text.output()
            print(f"Text value {rank}: {value}")

    print("")
    print("Testing Log Processor...")
    log = LogProcessor()
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")

    print("Processing data: [{'log_level': 'NOTICE', 'log_message': "
      "'Connection to server'}, {'log_level': 'ERROR', 'log_message': "
      "'Unauthorized access!!'}]")
    
    log.ingest([
    {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
    {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'},
    ])
    print("Extracting 2 values...")
    for i in range(2):
            rank, value = log.output()
            print(f"Log entry {rank}: {value}")
