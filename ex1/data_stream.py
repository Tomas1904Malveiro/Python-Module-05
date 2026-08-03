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


class DataStream:
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(element):
                    proc.ingest(element)
                    processed = True
                    break
            if not processed:
                print(f"DataStream error - Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            print(
                f"{proc.name}: total {proc.count} items processed, "
                f"remaining {len(proc.data_save)} on processor"
            )


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        self.data_save: list[str] = []
        self.count: int = 0
        self.name: str = "Numeric Processor"

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
        self.name: str = "Text Processor"

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
        self.name: str = "Log Processor"

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
    print("=== Code Nexus - Data Stream ===")
    print("")
    print("Initialize Data Stream...")

    stream = DataStream()
    stream.print_processors_stats()

    print("")
    print("Registering Numeric Processor")
    print("")
    numeric = NumericProcessor()
    stream.register_processor(numeric)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING",
             "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO",
             "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("")
    print("Registering other data processors")
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(text)
    stream.register_processor(log)

    batch = [
        "Hello world",
        [3.14, -1, 2.71],
        [
            {"log_level": "WARNING",
             "log_message": "Telnet access! Use ssh instead"},
            {"log_level": "INFO",
             "log_message": "User wil is connected"},
        ],
        42,
        ["Hi", "five"],
    ]

    print("Send the same batch again")
    stream.process_stream(batch)
    stream.print_processors_stats()
    print("")

    print("Consume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for i in range(3):
        numeric.output()
    for i in range(2):
        text.output()
    for i in range(1):
        log.output()
    stream.print_processors_stats()
