from abc import ABC, abstractmethod
from typing import Any, Protocol


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        print(",".join(value for _, value in data))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        pairs = ", ".join(f'"item_{rank}": "{value}"' for rank, value in data)
        print("{" + pairs + "}")


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
        ...

    def print_processors_stats(self) -> None:
        ...

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in self.processors:
            data: list[tuple[int, str]] = []
            for _ in range(nb):
                try:
                    data.append(proc.output())
                except IndexError:
                    break
            plugin.process_output(data)


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
    print("=== Code Nexus - Data Pipeline ===")
    print("")
    print("Initialize Data Stream...")

    stream = DataStream()
    stream.print_processors_stats()

    print("")
    print("Registering Processors")
    print("")
    numeric = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(numeric)
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

    stream.process_stream(batch)
    print(f"Send first batch of data on stream: {batch}")

    print("Send 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVExportPlugin()
    stream.output_pipeline(3, csv_plugin)
    stream.print_processors_stats()

    json_plugin = JSONExportPlugin()
    stream.output_pipeline(5, json_plugin)