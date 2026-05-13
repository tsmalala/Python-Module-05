from typing import Protocol
from abc import ABC, abstractmethod
from typing import Any


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[Any] = []
        self._count: int = 0
        self.total_processed = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self._data:
            raise Exception("No data available")

        element = self._data.pop(0)
        current_count = self._count
        self._count += 1
        return (current_count, element)


class DataStream:
    def __init__(self) -> None:
        self._processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self._processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            processed = False
            for processor in self._processors:
                if processor.validate(element):
                    processor.ingest(element)
                    processed = True
                    break
            if not processed:
                print(f"DataStream error - Can't process element in stream:"
                      f"{element}")

    def print_processors_stats(self) -> None:
        print("=== DataStream statistics ===")
        if not self._processors:
            print("No processor found, no data")
            return

        for element in self._processors:
            name = element.__class__.__name__
            print(
                f"{name}:"
                f"total {element.total_processed} items processed,"
                f"remaining {len(element._data)} on processor"
            )

    def export_processor_data(self, nb: int, plugin: ExportPlugin,
                              processor: DataProcessor) -> None:

        data: list[tuple[int, str]] = []
        i = 0
        while i < nb:
            try:
                data.append(processor.output())
                i += 1
            except Exception:
                break
        plugin.process_output(data)

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for processor in self._processors:
            self.export_processor_data(nb, plugin, processor)


class CSVExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        value: list[str] = []
        for item in data:
            value.append(item[1])
        print("CSV Output:")
        print(",".join(value))


class JSONExportPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        value: dict[str, str] = {}
        for item in data:
            value[f"item_{item[0]}"] = item[1]
        print("JSON Output:")
        print(value)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            if isinstance(data, (int, float)):
                self._data.append(str(data))
                self.total_processed += 1
            else:
                for item in data:
                    self._data.append(str(item))
                    self.total_processed += 1
        else:
            raise ValueError("Improper numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list):
            return (all(isinstance(item, str) for item in data))
        return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            if isinstance(data, str):
                self._data.append(data)
                self.total_processed += 1
            else:
                for item in data:
                    self._data.append(item)
                    self.total_processed += 1
        else:
            raise ValueError("Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            return (
                all((isinstance(k, str) and isinstance(v, str))
                    for k, v in data.items())
            )
        elif isinstance(data, list):
            return all(
                isinstance(item, dict) and
                all(isinstance(k, str) and isinstance(v, str)
                    for k, v in item.items())
                for item in data
            )
        return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            def d_format(d: dict[str, str]) -> str:
                return f"{d.get('log_level')}: {d.get('log_message')}"
            if isinstance(data, dict):
                self._data.append(d_format(data))
                self.total_processed += 1
            else:
                for dict_list in data:
                    self._data.append(d_format(dict_list))
                    self.total_processed += 1
        else:
            raise ValueError("Improper log data")


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")
    data = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WRNING',
          'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    print("Initialize Data Stream...\n")
    stream = DataStream()

    stream.print_processors_stats()

    print("\nRegistering Processors")
    print(f"\nSend first batch of data on stream: {data}\n")

    num = NumericProcessor()
    text = TextProcessor()
    log = LogProcessor()
    stream.register_processor(num)
    stream.register_processor(text)
    stream.register_processor(log)
    stream.process_stream(data)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_data = CSVExportPlugin()
    stream.output_pipeline(3, csv_data)

    print()
    stream.print_processors_stats()

    other_data = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE',
          'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    print(f"\nSend another batch of data: {other_data}")
    print()
    stream.process_stream(other_data)
    stream.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_data = JSONExportPlugin()
    stream.output_pipeline(5, json_data)
    print()
    stream.print_processors_stats()
