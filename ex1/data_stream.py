from typing import Any
from abc import ABC, abstractmethod


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


class DataStream():
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
                f"remaining {len(self._processors)} on processor"
            )


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list):
            return all(isinstance(item, (int, float)) for item in data)
        return False

    def ingest(self, data: Any) -> None:
        self.total_processed += 1
        if self.validate(data):
            if isinstance(data, (int, float)):
                self._data.append(str(data))
            else:
                for item in data:
                    self._data.append(str(item))
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
        self.total_processed += 1
        if self.validate(data):
            if isinstance(data, str):
                self._data.append(data)
            else:
                for item in data:
                    self._data.append(item)
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
        self.total_processed += 1
        if self.validate(data):
            def d_format(d: dict[str, str]) -> str:
                return f"{d.get('log_level')}: {d.get('log_message')}"
            if isinstance(data, dict):
                self._data.append(d_format(data))
            else:
                for dict_list in data:
                    self._data.append(d_format(dict_list))
        else:
            raise ValueError("Improper log data")
    

if __name__ == "__main__":
    print("=== Code Nexus - Data Stream ===\n")
    data = [
        "Hello world",
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead',
          'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    print("Initialize Data Stream...")
    stream = DataStream()

    stream.print_processors_stats()

    print("\nRegistering Numeric Processor\n")
    num = NumericProcessor()
    txt = TextProcessor()
    log = LogProcessor()

    stream.register_processor(num)
    stream.register_processor(txt)
    stream.register_processor(log)

    print(f"Send first batch of data on stream: {data}")
    stream.process_stream(data)
    # stream.print_processors_stats()
