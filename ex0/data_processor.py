#!/usr/bin/env python3
from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self) -> None:
        self._data: list[Any] = []
        self._count: int = 0

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
    print("=== Code Nexus - Data Processor ===\n")
    print("Testing Numeric Processor...")
    num = NumericProcessor()
    print(f"Trying to validate input '42': {num.validate(42)}")
    print(f"Trying to validate input 'Hello': {num.validate('Hello')}")
    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        num.ingest("foo")
    except Exception as e:
        print(f"Got exception: {e}")
    print("Processing data: [1, 2, 3, 4, 5]")
    num.ingest([1, 2, 3, 4, 5])
    print("Extracting 3 values...")
    i = 0
    while i < 3:
        numeric_value = num.output()
        print(f"Numeric value {numeric_value[0]}: {numeric_value[1]}")
        i += 1
    print("\nTesting Text Processor...")
    text = TextProcessor()
    print(f"Trying to validate input '42': {text.validate(42)}")
    print("Processing data: ['Hello', 'Nexus', 'World']")
    text.ingest(["Hello", "Nexus", "World"])
    print("Extracting 1 values...")
    text_value = text.output()
    print(f"Text value {text_value[0]}: {text_value[1]}")
    log = LogProcessor()
    print("\nTesting Log Processor...")
    print(f"Trying to validate input 'Hello': {log.validate('Hello')}")
    print("Processing data: [{'log_level': 'NOTICE', 'log_message':" +
          "'Connection to server'}, {'log_level': 'ERROR', 'log_message':" +
          "'Unauthorized access!!'}]")
    log.ingest([{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
                {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
                ])
    print("Extracting 2 values...")
    i = 0
    while i < 2:
        log_value = log.output()
        print(f"Log entry {log_value[0]}: {log_value[1]}")
        i += 1
