#!/usr/bin/env python3
"""
CODE NEXUS: Data Processor Foundation

This module implements the foundational architecture for a polymorphic data
processing system.
It defines an Abstract Base Class (ABC) `DataProcessor` that enforces a strict
interface for validation and processing. Concrete implementations
(Numeric, Text, Log)demonstrate method overriding and subtype polymorphism,
allowing heterogeneous data streams to be handled through a unified API with
comprehensive type safety.
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):
    def __init__(self) -> None:
        pass

    @abstractmethod
    def process(self, data: Any) -> str:
        '''Process the input data and return a formatted string result.
        Args:
            data: The input data to be processed, can be of any type.
        Returns:
            A string representing the processed result.
        '''
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        '''Validate the input data to ensure it meets the requirements.
        Args:
            data: The input data to be validated, can be of any type.
        Returns:
            True if the data is valid for processing, False otherwise.
        '''
        pass

    def format_output(self, result: str) -> str:
        '''Format the processed result for consistent output.
        Args:
            result: The raw result string from the process method.
        Returns:
            A formatted string ready for display or further use.
        '''
        return result


class NumericProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def process(self, data: Any) -> str:
        work_data = data if isinstance(data, List) else [data]

        nb_total = len(work_data)
        if nb_total == 0:
            raise ValueError("Cannot calculate average: data list is empty")

        sum_total = sum(data)
        avg = sum_total / nb_total

        stats: Dict[str, Union[int, float]] = {
            "sum": sum_total,
            "avg": avg,
            "count": nb_total
        }

        return (f"Processed {stats['count']} numeric values, "
                f"sum={stats['sum']}, "
                f"avg={stats['avg']:.1f}")

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True

        return isinstance(data, list) and all(
            isinstance(nb, (int, float)) for nb in data)


class TextProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def process(self, data: Any) -> str:
        nb_characters = len(data)
        nb_words = len(data.split())
        return (f"Processed text: {nb_characters} characters, "
                f"{nb_words} words")

    def validate(self, data: Any) -> bool:
        return isinstance(data, str)


class LogProcessor(DataProcessor):
    def __init__(self) -> None:
        super().__init__()

    def process(self, data: Any) -> str:
        words = data.split(":", 1)
        level = words[0].strip()
        message = words[1].strip()
        tag = "ALERT" if level == "ERROR" else level
        return f"[{tag}] {level} level detected: {message}"

    def validate(self, data: Any) -> bool:
        return isinstance(data, str) and ":" in data


def polymorphic_demo() -> None:
    '''Demonstrates polymorphic processing of different data types through a
    unified interface.'''

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    mixed_stream = List[tuple[DataProcessor, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World!"),
        (LogProcessor(), "INFO: System ready")
    ]
    for index, (proc, data) in enumerate(mixed_stream, 1):
        result: Optional[str] = None
        try:
            if proc.validate(data):
                result = proc.process(data)
            print(f"Result {index}: {result}")
        except Exception as e:
            print(f"Result {index}: System Error - {e}")
    print("\nFoundation systems online. Nexus ready for advanced streams.")


def data_processor_foundation() -> None:
    '''Initializes and demonstrates the DataProcessor framework with various
    data types'''

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor(),
    ]
    data_sample: List[Any] = [
        [1, 2, 3, 4, 5],
        "Hello Nexus World",
        "ERROR: Connection timeout"
    ]
    for proc, data in zip(processors, data_sample):
        print(f"Initializing {proc.__class__.__name__}...")
        print(f"Processing data: {repr(data)}")
        try:
            if not proc.validate(data):
                raise ValueError("Invalid data format for this processor")
            type_name = proc.__class__.__name__.replace("Processor", "")
            suffix = "entry" if type_name == "Log" else "data"
            print(f"Validation: {type_name} {suffix} verified")

            result = proc.process(data)
            formatted = proc.format_output(result)

            print(f"Output: {formatted}\n")

        except ValueError as e:
            print(f"Error: {e}\n")
        except Exception as e:
            print(f"Critical system Error: {e}\n")


if __name__ == "__main__":
    data_processor_foundation()
    polymorphic_demo()
