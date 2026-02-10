#!/usr/bin/env python3
"""
CODE NEXUS: Data Processor Foundation

This module implements the foundational architecture for a polymorphic data
processing system.
It defines an Abstract Base Class (ABC) `DataProcessor` that enforces a strict
interface for validation and processing.
Implementations(Numeric, Text, Log)demonstrate method overriding
and subtype polymorphism,
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataProcessor(ABC):
    """Abstract base class defining the interface for data processors."""
    def __init__(self) -> None:
        '''Initialize the DataProcessor base class.'''
        super().__init__()

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
        if not result:
            return ""
        return result[0].upper() + result[1:]


class NumericProcessor(DataProcessor):
    '''Concrete implementation of DataProcessor for numeric data.'''
    def __init__(self) -> None:
        '''Initialize the NumericProcessor.'''
        super().__init__()

    def process(self, data: Any) -> str:
        '''Process numeric data to calculate sum and average.
        Args:
            data: A single number or a list of numbers to be processed.
        Returns:
            A formatted string with the count, sum, and average of the numbers.
        '''
        work_data = data if isinstance(data, List) else [data]

        nb_total = len(work_data)
        if nb_total == 0:
            raise ValueError("Cannot calculate average: data list is empty")

        sum_total = sum(work_data)
        avg = sum_total / nb_total

        stats: Dict[str, Union[int, float]] = {
            "sum": sum_total,
            "avg": avg,
            "count": nb_total
        }

        return (f"processed {stats['count']} numeric values, "
                f"sum={stats['sum']}, "
                f"avg={stats['avg']:.1f}")

    def validate(self, data: Any) -> bool:
        '''Validate that the input data is either a number or a list of
        numbers.
        Args:
            data: The input data to be validated.
        Returns:
            True if the data is valid for numeric processing, False otherwise.
        '''
        if isinstance(data, (int, float)):
            return True

        return isinstance(data, list) and all(
            isinstance(nb, (int, float)) for nb in data)

    def format_output(self, result: str) -> str:
        '''Override the base class method to add a prefix for numeric results.
        Args:
            result: The raw result string from the process method.
        Returns:
            A formatted string with a "Numeric Result: " prefix.
        '''
        base_formatted = super().format_output(result)
        return f"Numeric Result: {base_formatted}"


class TextProcessor(DataProcessor):
    '''Concrete implementation of DataProcessor for text data.'''
    def __init__(self) -> None:
        '''Initialize the TextProcessor.'''
        super().__init__()

    def process(self, data: Any) -> str:
        '''Process text data to count characters and words.
        Args:
            data: A string of text to be processed.
        Returns:
            A formatted string with the count of characters and words in
            the text.
        '''
        nb_characters = len(data)
        nb_words = len(data.split())
        return (f"processed text: {nb_characters} characters, "
                f"{nb_words} words")

    def validate(self, data: Any) -> bool:
        '''Validate that the input data is a string.
        Args:
            data: The input data to be validated.
        Returns:
            True if the data is a string, False otherwise.
        '''
        return isinstance(data, str)

    def format_output(self, result: str) -> str:
        '''Override the base class method to add a prefix for text results.
        Args:
            result: The raw result string from the process method.
        Returns:
            A formatted string with a "Text Result: " prefix.
        '''
        base_formatted = super().format_output(result)
        return f"{base_formatted}"


class LogProcessor(DataProcessor):
    '''Concrete implementation of DataProcessor for log entries.'''

    def __init__(self) -> None:
        '''Initialize the LogProcessor.'''
        super().__init__()

    def process(self, data: Any) -> str:
        '''Process log data to extract log level and message.
        Args:
            data: A string representing a log entry, expected to contain a
            log level and message separated by a colon.
        Returns:
            A formatted string indicating the log level and message content.
        '''

        words = data.split(":", 1)
        level = words[0].strip()
        if len(words) <= 1 or not words[1].strip():
            message = "No message content"
        else:
            message = words[1].strip()

        tag = "ALERT" if level == "ERROR" else level
        return f"[{tag}] {level} level detected: {message}"

    def validate(self, data: Any) -> bool:
        '''Validate that the input data is a string containing a log level and
        message separated by a colon.
        Args:
            data: The input data to be validated.
        Returns:
            True if the data is a valid log entry, False otherwise.
        '''
        return isinstance(data, str) and ":" in data

    def format_output(self, result: str) -> str:
        '''Override the base class method to add a prefix for log results.
        Args:
            result: The raw result string from the process method.
        Returns:
            A formatted string with a "Log Result: " prefix.
        '''
        base_formatted = super().format_output(result)
        return f"{base_formatted}"


def polymorphic_demo() -> None:
    '''Demonstrates polymorphic processing of different data types through a
    unified interface.'''

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")
    mixed_stream: List[tuple[DataProcessor, Any]] = [
        (NumericProcessor(), [1, 2, 3]),
        (TextProcessor(), "Hello World!"),
        (LogProcessor(), "INFO: System ready")
    ]
    for index, (proc, data) in enumerate(mixed_stream, 1):
        result: Optional[str] = None
        try:
            if proc.validate(data):
                result = proc.format_output(proc.process(data))
            print(f"Result {index}: {result}")
        except Exception as e:
            print(f"Result {index}: System Error {e}")
    print("\nFoundation systems online. Nexus ready for advanced streams.")


def data_processor_foundation() -> None:
    '''Initializes and demonstrates the DataProcessor framework with various
    data types'''

    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===\n")
    tasks = [
        (NumericProcessor(), [1, 2, 3, 4, 5]),
        (TextProcessor(),    "Hello Nexus World"),
        (LogProcessor(),     "ERROR: Connection timeout")
    ]
    for processor, data in tasks:
        print(f"Initializing {processor.__class__.__name__}...")
        print(f"Processing data: {repr(data)}")
        try:
            if not processor.validate(data):
                raise ValueError("Invalid data format for this processor")

            type_name = processor.__class__.__name__.replace("Processor", "")
            suffix = "entry" if type_name == "Log" else "data"
            print(f"Validation: {type_name} {suffix} verified")

            result = processor.process(data)
            result = processor.format_output(result)
            print(f"Output: {result}\n")
        except ValueError as e:
            print(f"Error: {e}\n")
        except Exception as e:
            print(f"Critical system Error: {e}\n")


if __name__ == "__main__":
    data_processor_foundation()
    polymorphic_demo()
