#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol

class ProcessingStage(Protocol):
    """
    Protocol defining the interface for processing stages.
    Any class implementing process(data) -> Any satisfies this protocol.
    """
    def process(self, data: Any) -> Any:
        ...

class InputStage:
    """
    Stage 1: Simulates input validation and parsing.
    """
    def process(self, data: Any) -> Any:
        # Example logic: ensure data isn't empty
        if not data:
            raise ValueError("InputStage: Data cannot be empty.")
        return data


class TransformStage:
    """
    Stage 2: Simulates data transformation and enrichment.
    """
    def process(self, data: Any) -> Any:
        # Example logic: wrapper or modification
        if isinstance(data, str):
            return f"Transformed: {data}"
        return data


class OutputStage:
    """
    Stage 3: Simulates output formatting and delivery.
    """
    def process(self, data: Any) -> Any:
        # Example logic: final formatting
        return f"Output: {data}"


class ProcessingPipeline(ABC):
    """
    Abstract base class managing stages and orchestrating data flow.
    """
    def __init__(self):
        self.stages: List[Any] = []

    def add_stage(self, stage: Any) -> None:
        """
        Adds a processing stage to the pipeline.

        Args:
            stage: An object adhering to the ProcessingStage protocol (must have a process method).
        """
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Any:
        """
        Abstract method to process data through the pipeline.
        Must be overridden by specific adapter implementations.

        Args:
            data: The input data to be processed.

        Returns:
            The processed result.
        """
        pass
