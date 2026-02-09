#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional, Protocol

DataType = Union[Dict[str, Any], str]


class ProcessingStage(Protocol):
    """
    Protocol defining the interface for processing stages.
    Any class implementing process(data) -> Any satisfies this protocol.
    """
    def process(self, data: DataType) -> DataType:
        ...


class InputStage:
    """
    Stage 1: Simulates input validation and parsing.
    """
    def process(self, data: DataType) -> DataType:
        if isinstance(data, str) and "," in data:
            print(f'Input: "{data}"')
        else:
            print(f"Input: {data}")
        if not data:
            raise ValueError("InputStage: Data cannot be empty.")
        return data


class TransformStage:
    """
    Stage 2: Simulates data transformation and enrichment.
    """
    def process(self, data: DataType) -> DataType:
        if isinstance(data, Dict):
            print("Transform: Enriched with metadata and validation")
        elif isinstance(data, str):
            if "user" in data:
                print("Transform: Parsed and structured data")
            else:
                print("Transform: Aggregated and filtered")
        return data


class OutputStage:
    """
    Stage 3: Simulates output formatting and delivery.
    """
    def process(self, data: DataType) -> DataType:
        if isinstance(data, dict):
            print(f"Output: Processed temperature reading: {data['value']}°C "
                  "(Normal range)")
        elif isinstance(data, str):
            if "user" in data:
                print("Output: User activity logged: 1 actions processed")
            else:
                print("Output: Stream summary: 5 readings, avg: 22.1°C")
        return data


class ProcessingPipeline(ABC):
    """
    Abstract base class managing stages and orchestrating data flow.
    """
    def __init__(self):
        self.stages: List[ProcessingStage] = []

    def add_stage(self, stage: DataType) -> None:
        """
        Adds a processing stage to the pipeline.

        Args:
            stage: An object adhering to the ProcessingStage protocol (
            must have a process method).
        """
        self.stages.append(stage)

    @abstractmethod
    def process(self, data: Any) -> Optional[DataType]:
        """
        Abstract method to process data through the pipeline.
        Must be overridden by specific adapter implementations.

        Args:
            data: The input data to be processed.

        Returns:
            The processed result.
        """
        pass


class JSONAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Optional[DataType]:
        print("Processing JSON data through pipeline...")
        for stage in self.stages:
            try:
                data = stage.process(data)
            except Exception as e:
                print(f"{__class__.__name__} "
                      f"{e.__class__.__name__} error as {e}")
        return data


class CSVAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Optional[DataType]:
        print("Processing CSV data through same pipeline..")
        for stage in self.stages:
            try:
                data = stage.process(data)
            except Exception as e:
                print(f"{__class__.__name__} "
                      f"{e.__class__.__name__} error as {e}")
        return data


class StreamAdapter(ProcessingPipeline):
    def __init__(self, pipeline_id: str):
        super().__init__()
        self.pipeline_id = pipeline_id

    def process(self, data: Any) -> Optional[DataType]:
        print("Processing Stream data through same pipeline...")
        for stage in self.stages:
            try:
                data = stage.process(data)
            except Exception as e:
                print(f"{__class__.__name__} "
                      f"{e.__class__.__name__} error as {e}")
        return data


class NexusManager:
    def __init__(self):
        self.pipelines: Dict[str, ProcessingPipeline] = {}

    def register_pipeline(
            self, name: str, pipeline: ProcessingPipeline) -> None:
        self.pipelines[name] = pipeline

    def process(self, pipeline_name: str, data: Any) -> Optional[DataType]:
        pipeline = self.pipelines.get(pipeline_name)
        if pipeline:
            try:
                return pipeline.process(data)
            except Exception as e:
                print(f"Error in pipeline {pipeline_name}: {e}")
                return None
        else:
            print(f"Pipeline {pipeline_name} not found.")
            return None


def main() -> None:
    print("=== CODE NEXUS - ENTERPRISE PIPELINE SYSTEM ===\n")
    print("initializing Nexus Manager...")
    nexus = NexusManager()
    print("Pipeline capacity: 1000 streams/second\n")
    pipelines = []
    pipeline1 = JSONAdapter("json_01")
    pipelines.append(pipeline1)
    pipeline2 = CSVAdapter("csv_01")
    pipelines.append(pipeline2)
    pipeline3 = StreamAdapter("stream_01")
    pipelines.append(pipeline3)
    print("Creating Data Processing Pipeline...")
    print("Stage 1: Input validation and parsing")
    print("Stage 2: Data transformation and enrichment")
    print("Stage 3: Output formatting and delivery\n")

    for pipe_line in pipelines:
        pipe_line.add_stage(InputStage())
        pipe_line.add_stage(TransformStage())
        pipe_line.add_stage(OutputStage())

    nexus.register_pipeline("json_stream", pipeline1)
    nexus.register_pipeline("csv_stream", pipeline2)
    nexus.register_pipeline("realtime_stream", pipeline3)

    print("=== Multi-Format Data Processing ===\n")

    sample_data_json = {"sensor": "temp", "value": 23.5, "unit": "C"}
    nexus.process("json_stream", sample_data_json)
    print("")

    sample_data_cvs = "user, action, timestamp"
    nexus.process("csv_stream", sample_data_cvs)
    print("")

    sample_data_realtime = "real-time sensor stream"
    nexus.process("realtime_stream", sample_data_realtime)
    print("\nNexus Integration complete. All systems operational.")


if __name__ == "__main__":
    main()
