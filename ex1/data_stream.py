#!/usr/bin/env python3
"""
CODE NEXUS: Polymorphic Stream System
This module implements a polymorphic stream processing system
"""

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    """Abstract base class for different types of data streams.
    Enforces a common interface for processing, filtering, and stats retrieval,
    while allowing for specialized behavior in concrete stream types.
    Attributes:
        stream_id: A unique identifier for the data stream.
        type: A string representing the type of data stream
                (e.g., "Sensor", "Transaction", "Event").
    """

    def __init__(self, stream_id: str, type: str) -> None:
        """Initialize the data stream with an ID and type.
        Args:
            stream_id: A unique identifier for the data stream.
            type: A string representing the type of data stream
                    (e.g., "Sensor", "Transaction", "Event").
        """
        self.stream_id: str = stream_id
        self.type: str = type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of data items and return a summary string.
        Args:
            data_batch: A list of data items to be processed.
        Returns:
            A string summarizing the results of processing the batch.
        """
        pass

    def filter_data(
            self, data_batch: List[Any],
            criteria: Optional[str] = None) -> List[Any]:
        """Filter the data batch based on specified criteria.
        Args:
            data_batch: A list of data items to be filtered.
            criteria: An optional string specifying the filter criteria.
        Returns:
            A list of data items that meet the filter criteria.
        """

        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Retrieve statistics about the data stream.
        Returns:
            A dictionary containing statistics such as stream ID, type, and
            other relevant metrics.
        """

        return {
            "stream_id": self.stream_id,
            "type": self.type
            }

    def parse_line(self, item: Any) -> Optional[tuple[str, str]]:
        """Parse a data item into a key-value pair if possible.
        Args:
            item: The data item to be parsed, expected to be a string in the
                    format "key:value".
        Returns:
            A tuple of (key, value) if parsing is successful, or None if the
            item cannot be parsed.
        """

        try:
            if isinstance(item, str) and ":" in item:
                parts = item.split(":", 1)
                if len(parts) == 2:
                    return parts[0].strip().lower(), parts[1].strip()
        except Exception:
            pass
        return None


class SensorStream(DataStream):
    """Concrete implementation of DataStream for processing sensor data.
    This stream type focuses on analyzing temperature readings and generating
    alerts for extreme values.
    """

    def __init__(self, stream_id: str) -> None:
        """Initialize the sensor stream with a unique ID and set type to
        'Environmental Data'.
        Args:
            stream_id: A unique identifier for the sensor data stream.
        """

        super().__init__(stream_id, "Environmental Data")
        self.last_avg = 0.0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of sensor data, calculating average temperature and
        generating alerts for extreme values.
        Args:
            data_batch: A list of sensor data items, expected to contain
                        temperature readings in the format "temp:value".
        Returns:
            A string summarizing the average temperature and any alerts for
            extreme values.
        """

        temperatures: List[float] = []
        for item in data_batch:
            parsed = self.parse_line(item)
            if parsed:
                key, val_str = parsed
                try:
                    val = float(val_str)
                    if "temp" in key:
                        temperatures.append(val)
                except (ValueError):
                    continue

        if not temperatures:
            return f"Stream {self.stream_id}: No temperature data found"

        avg = sum(temperatures) / len(temperatures)
        self.last_avg = avg

        alerts = [temp for temp in temperatures if temp > 50 or temp < 0]
        status_msg = ""
        if alerts:
            status_msg = f", ALERT ({len(alerts)} extreme values)"

        return (f"Sensor analysis: {len(data_batch)} readings processed, "
                f"avg temp: {avg:.1f}°C{status_msg}")

    def filter_data(
            self, data_batch: List[Any],
            criteria: Optional[str] = None) -> List[Any]:
        """Filter sensor data based on specified criteria,
        such as critical temperature alerts.
        Args:
            data_batch: A list of sensor data items to be filtered.
            criteria: An optional string specifying the filter criteria (e.g.,
                    "critical" to filter for extreme temperature values).
        Returns:
            A list of sensor data items that meet the filter criteria.
        """

        filtered = []
        if criteria == "critical":
            for item in data_batch:
                parsed = self.parse_line(item)
                if parsed:
                    try:
                        val = float(parsed[1])
                        if val > 50 or val < 0:
                            filtered.append(item)
                    except ValueError:
                        continue
            return filtered
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Extend base stats with sensor-specific metrics, such as last average
        temperature.
            Returns:
                A dictionary containing base statistics along
                with sensor-specific metrics.
            """

        stats = super().get_stats()
        stats["last_average"] = self.last_avg
        return stats


class TransactionStream(DataStream):
    """Concrete implementation of DataStream for processing
    financial transaction data. This stream type focuses on analyzing
    buy/sell transactions and calculating net flow.
    """
    def __init__(self, stream_id: str) -> None:
        """Initialize the transaction stream with a unique ID and set type to
        'Financial Data'.
        Args:
            stream_id: A unique identifier for the transaction data stream.
        """

        super().__init__(stream_id, "Financial Data")
        self.net_flow = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of transaction data, calculating net flow of
        buy/sell transactions.
        Args:
            data_batch: A list of transaction data items, expected to contain
                        buy/sell transactions in the format "buy:value" or
                        "sell:value".
        Returns:
            A string summarizing the total number of transactions and the net
            flow of units bought/sold.
        """

        buys: List[int] = []
        sells: List[int] = []

        for item in data_batch:
            parsed = self.parse_line(item)
            if parsed:
                key, val_str = parsed
                try:
                    val = int(val_str)
                    if "buy" in key:
                        buys.append(val)
                    elif "sell" in key:
                        sells.append(val)
                except ValueError:
                    continue

        total_ops = len(buys) + len(sells)
        if total_ops == 0:
            return f"Stream {self.stream_id}: No transaction data found"
        net_flow = sum(buys) - sum(sells)

        self.net_flow = net_flow

        return (f"Transaction analysis: {total_ops} operations, "
                f"net flow: {net_flow:+} units")

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        """Filter transaction data based on specified criteria,
        such as large transactions.
        Args:
            data_batch: A list of transaction data items to be filtered.
            criteria: An optional string specifying the filter criteria (e.g.,
                    "large" to filter for transactions with values >= 100).
        Returns:
            A list of transaction data items that meet the filter criteria.
        """

        filtered = []
        if criteria == "large":
            for item in data_batch:
                parsed = self.parse_line(item)
                if parsed:
                    try:
                        val = int(parsed[1])
                        if val >= 100:
                            filtered.append(item)
                    except ValueError:
                        continue
            return filtered
        return super().filter_data(data_batch, criteria)

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Extend base stats with transaction-specific metrics, such
        as net flow.
        Returns:
            A dictionary containing base statistics along with
            transaction-specific metrics.
        """

        stats = super().get_stats()
        stats["net_flow"] = self.net_flow
        return stats


class EventStream(DataStream):
    """Concrete implementation of DataStream for processing system event data.
    This stream type focuses on analyzing event logs
    and counting occurrences of error events.
    """
    def __init__(self, stream_id: str) -> None:
        """Initialize the event stream with a unique ID and set type to
        'System Events'.
        Args:
            stream_id: A unique identifier for the event data stream.
        """

        super().__init__(stream_id, "System Events")
        self.total_errors: int = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        """Process a batch of event data, counting occurrences of error events.
        Args:
            data_batch: A list of event data items, expected to contain event
                        descriptions, with error events containing the word
                        "error".
        Returns:
            A string summarizing the total number of events processed and the
            count of error events detected.
        """

        error_count = 0

        for item in data_batch:
            if isinstance(item, str):
                if "error" in item.lower():
                    error_count += 1

        self.total_errors += error_count

        msg_error = "error detected" if error_count == 1 else "errors detected"

        return (f"Event analysis: {len(data_batch)} events, {error_count} "
                f"{msg_error}")

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        """Extend base stats with event-specific metrics, such as
        total error count.
        Returns:
            A dictionary containing base statistics along with event-specific
            metrics.
        """

        stats = super().get_stats()
        stats["total_errors"] = self.total_errors
        return stats


class StreamProcessor():
    """Class to manage and process multiple data streams through
    a unified interface.
    Attributes:
        streams: A dictionary mapping stream IDs to their corresponding
        DataStream instances.
    """
    def __init__(self) -> None:
        """Initialize the StreamProcessor with an empty dictionary
        of streams."""

        self.streams: Dict[str, DataStream] = {}

    def add_stream(self, stream: DataStream) -> None:
        """Add a new data stream to the processor.
        Args:
            stream: An instance of a DataStream subclass to be added to the
                    processor.
        """
        self.streams[stream.stream_id] = stream

    def process_stream(self, stream_id: str, data_batch: List[Any]) -> str:
        """Process a batch of data through the specified stream.
        Args:
            stream_id: The unique identifier of the stream to process the data
                        through.
            data_batch: A list of data items to be processed by the specified
                        stream.
            Returns:
                A string summarizing the results of processing the batch
                through the specified stream, or an error message
                if the stream is not found or processing fails.
        """

        if stream_id in self.streams:
            try:
                stream = self.streams[stream_id]
                return stream.process_batch(data_batch)
            except Exception as e:
                return (f"Error: Failed to process stream {stream_id}. "
                        f"Reason {str(e)}")
        else:
            return f"Error: Stream {stream_id} not found."


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")

    processor = StreamProcessor()

    print("\nInitializing Sensor Stream...")

    sensor = SensorStream("SENSOR_001")
    processor.add_stream(sensor)
    print(f"Stream ID: {sensor.stream_id}, Type: {sensor.type}")
    sensor_data = ["temp:22.5", "humidity:65", "pressure:1013"]
    clean_sensor = str(sensor_data).replace("'", "")
    print(f"Processing sensor batch: {clean_sensor}")
    print(processor.process_stream("SENSOR_001", sensor_data))

    print("\nInitializing Transaction Stream...")

    transaction = TransactionStream("TRANS_001")
    processor.add_stream(transaction)
    print(f"Stream ID: {transaction.stream_id}, Type: {transaction.type}")
    trans_data = ["buy:100", "sell:150", "buy:75"]
    clean_trans = str(trans_data).replace("'", "")
    print(f"Processing transaction batch: {clean_trans}")
    print(processor.process_stream("TRANS_001", trans_data))

    print("\nInitializing Event Stream...")

    event = EventStream("EVENT_001")
    processor.add_stream(event)
    print(f"Stream ID: {event.stream_id}, Type: {event.type}")
    event_data = ["login", "error", "logout"]
    clean_event = str(event_data).replace("'", "")
    print(f"Processing event batch: {clean_event}")
    print(processor.process_stream("EVENT_001", event_data))

    print("\n=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print("\nBatch 1 Results:")

    poly_sensor_data = ["temp:-20", "temp:-1"]
    poly_trans_data = ["buy:1120", "sell:15", "buy:12", "sell:11"]
    poly_event_data = ["login", "user", "logout"]

    res1 = processor.process_stream("SENSOR_001", poly_sensor_data)
    clean_res1 = res1.split(",")[0].replace("Sensor analysis", "Sensor data")
    print(f"- {clean_res1}")

    res2 = processor.process_stream("TRANS_001", poly_trans_data)
    clean_res2 = res2.split(",")[0].replace(
        "Transaction analysis", "Transaction data")
    print(f"- {clean_res2} processed")

    res3 = processor.process_stream("EVENT_001", poly_event_data)
    clean_res3 = res3.split(",")[0].replace("Event analysis", "Event data")
    print(f"- {clean_res3} processed")

    print("\nStream filtering active: High-priority data only")

    critical_sensors = sensor.filter_data(
        poly_sensor_data, criteria="critical")
    large_transactions = transaction.filter_data(
        poly_trans_data, criteria="large")

    print(f"Filtered results: {len(critical_sensors)} critical sensor alerts, "
          f"{len(large_transactions)} large transaction")
    print("\nAll streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    main()
