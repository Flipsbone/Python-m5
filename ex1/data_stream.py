#!/usr/bin/env python3

from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):

    def __init__(self, stream_id: str, type: str) -> None:
        self.stream_id = stream_id
        self.type = type

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(
            self, data_batch: List[Any],
            criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": self.type
            }

    def parse_line(self, item: Any) -> Optional[tuple[str, str]]:
        try:
            if isinstance(item, str) and ":" in item:
                parts = item.split(":", 1)
                if len(parts) == 2:
                    return parts[0].strip().lower(), parts[1].strip()
        except Exception:
            pass
        return None


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Environmental Data")
        self.last_avg = 0.0

    def process_batch(self, data_batch: List[Any]) -> str:
        temperatures = []
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
        stats = super().get_stats()
        stats["last_average"] = self.last_avg
        return stats


class TransactionStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "Financial Data")
        self.net_flow = 0

    def process_batch(self, data_batch: List[Any]) -> str:
        buys = []
        sells = []

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

        stats = super().get_stats()
        stats["net_flow"] = self.net_flow
        return stats


class EventStream(DataStream):

    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id, "System Events")
        self.total_errors = 0

    def process_batch(self, data_batch: List[Any]) -> str:
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
        stats = super().get_stats()
        stats["total_errors"] = self.total_errors
        return stats


class StreamProcessor():
    def __init__(self) -> None:
        self.streams = {}

    def add_stream(self, stream: DataStream) -> None:
        self.streams[stream.stream_id] = stream

    def process_stream(self, stream_id: str, data_batch: List[Any]) -> str:
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
    print(f"Processing sensor batch: {str(sensor_data).replace("'", "")}")
    print(processor.process_stream("SENSOR_001", sensor_data))

    print("\nInitializing Transaction Stream...")

    transaction = TransactionStream("TRANS_001")
    processor.add_stream(transaction)
    print(f"Stream ID: {transaction.stream_id}, Type: {transaction.type}")
    trans_data = ["buy:100", "sell:150", "buy:75"]
    print(f"Processing transaction batch: {str(trans_data).replace("'", "")}")
    print(processor.process_stream("TRANS_001", trans_data))

    print("\nInitializing Event Stream...")

    event = EventStream("EVENT_001")
    processor.add_stream(event)
    print(f"Stream ID: {event.stream_id}, Type: {event.type}")
    event_data = ["login", "error", "logout"]
    print(f"Processing event batch: {str(event_data).replace("'", "")}")
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
