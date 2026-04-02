"""
Real-Time Streaming Data Pipeline Simulation

Implements core streaming concepts:
- Event model with event-time semantics
- Windowed aggregation (tumbling, sliding, session)
- Watermark-based late data handling
- Checkpoint manager for fault tolerance
- Stream topology builder (source -> transform -> window -> sink)

This is a single-process simulation demonstrating the concepts
used in production systems like Apache Flink or Kafka Streams.
"""

from __future__ import annotations

import copy
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Core Data Models
# ---------------------------------------------------------------------------

@dataclass
class Event:
    """An immutable event flowing through the pipeline.

    Attributes:
        event_id: Unique identifier for deduplication.
        event_time: When the event actually occurred (seconds since epoch).
        key: Partition key used for keyed aggregations.
        value: Arbitrary payload carried by the event.
        processing_time: When the pipeline first saw this event.
    """

    event_id: str
    event_time: float
    key: str
    value: Any
    processing_time: float = field(default_factory=time.time)

    @staticmethod
    def create(event_time: float, key: str, value: Any) -> "Event":
        return Event(
            event_id=str(uuid.uuid4()),
            event_time=event_time,
            key=key,
            value=value,
            processing_time=time.time(),
        )

    def __repr__(self) -> str:
        return (
            f"Event(id={self.event_id[:8]}..., "
            f"time={self.event_time:.1f}, "
            f"key={self.key!r}, "
            f"value={self.value})"
        )


class WindowType(Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class Window:
    """A time-bounded window that collects events for aggregation.

    Attributes:
        window_type: Type of window (tumbling, sliding, session).
        key: Partition key this window belongs to.
        start_time: Window start (inclusive), seconds since epoch.
        end_time: Window end (exclusive), seconds since epoch.
        events: Events assigned to this window.
    """

    window_type: WindowType
    key: str
    start_time: float
    end_time: float
    events: List[Event] = field(default_factory=list)

    @property
    def window_id(self) -> str:
        return (
            f"{self.window_type.value}-{self.key}"
            f"-{self.start_time:.0f}-{self.end_time:.0f}"
        )

    @property
    def event_count(self) -> int:
        return len(self.events)

    def add_event(self, event: Event) -> None:
        self.events.append(event)

    def __repr__(self) -> str:
        return (
            f"Window({self.window_type.value}, key={self.key!r}, "
            f"[{self.start_time:.0f}, {self.end_time:.0f}), "
            f"events={self.event_count})"
        )


@dataclass
class WindowResult:
    """The output produced when a window fires."""

    window_id: str
    window_type: WindowType
    key: str
    start_time: float
    end_time: float
    aggregation: Dict[str, Any]
    event_count: int

    def __repr__(self) -> str:
        return (
            f"WindowResult({self.window_type.value}, key={self.key!r}, "
            f"[{self.start_time:.0f}, {self.end_time:.0f}), "
            f"count={self.event_count}, agg={self.aggregation})"
        )


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

class Watermark:
    """Tracks event-time progress with bounded out-of-orderness.

    The watermark asserts: no more events with timestamp <= watermark will arrive.

    Args:
        max_delay: Maximum expected out-of-orderness in seconds.
    """

    def __init__(self, max_delay: float = 5.0) -> None:
        self.max_delay = max_delay
        self._max_event_time: float = 0.0
        self._current_watermark: float = 0.0

    @property
    def current(self) -> float:
        return self._current_watermark

    def update(self, event_time: float) -> float:
        """Advance the watermark based on observed event time.

        Returns:
            The new watermark value.
        """
        if event_time > self._max_event_time:
            self._max_event_time = event_time
        self._current_watermark = self._max_event_time - self.max_delay
        return self._current_watermark

    def is_late(self, event_time: float) -> bool:
        """Check whether an event is late relative to the current watermark."""
        return event_time < self._current_watermark

    def __repr__(self) -> str:
        return (
            f"Watermark(current={self._current_watermark:.1f}, "
            f"max_seen={self._max_event_time:.1f}, "
            f"delay={self.max_delay})"
        )


# ---------------------------------------------------------------------------
# Window Assigners
# ---------------------------------------------------------------------------

class TumblingWindowAssigner:
    """Assigns events to fixed-size, non-overlapping windows."""

    def __init__(self, size_sec: float) -> None:
        self.size = size_sec

    def assign(self, event: Event) -> List[Tuple[float, float]]:
        start = (event.event_time // self.size) * self.size
        return [(start, start + self.size)]


class SlidingWindowAssigner:
    """Assigns events to overlapping windows defined by size and hop."""

    def __init__(self, size_sec: float, hop_sec: float) -> None:
        self.size = size_sec
        self.hop = hop_sec

    def assign(self, event: Event) -> List[Tuple[float, float]]:
        windows: List[Tuple[float, float]] = []
        # Find the latest window start that contains this event
        last_start = (event.event_time // self.hop) * self.hop
        earliest_start = last_start - self.size + self.hop
        start = earliest_start
        while start <= last_start:
            end = start + self.size
            if start <= event.event_time < end:
                windows.append((start, end))
            start += self.hop
        return windows


class SessionWindowAssigner:
    """Manages session windows that merge when events fall within the gap."""

    def __init__(self, gap_sec: float) -> None:
        self.gap = gap_sec

    def assign(self, event: Event) -> List[Tuple[float, float]]:
        # Each event initially creates a window [event_time, event_time + gap)
        return [(event.event_time, event.event_time + self.gap)]


# ---------------------------------------------------------------------------
# Stream Processor (Windowed Aggregation Engine)
# ---------------------------------------------------------------------------

class StreamProcessor:
    """Core stream processing engine with windowed aggregation.

    Supports tumbling, sliding, and session windows with watermark-based
    late event handling and configurable aggregation functions.
    """

    def __init__(
        self,
        window_type: WindowType,
        window_size: float = 10.0,
        hop_size: float = 5.0,
        session_gap: float = 10.0,
        max_watermark_delay: float = 5.0,
        allowed_lateness: float = 10.0,
        aggregation_fn: Optional[Callable[[List[Event]], Dict[str, Any]]] = None,
    ) -> None:
        self.window_type = window_type
        self.watermark = Watermark(max_delay=max_watermark_delay)
        self.allowed_lateness = allowed_lateness
        self.aggregation_fn = aggregation_fn or self._default_aggregation

        # Keyed windows: key -> list of Window objects
        self._windows: Dict[str, List[Window]] = {}
        self._results: List[WindowResult] = []
        self._late_events: List[Event] = []
        self._dropped_events: List[Event] = []

        # Configure assigner
        if window_type == WindowType.TUMBLING:
            self._assigner = TumblingWindowAssigner(window_size)
        elif window_type == WindowType.SLIDING:
            self._assigner = SlidingWindowAssigner(window_size, hop_size)
        elif window_type == WindowType.SESSION:
            self._assigner = SessionWindowAssigner(session_gap)
        else:
            raise ValueError(f"Unknown window type: {window_type}")

    @staticmethod
    def _default_aggregation(events: List[Event]) -> Dict[str, Any]:
        """Default aggregation: count and sum of numeric values."""
        values = [e.value for e in events if isinstance(e.value, (int, float))]
        return {
            "count": len(events),
            "sum": sum(values) if values else 0,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "avg": (sum(values) / len(values)) if values else None,
        }

    def process_event(self, event: Event) -> Optional[str]:
        """Process a single event through the windowing engine.

        Returns:
            A status string: 'processed', 'late_processed', or 'dropped'.
        """
        self.watermark.update(event.event_time)

        if self.watermark.is_late(event.event_time):
            lateness = self.watermark.current - event.event_time
            if lateness <= self.allowed_lateness:
                self._late_events.append(event)
                self._assign_to_windows(event)
                return "late_processed"
            else:
                self._dropped_events.append(event)
                return "dropped"

        self._assign_to_windows(event)
        self._fire_eligible_windows()
        return "processed"

    def _assign_to_windows(self, event: Event) -> None:
        """Assign an event to one or more windows based on the assigner."""
        key = event.key
        if key not in self._windows:
            self._windows[key] = []

        window_specs = self._assigner.assign(event)

        if self.window_type == WindowType.SESSION:
            self._assign_session_window(event, window_specs[0])
        else:
            for start, end in window_specs:
                window = self._find_or_create_window(key, start, end)
                window.add_event(event)

    def _find_or_create_window(
        self, key: str, start: float, end: float
    ) -> Window:
        """Find an existing window or create a new one."""
        for w in self._windows[key]:
            if abs(w.start_time - start) < 0.001 and abs(w.end_time - end) < 0.001:
                return w
        new_window = Window(
            window_type=self.window_type,
            key=key,
            start_time=start,
            end_time=end,
        )
        self._windows[key].append(new_window)
        return new_window

    def _assign_session_window(
        self, event: Event, spec: Tuple[float, float]
    ) -> None:
        """Assign an event to a session window, merging overlapping sessions."""
        key = event.key
        new_start, new_end = spec

        # Find all overlapping session windows
        overlapping: List[Window] = []
        non_overlapping: List[Window] = []
        for w in self._windows[key]:
            if w.start_time <= new_end and new_start <= w.end_time:
                overlapping.append(w)
            else:
                non_overlapping.append(w)

        if overlapping:
            # Merge all overlapping windows
            all_events = [event]
            merged_start = new_start
            merged_end = new_end
            for w in overlapping:
                all_events.extend(w.events)
                merged_start = min(merged_start, w.start_time)
                merged_end = max(merged_end, w.end_time)

            merged = Window(
                window_type=WindowType.SESSION,
                key=key,
                start_time=merged_start,
                end_time=merged_end,
            )
            for e in sorted(all_events, key=lambda x: x.event_time):
                merged.add_event(e)

            non_overlapping.append(merged)
            self._windows[key] = non_overlapping
        else:
            new_window = Window(
                window_type=WindowType.SESSION,
                key=key,
                start_time=new_start,
                end_time=new_end,
            )
            new_window.add_event(event)
            self._windows[key].append(new_window)

    def _fire_eligible_windows(self) -> None:
        """Fire windows whose end time is at or before the watermark."""
        for key in list(self._windows.keys()):
            remaining: List[Window] = []
            for window in self._windows[key]:
                if window.end_time <= self.watermark.current:
                    result = WindowResult(
                        window_id=window.window_id,
                        window_type=window.window_type,
                        key=window.key,
                        start_time=window.start_time,
                        end_time=window.end_time,
                        aggregation=self.aggregation_fn(window.events),
                        event_count=window.event_count,
                    )
                    self._results.append(result)
                else:
                    remaining.append(window)
            self._windows[key] = remaining

    def flush(self) -> List[WindowResult]:
        """Force-fire all remaining open windows. Used at end of stream."""
        flushed: List[WindowResult] = []
        for key in list(self._windows.keys()):
            for window in self._windows[key]:
                result = WindowResult(
                    window_id=window.window_id,
                    window_type=window.window_type,
                    key=window.key,
                    start_time=window.start_time,
                    end_time=window.end_time,
                    aggregation=self.aggregation_fn(window.events),
                    event_count=window.event_count,
                )
                flushed.append(result)
                self._results.append(result)
            self._windows[key] = []
        return flushed

    @property
    def results(self) -> List[WindowResult]:
        return list(self._results)

    @property
    def late_events(self) -> List[Event]:
        return list(self._late_events)

    @property
    def dropped_events(self) -> List[Event]:
        return list(self._dropped_events)


# ---------------------------------------------------------------------------
# Checkpoint Manager
# ---------------------------------------------------------------------------

@dataclass
class CheckpointData:
    """Snapshot of pipeline state at a point in time."""

    checkpoint_id: int
    timestamp: float
    watermark_state: float
    window_snapshots: Dict[str, List[Dict[str, Any]]]
    source_offsets: Dict[str, int]
    results_count: int


class CheckpointManager:
    """Manages periodic checkpoints for fault tolerance.

    Simulates the checkpoint-barrier algorithm: periodically snapshot
    the processor state so it can be restored after a failure.
    """

    def __init__(self, interval_events: int = 50) -> None:
        self.interval = interval_events
        self._checkpoints: List[CheckpointData] = []
        self._events_since_last: int = 0
        self._next_id: int = 1

    def on_event(self, processor: StreamProcessor, offset: int) -> Optional[CheckpointData]:
        """Called after each event. Triggers checkpoint if interval reached.

        Returns:
            CheckpointData if a checkpoint was created, else None.
        """
        self._events_since_last += 1
        if self._events_since_last >= self.interval:
            return self.create_checkpoint(processor, {"main": offset})
        return None

    def create_checkpoint(
        self,
        processor: StreamProcessor,
        source_offsets: Dict[str, int],
    ) -> CheckpointData:
        """Create a checkpoint by snapshotting processor state."""
        window_snapshots: Dict[str, List[Dict[str, Any]]] = {}
        for key, windows in processor._windows.items():
            window_snapshots[key] = [
                {
                    "window_id": w.window_id,
                    "start": w.start_time,
                    "end": w.end_time,
                    "event_count": w.event_count,
                    "event_ids": [e.event_id for e in w.events],
                }
                for w in windows
            ]

        cp = CheckpointData(
            checkpoint_id=self._next_id,
            timestamp=time.time(),
            watermark_state=processor.watermark.current,
            window_snapshots=window_snapshots,
            source_offsets=source_offsets,
            results_count=len(processor.results),
        )
        self._checkpoints.append(cp)
        self._next_id += 1
        self._events_since_last = 0
        return cp

    @property
    def latest(self) -> Optional[CheckpointData]:
        return self._checkpoints[-1] if self._checkpoints else None

    @property
    def all_checkpoints(self) -> List[CheckpointData]:
        return list(self._checkpoints)

    def __repr__(self) -> str:
        return (
            f"CheckpointManager(interval={self.interval}, "
            f"count={len(self._checkpoints)})"
        )


# ---------------------------------------------------------------------------
# Stream Topology Builder
# ---------------------------------------------------------------------------

class StreamTopology:
    """Builder for constructing a streaming pipeline topology.

    Defines a DAG: source -> transforms -> window -> aggregation -> sinks.

    Example::

        topology = (
            StreamTopology("click-analytics")
            .source("kafka", {"topic": "raw-clicks"})
            .filter(lambda e: e.value > 0)
            .map(lambda e: Event.create(e.event_time, e.key, e.value * 2))
            .key_by(lambda e: e.key)
            .tumbling_window(size_sec=60.0)
            .aggregate(custom_agg_fn)
            .sink("console", {})
        )
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._source_type: Optional[str] = None
        self._source_config: Dict[str, Any] = {}
        self._transforms: List[Tuple[str, Callable]] = []
        self._key_fn: Optional[Callable[[Event], str]] = None
        self._window_type: Optional[WindowType] = None
        self._window_config: Dict[str, float] = {}
        self._agg_fn: Optional[Callable[[List[Event]], Dict[str, Any]]] = None
        self._sinks: List[Tuple[str, Dict[str, Any]]] = []
        self._watermark_delay: float = 5.0
        self._allowed_lateness: float = 10.0

    # -- Builder methods ---------------------------------------------------

    def source(self, source_type: str, config: Dict[str, Any]) -> "StreamTopology":
        self._source_type = source_type
        self._source_config = config
        return self

    def filter(self, predicate: Callable[[Event], bool]) -> "StreamTopology":
        self._transforms.append(("filter", predicate))
        return self

    def map(self, mapper: Callable[[Event], Event]) -> "StreamTopology":
        self._transforms.append(("map", mapper))
        return self

    def key_by(self, key_fn: Callable[[Event], str]) -> "StreamTopology":
        self._key_fn = key_fn
        return self

    def tumbling_window(self, size_sec: float) -> "StreamTopology":
        self._window_type = WindowType.TUMBLING
        self._window_config = {"window_size": size_sec}
        return self

    def sliding_window(self, size_sec: float, hop_sec: float) -> "StreamTopology":
        self._window_type = WindowType.SLIDING
        self._window_config = {"window_size": size_sec, "hop_size": hop_sec}
        return self

    def session_window(self, gap_sec: float) -> "StreamTopology":
        self._window_type = WindowType.SESSION
        self._window_config = {"session_gap": gap_sec}
        return self

    def watermark_delay(self, delay_sec: float) -> "StreamTopology":
        self._watermark_delay = delay_sec
        return self

    def allowed_lateness(self, lateness_sec: float) -> "StreamTopology":
        self._allowed_lateness = lateness_sec
        return self

    def aggregate(
        self, agg_fn: Callable[[List[Event]], Dict[str, Any]]
    ) -> "StreamTopology":
        self._agg_fn = agg_fn
        return self

    def sink(self, sink_type: str, config: Dict[str, Any]) -> "StreamTopology":
        self._sinks.append((sink_type, config))
        return self

    # -- Execution ---------------------------------------------------------

    def build(self) -> "PipelineRunner":
        """Build and return an executable pipeline runner."""
        if not self._window_type:
            raise ValueError("Window type must be specified")
        if not self._sinks:
            raise ValueError("At least one sink must be specified")

        processor = StreamProcessor(
            window_type=self._window_type,
            max_watermark_delay=self._watermark_delay,
            allowed_lateness=self._allowed_lateness,
            aggregation_fn=self._agg_fn,
            **self._window_config,
        )
        return PipelineRunner(
            name=self.name,
            processor=processor,
            transforms=list(self._transforms),
            key_fn=self._key_fn,
            sinks=list(self._sinks),
        )

    def describe(self) -> str:
        """Return a human-readable description of the topology."""
        parts = [f"Topology: {self.name}"]
        parts.append(f"  Source: {self._source_type} {self._source_config}")
        for ttype, _ in self._transforms:
            parts.append(f"  -> {ttype}")
        if self._key_fn:
            parts.append("  -> key_by")
        if self._window_type:
            parts.append(
                f"  -> {self._window_type.value}_window {self._window_config}"
            )
        if self._agg_fn:
            parts.append(f"  -> aggregate ({self._agg_fn.__name__})")
        for stype, sconf in self._sinks:
            parts.append(f"  -> sink: {stype} {sconf}")
        return "\n".join(parts)


class PipelineRunner:
    """Executes a built topology against an event stream."""

    def __init__(
        self,
        name: str,
        processor: StreamProcessor,
        transforms: List[Tuple[str, Callable]],
        key_fn: Optional[Callable[[Event], str]],
        sinks: List[Tuple[str, Dict[str, Any]]],
    ) -> None:
        self.name = name
        self.processor = processor
        self.transforms = transforms
        self.key_fn = key_fn
        self.sinks = sinks
        self.checkpoint_mgr = CheckpointManager(interval_events=50)
        self._sink_outputs: Dict[str, List[WindowResult]] = {
            s[0]: [] for s in sinks
        }
        self._processed_count = 0
        self._previous_results_count = 0

    def process(self, events: List[Event]) -> Dict[str, Any]:
        """Run the full pipeline on a batch of events.

        Returns:
            Summary statistics of the pipeline run.
        """
        statuses: Dict[str, int] = {"processed": 0, "late_processed": 0, "dropped": 0}

        for i, event in enumerate(events):
            transformed = self._apply_transforms(event)
            if transformed is None:
                continue

            if self.key_fn:
                transformed = Event.create(
                    event_time=transformed.event_time,
                    key=self.key_fn(transformed),
                    value=transformed.value,
                )

            status = self.processor.process_event(transformed)
            if status:
                statuses[status] += 1
            self._processed_count += 1

            self.checkpoint_mgr.on_event(self.processor, i)

        # Deliver new results to sinks
        all_results = self.processor.results
        new_results = all_results[self._previous_results_count:]
        self._previous_results_count = len(all_results)

        for result in new_results:
            for sink_type, _ in self.sinks:
                self._sink_outputs[sink_type].append(result)

        return {
            "pipeline": self.name,
            "events_received": len(events),
            "statuses": statuses,
            "windows_fired": len(new_results),
            "total_results": len(all_results),
            "checkpoints": len(self.checkpoint_mgr.all_checkpoints),
            "watermark": self.processor.watermark.current,
        }

    def flush(self) -> List[WindowResult]:
        """Flush remaining windows and deliver to sinks."""
        flushed = self.processor.flush()
        for result in flushed:
            for sink_type, _ in self.sinks:
                self._sink_outputs[sink_type].append(result)
        return flushed

    def _apply_transforms(self, event: Event) -> Optional[Event]:
        current = event
        for ttype, fn in self.transforms:
            if ttype == "filter":
                if not fn(current):
                    return None
            elif ttype == "map":
                current = fn(current)
        return current

    def get_sink_output(self, sink_type: str) -> List[WindowResult]:
        return list(self._sink_outputs.get(sink_type, []))


# ---------------------------------------------------------------------------
# Demo / Main
# ---------------------------------------------------------------------------

def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}\n")


def demo_tumbling_windows() -> None:
    """Demonstrate tumbling window aggregation with late events."""
    _separator("DEMO 1: Tumbling Windows with Late Data")

    processor = StreamProcessor(
        window_type=WindowType.TUMBLING,
        window_size=10.0,
        max_watermark_delay=3.0,
        allowed_lateness=8.0,
    )

    # Simulate events across three 10-second windows
    events = [
        # Window [0, 10): normal events
        Event.create(event_time=1.0, key="user-A", value=10),
        Event.create(event_time=3.0, key="user-A", value=20),
        Event.create(event_time=5.0, key="user-B", value=15),
        Event.create(event_time=8.0, key="user-A", value=30),
        # Window [10, 20): normal events
        Event.create(event_time=11.0, key="user-A", value=25),
        Event.create(event_time=14.0, key="user-B", value=35),
        Event.create(event_time=17.0, key="user-A", value=40),
        # Advance watermark to fire window [0, 10)
        Event.create(event_time=20.0, key="user-A", value=50),
        # Late event for window [0, 10) -- within allowed lateness
        Event.create(event_time=7.0, key="user-A", value=5),
        # Window [20, 30): more events
        Event.create(event_time=22.0, key="user-A", value=60),
        Event.create(event_time=25.0, key="user-B", value=45),
        # Advance watermark further
        Event.create(event_time=35.0, key="user-A", value=70),
        # Very late event -- beyond allowed lateness -> dropped
        Event.create(event_time=2.0, key="user-A", value=1),
    ]

    print("Processing events...")
    for i, event in enumerate(events):
        status = processor.process_event(event)
        tag = ""
        if status == "late_processed":
            tag = " [LATE - accepted]"
        elif status == "dropped":
            tag = " [DROPPED - too late]"
        print(f"  [{i+1:2d}] time={event.event_time:5.1f} key={event.key:8s} "
              f"value={event.value:3} -> {status}{tag}")

    # Flush remaining windows
    flushed = processor.flush()

    print(f"\nWatermark: {processor.watermark}")
    print(f"\nFired window results ({len(processor.results)} total):")
    for r in processor.results:
        print(f"  {r}")
    print(f"\nLate events accepted: {len(processor.late_events)}")
    print(f"Events dropped (too late): {len(processor.dropped_events)}")


def demo_sliding_windows() -> None:
    """Demonstrate sliding window aggregation."""
    _separator("DEMO 2: Sliding Windows (Moving Average)")

    processor = StreamProcessor(
        window_type=WindowType.SLIDING,
        window_size=20.0,
        hop_size=10.0,
        max_watermark_delay=3.0,
        allowed_lateness=5.0,
    )

    events = [
        Event.create(event_time=2.0, key="sensor-1", value=100),
        Event.create(event_time=8.0, key="sensor-1", value=110),
        Event.create(event_time=12.0, key="sensor-1", value=105),
        Event.create(event_time=18.0, key="sensor-1", value=120),
        Event.create(event_time=25.0, key="sensor-1", value=115),
        Event.create(event_time=32.0, key="sensor-1", value=130),
        Event.create(event_time=38.0, key="sensor-1", value=125),
        Event.create(event_time=45.0, key="sensor-1", value=140),
    ]

    print("Processing sensor readings with sliding window (20s size, 10s hop)...")
    for event in events:
        processor.process_event(event)

    processor.flush()

    print(f"\nSliding window results ({len(processor.results)} total):")
    for r in processor.results:
        avg = r.aggregation.get("avg")
        avg_str = f"{avg:.1f}" if avg is not None else "N/A"
        print(f"  [{r.start_time:.0f}, {r.end_time:.0f}) "
              f"count={r.event_count} avg={avg_str}")


def demo_session_windows() -> None:
    """Demonstrate session window aggregation."""
    _separator("DEMO 3: Session Windows (User Activity)")

    processor = StreamProcessor(
        window_type=WindowType.SESSION,
        session_gap=5.0,
        max_watermark_delay=2.0,
        allowed_lateness=5.0,
    )

    events = [
        # Session 1 for user-X: activity at t=1, 3, 4
        Event.create(event_time=1.0, key="user-X", value="page_view"),
        Event.create(event_time=3.0, key="user-X", value="click"),
        Event.create(event_time=4.0, key="user-X", value="add_to_cart"),
        # Gap > 5s -> new session
        # Session 2 for user-X: activity at t=15, 17
        Event.create(event_time=15.0, key="user-X", value="page_view"),
        Event.create(event_time=17.0, key="user-X", value="purchase"),
        # Different user
        Event.create(event_time=2.0, key="user-Y", value="page_view"),
        # Advance time to fire sessions
        Event.create(event_time=30.0, key="user-X", value="page_view"),
    ]

    print("Processing user activity with session windows (gap=5s)...")
    for event in events:
        processor.process_event(event)

    processor.flush()

    print(f"\nSession window results ({len(processor.results)} total):")
    for r in processor.results:
        print(f"  key={r.key:8s} [{r.start_time:.0f}, {r.end_time:.0f}) "
              f"events={r.event_count}")


def demo_topology_builder() -> None:
    """Demonstrate the StreamTopology builder pattern."""
    _separator("DEMO 4: Stream Topology Builder")

    def fraud_score_agg(events: List[Event]) -> Dict[str, Any]:
        values = [e.value for e in events if isinstance(e.value, (int, float))]
        total = sum(values) if values else 0
        count = len(values)
        return {
            "transaction_count": count,
            "total_amount": total,
            "avg_amount": total / count if count > 0 else 0,
            "is_suspicious": count > 3 or total > 500,
        }

    topology = (
        StreamTopology("fraud-detection")
        .source("kafka", {"topic": "raw-transactions", "group": "fraud-detector"})
        .filter(lambda e: isinstance(e.value, (int, float)) and e.value > 0)
        .key_by(lambda e: e.key)
        .tumbling_window(size_sec=10.0)
        .watermark_delay(3.0)
        .allowed_lateness(5.0)
        .aggregate(fraud_score_agg)
        .sink("kafka", {"topic": "fraud-alerts"})
        .sink("console", {})
    )

    print("Topology definition:")
    print(topology.describe())

    runner = topology.build()

    events = [
        Event.create(event_time=1.0, key="account-100", value=50.0),
        Event.create(event_time=2.0, key="account-100", value=75.0),
        Event.create(event_time=3.0, key="account-200", value=200.0),
        Event.create(event_time=4.0, key="account-100", value=30.0),
        Event.create(event_time=5.0, key="account-100", value=500.0),
        Event.create(event_time=6.0, key="account-200", value=25.0),
        Event.create(event_time=8.0, key="account-100", value=100.0),
        # Advance watermark to fire windows
        Event.create(event_time=18.0, key="account-300", value=10.0),
        Event.create(event_time=25.0, key="account-300", value=20.0),
    ]

    print("\nRunning pipeline...")
    summary = runner.process(events)
    flushed = runner.flush()

    print(f"\nPipeline summary: {summary}")
    print(f"Flushed {len(flushed)} remaining windows")

    print("\nSink outputs (kafka):")
    for r in runner.get_sink_output("kafka"):
        status = "** SUSPICIOUS **" if r.aggregation.get("is_suspicious") else "ok"
        print(f"  {r.key}: {r.aggregation} -> {status}")


def demo_checkpointing() -> None:
    """Demonstrate checkpoint-based fault tolerance."""
    _separator("DEMO 5: Checkpointing for Fault Tolerance")

    processor = StreamProcessor(
        window_type=WindowType.TUMBLING,
        window_size=10.0,
        max_watermark_delay=2.0,
    )
    checkpoint_mgr = CheckpointManager(interval_events=5)

    events = [Event.create(event_time=float(i), key="k1", value=i) for i in range(15)]

    print("Processing events with checkpoints every 5 events...")
    for i, event in enumerate(events):
        processor.process_event(event)
        cp = checkpoint_mgr.on_event(processor, offset=i)
        if cp:
            print(f"  [Checkpoint {cp.checkpoint_id}] at offset={i}, "
                  f"watermark={cp.watermark_state:.1f}, "
                  f"windows={sum(len(v) for v in cp.window_snapshots.values())}, "
                  f"results_so_far={cp.results_count}")

    print(f"\nTotal checkpoints created: {len(checkpoint_mgr.all_checkpoints)}")

    latest = checkpoint_mgr.latest
    if latest:
        print(f"\nLatest checkpoint details:")
        print(f"  ID:             {latest.checkpoint_id}")
        print(f"  Watermark:      {latest.watermark_state:.1f}")
        print(f"  Source offsets:  {latest.source_offsets}")
        print(f"  Window keys:    {list(latest.window_snapshots.keys())}")
        for key, windows in latest.window_snapshots.items():
            for ws in windows:
                print(f"    {key}: window [{ws['start']:.0f}, {ws['end']:.0f}) "
                      f"with {ws['event_count']} events")

    print("\n-- Simulating failure and recovery --")
    if latest:
        print(f"  Would restore from checkpoint {latest.checkpoint_id}")
        print(f"  Reset Kafka offset to: {latest.source_offsets}")
        print(f"  Restore watermark to: {latest.watermark_state:.1f}")
        print(f"  Replay events from offset {latest.source_offsets.get('main', 0) + 1}")


def main() -> None:
    print("=" * 60)
    print("  Real-Time Streaming Data Pipeline Simulation")
    print("=" * 60)

    demo_tumbling_windows()
    demo_sliding_windows()
    demo_session_windows()
    demo_topology_builder()
    demo_checkpointing()

    _separator("ALL DEMOS COMPLETE")
    print("This simulation demonstrated:")
    print("  1. Tumbling windows with late data handling")
    print("  2. Sliding windows for moving averages")
    print("  3. Session windows for user activity tracking")
    print("  4. Topology builder pattern (source -> transform -> window -> sink)")
    print("  5. Checkpoint-based fault tolerance")
    print()


if __name__ == "__main__":
    main()
