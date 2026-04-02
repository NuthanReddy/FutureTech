"""
Metrics & Monitoring System -- Simulation

Implements core components of a metrics and monitoring platform:
- MetricPoint: immutable data point (name, tags, timestamp, value)
- TimeSeriesDB: in-memory TSDB with time-partitioned storage and downsampling
- Counter / Gauge / Histogram: typed metric collectors
- AlertRule / AlertEngine: threshold-based alerting with state machine
- QueryEngine: aggregation queries (avg, sum, min, max, count, percentiles)
"""

from __future__ import annotations

import bisect
import math
import statistics
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional


# ---------------------------------------------------------------------------
# 1. Data Model
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class MetricPoint:
    """A single time-series data point."""

    name: str
    tags: tuple[tuple[str, str], ...]  # sorted key-value pairs (hashable)
    timestamp: float  # unix epoch seconds
    value: float

    @staticmethod
    def make(name: str, tags: dict[str, str], value: float,
             timestamp: float | None = None) -> MetricPoint:
        sorted_tags = tuple(sorted(tags.items()))
        return MetricPoint(
            name=name,
            tags=sorted_tags,
            timestamp=timestamp if timestamp is not None else time.time(),
            value=value,
        )

    def series_key(self) -> tuple[str, tuple[tuple[str, str], ...]]:
        return (self.name, self.tags)

    def tags_dict(self) -> dict[str, str]:
        return dict(self.tags)


# ---------------------------------------------------------------------------
# 2. Time-Series Database (in-memory, time-partitioned)
# ---------------------------------------------------------------------------

class TimePartition:
    """Stores points for a fixed time window, sorted by timestamp."""

    def __init__(self, start: float, duration: float) -> None:
        self.start = start
        self.end = start + duration
        self.duration = duration
        # series_key -> sorted list of (timestamp, value)
        self.series: dict[tuple, list[tuple[float, float]]] = defaultdict(list)

    def contains(self, ts: float) -> bool:
        return self.start <= ts < self.end

    def insert(self, point: MetricPoint) -> None:
        key = point.series_key()
        series = self.series[key]
        bisect.insort(series, (point.timestamp, point.value))

    def query(self, key: tuple, start: float, end: float) -> list[tuple[float, float]]:
        series = self.series.get(key, [])
        lo = bisect.bisect_left(series, (start,))
        hi = bisect.bisect_right(series, (end, float("inf")))
        return series[lo:hi]


class TimeSeriesDB:
    """In-memory time-series database with time partitioning and downsampling."""

    def __init__(self, partition_duration: float = 3600.0) -> None:
        self.partition_duration = partition_duration
        self.partitions: list[TimePartition] = []
        # downsampled tiers: resolution_seconds -> {series_key -> [(ts, value)]}
        self.rollups: dict[int, dict[tuple, list[tuple[float, float]]]] = defaultdict(
            lambda: defaultdict(list)
        )

    def _get_or_create_partition(self, ts: float) -> TimePartition:
        start = ts - (ts % self.partition_duration)
        for p in self.partitions:
            if p.contains(ts):
                return p
        partition = TimePartition(start, self.partition_duration)
        self.partitions.append(partition)
        self.partitions.sort(key=lambda p: p.start)
        return partition

    def write(self, point: MetricPoint) -> None:
        partition = self._get_or_create_partition(point.timestamp)
        partition.insert(point)

    def write_batch(self, points: list[MetricPoint]) -> int:
        for p in points:
            self.write(p)
        return len(points)

    def query_raw(self, name: str, tags: dict[str, str],
                  start: float, end: float) -> list[tuple[float, float]]:
        key = (name, tuple(sorted(tags.items())))
        results: list[tuple[float, float]] = []
        for partition in self.partitions:
            if partition.end <= start or partition.start >= end:
                continue
            results.extend(partition.query(key, start, end))
        results.sort()
        return results

    def downsample(self, resolution_seconds: int) -> int:
        """Produce rollups at the given resolution from raw data."""
        count = 0
        rollup_store = self.rollups[resolution_seconds]
        for partition in self.partitions:
            for key, series in partition.series.items():
                buckets: dict[float, list[float]] = defaultdict(list)
                for ts, val in series:
                    bucket_start = ts - (ts % resolution_seconds)
                    buckets[bucket_start].append(val)
                for bucket_start, values in buckets.items():
                    avg_val = sum(values) / len(values)
                    rollup_store[key].append((bucket_start, avg_val))
                    count += 1
        # sort each series
        for key in rollup_store:
            rollup_store[key].sort()
        return count

    def query_rollup(self, name: str, tags: dict[str, str],
                     resolution: int, start: float,
                     end: float) -> list[tuple[float, float]]:
        key = (name, tuple(sorted(tags.items())))
        series = self.rollups.get(resolution, {}).get(key, [])
        lo = bisect.bisect_left(series, (start,))
        hi = bisect.bisect_right(series, (end, float("inf")))
        return series[lo:hi]

    def retention_drop(self, older_than: float) -> int:
        before = len(self.partitions)
        self.partitions = [p for p in self.partitions if p.end > older_than]
        return before - len(self.partitions)

    def partition_count(self) -> int:
        return len(self.partitions)

    def series_count(self) -> int:
        keys: set[tuple] = set()
        for p in self.partitions:
            keys.update(p.series.keys())
        return len(keys)


# ---------------------------------------------------------------------------
# 3. Metric Types: Counter, Gauge, Histogram
# ---------------------------------------------------------------------------

class Counter:
    """Monotonically increasing counter."""

    def __init__(self, name: str, tags: dict[str, str] | None = None) -> None:
        self.name = name
        self.tags = tags or {}
        self._value: float = 0.0

    def inc(self, amount: float = 1.0) -> None:
        if amount < 0:
            raise ValueError("Counter can only be incremented")
        self._value += amount

    def get(self) -> float:
        return self._value

    def collect(self) -> MetricPoint:
        return MetricPoint.make(self.name, self.tags, self._value)


class Gauge:
    """Value that can go up or down."""

    def __init__(self, name: str, tags: dict[str, str] | None = None) -> None:
        self.name = name
        self.tags = tags or {}
        self._value: float = 0.0

    def set(self, value: float) -> None:
        self._value = value

    def inc(self, amount: float = 1.0) -> None:
        self._value += amount

    def dec(self, amount: float = 1.0) -> None:
        self._value -= amount

    def get(self) -> float:
        return self._value

    def collect(self) -> MetricPoint:
        return MetricPoint.make(self.name, self.tags, self._value)


class Histogram:
    """Tracks value distribution using configurable buckets."""

    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

    def __init__(self, name: str, tags: dict[str, str] | None = None,
                 buckets: tuple[float, ...] | None = None) -> None:
        self.name = name
        self.tags = tags or {}
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self._values: list[float] = []
        self._bucket_counts: dict[float, int] = {b: 0 for b in self.buckets}
        self._sum: float = 0.0
        self._count: int = 0

    def observe(self, value: float) -> None:
        self._values.append(value)
        self._sum += value
        self._count += 1
        for b in self.buckets:
            if value <= b:
                self._bucket_counts[b] += 1

    def percentile(self, p: float) -> float:
        if not self._values:
            return 0.0
        sorted_vals = sorted(self._values)
        k = (p / 100.0) * (len(sorted_vals) - 1)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return sorted_vals[int(k)]
        return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)

    def collect_points(self) -> list[MetricPoint]:
        """Collect sum, count, and bucket metrics."""
        ts = time.time()
        points = [
            MetricPoint.make(f"{self.name}_sum", self.tags, self._sum, ts),
            MetricPoint.make(f"{self.name}_count", self.tags, float(self._count), ts),
        ]
        for b, cnt in self._bucket_counts.items():
            bucket_tags = {**self.tags, "le": str(b)}
            points.append(
                MetricPoint.make(f"{self.name}_bucket", bucket_tags, float(cnt), ts)
            )
        return points

    @property
    def count(self) -> int:
        return self._count

    @property
    def sum(self) -> float:
        return self._sum


# ---------------------------------------------------------------------------
# 4. Query Engine
# ---------------------------------------------------------------------------

class AggregationType(Enum):
    AVG = "avg"
    SUM = "sum"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    P50 = "p50"
    P95 = "p95"
    P99 = "p99"


class QueryEngine:
    """Runs aggregation queries against the TimeSeriesDB."""

    def __init__(self, db: TimeSeriesDB) -> None:
        self.db = db

    def query(self, name: str, tags: dict[str, str],
              start: float, end: float,
              aggregation: AggregationType) -> float | None:
        data = self.db.query_raw(name, tags, start, end)
        if not data:
            return None
        values = [v for _, v in data]
        return self._aggregate(values, aggregation)

    def query_over_time(self, name: str, tags: dict[str, str],
                        start: float, end: float,
                        step: float,
                        aggregation: AggregationType) -> list[tuple[float, float]]:
        """Query with step-aligned buckets, returning (bucket_start, agg_value)."""
        results: list[tuple[float, float]] = []
        t = start
        while t < end:
            bucket_end = min(t + step, end)
            data = self.db.query_raw(name, tags, t, bucket_end)
            if data:
                values = [v for _, v in data]
                agg = self._aggregate(values, aggregation)
                if agg is not None:
                    results.append((t, agg))
            t = bucket_end
        return results

    @staticmethod
    def _aggregate(values: list[float],
                   aggregation: AggregationType) -> float | None:
        if not values:
            return None
        if aggregation == AggregationType.AVG:
            return statistics.mean(values)
        elif aggregation == AggregationType.SUM:
            return sum(values)
        elif aggregation == AggregationType.MIN:
            return min(values)
        elif aggregation == AggregationType.MAX:
            return max(values)
        elif aggregation == AggregationType.COUNT:
            return float(len(values))
        elif aggregation == AggregationType.P50:
            return _percentile(values, 50)
        elif aggregation == AggregationType.P95:
            return _percentile(values, 95)
        elif aggregation == AggregationType.P99:
            return _percentile(values, 99)
        return None


def _percentile(values: list[float], p: float) -> float:
    sorted_vals = sorted(values)
    k = (p / 100.0) * (len(sorted_vals) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)


# ---------------------------------------------------------------------------
# 5. Alerting Engine
# ---------------------------------------------------------------------------

class AlertState(Enum):
    INACTIVE = "inactive"
    PENDING = "pending"
    FIRING = "firing"
    RESOLVED = "resolved"


@dataclass
class AlertRule:
    """Defines a threshold-based alert."""

    rule_id: str
    name: str
    metric_name: str
    tags: dict[str, str]
    condition: Callable[[float], bool]
    condition_str: str
    for_seconds: float  # how long condition must hold before firing
    severity: str = "warning"  # critical / warning / info

    @staticmethod
    def threshold_above(name: str, metric_name: str,
                        tags: dict[str, str], threshold: float,
                        for_seconds: float = 60.0,
                        severity: str = "warning") -> AlertRule:
        return AlertRule(
            rule_id=str(uuid.uuid4())[:8],
            name=name,
            metric_name=metric_name,
            tags=tags,
            condition=lambda v: v > threshold,
            condition_str=f"value > {threshold}",
            for_seconds=for_seconds,
            severity=severity,
        )

    @staticmethod
    def threshold_below(name: str, metric_name: str,
                        tags: dict[str, str], threshold: float,
                        for_seconds: float = 60.0,
                        severity: str = "warning") -> AlertRule:
        return AlertRule(
            rule_id=str(uuid.uuid4())[:8],
            name=name,
            metric_name=metric_name,
            tags=tags,
            condition=lambda v: v < threshold,
            condition_str=f"value < {threshold}",
            for_seconds=for_seconds,
            severity=severity,
        )


@dataclass
class AlertEvent:
    """Record of an alert firing or resolving."""

    alert_id: str
    rule_id: str
    rule_name: str
    state: AlertState
    fired_at: float
    resolved_at: Optional[float] = None
    metric_value: Optional[float] = None
    severity: str = "warning"


class AlertEngine:
    """Evaluates alert rules against the TSDB using a state machine."""

    def __init__(self, db: TimeSeriesDB) -> None:
        self.db = db
        self.rules: list[AlertRule] = []
        self.history: list[AlertEvent] = []
        # rule_id -> (state, pending_since)
        self._state: dict[str, tuple[AlertState, float]] = {}

    def add_rule(self, rule: AlertRule) -> None:
        self.rules.append(rule)
        self._state[rule.rule_id] = (AlertState.INACTIVE, 0.0)

    def evaluate(self, eval_time: float | None = None) -> list[AlertEvent]:
        """Evaluate all rules at eval_time. Returns new events."""
        now = eval_time if eval_time is not None else time.time()
        events: list[AlertEvent] = []

        for rule in self.rules:
            window_start = now - rule.for_seconds
            data = self.db.query_raw(rule.metric_name, rule.tags, window_start, now)

            if not data:
                self._try_resolve(rule, now, events)
                continue

            values = [v for _, v in data]
            avg_value = statistics.mean(values)
            current_state, pending_since = self._state[rule.rule_id]

            if rule.condition(avg_value):
                if current_state == AlertState.INACTIVE or current_state == AlertState.RESOLVED:
                    self._state[rule.rule_id] = (AlertState.PENDING, now)
                elif current_state == AlertState.PENDING:
                    if now - pending_since >= rule.for_seconds:
                        self._state[rule.rule_id] = (AlertState.FIRING, now)
                        event = AlertEvent(
                            alert_id=str(uuid.uuid4())[:8],
                            rule_id=rule.rule_id,
                            rule_name=rule.name,
                            state=AlertState.FIRING,
                            fired_at=now,
                            metric_value=avg_value,
                            severity=rule.severity,
                        )
                        events.append(event)
                        self.history.append(event)
            else:
                self._try_resolve(rule, now, events)

        return events

    def _try_resolve(self, rule: AlertRule, now: float,
                     events: list[AlertEvent]) -> None:
        current_state, _ = self._state[rule.rule_id]
        if current_state == AlertState.FIRING:
            self._state[rule.rule_id] = (AlertState.RESOLVED, now)
            event = AlertEvent(
                alert_id=str(uuid.uuid4())[:8],
                rule_id=rule.rule_id,
                rule_name=rule.name,
                state=AlertState.RESOLVED,
                fired_at=now,
                resolved_at=now,
                severity=rule.severity,
            )
            events.append(event)
            self.history.append(event)
        elif current_state == AlertState.PENDING:
            self._state[rule.rule_id] = (AlertState.INACTIVE, 0.0)

    def get_state(self, rule_id: str) -> AlertState:
        return self._state.get(rule_id, (AlertState.INACTIVE, 0.0))[0]


# ---------------------------------------------------------------------------
# 6. Demo / Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 70)
    print("  Metrics & Monitoring System -- Simulation")
    print("=" * 70)

    # -- Setup --
    db = TimeSeriesDB(partition_duration=60.0)  # 1-min partitions for demo
    query_engine = QueryEngine(db)
    alert_engine = AlertEngine(db)

    # ----------------------------------------------------------------
    # Section 1: Metric Types
    # ----------------------------------------------------------------
    print("\n--- 1. Metric Types (Counter, Gauge, Histogram) ---")

    # Counter
    req_counter = Counter("http_requests_total", {"service": "api", "method": "GET"})
    for _ in range(150):
        req_counter.inc()
    print(f"Counter [http_requests_total]: {req_counter.get()}")

    # Gauge
    cpu_gauge = Gauge("system_cpu_usage", {"host": "web-01"})
    cpu_gauge.set(0.45)
    cpu_gauge.inc(0.20)
    print(f"Gauge   [system_cpu_usage]:    {cpu_gauge.get():.2f}")

    # Histogram
    latency_hist = Histogram(
        "http_request_duration_seconds",
        {"service": "api"},
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
    )
    import random
    random.seed(42)
    for _ in range(1000):
        latency_hist.observe(random.expovariate(5.0))  # avg ~0.2s
    print(f"Histogram observations: {latency_hist.count}")
    print(f"  p50 = {latency_hist.percentile(50):.4f}s")
    print(f"  p95 = {latency_hist.percentile(95):.4f}s")
    print(f"  p99 = {latency_hist.percentile(99):.4f}s")

    # ----------------------------------------------------------------
    # Section 2: Write to TSDB
    # ----------------------------------------------------------------
    print("\n--- 2. Write Metrics to TSDB ---")

    base_time = 1700000000.0  # fixed epoch for reproducibility
    tags_web01 = {"host": "web-01"}
    tags_web02 = {"host": "web-02"}

    # Simulate 5 minutes of CPU data (1 point every 15s)
    points_written = 0
    for i in range(20):  # 20 * 15s = 5 min
        ts = base_time + i * 15.0
        # web-01: hovers around 0.7
        v1 = 0.7 + random.uniform(-0.1, 0.15)
        db.write(MetricPoint.make("cpu_usage", tags_web01, v1, ts))
        # web-02: hovers around 0.3
        v2 = 0.3 + random.uniform(-0.05, 0.05)
        db.write(MetricPoint.make("cpu_usage", tags_web02, v2, ts))
        points_written += 2

    print(f"Points written: {points_written}")
    print(f"Partitions created: {db.partition_count()}")
    print(f"Unique series: {db.series_count()}")

    # ----------------------------------------------------------------
    # Section 3: Query Engine
    # ----------------------------------------------------------------
    print("\n--- 3. Query Engine ---")

    q_start = base_time
    q_end = base_time + 300.0  # 5 minutes

    for agg in [AggregationType.AVG, AggregationType.MAX, AggregationType.P95]:
        val = query_engine.query("cpu_usage", tags_web01, q_start, q_end, agg)
        if val is not None:
            print(f"  cpu_usage (web-01) {agg.value:>5}: {val:.4f}")

    print("\n  Step query (1-min buckets, avg):")
    step_results = query_engine.query_over_time(
        "cpu_usage", tags_web01, q_start, q_end, step=60.0,
        aggregation=AggregationType.AVG,
    )
    for bucket_ts, avg_val in step_results:
        offset = int(bucket_ts - base_time)
        print(f"    t+{offset:>3}s: avg={avg_val:.4f}")

    # ----------------------------------------------------------------
    # Section 4: Downsampling
    # ----------------------------------------------------------------
    print("\n--- 4. Downsampling ---")

    rollup_count = db.downsample(resolution_seconds=60)
    print(f"1-min rollup points created: {rollup_count}")

    rollup_data = db.query_rollup("cpu_usage", tags_web01, 60, q_start, q_end)
    print(f"1-min rollup series for web-01: {len(rollup_data)} points")
    for ts, val in rollup_data:
        offset = int(ts - base_time)
        print(f"    t+{offset:>3}s: avg={val:.4f}")

    # ----------------------------------------------------------------
    # Section 5: Alerting Engine
    # ----------------------------------------------------------------
    print("\n--- 5. Alerting Engine ---")

    # Rule: alert if avg cpu > 0.6 for 60 seconds
    high_cpu_rule = AlertRule.threshold_above(
        name="HighCPU-web01",
        metric_name="cpu_usage",
        tags=tags_web01,
        threshold=0.6,
        for_seconds=60.0,
        severity="critical",
    )
    alert_engine.add_rule(high_cpu_rule)

    # Rule: alert if avg cpu < 0.1 (should NOT fire for web-02)
    low_cpu_rule = AlertRule.threshold_below(
        name="LowCPU-web02",
        metric_name="cpu_usage",
        tags=tags_web02,
        threshold=0.1,
        for_seconds=60.0,
        severity="warning",
    )
    alert_engine.add_rule(low_cpu_rule)

    print(f"Rules registered: {len(alert_engine.rules)}")
    for rule in alert_engine.rules:
        print(f"  [{rule.severity:>8}] {rule.name}: {rule.condition_str}")

    # First evaluation -> should enter PENDING (condition true but not held long enough)
    events = alert_engine.evaluate(eval_time=base_time + 60.0)
    state1 = alert_engine.get_state(high_cpu_rule.rule_id)
    print(f"\nEval @t+60s:  HighCPU state = {state1.value}")

    # Second evaluation -> after for_seconds elapsed, should FIRE
    events = alert_engine.evaluate(eval_time=base_time + 180.0)
    state2 = alert_engine.get_state(high_cpu_rule.rule_id)
    print(f"Eval @t+180s: HighCPU state = {state2.value}")
    for e in events:
        print(f"  ALERT FIRED: {e.rule_name} (severity={e.severity}, "
              f"value={e.metric_value:.4f})")

    # Simulate resolution: write low CPU values
    for i in range(8):
        ts = base_time + 300.0 + i * 15.0
        db.write(MetricPoint.make("cpu_usage", tags_web01, 0.2, ts))

    events = alert_engine.evaluate(eval_time=base_time + 420.0)
    state3 = alert_engine.get_state(high_cpu_rule.rule_id)
    print(f"Eval @t+420s: HighCPU state = {state3.value}")
    for e in events:
        print(f"  ALERT RESOLVED: {e.rule_name}")

    # Low CPU rule should stay inactive (web-02 is at ~0.3, not < 0.1)
    low_state = alert_engine.get_state(low_cpu_rule.rule_id)
    print(f"LowCPU-web02 state: {low_state.value}")

    # ----------------------------------------------------------------
    # Section 6: Alert History
    # ----------------------------------------------------------------
    print("\n--- 6. Alert History ---")
    for event in alert_engine.history:
        print(f"  [{event.state.value:>8}] {event.rule_name} "
              f"(alert_id={event.alert_id})")

    # ----------------------------------------------------------------
    # Section 7: Retention Policy
    # ----------------------------------------------------------------
    print("\n--- 7. Retention Policy ---")
    print(f"Partitions before retention: {db.partition_count()}")
    # Drop partitions older than t+120s
    dropped = db.retention_drop(older_than=base_time + 120.0)
    print(f"Partitions dropped (older than t+120s): {dropped}")
    print(f"Partitions after retention: {db.partition_count()}")

    # ----------------------------------------------------------------
    # Section 8: Batch Write Performance
    # ----------------------------------------------------------------
    print("\n--- 8. Batch Write Throughput ---")
    batch_points = [
        MetricPoint.make(
            "bench_metric",
            {"host": f"h-{i % 10}"},
            random.random() * 100,
            base_time + i * 0.01,
        )
        for i in range(10000)
    ]
    t0 = time.perf_counter()
    written = db.write_batch(batch_points)
    t1 = time.perf_counter()
    elapsed_ms = (t1 - t0) * 1000
    rate = written / (t1 - t0) if (t1 - t0) > 0 else 0
    print(f"Wrote {written} points in {elapsed_ms:.1f}ms ({rate:,.0f} points/sec)")

    # ----------------------------------------------------------------
    print("\n" + "=" * 70)
    print("  Simulation Complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
