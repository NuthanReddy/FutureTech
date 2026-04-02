"""Distributed Rate Limiter - Four Algorithm Implementations.

Demonstrates four common rate-limiting algorithms used in distributed systems:
    1. Token Bucket       - allows controlled bursts
    2. Sliding Window Log - exact per-window accuracy
    3. Sliding Window Counter - memory-efficient approximation
    4. Leaky Bucket       - constant-rate output

Each algorithm implements a common ``RateLimiter`` interface so they can be
swapped via the Strategy pattern.  A ``RateLimiterFactory`` selects the right
implementation based on a string algorithm name.

Usage::

    limiter = RateLimiterFactory.create("token_bucket", max_requests=10, window_seconds=1)
    result = limiter.allow_request("user_123")
    print(result)  # RateLimitResult(allowed=True, remaining=9, ...)
"""

from __future__ import annotations

import math
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RateLimitResult:
    """Result returned by every rate-limit check."""

    allowed: bool
    limit: int
    remaining: int
    reset_at: float
    retry_after: Optional[float] = None

    def __str__(self) -> str:
        status = "ALLOWED" if self.allowed else "DENIED"
        retry = f", retry_after={self.retry_after:.2f}s" if self.retry_after else ""
        return f"{status} (remaining={self.remaining}/{self.limit}{retry})"


class Algorithm(str, Enum):
    """Supported rate-limiting algorithms."""

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW_LOG = "sliding_window_log"
    SLIDING_WINDOW_COUNTER = "sliding_window_counter"
    LEAKY_BUCKET = "leaky_bucket"


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class RateLimiter(ABC):
    """Common interface for all rate-limiting algorithms."""

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    @abstractmethod
    def allow_request(self, key: str) -> RateLimitResult:
        """Check whether a request identified by *key* should be allowed.

        Args:
            key: Client identifier (user ID, IP, API key).

        Returns:
            A ``RateLimitResult`` with the decision and metadata.
        """

    @abstractmethod
    def _get_algorithm_name(self) -> str:
        """Return a human-readable algorithm name."""


# ---------------------------------------------------------------------------
# 1. Token Bucket
# ---------------------------------------------------------------------------

@dataclass
class _TokenBucketState:
    tokens: float
    last_refill: float


class TokenBucket(RateLimiter):
    """Token Bucket rate limiter.

    A bucket holds up to *max_requests* tokens.  Tokens refill at a steady
    rate of ``max_requests / window_seconds`` per second.  Each request
    consumes one token.  Allows **bursts** up to the bucket capacity.
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
        refill_rate: Optional[float] = None,
    ) -> None:
        super().__init__(max_requests, window_seconds)
        self.capacity = max_requests
        self.refill_rate = refill_rate or (max_requests / window_seconds)
        self._buckets: Dict[str, _TokenBucketState] = {}

    def allow_request(self, key: str) -> RateLimitResult:
        now = time.monotonic()

        if key not in self._buckets:
            self._buckets[key] = _TokenBucketState(
                tokens=float(self.capacity), last_refill=now
            )

        state = self._buckets[key]

        # Refill tokens based on elapsed time
        elapsed = now - state.last_refill
        state.tokens = min(self.capacity, state.tokens + elapsed * self.refill_rate)
        state.last_refill = now

        reset_at = now + self.window_seconds

        if state.tokens >= 1.0:
            state.tokens -= 1.0
            return RateLimitResult(
                allowed=True,
                limit=self.capacity,
                remaining=int(state.tokens),
                reset_at=reset_at,
            )

        retry_after = (1.0 - state.tokens) / self.refill_rate
        return RateLimitResult(
            allowed=False,
            limit=self.capacity,
            remaining=0,
            reset_at=reset_at,
            retry_after=retry_after,
        )

    def _get_algorithm_name(self) -> str:
        return "Token Bucket"


# ---------------------------------------------------------------------------
# 2. Sliding Window Log
# ---------------------------------------------------------------------------

class SlidingWindowLog(RateLimiter):
    """Sliding Window Log rate limiter.

    Maintains a sorted list of request timestamps per client.  Old entries
    (outside the current window) are pruned on every check.  Provides
    **exact** per-window accuracy at the cost of O(N) memory per client.
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        super().__init__(max_requests, window_seconds)
        self._logs: Dict[str, List[float]] = {}

    def allow_request(self, key: str) -> RateLimitResult:
        now = time.monotonic()
        window_start = now - self.window_seconds

        if key not in self._logs:
            self._logs[key] = []

        log = self._logs[key]

        # Prune entries outside the window
        while log and log[0] <= window_start:
            log.pop(0)

        reset_at = (log[0] + self.window_seconds) if log else (now + self.window_seconds)
        remaining = self.max_requests - len(log)

        if len(log) < self.max_requests:
            log.append(now)
            remaining -= 1  # just consumed one
            return RateLimitResult(
                allowed=True,
                limit=self.max_requests,
                remaining=max(0, remaining),
                reset_at=reset_at,
            )

        retry_after = log[0] + self.window_seconds - now
        return RateLimitResult(
            allowed=False,
            limit=self.max_requests,
            remaining=0,
            reset_at=reset_at,
            retry_after=max(0.0, retry_after),
        )

    def _get_algorithm_name(self) -> str:
        return "Sliding Window Log"


# ---------------------------------------------------------------------------
# 3. Sliding Window Counter
# ---------------------------------------------------------------------------

@dataclass
class _WindowCounterState:
    prev_count: int = 0
    curr_count: int = 0
    window_start: float = 0.0


class SlidingWindowCounter(RateLimiter):
    """Sliding Window Counter rate limiter.

    Divides time into fixed windows and keeps a counter for the current and
    previous window.  The effective request count is approximated as::

        estimated = prev_count * overlap_ratio + curr_count

    Very **memory efficient** (O(1) per client) with good-enough precision
    for most API rate-limiting use cases.
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        super().__init__(max_requests, window_seconds)
        self._counters: Dict[str, _WindowCounterState] = {}

    def _rotate_window(self, state: _WindowCounterState, now: float) -> None:
        """Advance windows if the current one has expired."""
        window_size = self.window_seconds

        if state.window_start == 0.0:
            state.window_start = math.floor(now / window_size) * window_size
            return

        current_window = math.floor(now / window_size) * window_size

        if current_window > state.window_start:
            windows_elapsed = int(
                (current_window - state.window_start) / window_size
            )
            if windows_elapsed == 1:
                state.prev_count = state.curr_count
            else:
                # More than one window has passed; previous data is stale
                state.prev_count = 0
            state.curr_count = 0
            state.window_start = current_window

    def allow_request(self, key: str) -> RateLimitResult:
        now = time.monotonic()

        if key not in self._counters:
            self._counters[key] = _WindowCounterState()

        state = self._counters[key]
        self._rotate_window(state, now)

        elapsed_in_window = now - state.window_start
        overlap_ratio = 1.0 - (elapsed_in_window / self.window_seconds)
        estimated = state.prev_count * overlap_ratio + state.curr_count

        reset_at = state.window_start + self.window_seconds
        remaining = max(0, int(self.max_requests - estimated))

        if estimated < self.max_requests:
            state.curr_count += 1
            remaining = max(0, int(self.max_requests - estimated - 1))
            return RateLimitResult(
                allowed=True,
                limit=self.max_requests,
                remaining=remaining,
                reset_at=reset_at,
            )

        retry_after = reset_at - now
        return RateLimitResult(
            allowed=False,
            limit=self.max_requests,
            remaining=0,
            reset_at=reset_at,
            retry_after=max(0.0, retry_after),
        )

    def _get_algorithm_name(self) -> str:
        return "Sliding Window Counter"


# ---------------------------------------------------------------------------
# 4. Leaky Bucket
# ---------------------------------------------------------------------------

@dataclass
class _LeakyBucketState:
    queue_size: float = 0.0
    last_leak: float = 0.0


class LeakyBucket(RateLimiter):
    """Leaky Bucket rate limiter.

    Requests enter a virtual queue of fixed capacity.  The queue drains
    (leaks) at a constant rate.  If the queue is full the request is
    rejected.  Produces **constant-rate** output regardless of input
    burstiness.
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
        leak_rate: Optional[float] = None,
    ) -> None:
        super().__init__(max_requests, window_seconds)
        self.capacity = max_requests
        self.leak_rate = leak_rate or (max_requests / window_seconds)
        self._buckets: Dict[str, _LeakyBucketState] = {}

    def allow_request(self, key: str) -> RateLimitResult:
        now = time.monotonic()

        if key not in self._buckets:
            self._buckets[key] = _LeakyBucketState(queue_size=0.0, last_leak=now)

        state = self._buckets[key]

        # Drain the queue based on elapsed time
        elapsed = now - state.last_leak
        leaked = elapsed * self.leak_rate
        state.queue_size = max(0.0, state.queue_size - leaked)
        state.last_leak = now

        reset_at = now + (state.queue_size / self.leak_rate) if state.queue_size > 0 else now
        remaining = max(0, int(self.capacity - state.queue_size))

        if state.queue_size < self.capacity:
            state.queue_size += 1.0
            remaining = max(0, int(self.capacity - state.queue_size))
            return RateLimitResult(
                allowed=True,
                limit=self.capacity,
                remaining=remaining,
                reset_at=reset_at,
            )

        retry_after = 1.0 / self.leak_rate
        return RateLimitResult(
            allowed=False,
            limit=self.capacity,
            remaining=0,
            reset_at=reset_at,
            retry_after=retry_after,
        )

    def _get_algorithm_name(self) -> str:
        return "Leaky Bucket"


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

class RateLimiterFactory:
    """Factory that creates the appropriate ``RateLimiter`` from a name.

    Example::

        limiter = RateLimiterFactory.create("token_bucket", max_requests=5, window_seconds=1)
    """

    _registry: Dict[str, type] = {
        Algorithm.TOKEN_BUCKET.value: TokenBucket,
        Algorithm.SLIDING_WINDOW_LOG.value: SlidingWindowLog,
        Algorithm.SLIDING_WINDOW_COUNTER.value: SlidingWindowCounter,
        Algorithm.LEAKY_BUCKET.value: LeakyBucket,
    }

    @classmethod
    def create(
        cls,
        algorithm: str,
        max_requests: int,
        window_seconds: float,
        **kwargs,
    ) -> RateLimiter:
        """Create a rate limiter of the given algorithm type.

        Args:
            algorithm: One of ``token_bucket``, ``sliding_window_log``,
                       ``sliding_window_counter``, ``leaky_bucket``.
            max_requests: Maximum number of requests allowed per window.
            window_seconds: Duration of the rate-limit window in seconds.
            **kwargs: Extra keyword args forwarded to the constructor
                      (e.g. ``refill_rate`` for token bucket).

        Returns:
            A configured ``RateLimiter`` instance.

        Raises:
            ValueError: If the algorithm name is not recognised.
        """
        impl_cls = cls._registry.get(algorithm)
        if impl_cls is None:
            supported = ", ".join(sorted(cls._registry.keys()))
            raise ValueError(
                f"Unknown algorithm '{algorithm}'. Supported: {supported}"
            )
        return impl_cls(max_requests=max_requests, window_seconds=window_seconds, **kwargs)

    @classmethod
    def supported_algorithms(cls) -> List[str]:
        """Return the list of supported algorithm names."""
        return sorted(cls._registry.keys())


# ---------------------------------------------------------------------------
# Comparison demo
# ---------------------------------------------------------------------------

def _run_comparison_demo() -> None:
    """Send identical request patterns through all four algorithms and
    compare their behaviour side by side."""

    print("=" * 72)
    print("  Distributed Rate Limiter - Algorithm Comparison Demo")
    print("=" * 72)

    algorithms = RateLimiterFactory.supported_algorithms()
    max_requests = 5
    window_seconds = 1.0

    # -- Phase 1: Burst of requests ----------------------------------------
    print("\n--- Phase 1: Burst of 8 rapid requests (limit=5/sec) ---\n")

    limiters = {
        name: RateLimiterFactory.create(name, max_requests=max_requests, window_seconds=window_seconds)
        for name in algorithms
    }

    header = f"{'Req':>3}  "
    for name in algorithms:
        header += f"{name:<28}"
    print(header)
    print("-" * len(header))

    for i in range(1, 9):
        row = f"{i:>3}  "
        for name in algorithms:
            result = limiters[name].allow_request("user_1")
            row += f"{str(result):<28}"
        print(row)

    # -- Phase 2: Wait, then retry -----------------------------------------
    print("\n--- Phase 2: Wait 1.1 seconds, then send 3 more requests ---\n")
    time.sleep(1.1)

    print(header)
    print("-" * len(header))

    for i in range(9, 12):
        row = f"{i:>3}  "
        for name in algorithms:
            result = limiters[name].allow_request("user_1")
            row += f"{str(result):<28}"
        print(row)

    # -- Phase 3: Steady trickle -------------------------------------------
    print("\n--- Phase 3: Steady trickle (1 req every 0.25s, 8 requests) ---\n")

    limiters2 = {
        name: RateLimiterFactory.create(name, max_requests=max_requests, window_seconds=window_seconds)
        for name in algorithms
    }

    print(header)
    print("-" * len(header))

    for i in range(1, 9):
        row = f"{i:>3}  "
        for name in algorithms:
            result = limiters2[name].allow_request("user_2")
            row += f"{str(result):<28}"
        print(row)
        if i < 8:
            time.sleep(0.25)

    # -- Phase 4: Multiple clients -----------------------------------------
    print("\n--- Phase 4: Two clients sharing same limiter ---\n")

    shared = RateLimiterFactory.create(
        "token_bucket", max_requests=5, window_seconds=1.0
    )

    print(f"{'Req':>3}  {'Client':<10} {'Result':<30}")
    print("-" * 46)

    for i in range(1, 9):
        client = "alice" if i % 2 == 1 else "bob"
        result = shared.allow_request(client)
        print(f"{i:>3}  {client:<10} {str(result):<30}")

    # -- Summary ------------------------------------------------------------
    print("\n" + "=" * 72)
    print("  Summary of Behaviour Differences")
    print("=" * 72)
    print("""
  Token Bucket:
    - Allows initial burst up to capacity, then refills gradually.
    - Good for APIs that tolerate short bursts.

  Sliding Window Log:
    - Tracks every request timestamp; exact counting.
    - Highest memory cost but most precise.

  Sliding Window Counter:
    - Approximates sliding window with two fixed-window counters.
    - Very memory-efficient; slight over/under-count at boundaries.

  Leaky Bucket:
    - Drains at constant rate; no bursts allowed.
    - Best for protecting downstream services from traffic spikes.
""")
    print("Supported algorithms:", RateLimiterFactory.supported_algorithms())


if __name__ == "__main__":
    _run_comparison_demo()
