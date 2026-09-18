"""LeetCode 2034 - Stock Price Fluctuation.

Problem summary:
You receive ``update(timestamp, price)`` calls for a stock.  A later update may
correct the price at an existing timestamp.  Support three queries:

* ``current()``: price at the latest timestamp seen so far
* ``maximum()``: highest current price across all timestamps
* ``minimum()``: lowest current price across all timestamps

Approach:
Store the latest price for each timestamp in a dictionary.  Also push every
update into a min-heap and a max-heap.  When ``maximum()`` or ``minimum()`` is
called, discard stale heap entries until the heap top matches the dictionary's
current price for that timestamp.  This is the standard lazy-deletion pattern.

LeetCode guarantees that query methods are only called after at least one
``update``.  This module raises ``ValueError`` defensively when that contract is
violated in local use.

Complexity:
* ``update``: O(log n)
* ``current``: O(1)
* ``maximum`` / ``minimum``: amortized O(log n)
* Extra space: O(u + k), where ``u`` is unique timestamps and ``k`` is total
  pushed heap entries still retained for lazy deletion
"""

from __future__ import annotations

import heapq


class StockPrice:
    """Track the latest, maximum, and minimum stock prices."""

    def __init__(self) -> None:
        self._latest_timestamp: int | None = None
        self._prices_by_timestamp: dict[int, int] = {}
        self._min_heap: list[tuple[int, int]] = []
        self._max_heap: list[tuple[int, int]] = []

    def update(self, timestamp: int, price: int) -> None:
        """Record or correct the price at ``timestamp``."""
        self._prices_by_timestamp[timestamp] = price
        self._latest_timestamp = (
            timestamp
            if self._latest_timestamp is None
            else max(self._latest_timestamp, timestamp)
        )
        heapq.heappush(self._min_heap, (price, timestamp))
        heapq.heappush(self._max_heap, (-price, timestamp))

    def current(self) -> int:
        """Return the price at the latest timestamp seen so far."""
        if self._latest_timestamp is None:
            raise ValueError("current price requires at least one update")
        return self._prices_by_timestamp[self._latest_timestamp]

    def maximum(self) -> int:
        """Return the highest current price across all timestamps."""
        self._ensure_has_prices()
        self._discard_stale_max_entries()
        return -self._max_heap[0][0]

    def minimum(self) -> int:
        """Return the lowest current price across all timestamps."""
        self._ensure_has_prices()
        self._discard_stale_min_entries()
        return self._min_heap[0][0]

    def _ensure_has_prices(self) -> None:
        if not self._prices_by_timestamp:
            raise ValueError("price queries require at least one update")

    def _discard_stale_max_entries(self) -> None:
        while self._max_heap:
            neg_price, timestamp = self._max_heap[0]
            if self._prices_by_timestamp.get(timestamp) == -neg_price:
                return
            heapq.heappop(self._max_heap)

    def _discard_stale_min_entries(self) -> None:
        while self._min_heap:
            price, timestamp = self._min_heap[0]
            if self._prices_by_timestamp.get(timestamp) == price:
                return
            heapq.heappop(self._min_heap)


if __name__ == "__main__":
    tracker = StockPrice()

    tracker.update(1, 10)
    tracker.update(2, 5)
    assert tracker.current() == 5
    assert tracker.maximum() == 10

    tracker.update(1, 3)  # correct timestamp 1
    assert tracker.maximum() == 5
    assert tracker.minimum() == 3

    tracker.update(4, 2)
    assert tracker.current() == 2
    assert tracker.minimum() == 2

    print("StockPrice example passed.")
