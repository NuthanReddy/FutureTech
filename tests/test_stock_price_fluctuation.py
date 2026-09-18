"""Focused tests for the heap-based Stock Price Fluctuation solution."""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from Problems.Heap.stock_price_fluctuation import StockPrice


def test_stock_price_matches_leetcode_example_flow() -> None:
    tracker = StockPrice()

    tracker.update(1, 10)
    tracker.update(2, 5)
    assert tracker.current() == 5
    assert tracker.maximum() == 10

    tracker.update(1, 3)
    assert tracker.maximum() == 5

    tracker.update(4, 2)
    assert tracker.minimum() == 2
    assert tracker.current() == 2


def test_stock_price_current_uses_latest_timestamp_after_corrections() -> None:
    tracker = StockPrice()

    tracker.update(4, 7)
    tracker.update(2, 11)
    tracker.update(4, 9)

    assert tracker.current() == 9
    assert tracker.maximum() == 11
    assert tracker.minimum() == 9


def test_stock_price_lazy_deletion_discards_stale_extrema() -> None:
    tracker = StockPrice()

    tracker.update(1, 8)
    tracker.update(2, 6)
    tracker.update(3, 9)
    tracker.update(3, 4)  # stale max of 9 remains in heap
    tracker.update(1, 10)  # stale min of 8 remains in heap

    assert tracker.maximum() == 10
    assert tracker.minimum() == 4


@pytest.mark.parametrize("query_name", ["current", "maximum", "minimum"])
def test_stock_price_rejects_queries_before_first_update(query_name: str) -> None:
    tracker = StockPrice()

    with pytest.raises(ValueError, match="at least one update"):
        getattr(tracker, query_name)()
