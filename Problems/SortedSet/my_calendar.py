# My Calendar I — LeetCode 729
#
# Problem:
#   Implement a calendar that prevents double-booking.  Each event is a
#   half-open interval [start, end).
#
#   book(start, end) → True if the event can be added without overlapping any
#                      existing event, False otherwise.
#
# Approach:
#   Store booked intervals as (start, end) tuples in a SortedSet (from
#   DataStructures/SortedSet.py).  Tuples compare lexicographically, so
#   ordering by start time is natural.
#
#   To check for overlaps when booking [s, e):
#     1. Use floor((s, float('inf'))) to find the greatest existing interval
#        whose start ≤ s.  If its end > s, there is overlap.
#     2. Use ceiling((s, 0)) to find the smallest existing interval whose
#        start ≥ s.  If s + ... overlaps, i.e. its start < e, there is overlap.
#
# Complexity:
#   Time:  O(log n) per book (AVL-backed SortedSet)
#   Space: O(n)

import sys
import os
from typing import List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.SortedSet import SortedSet


class MyCalendar:
    """Calendar that rejects double-bookings.

    Examples:
        >>> cal = MyCalendar()
        >>> cal.book(10, 20)
        True
        >>> cal.book(15, 25)
        False
        >>> cal.book(20, 30)
        True
    """

    def __init__(self) -> None:
        self._events: SortedSet = SortedSet()

    def book(self, start: int, end: int) -> bool:
        """Attempt to book [start, end).  Returns True on success.

        Args:
            start: Inclusive start time.
            end:   Exclusive end time (start < end).

        Returns:
            True if the event was booked, False if it conflicts.
        """
        if start >= end:
            return False

        # Check the interval that starts at or just before *start*.
        prev = self._events.floor((start, float("inf")))
        if prev is not None:
            prev_start, prev_end = prev
            if prev_end > start:
                return False

        # Check the interval that starts at or just after *start*.
        nxt = self._events.ceiling((start, 0))
        if nxt is not None:
            nxt_start, _nxt_end = nxt
            if nxt_start < end:
                return False

        self._events.add((start, end))
        return True


def process_bookings(bookings: List[Tuple[int, int]]) -> List[bool]:
    """Run a list of booking attempts and return the results.

    Args:
        bookings: List of (start, end) pairs.

    Returns:
        List of booleans corresponding to each booking attempt.

    Examples:
        >>> process_bookings([(10, 20), (15, 25), (20, 30)])
        [True, False, True]
    """
    cal = MyCalendar()
    return [cal.book(s, e) for s, e in bookings]


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _run_tests() -> None:
    """Verify correctness with several test scenarios."""

    # --- LeetCode example ---
    assert process_bookings([(10, 20), (15, 25), (20, 30)]) == [True, False, True]

    # --- No overlap at all ---
    assert process_bookings([(0, 5), (5, 10), (10, 15)]) == [True, True, True]

    # --- Complete overlap ---
    assert process_bookings([(0, 10), (0, 10)]) == [True, False]

    # --- New event entirely inside existing ---
    assert process_bookings([(0, 20), (5, 15)]) == [True, False]

    # --- Existing event entirely inside new ---
    assert process_bookings([(5, 15), (0, 20)]) == [True, False]

    # --- Adjacent events (touching boundaries are OK) ---
    assert process_bookings([(0, 5), (5, 10), (10, 20)]) == [True, True, True]

    # --- Single-unit intervals ---
    assert process_bookings([(1, 2), (2, 3), (3, 4), (1, 2)]) == [
        True, True, True, False
    ]

    # --- Reverse insertion order ---
    assert process_bookings([(20, 30), (10, 20), (0, 10)]) == [True, True, True]

    # --- Partial overlap from the left ---
    assert process_bookings([(10, 20), (5, 15)]) == [True, False]

    # --- Partial overlap from the right ---
    assert process_bookings([(10, 20), (15, 25)]) == [True, False]

    # --- Invalid interval (start >= end) ---
    assert process_bookings([(5, 5), (10, 5)]) == [False, False]

    # --- Stress test: non-overlapping unit intervals ---
    bookings = [(i, i + 1) for i in range(100)]
    results = process_bookings(bookings)
    assert all(results), "unit intervals should all succeed"

    # --- Stress test: all overlapping with first ---
    bookings = [(0, 100)] + [(i, i + 2) for i in range(99)]
    results = process_bookings(bookings)
    assert results[0] is True
    assert all(r is False for r in results[1:])

    print("All my_calendar tests passed ✓")


if __name__ == "__main__":
    _run_tests()
