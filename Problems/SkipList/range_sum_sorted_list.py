# ---------------------------------------------------------------------------
# Problem: Range Sum on a Dynamic Sorted Collection (using a Skip List)
# ---------------------------------------------------------------------------
# Maintain a dynamic sorted collection supporting three operations:
#
#   insert(val)           — add a value to the collection
#   delete(val)           — remove one occurrence of val (return False if absent)
#   range_sum(low, high)  — return the sum of all values in [low, high]
#
# Implementation:
#   We use a SkipList from DataStructures/SkipList.py as the sorted backbone.
#   Because SkipList keys must be unique, we store (value, count) pairs and
#   manage duplicates via the count field.
#
#   range_sum iterates through the skip list's sorted elements and sums
#   those within [low, high].
#
# Complexity:
#   insert     : O(log n)  (skip list insert/update)
#   delete     : O(log n)
#   range_sum  : O(log n + r)  where r = number of distinct keys in [low, high]
# ---------------------------------------------------------------------------

from __future__ import annotations

import os
import sys

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(_project_root, "DataStructures"))

from SkipList import SkipList


class RangeSumSortedList:
    """A dynamic sorted collection supporting insert, delete, and range_sum.

    Internally uses a SkipList keyed by value; the associated SkipList value
    stores the count of that element.
    """

    def __init__(self) -> None:
        self._sl = SkipList()
        self._total_count = 0

    def insert(self, val: int) -> None:
        """Insert *val* into the collection.

        Duplicates are allowed — an internal counter tracks occurrences.

        Args:
            val: The integer value to add.
        """
        existing_count = self._sl.search(val)
        if existing_count is not None:
            self._sl.insert(val, existing_count + 1)
        else:
            self._sl.insert(val, 1)
        self._total_count += 1

    def delete(self, val: int) -> bool:
        """Remove one occurrence of *val*.

        Args:
            val: The value to remove.

        Returns:
            True if the value was present and removed, False otherwise.
        """
        existing_count = self._sl.search(val)
        if existing_count is None:
            return False
        if existing_count == 1:
            self._sl.delete(val)
        else:
            self._sl.insert(val, existing_count - 1)
        self._total_count -= 1
        return True

    def range_sum(self, low: int, high: int) -> int:
        """Return the sum of all values in [low, high].

        Each duplicate contributes independently (e.g. if 5 appears
        three times, it adds 15 to any range containing 5).

        Args:
            low: Lower bound (inclusive).
            high: Upper bound (inclusive).

        Returns:
            The sum of qualifying values.
        """
        if low > high:
            return 0

        total = 0
        for key, count in self._sl:
            if key > high:
                break
            if key >= low:
                total += key * count
        return total

    def to_sorted_list(self) -> list[int]:
        """Return all elements as a sorted list (expanding duplicates)."""
        result: list[int] = []
        for key, count in self._sl:
            result.extend([key] * count)
        return result

    def __len__(self) -> int:
        return self._total_count

    def __repr__(self) -> str:
        return f"RangeSumSortedList({self.to_sorted_list()})"


if __name__ == "__main__":
    print("=" * 60)
    print("  Range Sum on a Dynamic Sorted Collection (Skip List)")
    print("=" * 60)

    # -- Walkthrough example ----------------------------------------------------
    print("\n--- Walkthrough ---")
    rsl = RangeSumSortedList()

    values_to_insert = [10, 20, 30, 40, 50, 15, 25, 35]
    for v in values_to_insert:
        rsl.insert(v)
    print(f"  Inserted: {values_to_insert}")
    print(f"  Sorted  : {rsl.to_sorted_list()}")
    print(f"  Size    : {len(rsl)}")

    s = rsl.range_sum(15, 35)
    print(f"  range_sum(15, 35) = {s}")
    assert s == 15 + 20 + 25 + 30 + 35, f"Expected 125, got {s}"
    print(f"  Expected 125      ✓")

    s_all = rsl.range_sum(0, 100)
    print(f"  range_sum(0, 100) = {s_all}")
    assert s_all == sum(values_to_insert)
    print(f"  Expected {sum(values_to_insert)}      ✓")

    # -- Delete and re-query ---------------------------------------------------
    print("\n--- Delete and re-query ---")
    rsl.delete(20)
    rsl.delete(30)
    print(f"  After deleting 20 and 30: {rsl.to_sorted_list()}")

    s2 = rsl.range_sum(15, 35)
    print(f"  range_sum(15, 35) = {s2}")
    assert s2 == 15 + 25 + 35, f"Expected 75, got {s2}"
    print(f"  Expected 75       ✓")

    # -- Duplicates ------------------------------------------------------------
    print("\n--- Duplicates ---")
    rsl2 = RangeSumSortedList()
    for v in [5, 5, 5, 10, 10]:
        rsl2.insert(v)
    print(f"  Collection: {rsl2.to_sorted_list()}")
    s3 = rsl2.range_sum(5, 10)
    print(f"  range_sum(5, 10) = {s3}")
    assert s3 == 5 * 3 + 10 * 2, f"Expected 35, got {s3}"
    print(f"  Expected 35       ✓")

    rsl2.delete(5)
    print(f"  After deleting one 5: {rsl2.to_sorted_list()}")
    s4 = rsl2.range_sum(5, 10)
    assert s4 == 5 * 2 + 10 * 2, f"Expected 30, got {s4}"
    print(f"  range_sum(5, 10) = {s4}  (expected 30) ✓")

    # -- Edge: empty collection ------------------------------------------------
    print("\n--- Edge: empty collection ---")
    empty = RangeSumSortedList()
    assert empty.range_sum(0, 100) == 0
    assert len(empty) == 0
    print("  range_sum on empty = 0  ✓")

    # -- Edge: delete non-existent --------------------------------------------
    print("\n--- Edge: delete non-existent ---")
    assert not empty.delete(42)
    print("  delete(42) on empty -> False  ✓")

    # -- Edge: low > high ------------------------------------------------------
    print("\n--- Edge: low > high ---")
    rsl3 = RangeSumSortedList()
    rsl3.insert(10)
    assert rsl3.range_sum(50, 10) == 0
    print("  range_sum(50, 10) = 0  ✓")

    # -- Edge: single element --------------------------------------------------
    print("\n--- Edge: single element ---")
    rsl4 = RangeSumSortedList()
    rsl4.insert(7)
    assert rsl4.range_sum(7, 7) == 7
    assert rsl4.range_sum(0, 6) == 0
    assert rsl4.range_sum(8, 100) == 0
    print("  Single element range queries  ✓")

    # -- Larger mixed-operations test ------------------------------------------
    print("\n--- Larger mixed-operations test ---")
    rsl5 = RangeSumSortedList()
    for v in range(1, 101):
        rsl5.insert(v)
    assert rsl5.range_sum(1, 100) == 5050
    print("  Sum 1..100 = 5050  ✓")

    for v in range(51, 101):
        rsl5.delete(v)
    assert rsl5.range_sum(1, 100) == sum(range(1, 51))
    print(f"  After removing 51..100, sum = {sum(range(1, 51))}  ✓")

    # Negative values
    rsl6 = RangeSumSortedList()
    for v in [-10, -5, 0, 5, 10]:
        rsl6.insert(v)
    assert rsl6.range_sum(-10, 10) == 0
    assert rsl6.range_sum(-10, -5) == -15
    assert rsl6.range_sum(0, 10) == 15
    print("  Negative values handled  ✓")

    print(f"\n  Final repr: {rsl6}")
    print("\nAll checks passed ✓")
