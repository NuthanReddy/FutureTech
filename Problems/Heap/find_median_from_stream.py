# -----------------------------------------------------------------------
# Find Median from Data Stream
#
# Problem:
#   Design a data structure that supports:
#     - add_num(num)    — add an integer from the stream
#     - find_median()   — return the median of all elements so far
#
# Approach — two heaps:
#   - max_heap (MaxHeap): stores the LOWER half of the numbers.
#     Its root is the largest value in the lower half.
#   - min_heap (MinHeap): stores the UPPER half of the numbers.
#     Its root is the smallest value in the upper half.
#
#   Invariant: len(max_heap) == len(min_heap)          (even total)
#           or len(max_heap) == len(min_heap) + 1      (odd total)
#
#   Median:
#     - odd count  → max_heap.peek()
#     - even count → (max_heap.peek() + min_heap.peek()) / 2
#
# Time complexity:  O(log n) per add_num, O(1) for find_median.
# Space complexity: O(n) total for storing all elements.
# -----------------------------------------------------------------------

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.Heap import MaxHeap, MinHeap


class MedianFinder:
    """Finds the running median from a stream of integers.

    Uses a MaxHeap for the lower half and a MinHeap for the upper half
    so that the median is always accessible in O(1).

    Examples:
        >>> mf = MedianFinder()
        >>> mf.add_num(1); mf.add_num(2)
        >>> mf.find_median()
        1.5
        >>> mf.add_num(3)
        >>> mf.find_median()
        2.0
    """

    def __init__(self) -> None:
        self._lo = MaxHeap()  # lower half — max at root
        self._hi = MinHeap()  # upper half — min at root

    def add_num(self, num: int) -> None:
        """Add *num* to the data structure.  O(log n)."""
        # Always push to max_heap first
        if not self._lo or num <= self._lo.peek():
            self._lo.push(num)
        else:
            self._hi.push(num)

        # Rebalance: lo may have at most 1 more element than hi
        if len(self._lo) > len(self._hi) + 1:
            self._hi.push(self._lo.pop())
        elif len(self._hi) > len(self._lo):
            self._lo.push(self._hi.pop())

    def find_median(self) -> float:
        """Return the current median.  O(1).

        Raises:
            ValueError: If no numbers have been added yet.
        """
        if not self._lo and not self._hi:
            raise ValueError("no numbers added yet")

        if len(self._lo) > len(self._hi):
            return float(self._lo.peek())
        return (self._lo.peek() + self._hi.peek()) / 2.0


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------

if __name__ == "__main__":
    # Basic stream
    mf = MedianFinder()
    mf.add_num(1)
    assert mf.find_median() == 1.0
    print(f"After [1]: median = {mf.find_median()}")

    mf.add_num(2)
    assert mf.find_median() == 1.5
    print(f"After [1,2]: median = {mf.find_median()}")

    mf.add_num(3)
    assert mf.find_median() == 2.0
    print(f"After [1,2,3]: median = {mf.find_median()}")

    # Even count
    mf.add_num(4)
    assert mf.find_median() == 2.5
    print(f"After [1,2,3,4]: median = {mf.find_median()}")

    # Stream with duplicates
    mf2 = MedianFinder()
    for n in [5, 5, 5, 5]:
        mf2.add_num(n)
    assert mf2.find_median() == 5.0
    print(f"All fives: median = {mf2.find_median()}")

    # Negative numbers
    mf3 = MedianFinder()
    for n in [-3, -1, 0, 2, 4]:
        mf3.add_num(n)
    assert mf3.find_median() == 0.0
    print(f"Negatives [-3,-1,0,2,4]: median = {mf3.find_median()}")

    # Single element
    mf4 = MedianFinder()
    mf4.add_num(42)
    assert mf4.find_median() == 42.0
    print(f"Single [42]: median = {mf4.find_median()}")

    # Empty — should raise
    mf5 = MedianFinder()
    try:
        mf5.find_median()
        assert False, "should have raised ValueError"
    except ValueError:
        print("Empty median raises ValueError: OK")

    # Descending order input
    mf6 = MedianFinder()
    for n in [10, 8, 6, 4, 2]:
        mf6.add_num(n)
    assert mf6.find_median() == 6.0
    print(f"Descending [10,8,6,4,2]: median = {mf6.find_median()}")

    # Large interleaved input
    mf7 = MedianFinder()
    for n in [1, 100, 2, 99, 3, 98]:
        mf7.add_num(n)
    # sorted: [1, 2, 3, 98, 99, 100] → median = (3 + 98) / 2 = 50.5
    assert mf7.find_median() == 50.5
    print(f"Interleaved: median = {mf7.find_median()}")

    print("\nAll tests passed!")
