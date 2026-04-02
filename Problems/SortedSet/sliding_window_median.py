# Sliding Window Median — LeetCode 480
#
# Problem:
#   Given an array of integers nums and an integer k, there is a sliding window
#   of size k moving from left to right.  Return the median of each window.
#
#   The median of a sorted window of size k is:
#     • The middle element when k is odd.
#     • The average of the two middle elements when k is even.
#
# Approach:
#   Use SortedSet from DataStructures/SortedSet.py.
#   Because SortedSet ignores duplicates, store (value, index) tuples so every
#   element is unique.  Tuples are compared lexicographically which preserves
#   value ordering, with the index breaking ties.
#
#   For each window position:
#     1. Add the incoming element.
#     2. Remove the outgoing element.
#     3. Read the median via sorted iteration or range_query.
#
# Complexity:
#   Time:  O(n·k) — each window reads the median by collecting sorted elements
#          (could be optimised with an order-statistic tree, but this solution
#          focuses on reusing the SortedSet API).
#   Space: O(k)

import sys
import os
from typing import List, Tuple

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.SortedSet import SortedSet


def sliding_window_median(nums: List[int], k: int) -> List[float]:
    """Return the median of every contiguous window of size *k*.

    Args:
        nums: List of integers.
        k:    Window size (1 ≤ k ≤ len(nums)).

    Returns:
        List of medians, one per window position.

    Examples:
        >>> sliding_window_median([1, 3, -1, -3, 5, 3, 6, 7], 3)
        [1.0, -1.0, -1.0, 3.0, 5.0, 6.0]
        >>> sliding_window_median([1, 2], 1)
        [1.0, 2.0]
    """
    n = len(nums)
    if n == 0 or k <= 0:
        return []

    ss: SortedSet = SortedSet()
    result: List[float] = []

    def _get_median() -> float:
        """Extract the median from the current SortedSet contents."""
        elems: List[Tuple[int, int]] = list(ss)  # sorted (value, index) tuples
        mid = len(elems) // 2
        if len(elems) % 2 == 1:
            return float(elems[mid][0])
        return (elems[mid - 1][0] + elems[mid][0]) / 2.0

    # Build the initial window.
    for i in range(k):
        ss.add((nums[i], i))

    result.append(_get_median())

    # Slide the window.
    for i in range(k, n):
        ss.add((nums[i], i))
        ss.remove((nums[i - k], i - k))
        result.append(_get_median())

    return result


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _run_tests() -> None:
    """Verify correctness with several test scenarios."""

    # --- LeetCode example ---
    assert sliding_window_median([1, 3, -1, -3, 5, 3, 6, 7], 3) == [
        1.0, -1.0, -1.0, 3.0, 5.0, 6.0
    ]

    # --- Window size 1 (each element is its own median) ---
    assert sliding_window_median([5, 3, 8, 1], 1) == [5.0, 3.0, 8.0, 1.0]

    # --- Window size equals array length ---
    assert sliding_window_median([1, 2, 3, 4, 5], 5) == [3.0]

    # --- Even window size ---
    assert sliding_window_median([1, 2, 3, 4], 2) == [1.5, 2.5, 3.5]

    # --- All identical elements ---
    assert sliding_window_median([7, 7, 7, 7], 3) == [7.0, 7.0]

    # --- Two elements, window 2 ---
    assert sliding_window_median([1, 2], 2) == [1.5]

    # --- Negative numbers ---
    assert sliding_window_median([-1, -3, -5, -7], 2) == [-2.0, -4.0, -6.0]

    # --- Single element ---
    assert sliding_window_median([42], 1) == [42.0]

    # --- Empty input ---
    assert sliding_window_median([], 3) == []

    # --- Large window with even size ---
    assert sliding_window_median([1, 4, 2, 3], 4) == [2.5]

    # --- Larger example (brute-force check) ---
    import random
    random.seed(123)
    arr = random.choices(range(-20, 21), k=50)
    k = 7
    result = sliding_window_median(arr, k)
    for i in range(len(arr) - k + 1):
        window = sorted(arr[i : i + k])
        mid = k // 2
        expected = float(window[mid]) if k % 2 == 1 else (window[mid - 1] + window[mid]) / 2.0
        assert abs(result[i] - expected) < 1e-9, (
            f"mismatch at window {i}: got {result[i]}, expected {expected}"
        )

    print("All sliding_window_median tests passed ✓")


if __name__ == "__main__":
    _run_tests()
