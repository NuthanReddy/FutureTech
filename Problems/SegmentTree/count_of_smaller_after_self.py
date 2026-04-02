# Count of Smaller Numbers After Self
#
# Problem (LeetCode 315):
#   Given an integer array nums, return a list counts where counts[i] is the
#   number of elements to the right of nums[i] that are strictly smaller than
#   nums[i].
#
# Approach:
#   1. Coordinate-compress the values so they map to [0, m-1].
#   2. Traverse the array from right to left.
#   3. Maintain a Segment Tree over the compressed value range that stores
#      *frequencies* (sum merge, identity 0).
#   4. For each element with compressed value v:
#        - query(0, v-1) gives the count of already-seen values < current.
#        - update v's frequency by +1.
#
# Complexity:
#   Time:  O(n log m)  where m = number of distinct values
#   Space: O(n + m)

import sys
import os
from typing import List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.SegmentTree import SegmentTree


def count_smaller_after_self(nums: List[int]) -> List[int]:
    """Return a list where result[i] = count of nums[j] < nums[i] for j > i.

    Args:
        nums: List of integers (may contain negatives and duplicates).

    Returns:
        List of counts, same length as *nums*.

    Examples:
        >>> count_smaller_after_self([5, 2, 6, 1])
        [2, 1, 1, 0]
        >>> count_smaller_after_self([1])
        [0]
        >>> count_smaller_after_self([])
        []
    """
    if not nums:
        return []

    # --- coordinate compression ---
    sorted_unique = sorted(set(nums))
    rank = {v: i for i, v in enumerate(sorted_unique)}
    m = len(sorted_unique)

    # Segment tree as a frequency array over [0, m-1], using sum.
    freq = [0] * m
    st = SegmentTree(data=freq, merge=lambda a, b: a + b, identity=0)

    result: List[int] = [0] * len(nums)

    for i in range(len(nums) - 1, -1, -1):
        v = rank[nums[i]]
        # Count elements already inserted whose rank < v.
        if v > 0:
            result[i] = st.query(0, v - 1)
        else:
            result[i] = 0
        # Increment frequency at rank v.
        current_freq = st.query(v, v)
        st.update(v, current_freq + 1)

    return result


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _run_tests() -> None:
    """Verify correctness with several test scenarios."""

    # --- Classic example ---
    assert count_smaller_after_self([5, 2, 6, 1]) == [2, 1, 1, 0]

    # --- Single element ---
    assert count_smaller_after_self([1]) == [0]

    # --- Empty ---
    assert count_smaller_after_self([]) == []

    # --- Already sorted ascending ---
    assert count_smaller_after_self([1, 2, 3, 4, 5]) == [0, 0, 0, 0, 0]

    # --- Sorted descending ---
    assert count_smaller_after_self([5, 4, 3, 2, 1]) == [4, 3, 2, 1, 0]

    # --- All equal ---
    assert count_smaller_after_self([3, 3, 3, 3]) == [0, 0, 0, 0]

    # --- Two elements ---
    assert count_smaller_after_self([2, 1]) == [1, 0]
    assert count_smaller_after_self([1, 2]) == [0, 0]

    # --- Negative numbers ---
    assert count_smaller_after_self([-1, -2, 0, -3]) == [2, 1, 1, 0]

    # --- Duplicates ---
    assert count_smaller_after_self([2, 0, 1, 0]) == [3, 0, 1, 0]

    # --- Larger array (sanity check) ---
    import random
    random.seed(42)
    arr = random.choices(range(-50, 51), k=200)
    result = count_smaller_after_self(arr)
    # Brute-force verification
    for i in range(len(arr)):
        expected = sum(1 for j in range(i + 1, len(arr)) if arr[j] < arr[i])
        assert result[i] == expected, (
            f"mismatch at index {i}: got {result[i]}, expected {expected}"
        )

    print("All count_smaller_after_self tests passed ✓")


if __name__ == "__main__":
    _run_tests()
