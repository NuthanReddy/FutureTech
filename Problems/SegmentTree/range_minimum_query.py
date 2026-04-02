# Range Minimum Query (RMQ)
#
# Problem:
#   Given an array of integers, efficiently answer multiple queries of the form:
#   "What is the minimum value in the subarray arr[l..r]?"
#   Additionally, support point updates that change a single element.
#
# Approach:
#   Build a Segment Tree with the *min* merge operation and identity = +∞.
#   - build:  O(n)
#   - query:  O(log n) per query
#   - update: O(log n) per update
#
# Complexity:
#   Time:  O(n + q log n)  where q is the number of operations
#   Space: O(n)

import sys
import os
from typing import List, Tuple, Union

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.SegmentTree import SegmentTree


def range_minimum_query(
    arr: List[int],
    operations: List[Tuple[str, int, int]],
) -> List[int]:
    """Process a mix of range-min queries and point updates on *arr*.

    Args:
        arr: The initial array of integers.
        operations: Each operation is a tuple:
            ("query", l, r) — return min(arr[l..r])  (inclusive)
            ("update", i, v) — set arr[i] = v

    Returns:
        A list containing the result for every "query" operation, in order.

    Examples:
        >>> range_minimum_query([1, 3, 2, 7, 9, 11], [("query", 1, 4)])
        [2]
        >>> range_minimum_query([5], [("query", 0, 0)])
        [5]
    """
    if not arr:
        return []

    st = SegmentTree(data=arr, merge=min, identity=float("inf"))
    results: List[int] = []

    for op, a, b in operations:
        if op == "query":
            results.append(st.query(a, b))
        elif op == "update":
            st.update(a, b)
        else:
            raise ValueError(f"unknown operation: {op!r}")

    return results


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _run_tests() -> None:
    """Verify correctness with several test scenarios."""

    # --- Basic range queries ---
    arr = [1, 3, 2, 7, 9, 11]
    ops: List[Tuple[str, int, int]] = [
        ("query", 0, 5),   # min of entire array → 1
        ("query", 1, 4),   # min(3,2,7,9) → 2
        ("query", 2, 2),   # single element → 2
    ]
    assert range_minimum_query(arr, ops) == [1, 2, 2], "basic queries failed"

    # --- Update then query ---
    ops = [
        ("query", 0, 2),   # min(1,3,2) → 1
        ("update", 0, 10), # arr becomes [10,3,2,7,9,11]
        ("query", 0, 2),   # min(10,3,2) → 2
        ("update", 2, 0),  # arr becomes [10,3,0,7,9,11]
        ("query", 0, 5),   # min of all → 0
        ("query", 3, 5),   # min(7,9,11) → 7
    ]
    assert range_minimum_query(arr, ops) == [1, 2, 0, 7], "update+query failed"

    # --- Single-element array ---
    assert range_minimum_query([42], [("query", 0, 0)]) == [42]
    assert range_minimum_query(
        [42], [("update", 0, -1), ("query", 0, 0)]
    ) == [-1]

    # --- Empty array ---
    assert range_minimum_query([], []) == []

    # --- All identical elements ---
    arr = [5, 5, 5, 5]
    ops = [("query", 0, 3), ("update", 2, 1), ("query", 0, 3)]
    assert range_minimum_query(arr, ops) == [5, 1], "identical elements failed"

    # --- Negative values ---
    arr = [-3, -1, -4, -1, -5, -9]
    ops = [
        ("query", 0, 5),   # → -9
        ("query", 2, 4),   # min(-4,-1,-5) → -5
        ("update", 5, 0),  # arr becomes [-3,-1,-4,-1,-5,0]
        ("query", 0, 5),   # → -5
    ]
    assert range_minimum_query(arr, ops) == [-9, -5, -5], "negative values failed"

    # --- Large sequential test ---
    n = 1000
    arr = list(range(n, 0, -1))  # [1000, 999, ..., 1]
    st = SegmentTree(data=arr, merge=min, identity=float("inf"))
    assert st.query(0, n - 1) == 1
    st.update(n - 1, 2000)  # remove the global min
    assert st.query(0, n - 1) == 2

    print("All range_minimum_query tests passed ✓")


if __name__ == "__main__":
    _run_tests()
