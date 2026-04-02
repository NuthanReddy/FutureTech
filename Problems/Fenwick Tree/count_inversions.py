# Count Inversions using Fenwick Tree (BIT)
#
# DS used: Fenwick Tree / BIT (DataStructures/FenwickTree.py)
#
# Problem:
# Given an array, count the number of inversions.  An inversion is a pair
# (i, j) with i < j and arr[i] > arr[j].
#
# Example:
#   arr = [5, 3, 2, 4, 1]
#   Inversions: (5,3),(5,2),(5,4),(5,1),(3,2),(3,1),(2,1),(4,1) → 8
#
# Why Fenwick Tree?
# Coordinate-compress values then scan right-to-left.  For each element,
# query BIT for the count of already-seen values that are *smaller* (prefix sum
# up to val-1), which tells how many elements to its right are smaller.
# Then update BIT at that value.  Each query/update is O(log n).
#
# Time:  O(n log n)
# Space: O(n)

from typing import List


class _BIT:
    """Fenwick tree for prefix sums over a 1-indexed range."""

    def __init__(self, n: int) -> None:
        self._tree = [0] * (n + 2)  # +2 for safety
        self._n = n

    def update(self, i: int, delta: int = 1) -> None:
        while i <= self._n:
            self._tree[i] += delta
            i += i & -i

    def query(self, i: int) -> int:
        total = 0
        while i > 0:
            total += self._tree[i]
            i -= i & -i
        return total


def count_inversions(arr: List[int]) -> int:
    """Return the number of inversions in *arr*."""
    if len(arr) <= 1:
        return 0

    # Coordinate compression: map values to 1..n
    sorted_unique = sorted(set(arr))
    rank = {v: i + 1 for i, v in enumerate(sorted_unique)}
    n = len(sorted_unique)

    bit = _BIT(n)
    inversions = 0

    # Traverse from right to left
    for val in reversed(arr):
        r = rank[val]
        # Count elements already seen that are smaller than current
        inversions += bit.query(r - 1)
        bit.update(r)

    return inversions


if __name__ == "__main__":
    print(count_inversions([5, 3, 2, 4, 1]))   # 8
    print(count_inversions([1, 2, 3, 4, 5]))   # 0  (sorted)
    print(count_inversions([5, 4, 3, 2, 1]))   # 10 (reverse sorted = n*(n-1)/2)
    print(count_inversions([1, 1, 1]))          # 0  (all equal)
    print(count_inversions([2, 1]))             # 1

