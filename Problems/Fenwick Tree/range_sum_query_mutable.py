# Range Sum Query – Mutable (LeetCode 307)
#
# DS used: Fenwick Tree / BIT (DataStructures/FenwickTree.py)
#
# Problem:
# Given an integer array nums, handle two types of queries:
#   1. update(index, val) — set nums[index] = val
#   2. sum_range(left, right) — return sum of nums[left..right]
#
# Example:
#   nums = [1, 3, 5]
#   sum_range(0, 2) → 9
#   update(1, 2)          # nums becomes [1, 2, 5]
#   sum_range(0, 2) → 8
#
# Why Fenwick Tree?
# A naive array gives O(1) update but O(n) query, or O(n) update with O(1)
# query using prefix sums.  A Fenwick tree balances both at O(log n) each.
#
# Time:  O(n) build, O(log n) per update, O(log n) per query
# Space: O(n)

from typing import List


class NumArray:
    """Mutable range-sum structure backed by a Fenwick tree."""

    def __init__(self, nums: List[int]) -> None:
        self._n = len(nums)
        self._nums = list(nums)
        self._tree = [0] * (self._n + 1)
        for i, v in enumerate(nums):
            self._add(i + 1, v)

    def update(self, index: int, val: int) -> None:
        """Set nums[index] = val."""
        delta = val - self._nums[index]
        self._nums[index] = val
        self._add(index + 1, delta)

    def sum_range(self, left: int, right: int) -> int:
        """Return sum of nums[left..right] inclusive."""
        return self._prefix(right + 1) - self._prefix(left)

    def _add(self, i: int, delta: int) -> None:
        while i <= self._n:
            self._tree[i] += delta
            i += i & -i

    def _prefix(self, i: int) -> int:
        total = 0
        while i > 0:
            total += self._tree[i]
            i -= i & -i
        return total


if __name__ == "__main__":
    na = NumArray([1, 3, 5])
    print(na.sum_range(0, 2))  # 9
    na.update(1, 2)
    print(na.sum_range(0, 2))  # 8
    print(na.sum_range(1, 2))  # 7

