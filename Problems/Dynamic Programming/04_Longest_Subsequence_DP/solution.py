"""Longest Increasing Subsequence example for subsequence DP."""

from functools import lru_cache
from typing import List


def lis_memo(nums: List[int]) -> int:
    """Return LIS length using memoized DFS with previous index state."""
    n = len(nums)

    @lru_cache(maxsize=None)
    def dfs(index: int, prev_index: int) -> int:
        if index == n:
            return 0

        skip_current = dfs(index + 1, prev_index)

        take_current = 0
        if prev_index == -1 or nums[index] > nums[prev_index]:
            take_current = 1 + dfs(index + 1, index)

        return max(skip_current, take_current)

    return dfs(0, -1)


def lis_tab(nums: List[int]) -> int:
    """Return LIS length using O(n^2) bottom-up tabulation."""
    if not nums:
        return 0

    n = len(nums)
    dp = [1] * n

    for index in range(n):
        for prev in range(index):
            if nums[prev] < nums[index]:
                dp[index] = max(dp[index], dp[prev] + 1)

    return max(dp)

