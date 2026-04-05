"""House Robber example for 1D Linear Dynamic Programming."""

from functools import lru_cache
from typing import List


def rob_memo(nums: List[int]) -> int:
    """Return max robbed amount using memoized DFS."""
    n = len(nums)

    @lru_cache(maxsize=None)
    def dfs(index: int) -> int:
        if index >= n:
            return 0

        skip_current = dfs(index + 1)
        rob_current = nums[index] + dfs(index + 2)
        return max(skip_current, rob_current)

    return dfs(0)


def rob_tab(nums: List[int]) -> int:
    """Return max robbed amount using bottom-up tabulation."""
    n = len(nums)
    dp = [0] * (n + 2)

    for index in range(n - 1, -1, -1):
        skip_current = dp[index + 1]
        rob_current = nums[index] + dp[index + 2]
        dp[index] = max(skip_current, rob_current)

    return dp[0]

