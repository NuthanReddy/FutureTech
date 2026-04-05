"""0/1 Knapsack example for Knapsack/Subset Dynamic Programming."""

from functools import lru_cache
from typing import List


def knapsack_memo(weights: List[int], values: List[int], capacity: int) -> int:
    """Return max value using memoized DFS for 0/1 knapsack."""
    n = len(weights)

    @lru_cache(maxsize=None)
    def dfs(index: int, remaining_capacity: int) -> int:
        if index == n or remaining_capacity == 0:
            return 0

        skip_item = dfs(index + 1, remaining_capacity)

        take_item = 0
        if weights[index] <= remaining_capacity:
            take_item = values[index] + dfs(index + 1, remaining_capacity - weights[index])

        return max(skip_item, take_item)

    return dfs(0, capacity)


def knapsack_tab(weights: List[int], values: List[int], capacity: int) -> int:
    """Return max value using bottom-up tabulation for 0/1 knapsack."""
    n = len(weights)
    dp = [[0] * (capacity + 1) for _ in range(n + 1)]

    for index in range(n - 1, -1, -1):
        for current_capacity in range(capacity + 1):
            skip_item = dp[index + 1][current_capacity]

            take_item = 0
            if weights[index] <= current_capacity:
                take_item = values[index] + dp[index + 1][current_capacity - weights[index]]

            dp[index][current_capacity] = max(skip_item, take_item)

    return dp[0][capacity]

