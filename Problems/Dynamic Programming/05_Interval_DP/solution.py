"""Matrix Chain Multiplication example for interval DP."""

from functools import lru_cache
from math import inf
from typing import List


def matrix_chain_memo(dimensions: List[int]) -> int:
    """Return minimum multiplication cost using memoized interval DP."""
    matrix_count = len(dimensions) - 1
    if matrix_count <= 1:
        return 0

    @lru_cache(maxsize=None)
    def dfs(left: int, right: int) -> int:
        if left == right:
            return 0

        best_cost = inf
        for split in range(left, right):
            left_cost = dfs(left, split)
            right_cost = dfs(split + 1, right)
            merge_cost = dimensions[left] * dimensions[split + 1] * dimensions[right + 1]
            best_cost = min(best_cost, left_cost + right_cost + merge_cost)

        return best_cost

    return dfs(0, matrix_count - 1)


def matrix_chain_tab(dimensions: List[int]) -> int:
    """Return minimum multiplication cost using bottom-up interval DP."""
    matrix_count = len(dimensions) - 1
    if matrix_count <= 1:
        return 0

    dp = [[0] * matrix_count for _ in range(matrix_count)]

    for chain_length in range(2, matrix_count + 1):
        for left in range(0, matrix_count - chain_length + 1):
            right = left + chain_length - 1
            dp[left][right] = inf

            for split in range(left, right):
                merge_cost = dimensions[left] * dimensions[split + 1] * dimensions[right + 1]
                total_cost = dp[left][split] + dp[split + 1][right] + merge_cost
                dp[left][right] = min(dp[left][right], total_cost)

    return dp[0][matrix_count - 1]

