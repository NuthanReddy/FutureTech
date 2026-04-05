"""Minimum Path Sum example for 2D Grid Dynamic Programming."""

from functools import lru_cache
from math import inf
from typing import List


Grid = List[List[int]]


def min_path_sum_memo(grid: Grid) -> int:
    """Return minimum path sum using memoized DFS."""
    if not grid or not grid[0]:
        return 0

    rows = len(grid)
    cols = len(grid[0])

    @lru_cache(maxsize=None)
    def dfs(row: int, col: int) -> int:
        if row >= rows or col >= cols:
            return inf

        if row == rows - 1 and col == cols - 1:
            return grid[row][col]

        down = dfs(row + 1, col)
        right = dfs(row, col + 1)
        return grid[row][col] + min(down, right)

    return dfs(0, 0)


def min_path_sum_tab(grid: Grid) -> int:
    """Return minimum path sum using bottom-up tabulation."""
    if not grid or not grid[0]:
        return 0

    rows = len(grid)
    cols = len(grid[0])
    dp = [[0] * cols for _ in range(rows)]

    dp[0][0] = grid[0][0]

    for col in range(1, cols):
        dp[0][col] = dp[0][col - 1] + grid[0][col]

    for row in range(1, rows):
        dp[row][0] = dp[row - 1][0] + grid[row][0]

    for row in range(1, rows):
        for col in range(1, cols):
            dp[row][col] = grid[row][col] + min(dp[row - 1][col], dp[row][col - 1])

    return dp[rows - 1][cols - 1]

