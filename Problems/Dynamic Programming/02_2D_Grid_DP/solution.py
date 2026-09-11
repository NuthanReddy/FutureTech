"""Top Interview 150 grid and matrix DP problems.

Problems implemented:

* **Minimum Path Sum:** Given an ``m x n`` grid of non-negative values, move
  only right or down from the top-left to the bottom-right and return the
  minimum path sum.  Input is ``grid: list[list[int]]``; output is an integer.
  Standard constraints are ``1 <= m, n <= 200`` and ``0 <= grid[r][c] <= 200``.
* **Unique Paths:** Given an empty ``m x n`` grid, count paths from the
  top-left to the bottom-right using only right and down moves.  Inputs are
  positive ``rows`` and ``cols``; output is an integer count.  Typical
  constraints are ``1 <= rows, cols <= 100``.
"""

from functools import lru_cache
from math import inf
from typing import List


Grid = List[List[int]]


def min_path_sum_memo(grid: Grid) -> int:
    """Return minimum path sum using memoized DFS.

    ``best(r, c)`` is the cheapest cost from this cell to the destination.
    Out-of-bounds moves are impossible and use ``inf`` so they can never win
    the minimum.  The destination returns its own value because it still must
    be paid exactly once.
    """
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
    """Return minimum path sum using a start-to-destination table.

    First row and first column have only one predecessor; initializing them
    explicitly prevents accidentally treating an unavailable direction as a
    free path.
    """
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


def unique_paths_memo(rows: int, cols: int) -> int:
    """Solve Unique Paths: count right/down routes through an empty grid.

    The state is a coordinate, and each path is decomposed by its first move.
    A one-cell grid has one path (doing nothing), while non-positive
    dimensions have no valid grid.
    """
    if rows <= 0 or cols <= 0:
        return 0

    @lru_cache(maxsize=None)
    def paths(row: int, col: int) -> int:
        if row == rows - 1 and col == cols - 1:
            return 1
        if row >= rows or col >= cols:
            return 0
        return paths(row + 1, col) + paths(row, col + 1)

    return paths(0, 0)


def unique_paths_tab(rows: int, cols: int) -> int:
    """Count right/down paths with a rolling one-dimensional DP row."""
    if rows <= 0 or cols <= 0:
        return 0

    # dp[col] is the number of paths to the current row's cell.  The left
    # value is already updated for this row; dp[col] still holds the top value.
    dp = [1] * cols
    for _ in range(1, rows):
        for col in range(1, cols):
            dp[col] += dp[col - 1]
    return dp[-1]
