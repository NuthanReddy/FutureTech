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

    Pattern identification: cheapest right/down route -> coordinate suffix DP ->
    each cell adds its cost to the cheapest legal successor.

    ``best(r, c)`` is the cheapest cost from this cell to the destination.
    Out-of-bounds moves are impossible and use ``inf`` so they can never win
    the minimum.  The destination returns its own value because it still must
    be paid exactly once.
    """
    # 1. Output: Return the cheapest top-left to bottom-right path sum.
    # 2. Structure: Right/down routes can reach the same cell, which has the same
    #    remaining best cost; moves never circle back, so smaller routes finish.
    # 3. Constraints: Assume a rectangular grid; empty input gives 0.
    #    O(rows*cols) time/space; recursive path depth must fit Python's limit.
    # 4. Choice: Remember the cheapest cell-to-destination cost; outside costs infinity,
    #    the destination costs its value, and other cells add min(down, right).
    # 5. Why it works: Every route starts with one legal successor; its best
    #    suffix plus this cell's cost finds the cheapest complete route.
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

    Pattern identification: cheapest right/down route -> coordinate prefix DP ->
    each cell adds its cost to the optimal top/left predecessor.

    First row and first column have only one predecessor; initializing them
    explicitly prevents accidentally treating an unavailable direction as a
    free path.
    """
    # 1. Output: Return the minimum sum along a right/down path across the grid.
    # 2. Structure: Right/down moves mean a cell only needs its top/left costs;
    #    filling rows in order solves those neighbors before they are needed.
    # 3. Constraints: Assume a rectangular grid; empty input gives 0.
    #    O(rows*cols) time and space; the input grid is not changed.
    # 4. Choice: Store the cheapest start-to-cell cost; seed the start and single-route edges,
    #    then fill each other cell with its value plus min(top cost, left cost).
    # 5. Why it works: Both predecessors are solved before this cell;
    #    choosing their cheaper path covers every possible final move.
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

    Pattern identification: count right/down routes -> coordinate counting DP ->
    disjoint first moves sum all paths; the destination contributes one.

    The state is a coordinate, and each path is decomposed by its first move.
    A one-cell grid has one path (doing nothing), while non-positive
    dimensions have no valid grid.
    """
    # 1. Output: Return the number of right/down paths through an empty grid.
    # 2. Structure: Right/down routes share cells, so their remaining counts repeat.
    #    Count both next moves rather than choosing one cheapest-looking move.
    # 3. Constraints: Non-positive dimensions give 0; one cell gives 1.
    #    O(rows*cols) time/space; recursion depth must fit Python's limit.
    # 4. Choice: Remember remaining routes per cell; destination=1, outside=0,
    #    otherwise add the counts for down and right.
    # 5. Why it works: Down-first and right-first paths are distinct and
    #    exhaust all routes, so adding them neither misses nor double-counts paths.
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
    """Count right/down paths with a rolling one-dimensional DP row.

    Pattern identification: count right/down routes -> rolling grid-count DP ->
    each update sums the old top count and the current row's left count.
    """
    # 1. Output: Return the total number of right/down routes to the bottom-right.
    # 2. Structure: Right/down arrivals come only from above or left;
    #    those two counts are enough, so earlier rows need not all be stored.
    # 3. Constraints: Non-positive dimensions give 0. O(rows*cols) time and
    #    O(cols) stored counts, treating integer arithmetic as constant cost.
    # 4. Choice: Seed the first row with ones; update left to right with
    #    dp[col] += dp[col-1], combining old top and newly updated left.
    # 5. Why it works: The two incoming route groups are disjoint; this order
    #    preserves exactly the predecessor counts needed for each new cell.
    if rows <= 0 or cols <= 0:
        return 0

    # dp[col] is the number of paths to the current row's cell.  The left
    # value is already updated for this row; dp[col] still holds the top value.
    dp = [1] * cols
    for _ in range(1, rows):
        for col in range(1, cols):
            dp[col] += dp[col - 1]
    return dp[-1]
