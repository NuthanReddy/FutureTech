"""Number of Islands (LeetCode 200).

Problem: Given a rectangular grid of ``"1"`` land and ``"0"`` water, return
the number of 4-directionally connected islands.  The input may be mutated.
Output: one integer, the island count.  Constraints: rows and columns are
non-negative; diagonal cells are not adjacent.
"""

from collections import deque
from typing import List


def num_islands(grid: List[List[str]]) -> int:
    """Return the number of 4-directionally connected land components.

    We mark a cell as soon as it enters the queue.  Therefore no cell can be
    enqueued twice, and each island contributes exactly one counter increment.
    """
    if not grid or not grid[0]:
        return 0
    rows, cols = len(grid), len(grid[0])
    islands = 0
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] != "1":
                continue
            islands += 1
            grid[r][c] = "0"
            queue = deque([(r, c)])
            while queue:
                cr, cc = queue.popleft()
                for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                    nr, nc = cr + dr, cc + dc
                    if 0 <= nr < rows and 0 <= nc < cols and grid[nr][nc] == "1":
                        grid[nr][nc] = "0"
                        queue.append((nr, nc))
    return islands
