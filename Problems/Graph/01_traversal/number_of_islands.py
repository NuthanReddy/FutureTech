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

    Pattern identification: count connected land regions -> BFS flood fill;
    mark on enqueue so each component starts exactly one search.

    We mark a cell as soon as it enters the queue.  Therefore no cell can be
    enqueued twice, and each island contributes exactly one counter increment.
    """
    # 1. Output: Return the number of separate land islands.
    # 2. Structure: "1" cells connect only vertically or horizontally in a rectangular grid.
    # 3. Constraints: The grid may be changed, so marking land as water replaces a separate visited set.
    # 4. Choice: Start BFS at each remaining land cell, count it, and mark queued land as water.
    # 5. Why it works: Flood fill consumes exactly one component, so it cannot be counted again.
    #    Every cell is examined a constant number of times: O(R*C) time and worst-case queue space.
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
