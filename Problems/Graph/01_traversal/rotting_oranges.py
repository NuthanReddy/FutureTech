"""Rotting Oranges (LeetCode 994).

Problem: In a grid, ``0`` is empty, ``1`` is fresh, and ``2`` is rotten.
Each minute, rotten oranges infect orthogonally adjacent fresh oranges.
Output: minimum minutes until no fresh orange remains, or ``-1`` if impossible.
Constraints: the grid is rectangular and may contain no oranges.
"""

from collections import deque
from typing import List


def oranges_rotting(grid: List[List[int]]) -> int:
    """Return minutes until all reachable fresh oranges rot, or ``-1``."""
    if not grid or not grid[0]:
        return 0
    rows, cols = len(grid), len(grid[0])
    queue = deque()
    fresh = 0
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] == 2:
                queue.append((r, c))
            elif grid[r][c] == 1:
                fresh += 1
    minutes = 0
    while queue and fresh:
        for _ in range(len(queue)):
            r, c = queue.popleft()
            for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                nr, nc = r + dr, c + dc
                if 0 <= nr < rows and 0 <= nc < cols and grid[nr][nc] == 1:
                    grid[nr][nc] = 2
                    fresh -= 1
                    queue.append((nr, nc))
        minutes += 1
    return minutes if fresh == 0 else -1
