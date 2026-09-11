"""Pacific Atlantic Water Flow (LeetCode 417).

Problem: Given a height matrix, water flows from a cell to an orthogonal
neighbor of equal or lower height.  Return cells that can reach both the
Pacific (top/left border) and Atlantic (bottom/right border).  Output: a list
of ``[row, column]`` coordinates.  Constraints: the matrix is rectangular;
coordinates are zero-based.
"""

from typing import List, Set, Tuple


def pacific_atlantic(heights: List[List[int]]) -> List[List[int]]:
    """Return cells from which water can reach both oceans."""
    if not heights or not heights[0]:
        return []
    rows, cols = len(heights), len(heights[0])

    def reachable(starts: List[Tuple[int, int]]) -> Set[Tuple[int, int]]:
        seen = set(starts)
        stack = list(starts)
        while stack:
            r, c = stack.pop()
            for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                nr, nc = r + dr, c + dc
                # Reverse edge: from ocean inward, height may not decrease.
                if (0 <= nr < rows and 0 <= nc < cols
                        and (nr, nc) not in seen
                        and heights[nr][nc] >= heights[r][c]):
                    seen.add((nr, nc))
                    stack.append((nr, nc))
        return seen

    pacific = reachable([(r, 0) for r in range(rows)] + [(0, c) for c in range(cols)])
    atlantic = reachable([(r, cols - 1) for r in range(rows)] + [(rows - 1, c) for c in range(cols)])
    return [[r, c] for r in range(rows) for c in range(cols) if (r, c) in pacific and (r, c) in atlantic]
