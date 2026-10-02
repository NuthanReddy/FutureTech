"""Surrounded Regions (LeetCode 130).

Problem: Given a rectangular board of ``"X"`` and ``"O"``, replace every
region of ``"O"`` not connected to the border by ``"X"``.  Output: mutate the
board in place and return ``None``.  Constraints: adjacency is four-directional
and border-connected regions must remain unchanged.
"""

from collections import deque
from typing import List


def solve(board: List[List[str]]) -> None:
    """Capture interior ``O`` regions in-place.

    Pattern identification: preserve boundary-connected regions -> border BFS;
    marked cells are exactly the discovered safe region, not enclosed cells.

    The invariant is that every temporary ``#`` is reachable from a border
    cell, so it must not be captured.  All remaining ``O`` cells are enclosed.
    """
    # 1. Output: Change enclosed O regions to X in place; return no result.
    # 2. Structure: Only four-directional O paths reaching a board border are safe.
    # 3. Constraints: The board is rectangular and contains X/O; empty input needs no changes.
    # 4. Choice: BFS from border O cells using # as a safe marker, then restore markers and capture O.
    # 5. Why it works: Marked cells are exactly border-connected cells; every unmarked O is enclosed.
    #    Each cell is processed a constant number of times: O(R*C) time and worst-case queue space.
    if not board or not board[0]:
        return
    rows, cols = len(board), len(board[0])
    queue = deque()
    for r in range(rows):
        for c in (0, cols - 1):
            if board[r][c] == "O":
                board[r][c] = "#"
                queue.append((r, c))
    for c in range(cols):
        for r in (0, rows - 1):
            if board[r][c] == "O":
                board[r][c] = "#"
                queue.append((r, c))
    while queue:
        r, c = queue.popleft()
        for dr, dc in ((1, 0), (-1, 0), (0, 1), (0, -1)):
            nr, nc = r + dr, c + dc
            if 0 <= nr < rows and 0 <= nc < cols and board[nr][nc] == "O":
                board[nr][nc] = "#"
                queue.append((nr, nc))
    for r in range(rows):
        for c in range(cols):
            board[r][c] = "O" if board[r][c] == "#" else ("X" if board[r][c] == "O" else board[r][c])
