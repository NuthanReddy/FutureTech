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

    The invariant is that every temporary ``#`` is reachable from a border
    cell, so it must not be captured.  All remaining ``O`` cells are enclosed.
    """
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
