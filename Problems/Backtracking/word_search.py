"""Top Interview 150: Word Search.

Problem statement:
    Input: a rectangular character grid ``board`` and a string ``word``.
    Required output: True if ``word`` can be formed by adjacent horizontal or
    vertical cells without reusing a cell in the same path; otherwise False.
    Key constraints: the canonical prompt uses small boards (m, n <= 6) and a
    non-empty word. The board is restored before the function returns.

Backtracking invariant:
    The current path matches ``word[:index]`` and cells already in the path are
    marked with ``VISITED``. Every return path restores the cell before another
    branch sees the board.

Alternative:
    A separate ``visited`` set is clearer for immutable inputs. In-place
    marking is common in interviews because it keeps auxiliary space to the
    recursion depth.

Complexity:
    Time: O(rows * cols * 4^L), where L is len(word). The first step has up to
    rows*cols starts and each next step branches in four directions.
    Space: O(L) recursion depth.
"""

from __future__ import annotations


VISITED = "#"
DIRECTIONS = ((1, 0), (-1, 0), (0, 1), (0, -1))


def exist(board: list[list[str]], word: str) -> bool:
    """Return True if ``word`` can be formed by adjacent board cells."""
    if not word:
        return True
    if not board or not board[0]:
        return False

    rows = len(board)
    cols = len(board[0])
    if len(word) > rows * cols:
        return False

    def dfs(row: int, col: int, index: int) -> bool:
        if index == len(word):
            return True
        if (
            row < 0
            or row >= rows
            or col < 0
            or col >= cols
            or board[row][col] != word[index]
        ):
            return False

        original = board[row][col]
        board[row][col] = VISITED
        for row_delta, col_delta in DIRECTIONS:
            if dfs(row + row_delta, col + col_delta, index + 1):
                board[row][col] = original
                return True
        board[row][col] = original
        return False

    for row in range(rows):
        for col in range(cols):
            if dfs(row, col, 0):
                return True
    return False


if __name__ == "__main__":
    sample_board = [
        ["A", "B", "C", "E"],
        ["S", "F", "C", "S"],
        ["A", "D", "E", "E"],
    ]
    print(exist(sample_board, "ABCCED"))
