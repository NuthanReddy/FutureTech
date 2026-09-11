"""Top Interview 150: N-Queens II.

Problem statement:
    Input: an integer ``n``, the side length of an n x n chessboard.
    Required output: the count of distinct ways to place n queens so that no
    two queens share a row, column, or diagonal.
    Key constraints: the canonical prompt uses 1 <= n <= 9. This module also
    supports n == 0 as one empty placement.

Backtracking invariant:
    One queen is placed in each processed row. ``columns``, ``diag_down`` and
    ``diag_up`` contain exactly the attacked lines from the current partial
    board. A cell is safe iff none of those three sets contains its identifiers.

Diagonal identifiers:
    row - column is constant on a top-left to bottom-right diagonal.
    row + column is constant on a top-right to bottom-left diagonal.

Alternative:
    Bit masks are faster and are preferred for very large n, but sets make the
    constraints explicit and are easier to learn/debug.

Complexity:
    Time: O(n!) upper bound after column pruning.
    Space: O(n) for recursion and constraint sets.
"""

from __future__ import annotations


def total_n_queens(n: int) -> int:
    """Return the number of valid n-queens placements."""
    if n < 0:
        raise ValueError("n must be non-negative")

    columns: set[int] = set()
    diag_down: set[int] = set()
    diag_up: set[int] = set()

    def dfs(row: int) -> int:
        if row == n:
            return 1

        count = 0
        for column in range(n):
            down_key = row - column
            up_key = row + column
            if (
                column in columns
                or down_key in diag_down
                or up_key in diag_up
            ):
                continue

            columns.add(column)
            diag_down.add(down_key)
            diag_up.add(up_key)
            count += dfs(row + 1)
            diag_up.remove(up_key)
            diag_down.remove(down_key)
            columns.remove(column)

        return count

    return dfs(0)


if __name__ == "__main__":
    print(total_n_queens(4))
