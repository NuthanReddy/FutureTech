"""Top Interview 150: N-Queens II.

Pattern identification:
    Count nonattacking placements -> row-wise constraint backtracking;
    keep one queen per row and reject occupied columns or diagonals.

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
    # 1. Output: Count ways to place n nonattacking queens; n=0 counts one empty board.
    # 2. Structure: These are competing placements: a queen blocks its column and two diagonals for later rows.
    # 3. Constraints: n must be nonnegative; placement search can take factorial-scale work.
    # 4. Choice: A safe spot is not necessarily part of a full board; try every safe column and undo after recursion.
    # 5. Why it works: The sets describe exactly the queens on the current path;
    # rejecting attacks and undoing their keys enumerates every safe board once.
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
            # Remove all three marks before trying another column in this row.
            diag_up.remove(up_key)
            diag_down.remove(down_key)
            columns.remove(column)

        return count

    return dfs(0)


if __name__ == "__main__":
    print(total_n_queens(4))
