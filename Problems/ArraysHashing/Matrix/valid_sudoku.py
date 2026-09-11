"""Valid Sudoku - row, column, and sub-box set invariants.

Problem statement:
    Given a partially filled 9-by-9 Sudoku board, return whether its filled
    cells obey the Sudoku rules: each row, column, and 3-by-3 box contains no
    repeated digit.  Empty cells are ``"."``.  The board need not be solvable;
    only the current placement is checked.  This standalone function returns
    ``False`` for a board that is not 9 by 9.
"""

from __future__ import annotations


def is_valid_sudoku(board: list[list[str]]) -> bool:
    """Return whether all filled cells satisfy Sudoku uniqueness rules.

    Each set records values already observed in one constraint region.  For a
    cell ``(row, column)``, its box index is ``(row // 3) * 3 + column // 3``.
    The duplicate check happens before insertion, preserving the invariant
    that every set contains only unique values from its region.

    The LeetCode input is always 9x9, but returning ``False`` for malformed
    dimensions makes this standalone helper explicit rather than failing with
    an unrelated index error.  Dots represent empty cells and are ignored.

    Complexity:
        Time ``O(1)`` for the fixed 9x9 board (or ``O(r*c)`` generally),
        space ``O(1)`` for the fixed number of regions.
    """
    if len(board) != 9 or any(len(row) != 9 for row in board):
        return False

    rows = [set[str]() for _ in range(9)]
    columns = [set[str]() for _ in range(9)]
    boxes = [set[str]() for _ in range(9)]

    for row_index, row in enumerate(board):
        for column_index, value in enumerate(row):
            if value == ".":
                continue
            box_index = (row_index // 3) * 3 + column_index // 3
            if (
                value in rows[row_index]
                or value in columns[column_index]
                or value in boxes[box_index]
            ):
                return False
            rows[row_index].add(value)
            columns[column_index].add(value)
            boxes[box_index].add(value)
    return True


if __name__ == "__main__":
    board = [["."] * 9 for _ in range(9)]
    board[0][0] = "5"
    board[1][1] = "5"
    assert is_valid_sudoku(board)
    board[1][0] = "5"
    assert not is_valid_sudoku(board)
    print("is_valid_sudoku examples passed")
