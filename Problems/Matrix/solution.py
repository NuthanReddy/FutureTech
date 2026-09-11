"""Top Interview 150 matrix problems, implemented in-place where appropriate.

Problem statements:
* ``is_valid_sudoku``: Given a partially filled 9x9 Sudoku board, decide
  whether each row, column, and 3x3 box contains no repeated digit 1-9.
* ``spiral_order``: Given an ``m x n`` matrix, return all values in clockwise
  spiral order.
* ``rotate``: Given an ``n x n`` matrix, rotate it 90 degrees clockwise
  in-place and return nothing.
* ``set_zeroes``: Given a matrix, if a cell is zero, set its entire row and
  column to zero in-place.
* ``game_of_life``: Given a binary board, advance Conway's Game of Life by
  one generation in-place using the eight-neighbor rules.

Inputs are rectangular integer matrices except Sudoku, whose cells are digits
or ``"."``.  Matrix dimensions may be empty where the function permits it;
rotation requires a square matrix.  Outputs are a boolean, a traversal list,
or an in-place mutation (``None`` return).  The intended constraints are
``m, n >= 0`` and, for Sudoku, exactly 9 rows and columns.
"""

from typing import List


def is_valid_sudoku(board: List[List[str]]) -> bool:
    """Return whether ``board`` obeys Sudoku row, column, and box rules."""
    rows = [set() for _ in range(9)]
    columns = [set() for _ in range(9)]
    boxes = [set() for _ in range(9)]
    for row in range(9):
        for column in range(9):
            value = board[row][column]
            if value == ".":
                continue
            box = (row // 3) * 3 + column // 3
            if value in rows[row] or value in columns[column] or value in boxes[box]:
                return False
            rows[row].add(value)
            columns[column].add(value)
            boxes[box].add(value)
    return True


def spiral_order(matrix: List[List[int]]) -> List[int]:
    """Return rectangular ``matrix`` values in clockwise spiral order."""
    if not matrix or not matrix[0]:
        return []
    top, bottom, left, right = 0, len(matrix) - 1, 0, len(matrix[0]) - 1
    result: List[int] = []
    while top <= bottom and left <= right:
        result.extend(matrix[top][left : right + 1])
        top += 1
        for row in range(top, bottom + 1):
            result.append(matrix[row][right])
        right -= 1
        if top <= bottom:
            result.extend(matrix[bottom][left : right + 1][::-1])
            bottom -= 1
        if left <= right:
            for row in range(bottom, top - 1, -1):
                result.append(matrix[row][left])
            left += 1
    return result


def rotate(matrix: List[List[int]]) -> None:
    """Rotate square ``matrix`` 90 degrees clockwise in-place."""
    n = len(matrix)
    for row in range(n):
        for column in range(row + 1, n):
            matrix[row][column], matrix[column][row] = matrix[column][row], matrix[row][column]
    for row in matrix:
        row.reverse()


def set_zeroes(matrix: List[List[int]]) -> None:
    """Zero affected rows and columns of ``matrix`` in-place."""
    if not matrix or not matrix[0]:
        return
    rows, columns = len(matrix), len(matrix[0])
    first_row_zero = any(matrix[0][column] == 0 for column in range(columns))
    first_column_zero = any(matrix[row][0] == 0 for row in range(rows))
    for row in range(1, rows):
        for column in range(1, columns):
            if matrix[row][column] == 0:
                matrix[row][0] = matrix[0][column] = 0
    for row in range(1, rows):
        for column in range(1, columns):
            if matrix[row][0] == 0 or matrix[0][column] == 0:
                matrix[row][column] = 0
    if first_row_zero:
        matrix[0] = [0] * columns
    if first_column_zero:
        for row in range(rows):
            matrix[row][0] = 0


def game_of_life(board: List[List[int]]) -> None:
    """Advance binary ``board`` by one Conway's Game of Life generation."""
    if not board or not board[0]:
        return
    rows, columns = len(board), len(board[0])
    for row in range(rows):
        for column in range(columns):
            live_neighbors = 0
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    if dr == dc == 0:
                        continue
                    nr, nc = row + dr, column + dc
                    if 0 <= nr < rows and 0 <= nc < columns:
                        live_neighbors += board[nr][nc] & 1
            alive = board[row][column] & 1
            if (alive and live_neighbors in (2, 3)) or (not alive and live_neighbors == 3):
                board[row][column] |= 2  # bit 1 is the next state
    for row in range(rows):
        for column in range(columns):
            board[row][column] >>= 1
