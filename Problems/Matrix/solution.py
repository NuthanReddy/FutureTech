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
    """Return whether ``board`` obeys Sudoku row, column, and box rules.

    Pattern identification: overlapping uniqueness constraints -> region sets;
    each set records only previously visited digits in its row, column, or box.
    """
    # 1. Output: Return whether filled cells obey row, column, and box uniqueness rules.
    # 2. Structure: A digit must be absent from its row, column, and box; sets remember exactly those earlier digits.
    # 3. Constraints: Assume a 9-by-9 board containing digits or "."; this variant does not validate dimensions.
    # 4. Choice: Skip dots, check three region sets for duplicates, then record the digit in each set.
    # 5. Why it works: Each set remembers earlier digits in its region, so duplicate checks cover all conflicts.
    #    Fixed board dimensions make time and extra space O(1); solvability is not tested.
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
    """Return rectangular ``matrix`` values in clockwise spiral order.

    Pattern identification: clockwise outer-ring traversal -> shrinking boundaries;
    the remaining rectangle contains exactly the unvisited cells.
    """
    # 1. Output: Return every matrix value in clockwise spiral order.
    # 2. Structure: The visiting order follows outer edges, not arbitrary neighbors; removing them leaves another rectangle.
    # 3. Constraints: Assume rectangular input; empty matrices return [], and collapsed edges must not be repeated.
    # 4. Choice: Maintain four bounds, append each edge, and shrink its bound before the next edge.
    # 5. Why it works: The bounds always enclose exactly the unvisited cells; guards skip already consumed edges.
    #    O(rows*columns) time; row slices use O(columns) temporary space beyond the output.
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
    """Rotate square ``matrix`` 90 degrees clockwise in-place.

    Pattern identification: square clockwise coordinate transform -> transpose/reverse;
    swap each off-diagonal pair once, then reverse rows to map (r, c) to (c, n-1-r).
    """
    # 1. Output: Rotate the matrix 90 degrees clockwise in place and return None.
    # 2. Structure: Rotation changes positions, not values; the square shape lets us swap rows and columns in the same grid.
    # 3. Constraints: Require a square matrix; preserve the grid rather than allocate a rotated copy.
    # 4. Choice: Transpose by swapping pairs above the diagonal, then reverse rows to complete the clockwise mapping.
    # 5. Why it works: (row, column) becomes (column, n-1-row), the clockwise destination; each pair swaps once.
    #    O(n^2) time and O(1) extra space.
    n = len(matrix)
    for row in range(n):
        for column in range(row + 1, n):
            matrix[row][column], matrix[column][row] = matrix[column][row], matrix[row][column]
    for row in matrix:
        row.reverse()


def set_zeroes(matrix: List[List[int]]) -> None:
    """Zero affected rows and columns of ``matrix`` in-place.

    Pattern identification: original zeros trigger whole lines -> first-row/column markers;
    markers preserve original triggers; separate flags preserve the marker lines' fate.
    """
    # 1. Output: Mutate the matrix so every original zero clears its entire row and column.
    # 2. Structure: One original zero clears a whole row/column; each line needs only a yes/no flag, not every zero's position.
    # 3. Constraints: Assume rectangular input; new zeros must not trigger additional clearing.
    # 4. Choice: Reuse the first row/column as flags instead of separate sets; save their own fate before marking and clearing.
    # 5. Why it works: Marking finishes before clearing, so only original zeros determine which lines are affected.
    #    O(rows*columns) time; replacing a cleared first row allocates O(columns) temporary space.
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
    """Advance binary ``board`` by one Conway's Game of Life generation.

    Pattern identification: simultaneous neighbor-dependent update -> old/new bit packing;
    low bits retain original states until every next state is encoded in bit 1.
    """
    # 1. Output: Advance the board by one Game of Life generation in place, returning None.
    # 2. Structure: All cells change together using old neighbors, so ordinary immediate overwrites would change later answers.
    # 3. Constraints: Assume a rectangular binary board; out-of-bounds neighbors do not count, and empty input is unchanged.
    # 4. Choice: Store old/new states in two bits of each cell instead of a second board; finally shift to keep only new states.
    # 5. Why it works: Writing bit 1 never changes bit 0, so later cells still read the same original generation.
    #    Constant neighbors per cell give O(rows*columns) time and O(1) extra space.
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
