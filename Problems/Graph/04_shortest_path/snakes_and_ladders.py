"""Snakes and Ladders (LeetCode 909).

Problem: Given an ``n x n`` board numbered in boustrophedon order, find the
minimum dice throws from square 1 to square ``n*n``; a landing on a snake or
ladder moves immediately to its destination.  Output: the minimum throws, or
``-1`` if unreachable.  Constraints: each die result is 1..6 and ``-1``
denotes no snake or ladder.
"""

from collections import deque
from typing import List


def snakes_and_ladders(board: List[List[int]]) -> int:
    """Return the minimum number of dice throws needed to reach the end.

    Pattern identification: minimum equal-cost throws with landing jumps -> BFS;
    first discovery of a post-jump square uses the fewest throws.
    """
    # 1. Output: Return the fewest dice throws to the final square, or -1 when unreachable.
    # 2. Structure: Alternating row numbering maps squares to cells; each throw is one equal-cost edge.
    # 3. Constraints: The board is square; -1 means no jump, and only one landing jump is applied.
    # 4. Choice: BFS over (square, throws), clamp rolls at the target, and mark post-jump destinations.
    # 5. Why it works: BFS first reaches each destination with the fewest throws, including its landing jump.
    #    Each of n*n squares tries six rolls: O(n*n) time and space.
    n = len(board)
    if n == 0:
        return -1

    def square_to_cell(square: int) -> tuple[int, int]:
        row_from_bottom, offset = divmod(square - 1, n)
        row = n - 1 - row_from_bottom
        col = offset if row_from_bottom % 2 == 0 else n - 1 - offset
        return row, col

    target = n * n
    queue = deque([(1, 0)])
    seen = {1}
    while queue:
        square, throws = queue.popleft()
        if square == target:
            return throws
        for roll in range(1, 7):
            landing = min(square + roll, target)
            r, c = square_to_cell(landing)
            destination = board[r][c] if board[r][c] != -1 else landing
            if destination not in seen:
                seen.add(destination)
                queue.append((destination, throws + 1))
    return -1
