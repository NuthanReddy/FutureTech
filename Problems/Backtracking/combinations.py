"""Top Interview 150: Combinations.

Pattern identification:
    Unordered size-k selections -> increasing-index backtracking;
    never reuse a value and prune starts that leave too few choices.

Problem statement:
    Input: two integers ``n`` and ``k``.
    Required output: every size-k combination chosen from integers 1..n, with
    each combination in increasing order and no duplicate combinations.
    Key constraints: the canonical prompt uses 1 <= n <= 20 and 1 <= k <= n.
    This module also handles k == 0 as [[]] and k > n as [].

Backtracking invariant:
    ``path`` is strictly increasing. The next loop starts at ``start``, so a
    number can never be reused and the same set cannot be produced in a
    different order.

Alternative:
    A choose/skip recursion can model each integer as a binary decision. The
    loop form below is shorter and makes pruning easier because it can cap the
    largest starting value that still leaves enough numbers.

Complexity:
    Time: O(C(n, k) * k) to copy every valid combination.
    Space: O(k) recursion/path space, excluding the output.
"""

from __future__ import annotations


def combine(n: int, k: int) -> list[list[int]]:
    """Return all combinations of ``k`` numbers chosen from ``1..n``."""
    # 1. Output: List every size-k selection from 1 through n.
    # 2. Structure: All size-k selections are required, not just one; increasing order removes reordered copies.
    # 3. Constraints: Negative inputs are rejected; k=0 gives [[]], and k>n gives [].
    # 4. Choice: Try each possible next value and undo it; skip starts with too few numbers left to fill k slots.
    # 5. Why it works: Every selection has one increasing path, and pruning only removes
    # paths that cannot reach k values; copying answers keeps later undo steps separate.
    if k < 0 or n < 0:
        raise ValueError("n and k must be non-negative")
    if k == 0:
        return [[]]
    if k > n:
        return []

    result: list[list[int]] = []
    path: list[int] = []

    def dfs(start: int) -> None:
        if len(path) == k:
            result.append(path.copy())
            return

        slots_left = k - len(path)
        # Last legal start leaves at least ``slots_left`` numbers available.
        max_start = n - slots_left + 1
        for value in range(start, max_start + 1):
            path.append(value)
            dfs(value + 1)
            path.pop()

    dfs(1)
    return result


if __name__ == "__main__":
    print(combine(4, 2))
