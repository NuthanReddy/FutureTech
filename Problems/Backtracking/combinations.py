"""Top Interview 150: Combinations.

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
