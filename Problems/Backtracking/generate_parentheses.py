"""Top Interview 150: Generate Parentheses.

Pattern identification:
    Enumerate balanced strings -> constrained binary backtracking;
    keep close_count <= open_count <= n at every prefix.

Problem statement:
    Input: an integer ``n``, the number of parentheses pairs.
    Required output: all well-formed parentheses strings containing exactly
    ``n`` opening and ``n`` closing parentheses.
    Key constraints: the canonical prompt uses 1 <= n <= 8. This module also
    accepts n == 0, returning [""].

Backtracking invariant:
    At every prefix, ``close_count <= open_count <= n``. Because invalid
    prefixes are never created, every complete length-2n path is valid.

Alternative:
    Generate all 2^(2n) strings and filter by balance. That is much simpler to
    write but wastes almost all work; constrained DFS only visits valid
    prefixes.

Complexity:
    Time: O(C_n * n), where C_n is the nth Catalan number and each answer is
    joined/copied.
    Space: O(n) recursion/path space, excluding output.
"""

from __future__ import annotations


def generate_parenthesis(n: int) -> list[str]:
    """Return all valid strings with ``n`` pairs of parentheses."""
    # 1. Output: List all balanced strings containing n pairs; n=0 gives [""].
    # 2. Structure: All strings are needed; a closing bracket must have an earlier unmatched opening.
    # 3. Constraints: n must be nonnegative; the number of answers grows quickly.
    # 4. Choice: Backtrack over legal next brackets: "(" if opens<n, ")" if closes<opens; then undo.
    # 5. Why it works: The counts always satisfy closes<=opens<=n, so a length-2n
    # path is balanced, and trying both legal next characters misses no valid string.
    if n < 0:
        raise ValueError("n must be non-negative")

    result: list[str] = []
    path: list[str] = []

    def dfs(open_count: int, close_count: int) -> None:
        if len(path) == 2 * n:
            result.append("".join(path))
            return

        if open_count < n:
            path.append("(")
            dfs(open_count + 1, close_count)
            path.pop()

        if close_count < open_count:
            path.append(")")
            dfs(open_count, close_count + 1)
            path.pop()

    dfs(0, 0)
    return result


if __name__ == "__main__":
    print(generate_parenthesis(3))
