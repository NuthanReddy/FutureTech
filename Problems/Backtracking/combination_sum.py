"""Top Interview 150: Combination Sum.

Problem statement:
    Input: a list of positive candidate integers and a non-negative ``target``.
    Required output: unique combinations whose values sum to ``target``; each
    candidate may be reused unlimited times.
    Key constraints: the canonical prompt gives distinct positive candidates
    and positive target. This module deduplicates candidates defensively and
    rejects non-positive candidates because they break the decreasing-target
    invariant.

Backtracking invariant:
    ``path`` is non-decreasing by candidate index. Recursive calls pass the
    current index, not the next index, when reusing a candidate is allowed.
    This removes duplicate permutations such as [2, 3, 2].

Alternative:
    A dynamic-programming table can count combinations, but it is less natural
    when the task is to list the combinations themselves.

Complexity:
    Time: exponential in ``target / min(candidates)`` in the worst case, plus
    the cost of copying each answer.
    Space: O(target / min(candidates)) recursion/path depth, excluding output.
"""

from __future__ import annotations


def combination_sum(candidates: list[int], target: int) -> list[list[int]]:
    """Return unique combinations that sum to ``target``."""
    if target < 0:
        raise ValueError("target must be non-negative")
    if any(candidate <= 0 for candidate in candidates):
        raise ValueError("candidates must be positive integers")

    unique_candidates = sorted(set(candidates))
    result: list[list[int]] = []
    path: list[int] = []

    def dfs(start_index: int, remaining: int) -> None:
        if remaining == 0:
            result.append(path.copy())
            return

        for index in range(start_index, len(unique_candidates)):
            candidate = unique_candidates[index]
            if candidate > remaining:
                break
            path.append(candidate)
            dfs(index, remaining - candidate)
            path.pop()

    dfs(0, target)
    return result


if __name__ == "__main__":
    print(combination_sum([2, 3, 6, 7], 7))
