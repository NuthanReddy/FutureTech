"""Top Interview 150: Permutations.

Problem statement:
    Input: a list ``nums`` of distinct integers.
    Required output: every possible ordering of all values in ``nums``.
    Key constraints: the canonical prompt uses 1 <= len(nums) <= 6 and all
    values are distinct, so duplicate-skipping logic is unnecessary.

Backtracking invariant:
    ``path`` contains no duplicates and ``used[i]`` is true exactly when
    ``nums[i]`` is already in ``path``. The invariant is restored by clearing
    the flag immediately after the recursive call returns.

Alternative:
    In-place swapping is also common and saves the ``used`` array. The explicit
    path/used version is easier to audit in learning code and avoids mutating
    the caller's list.

Complexity:
    Time: O(n! * n) because each permutation is copied.
    Space: O(n) recursion/path/used space, excluding the output.
"""

from __future__ import annotations


def permute(nums: list[int]) -> list[list[int]]:
    """Return all permutations of distinct values in ``nums``."""
    result: list[list[int]] = []
    path: list[int] = []
    used = [False] * len(nums)

    def dfs() -> None:
        if len(path) == len(nums):
            result.append(path.copy())
            return

        for index, value in enumerate(nums):
            if used[index]:
                continue
            used[index] = True
            path.append(value)
            dfs()
            path.pop()
            used[index] = False

    dfs()
    return result


if __name__ == "__main__":
    print(permute([1, 2, 3]))
