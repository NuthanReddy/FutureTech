"""Top Interview 150 sequence-comparison and subsequence DP problems.

Problems implemented:

* **Longest Increasing Subsequence:** Given an integer array, return the
  length of the longest strictly increasing subsequence; selected values need
  not be contiguous.  Input is ``nums``; output is an integer.  Typical
  bounds allow up to ``2_500`` values.
* **Edit Distance:** Given two strings, return the minimum insertions,
  deletions, or substitutions needed to transform the first into the second.
  Inputs are ``source`` and ``target``; output is a non-negative integer.
  Standard bounds are string lengths up to ``500``.
"""

from functools import lru_cache
from typing import List


def lis_memo(nums: List[int]) -> int:
    """Return LIS length using include/skip memoization."""
    n = len(nums)

    @lru_cache(maxsize=None)
    def dfs(index: int, prev_index: int) -> int:
        if index == n:
            return 0

        skip_current = dfs(index + 1, prev_index)

        take_current = 0
        if prev_index == -1 or nums[index] > nums[prev_index]:
            take_current = 1 + dfs(index + 1, index)

        return max(skip_current, take_current)

    return dfs(0, -1)


def lis_tab(nums: List[int]) -> int:
    """Return LIS length using O(n²) bottom-up tabulation."""
    if not nums:
        return 0

    n = len(nums)
    dp = [1] * n

    for index in range(n):
        for prev in range(index):
            if nums[prev] < nums[index]:
                dp[index] = max(dp[index], dp[prev] + 1)

    return max(dp)


def edit_distance_memo(source: str, target: str) -> int:
    """Solve Edit Distance: minimize edits transforming ``source`` to ``target``.

    ``distance(i, j)`` is the minimum edits to turn ``source[i:]`` into
    ``target[j:]``.  If characters match, no edit is needed.  Otherwise the
    three transitions are insertion, deletion, and replacement.
    """
    @lru_cache(maxsize=None)
    def distance(i: int, j: int) -> int:
        if i == len(source):
            return len(target) - j  # Insert the remaining target suffix.
        if j == len(target):
            return len(source) - i  # Delete the remaining source suffix.
        if source[i] == target[j]:
            return distance(i + 1, j + 1)
        return 1 + min(
            distance(i, j + 1),      # Insert target[j].
            distance(i + 1, j),      # Delete source[i].
            distance(i + 1, j + 1),  # Replace source[i].
        )

    return distance(0, 0)


def edit_distance_tab(source: str, target: str) -> int:
    """Return Levenshtein distance using prefix tabulation.

    ``dp[i][j]`` compares the first ``i`` source characters with the first
    ``j`` target characters.  Empty-prefix initialization supplies the only
    possible operation: deleting or inserting every remaining character.
    """
    rows, cols = len(source), len(target)
    dp = [[0] * (cols + 1) for _ in range(rows + 1)]
    for i in range(rows + 1):
        dp[i][0] = i
    for j in range(cols + 1):
        dp[0][j] = j

    for i in range(1, rows + 1):
        for j in range(1, cols + 1):
            if source[i - 1] == target[j - 1]:
                dp[i][j] = dp[i - 1][j - 1]
            else:
                dp[i][j] = 1 + min(
                    dp[i][j - 1],      # Insert.
                    dp[i - 1][j],      # Delete.
                    dp[i - 1][j - 1],  # Replace.
                )
    return dp[rows][cols]
