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
    """Return LIS length using include/skip memoization.

    Pattern identification: increasing non-contiguous choices -> index/previous DP ->
    take requires a strictly larger value; take/skip exhausts valid subsequences.
    """
    # 1. Output: Return the length of the longest strictly increasing subsequence.
    # 2. Structure: Values may be skipped but cannot be reordered; whether a value
    #    can follow depends on the previous choice, so position alone is not enough.
    # 3. Constraints: Empty input gives 0; equal values cannot extend the sequence.
    #    O(n^2) time/space; the O(n) recursive depth must fit Python's limit.
    # 4. Choice: Remember the best remaining length for (index, prev_index), with -1 meaning no choice;
    #    at the end return 0, otherwise skip or take a strictly larger value.
    # 5. Why it works: Every increasing subsequence chooses one of these
    #    legal branches at each index; their maximum yields the longest length.
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
    """Return LIS length using O(n²) bottom-up tabulation.

    Pattern identification: increasing non-contiguous choices -> best-ending-index DP ->
    each state extends only earlier, strictly smaller endpoints.
    """
    # 1. Output: Return the length of the longest strictly increasing subsequence.
    # 2. Structure: Values may have gaps but keep their order; any longer increasing
    #    sequence ends after a smaller earlier value whose best length can be reused.
    # 3. Constraints: Empty input gives 0; duplicates do not count as increases.
    #    O(n^2) time and O(n) space; selected values need not be adjacent.
    # 4. Choice: dp[index] is the best length ending there; seed it at 1 and test all earlier
    #    smaller endpoints and extend their length by one, then return max(dp).
    # 5. Why it works: Every longer subsequence has an earlier final predecessor;
    #    testing all such indices finds the best length for every endpoint.
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

    Pattern identification: insert/delete/replace between strings -> two-suffix DP ->
    match or one edit reduces the unresolved suffixes with minimum total cost.

    ``distance(i, j)`` is the minimum edits to turn ``source[i:]`` into
    ``target[j:]``.  If characters match, no edit is needed.  Otherwise the
    three transitions are insertion, deletion, and replacement.
    """
    # 1. Output: Return the fewest insertions, deletions, or replacements to reach target.
    # 2. Structure: Insert/delete/replace each leaves shorter remaining strings;
    #    different edit choices reach the same position pair, so reuse its cost.
    # 3. Constraints: Empty suffixes require inserting/deleting their remaining length.
    #    O((m+1)*(n+1)) time/space; recursive depth up to m+n must fit Python's limit.
    # 4. Choice: Remember the fewest edits for source[i:] and target[j:]; matches advance both;
    #    otherwise add one to the minimum insert, delete, or replace suffix cost.
    # 5. Why it works: Equal leading characters need no edit; otherwise one
    #    of the three edits begins an optimal transformation, and all are tried.
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

    Pattern identification: insert/delete/replace between strings -> two-prefix DP ->
    each state minimizes left/top/diagonal edits with exact empty-prefix costs.

    ``dp[i][j]`` compares the first ``i`` source characters with the first
    ``j`` target characters.  Empty-prefix initialization supplies the only
    possible operation: deleting or inserting every remaining character.
    """
    # 1. Output: Return the minimum edits needed to turn source into target.
    # 2. Structure: Each final edit removes a source character, a target character,
    #    or both from consideration; smaller beginning-pair costs can be reused.
    # 3. Constraints: Empty strings are allowed. For lengths m and n,
    #    O((m+1)*(n+1)) time and space; operations each cost one.
    # 4. Choice: dp[i][j] is the cost for source[:i] versus target[:j]; seed empty costs with lengths.
    #    Matches use the diagonal; mismatches add one to min(left, top, diagonal).
    # 5. Why it works: These neighbors cover inserting, deleting, and replacing;
    #    already optimal shorter-prefix costs make each new cell optimal too.
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
