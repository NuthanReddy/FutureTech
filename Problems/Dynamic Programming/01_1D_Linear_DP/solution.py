"""Top Interview 150 problems that use one-dimensional DP.

The functions deliberately keep both a top-down and a bottom-up version.  The
two implementations are useful for comparing *state design* (what a state
means) with *evaluation order* (when its dependencies are available).

Problems implemented:

* **House Robber:** Given non-negative money amounts in a row of houses, return
  the largest amount that can be robbed without robbing adjacent houses.
  Input is ``nums: list[int]``; output is an integer maximum.  The usual
  constraints are ``0 <= len(nums) <= 100`` and ``0 <= nums[i] <= 400``.
* **Climbing Stairs:** Given ``n`` stairs, count distinct ways to reach the
  top when each move climbs one or two stairs.  Input is ``n: int``; output
  is a count.  The standard constraint is ``1 <= n <= 45``.
* **Word Break:** Given a string and a dictionary, decide whether the string
  can be segmented into one or more dictionary words.  Input is ``text`` and
  ``words``; output is a boolean.  Words are non-empty and the string length
  and dictionary size are finite positive bounds.
"""

from functools import lru_cache
from typing import List


def rob_memo(nums: List[int]) -> int:
    """Return the maximum non-adjacent sum using memoized take/skip DP.

    ``dfs(i)`` means "the best amount obtainable from houses ``i`` onward".
    At every index the complete choice is either to skip ``i`` or rob it and
    therefore skip ``i + 1``.  The answer at index zero is the final state.
    """
    n = len(nums)

    @lru_cache(maxsize=None)
    def dfs(index: int) -> int:
        if index >= n:
            return 0

        skip_current = dfs(index + 1)
        rob_current = nums[index] + dfs(index + 2)
        return max(skip_current, rob_current)

    return dfs(0)


def rob_tab(nums: List[int]) -> int:
    """Return the maximum non-adjacent sum using bottom-up tabulation.

    The two zero-valued sentinel cells represent the terminal states ``n`` and
    ``n + 1``.  They make the ``i + 2`` transition safe without special cases.
    """
    n = len(nums)
    dp = [0] * (n + 2)

    for index in range(n - 1, -1, -1):
        skip_current = dp[index + 1]
        rob_current = nums[index] + dp[index + 2]
        dp[index] = max(skip_current, rob_current)

    return dp[0]


def climb_stairs_memo(n: int) -> int:
    """Solve Climbing Stairs: count one/two-step routes to stair ``n``.

    State ``ways(step)`` counts paths from that step to the top.  The terminal
    state ``ways(n) = 1`` is important: reaching the top is one completed
    route, not zero routes.  ``ways(n + 1) = 0`` is an invalid overshoot.
    """
    if n < 0:
        return 0

    @lru_cache(maxsize=None)
    def ways(step: int) -> int:
        if step == n:
            return 1
        if step > n:
            return 0
        return ways(step + 1) + ways(step + 2)

    return ways(0)


def climb_stairs_tab(n: int) -> int:
    """Count stair-climbing paths with an iterative Fibonacci-style DP."""
    if n < 0:
        return 0
    if n <= 1:
        return 1

    # ``previous`` and ``current`` are ways to reach steps n-2 and n-1.
    # The next value carries both possible final moves into the new step.
    previous, current = 1, 1
    for _ in range(2, n + 1):
        previous, current = current, previous + current
    return current


def word_break_memo(text: str, words: List[str]) -> bool:
    """Solve Word Break: decide whether ``text`` is dictionary-segmentable.

    ``can_break(i)`` describes the suffix beginning at ``i``.  Trying every
    word is the transition; a successful word carries the solution to its
    ending index.  The empty suffix is a valid completed segmentation.
    """
    dictionary = set(words)
    n = len(text)

    @lru_cache(maxsize=None)
    def can_break(start: int) -> bool:
        if start == n:
            return True
        return any(
            text.startswith(word, start)
            and can_break(start + len(word))
            for word in dictionary
            if word
        )

    return can_break(0)


def word_break_tab(text: str, words: List[str]) -> bool:
    """Return word-break feasibility using prefix tabulation.

    ``dp[end]`` is true when ``text[:end]`` is segmentable.  For each reachable
    prefix, append each dictionary word and carry the truth value forward.
    """
    dictionary = set(words)
    dp = [False] * (len(text) + 1)
    dp[0] = True  # The empty prefix is the seed for the first word.

    for end in range(1, len(text) + 1):
        for word in dictionary:
            if word and len(word) <= end:
                start = end - len(word)
                if dp[start] and text[start:end] == word:
                    dp[end] = True
                    break
    return dp[-1]
