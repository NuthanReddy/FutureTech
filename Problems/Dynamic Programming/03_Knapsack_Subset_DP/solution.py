"""Top Interview 150 capacity, amount, and subset DP problems.

Problems implemented:

* **0/1 Knapsack:** Given item weights, values, and a capacity, choose each
  item at most once to maximize value without exceeding capacity.  Inputs are
  ``weights``, ``values``, and ``capacity``; output is an integer maximum.
* **Coin Change:** Given coin denominations and a target amount, return the
  fewest coins needed to make the amount, with unlimited reuse of each coin,
  or ``-1`` when impossible.  Inputs are ``coins`` and ``amount``; output is
  an integer.  Standard bounds include ``1 <= len(coins) <= 12`` and
  ``0 <= amount <= 10_000``.
* **Partition Equal Subset Sum:** Given positive integers, decide whether they
  can be split into two subsets with equal sums.  Input is ``nums``; output is
  boolean.  The usual constraint is ``1 <= len(nums) <= 200`` with values up
  to about ``100``.
"""

from functools import lru_cache
from typing import List


def knapsack_memo(weights: List[int], values: List[int], capacity: int) -> int:
    """Return max value for 0/1 knapsack using take/skip states."""
    n = len(weights)

    @lru_cache(maxsize=None)
    def dfs(index: int, remaining_capacity: int) -> int:
        if index == n or remaining_capacity == 0:
            return 0

        skip_item = dfs(index + 1, remaining_capacity)

        take_item = 0
        if weights[index] <= remaining_capacity:
            take_item = values[index] + dfs(index + 1, remaining_capacity - weights[index])

        return max(skip_item, take_item)

    return dfs(0, capacity)


def knapsack_tab(weights: List[int], values: List[int], capacity: int) -> int:
    """Return max value for 0/1 knapsack using bottom-up tabulation."""
    n = len(weights)
    dp = [[0] * (capacity + 1) for _ in range(n + 1)]

    for index in range(n - 1, -1, -1):
        for current_capacity in range(capacity + 1):
            skip_item = dp[index + 1][current_capacity]

            take_item = 0
            if weights[index] <= current_capacity:
                take_item = values[index] + dp[index + 1][current_capacity - weights[index]]

            dp[index][current_capacity] = max(skip_item, take_item)

    return dp[0][capacity]


def coin_change_memo(coins: List[int], amount: int) -> int:
    """Solve Coin Change: minimize reusable coins needed to make ``amount``.

    ``best(remaining)`` tries every coin and may reuse it, so the transition
    stays at the same item set rather than advancing an item index.  ``inf``
    represents an impossible remainder and is converted to ``-1`` at the API
    boundary.
    """
    if amount < 0:
        return -1
    usable_coins = tuple(coin for coin in coins if coin > 0)

    @lru_cache(maxsize=None)
    def best(remaining: int) -> int:
        if remaining == 0:
            return 0
        answer = float("inf")
        for coin in usable_coins:
            if coin <= remaining:
                answer = min(answer, 1 + best(remaining - coin))
        return answer

    result = best(amount)
    return -1 if result == float("inf") else int(result)


def coin_change_tab(coins: List[int], amount: int) -> int:
    """Return the minimum coin count using forward unbounded transitions."""
    if amount < 0:
        return -1
    dp = [amount + 1] * (amount + 1)
    dp[0] = 0  # Zero coins make sum zero; every other state is initially unknown.

    for current in range(1, amount + 1):
        for coin in coins:
            if coin > 0 and coin <= current:
                dp[current] = min(dp[current], dp[current - coin] + 1)
    return -1 if dp[amount] == amount + 1 else dp[amount]


def can_partition_memo(nums: List[int]) -> bool:
    """Solve Partition Equal Subset Sum: test for an equal-sum split."""
    total = sum(nums)
    if total % 2:
        return False
    target = total // 2

    @lru_cache(maxsize=None)
    def possible(index: int, remaining: int) -> bool:
        if remaining == 0:
            return True
        if index == len(nums) or remaining < 0:
            return False
        return possible(index + 1, remaining) or possible(
            index + 1, remaining - nums[index]
        )

    return possible(0, target)


def can_partition_tab(nums: List[int]) -> bool:
    """Solve equal partition as 0/1 subset-sum with a descending loop.

    Descending targets are essential: they ensure each input number updates a
    state only once, whereas ascending targets would accidentally reuse it.
    """
    total = sum(nums)
    if total % 2:
        return False
    target = total // 2
    reachable = [False] * (target + 1)
    reachable[0] = True

    for number in nums:
        for current in range(target, number - 1, -1):
            reachable[current] = reachable[current] or reachable[current - number]
    return reachable[target]
