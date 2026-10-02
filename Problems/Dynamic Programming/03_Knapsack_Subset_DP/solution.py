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
    """Return max value for 0/1 knapsack using take/skip states.

    Pattern identification: each item once under capacity -> item/resource DP ->
    both take and skip advance the index, preventing reuse.
    """
    # 1. Output: Return the largest total value that fits the capacity.
    # 2. Structure: Each item fits or does not fit the remaining capacity;
    #    the same remaining items/capacity recur, and a biggest-value pick may lose.
    # 3. Constraints: Assume equal-length lists, positive weights, non-negative
    #    capacity C. O(n*(C+1)) time/space; recursive depth must fit Python's limit.
    # 4. Choice: Remember best value for each (index, remaining_capacity); no items/capacity gives 0.
    #    Skip the item, or take it if it fits, advancing the index either way.
    # 5. Why it works: Take/skip covers every feasible subset, and advancing
    #    the index prevents reusing an item; max selects the best subset value.
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
    """Return max value for 0/1 knapsack using bottom-up tabulation.

    Pattern identification: each item once under capacity -> suffix/capacity table ->
    transitions read only the next item row, keeping every choice 0/1.
    """
    # 1. Output: Return the maximum value of a subset that fits the capacity.
    # 2. Structure: Each item is used once; taking it changes capacity while
    #    both take/skip leave the same later items, so compare their stored best values.
    # 3. Constraints: Assume equal-length lists, positive weights, non-negative
    #    capacity C. O(n*(C+1)) time and space; empty items give 0.
    # 4. Choice: Store best value per item/capacity; start the no-items row at zero and fill backward;
    #    compare skipping with taking plus the next row's reduced-capacity value.
    # 5. Why it works: Reading only the next row uses each item at most once;
    #    both legal choices are evaluated using already optimal suffix results.
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

    Pattern identification: unlimited coins, minimum count -> remaining-amount DP ->
    each positive coin reduces the remainder without removing denominations.

    ``best(remaining)`` tries every coin and may reuse it, so the transition
    stays at the same item set rather than advancing an item index.  ``inf``
    represents an impossible remainder and is converted to ``-1`` at the API
    boundary.
    """
    # 1. Output: Return the fewest reusable coins making amount, or -1 if impossible.
    # 2. Structure: Coins may be reused, and different choices leave the same amounts.
    #    Biggest-first is unsafe: for [1,3,4] and 6, 3+3 beats 4+1+1.
    # 3. Constraints: Ignore non-positive coins; negative amount gives -1.
    #    For k coins and amount A: O(k*(A+1)) time, O(A+k) space; recursion is limited.
    # 4. Choice: Remember the fewest coins for each remaining amount; zero needs zero coins.
    #    Try every fitting coin plus one, keeping infinity for impossible states.
    # 5. Why it works: Every non-empty payment has a first coin tried here;
    #    minimizing its optimal remainder gives the fewest coins overall.
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
    """Return the minimum coin count using forward unbounded transitions.

    Pattern identification: unlimited coins, minimum count -> ascending amount DP ->
    smaller amounts are optimal and may already include the same coin.
    """
    # 1. Output: Return the minimum number of coins making amount, or -1.
    # 2. Structure: Any last coin leaves a smaller amount; positive coins let
    #    those answers be filled first, without assuming biggest-first is best.
    # 3. Constraints: Ignore non-positive coins; negative amount gives -1.
    #    With k coins and amount A: O(k*(A+1)) time and O(A+1) space.
    # 4. Choice: dp[a] stores the fewest coins for a; set dp[0]=0, others=A+1, and fill upward
    #    with min(dp[current], dp[current-coin]+1) for each fitting coin.
    # 5. Why it works: Smaller amounts are solved first and can reuse coins;
    #    A+1 exceeds any feasible count, so it safely marks impossibility.
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
    """Solve Partition Equal Subset Sum: test for an equal-sum split.

    Pattern identification: equal halves with each number once -> half-sum 0/1 DP ->
    take/skip advances the index; an odd total cannot split equally.
    """
    # 1. Output: Return whether the numbers split into two subsets with equal sums.
    # 2. Structure: One subset must total half the overall sum; each number
    #    can belong to it once, and only index/remaining sum affect later choices.
    # 3. Constraints: Assume positive integers; empty input is True, odd total is False.
    #    For half-sum T: O(n*(T+1)) time/space; recursive depth must fit Python's limit.
    # 4. Choice: Remember whether (index, remaining) can succeed; zero succeeds, exhausted
    #    items or negative remaining fails; try skip/take with the next index.
    # 5. Why it works: Both choices enumerate all subsets without reuse;
    #    a subset reaching half the total leaves exactly the same sum outside it.
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

    Pattern identification: equal halves with each number once -> half-sum reachability ->
    descending updates read pre-item states, preventing reuse.

    Descending targets are essential: they ensure each input number updates a
    state only once, whereas ascending targets would accidentally reuse it.
    """
    # 1. Output: Return whether two equal-sum subsets can contain all the numbers.
    # 2. Structure: One half-sum subset guarantees an equal other half;
    #    reuse is forbidden, so each number must extend only earlier-number sums.
    # 3. Constraints: Assume positive integers; empty input is True, odd total is False.
    #    For half-sum T: O(n*(T+1)) time and O(T+1) space.
    # 4. Choice: reachable[s] says earlier numbers can make s; seed zero=True and update downward
    #    using reachable[current] or reachable[current-number].
    # 5. Why it works: Descending order reads states from before this number,
    #    preventing reuse; all take/skip subsets are represented after each pass.
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
