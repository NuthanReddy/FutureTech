"""Stock cooldown example for state-machine DP."""

from functools import lru_cache
from math import inf
from typing import List


def max_profit_memo(prices: List[int]) -> int:
    """Return max profit using memoized DFS state machine.

    Pattern identification: a sale blocks next-day buying -> day/can-buy DP ->
    selling jumps two days; each state maximizes only legal actions.
    """
    # 1. Output: Return the largest stock profit with one cooldown day after each sale.
    # 2. Structure: Prices form a day-by-day list, but a locally profitable sale
    #    blocks tomorrow's buy; compare future profits for holding/not holding.
    # 3. Constraints: Assume non-negative prices and at most one held share.
    #    Empty input gives 0; O(n) time/space, with recursion limited by Python's stack.
    # 4. Choice: Remember best future profit for (day, can_buy); past the end gives 0. Compare buy/skip
    #    or sell/hold; a sale resumes buying at day+2 to enforce cooldown.
    # 5. Why it works: Each state tries all legal actions and adds their future
    #    best profit; non-negative prices mean abandoning a final buy cannot help.
    n = len(prices)

    @lru_cache(maxsize=None)
    def dfs(day: int, can_buy: bool) -> int:
        if day >= n:
            return 0

        if can_buy:
            buy_now = -prices[day] + dfs(day + 1, False)
            skip_day = dfs(day + 1, True)
            return max(buy_now, skip_day)

        sell_now = prices[day] + dfs(day + 2, True)
        hold_stock = dfs(day + 1, False)
        return max(sell_now, hold_stock)

    return dfs(0, True)


def max_profit_tab(prices: List[int]) -> int:
    """Return max profit using iterative state-machine tabulation.

    Pattern identification: a sale blocks next-day buying -> hold/sold/rest DP ->
    buy reads prior rest, and all updates preserve previous-day dependencies.
    """
    # 1. Output: Return the maximum realized profit with a one-day sale cooldown.
    # 2. Structure: Not holding stock is not enough information: selling today
    #    forbids tomorrow's buy, unlike resting today, so keep those cases separate.
    # 3. Constraints: Assume non-negative prices and one held share at most.
    #    Empty input gives 0; O(n) time and O(1) extra space.
    # 4. Choice: Store best profit for hold/sold/rest; seed impossible/impossible/0. Sell from old hold,
    #    buy from old rest, and rest from old rest or yesterday's sold.
    # 5. Why it works: These states cover all legal histories; buying cannot
    #    use yesterday's sold profit, so the updates preserve the cooldown.
    hold = -inf
    sold = -inf
    rest = 0

    for price in prices:
        # Keep yesterday's sale and read old hold/rest before replacing them.
        previous_sold = sold
        sold = hold + price
        hold = max(hold, rest - price)
        rest = max(rest, previous_sold)

    return max(sold, rest)
