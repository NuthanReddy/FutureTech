"""Stock cooldown example for state-machine DP."""

from functools import lru_cache
from math import inf
from typing import List


def max_profit_memo(prices: List[int]) -> int:
    """Return max profit using memoized DFS state machine."""
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
    """Return max profit using iterative state-machine tabulation."""
    hold = -inf
    sold = -inf
    rest = 0

    for price in prices:
        previous_sold = sold
        sold = hold + price
        hold = max(hold, rest - price)
        rest = max(rest, previous_sold)

    return max(sold, rest)

