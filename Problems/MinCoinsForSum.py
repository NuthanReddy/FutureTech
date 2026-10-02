# Find the minimum number of coins required for a given amount with a given set of coins
# Pattern identification: minimum coins with reusable denominations -> unbounded amount DP;
# intended invariant: dp[i] is the best cost from smaller amounts; legacy zero-amount handling is absent.
import math


def min_coins(amount, allowed_coins=[1, 2, 5, 10]):
    # 1. Output: Return the fewest reusable coins making amount, or infinity if unreachable.
    # 2. Structure: Different coin choices reach the same smaller amounts; save those minimum costs in a table.
    # 3. Constraints: Assume positive integer coins and amount>0; zero amount leaves i undefined.
    # 4. Choice: Try every last coin, not just the largest; seed single coins and minimize dp[i-coin]+1.
    # 5. Why it works: For positive amounts, each solution starts from a seeded coin
    # and extends a smaller solved amount; dp[0] is never seeded, so the empty-sum case fails.
    if amount in allowed_coins:
        return 1

    dp = [math.inf] * (amount + 1)

    for coin in allowed_coins:
        if coin < amount:
            dp[coin] = 1

    for i in range(1, amount+1):
        for coin in allowed_coins:
            if i >= coin:
                dp[i] = min(dp[i], dp[i-coin]+1)

    return dp[i]


print(min_coins(1))
print(min_coins(3))
print(min_coins(4))
print(min_coins(5))
print(min_coins(6))
print(min_coins(7))
print(min_coins(8))
