"""Small runner for State-machine DP (Stock Cooldown)."""

from solution import max_profit_memo, max_profit_tab


if __name__ == "__main__":
    prices = [1, 2, 3, 0, 2]

    memo_answer = max_profit_memo(prices)
    tab_answer = max_profit_tab(prices)

    print("Pattern: State-machine DP (Stock)")
    print(f"Prices: {prices}")
    print(f"Memoization answer: {memo_answer}")
    print(f"Tabulation answer: {tab_answer}")

