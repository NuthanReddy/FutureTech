"""Small runner for Knapsack/Subset DP (0/1 Knapsack)."""

from solution import knapsack_memo, knapsack_tab


if __name__ == "__main__":
    weights = [1, 3, 4, 5]
    values = [1, 4, 5, 7]
    capacity = 7

    memo_answer = knapsack_memo(weights, values, capacity)
    tab_answer = knapsack_tab(weights, values, capacity)

    print("Pattern: Knapsack / Subset DP")
    print(f"Weights: {weights}")
    print(f"Values: {values}")
    print(f"Capacity: {capacity}")
    print(f"Memoization answer: {memo_answer}")
    print(f"Tabulation answer: {tab_answer}")

