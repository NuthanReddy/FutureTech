"""Small runner for 1D Linear DP (House Robber)."""

from solution import rob_memo, rob_tab


if __name__ == "__main__":
    houses = [2, 7, 9, 3, 1]

    memo_answer = rob_memo(houses)
    tab_answer = rob_tab(houses)

    print("Pattern: 1D Linear DP")
    print(f"Houses: {houses}")
    print(f"Memoization answer: {memo_answer}")
    print(f"Tabulation answer: {tab_answer}")

