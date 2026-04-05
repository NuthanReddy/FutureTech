"""Small runner for Interval DP (Matrix Chain Multiplication)."""

from solution import matrix_chain_memo, matrix_chain_tab


if __name__ == "__main__":
    dimensions = [40, 20, 30, 10, 30]

    memo_answer = matrix_chain_memo(dimensions)
    tab_answer = matrix_chain_tab(dimensions)

    print("Pattern: Interval DP")
    print(f"Dimensions: {dimensions}")
    print(f"Memoization answer: {memo_answer}")
    print(f"Tabulation answer: {tab_answer}")

