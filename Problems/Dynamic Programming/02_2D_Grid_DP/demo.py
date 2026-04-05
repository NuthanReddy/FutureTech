"""Small runner for 2D Grid DP (Minimum Path Sum)."""

from solution import min_path_sum_memo, min_path_sum_tab


if __name__ == "__main__":
    grid = [
        [1, 3, 1],
        [1, 5, 1],
        [4, 2, 1],
    ]

    memo_answer = min_path_sum_memo(grid)
    tab_answer = min_path_sum_tab(grid)

    print("Pattern: 2D Grid DP")
    print(f"Grid: {grid}")
    print(f"Memoization answer: {memo_answer}")
    print(f"Tabulation answer: {tab_answer}")

