"""Small runner for Longest Subsequence DP (LIS)."""

from solution import lis_memo, lis_tab


if __name__ == "__main__":
    numbers = [10, 9, 2, 5, 3, 7, 101, 18]

    memo_answer = lis_memo(numbers)
    tab_answer = lis_tab(numbers)

    print("Pattern: Longest Subsequence DP")
    print(f"Numbers: {numbers}")
    print(f"Memoization answer: {memo_answer}")
    print(f"Tabulation answer: {tab_answer}")

