"""Top Interview 150: Letter Combinations of a Phone Number.

Problem statement:
    Input: a string ``digits`` containing only keypad digits 2 through 9.
    Required output: all possible strings formed by choosing one mapped letter
    for each digit, preserving digit order. Return [] for an empty input.
    Key constraints: the canonical prompt uses 0 <= len(digits) <= 4. Digits 0
    and 1 have no mapping and this module rejects them.

Backtracking invariant:
    ``path`` has exactly one chosen letter for each processed digit
    ``digits[:index]``. No branch mutates another branch because the last
    letter is popped before the next candidate is tried.

Alternative:
    Iteratively build a Cartesian product with a queue/list. That avoids
    recursion but stores every partial layer; DFS keeps only the current path
    plus the final output.

Complexity:
    Time: O(4^n * n), where n is len(digits). The ``* n`` is for joining each
    complete path. Digits 7 and 9 have four choices, all others have three.
    Space: O(n) recursion/path space, excluding the required output.
"""

from __future__ import annotations


PHONE_LETTERS: dict[str, str] = {
    "2": "abc",
    "3": "def",
    "4": "ghi",
    "5": "jkl",
    "6": "mno",
    "7": "pqrs",
    "8": "tuv",
    "9": "wxyz",
}


def letter_combinations(digits: str) -> list[str]:
    """Return all keypad letter combinations for ``digits``."""
    if not digits:
        return []

    if any(digit not in PHONE_LETTERS for digit in digits):
        raise ValueError("digits must contain only characters '2' through '9'")

    combinations: list[str] = []
    path: list[str] = []

    def dfs(index: int) -> None:
        if index == len(digits):
            combinations.append("".join(path))
            return

        for letter in PHONE_LETTERS[digits[index]]:
            path.append(letter)
            dfs(index + 1)
            path.pop()

    dfs(0)
    return combinations


if __name__ == "__main__":
    print(letter_combinations("23"))
