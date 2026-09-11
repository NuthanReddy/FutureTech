"""Sliding Window Maximum.

Problem: Given an integer array and a fixed window size, slide the window one
position at a time and report the maximum value in every window.

Input: ``values`` and positive ``window_size`` ``k``.
Output: a list of ``n-k+1`` window maxima in left-to-right order; this
implementation returns ``[]`` when ``k <= 0`` or ``k > n``.
Constraints: windows are contiguous and fixed-size; values may be negative,
duplicated, or empty.
"""

from __future__ import annotations

from collections import deque


def max_sliding_window(values: list[int], window_size: int) -> list[int]:
    """Return the maximum value for every contiguous window.

    The deque contains indices whose values are decreasing.  The front is
    therefore the current maximum; expired indices leave from the front and
    newly dominated indices leave from the back.  Indices are used so expiry
    can be checked even when values repeat.

    Complexity: O(n) time and O(k) space, where k is ``window_size``.
    """
    if window_size <= 0 or window_size > len(values):
        return []

    candidates: deque[int] = deque()
    result: list[int] = []
    for index, value in enumerate(values):
        while candidates and candidates[0] <= index - window_size:
            candidates.popleft()
        while candidates and values[candidates[-1]] <= value:
            candidates.pop()
        candidates.append(index)
        if index >= window_size - 1:
            result.append(values[candidates[0]])
    return result


if __name__ == "__main__":
    assert max_sliding_window([1, 3, -1, -3, 5, 3, 6, 7], 3) == [3, 3, 5, 5, 6, 7]
    assert max_sliding_window([1], 1) == [1]
    assert max_sliding_window([1, 2], 3) == []
    print("Sliding Window Maximum: all checks passed")
