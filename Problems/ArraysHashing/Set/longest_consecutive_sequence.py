"""Longest Consecutive Sequence - set starts.

Problem statement:
    Given an unsorted integer list ``nums``, return the length of its longest
    consecutive-integer sequence.  Sequence members need not be adjacent in
    the input, duplicates do not extend a sequence, and an empty list returns
    zero.  The required LeetCode solution has average ``O(n)`` time.
"""

from __future__ import annotations


def longest_consecutive(nums: list[int]) -> int:
    """Return the length of the longest consecutive integer sequence.

    A number starts a sequence only when its predecessor is absent.  From
    each such start, the inner loop advances through present successors.
    Every value belongs to at most one forward scan because non-start values
    are never used as scan origins, yielding linear average time rather than
    repeatedly sorting or rescanning every sequence.

    Duplicates are removed before traversal: they do not extend a consecutive
    run and otherwise could make a scan count the same value more than once.

    Complexity:
        Time ``O(n)`` average, space ``O(n)``.
    """
    values = set(nums)
    longest = 0

    for value in values:
        if value - 1 in values:
            continue

        length = 1
        while value + length in values:
            length += 1
        longest = max(longest, length)

    return longest


if __name__ == "__main__":
    assert longest_consecutive([100, 4, 200, 1, 3, 2]) == 4
    assert longest_consecutive([0, 3, 7, 2, 5, 8, 4, 6, 0, 1]) == 9
    print("longest_consecutive examples passed")
