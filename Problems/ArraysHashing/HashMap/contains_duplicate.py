"""Contains Duplicate - set membership pattern.

Problem statement:
    Given an integer list ``nums``, return ``True`` if any integer appears at
    least twice; otherwise return ``False``.  Values may be negative and the
    list may be empty.  The desired solution runs in linear average time.

A set is the smallest state that preserves exactly the fact needed by the
decision: which values have already appeared.
"""

from __future__ import annotations


def contains_duplicate(nums: list[int]) -> bool:
    """Return ``True`` when *nums* contains a repeated value.

    The invariant after processing each item is that ``seen`` contains every
    value in the processed prefix.  Therefore a value already in ``seen`` is
    both necessary and sufficient evidence of a duplicate.

    A sort-based solution would also work, but it changes the input order (or
    requires a copy) and costs ``O(n log n)``.  The set keeps the intended
    linear-time membership check and does not mutate the caller's list.

    Complexity:
        Time ``O(n)`` average, space ``O(n)``.
    """
    seen: set[int] = set()
    for number in nums:
        if number in seen:
            return True
        seen.add(number)
    return False


if __name__ == "__main__":
    assert contains_duplicate([1, 2, 3, 1])
    assert not contains_duplicate([1, 2, 3, 4])
    print("contains_duplicate examples passed")
