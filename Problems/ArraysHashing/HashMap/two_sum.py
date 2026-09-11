"""Two Sum - complement lookup pattern.

Problem statement:
    Given an integer list ``nums`` and integer ``target``, return the indices
    of two distinct elements whose values add to ``target``.  Return an empty
    list when no pair exists.  A valid LeetCode input guarantees one answer,
    but this standalone function also handles absent pairs.  The same index
    cannot be used twice, and answer order is not significant.
"""

from __future__ import annotations


def two_sum(nums: list[int], target: int) -> list[int]:
    """Return indices of a pair summing to *target*, or ``[]`` if absent.

    The dictionary stores an earlier value's index.  At index ``i``, the only
    possible partner is ``target - nums[i]``; checking that complement before
    storing the current value prevents using the same element twice.

    The invariant is that ``seen`` contains exactly the values and indices
    from the already processed prefix.  A one-pass dictionary is preferred to
    the tempting nested-loop brute force because it reduces time from
    ``O(n²)`` to average ``O(n)`` while using ``O(n)`` space.

    Complexity:
        Time ``O(n)`` average, space ``O(n)``.
    """
    seen: dict[int, int] = {}
    for index, number in enumerate(nums):
        complement = target - number
        if complement in seen:
            return [seen[complement], index]
        seen[number] = index
    return []


if __name__ == "__main__":
    assert two_sum([2, 7, 11, 15], 9) == [0, 1]
    assert two_sum([3, 3], 6) == [0, 1]
    print("two_sum examples passed")
