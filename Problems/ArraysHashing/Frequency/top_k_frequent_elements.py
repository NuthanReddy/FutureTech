"""Top K Frequent Elements - frequency buckets.

Problem statement:
    Given an integer list ``nums`` and integer ``k``, return the ``k`` values
    that occur most frequently, in any order.  The LeetCode contract has
    ``1 <= k <=`` the number of distinct values and guarantees the answer is
    unique.  This standalone helper additionally returns an empty list for
    empty input or non-positive ``k`` and all distinct values when ``k`` is
    oversized.
"""

from __future__ import annotations

from collections import Counter


def top_k_frequent(nums: list[int], k: int) -> list[int]:
    """Return up to *k* values with the largest frequencies.

    ``buckets[count]`` contains values seen exactly ``count`` times.  Since a
    list of length ``len(nums) + 1`` covers every possible count, scanning
    buckets from high to low avoids sorting all distinct values.

    The scan stops after collecting ``k`` values.  The input contract
    normally guarantees ``1 <= k <= number of distinct values``; accepting
    ``k <= 0`` and oversized ``k`` makes this helper safe and convenient
    without changing valid-case behavior.

    The earlier ``Problems/Heap/top_k_frequent_elements.py`` implementation
    was a genuine overlap, but it coupled this problem to the repository's
    custom heap and used ``O(u log k)`` time.  This canonical version is
    bucket-based, so the old module re-exports it rather than maintaining two
    algorithms.

    Complexity:
        Time ``O(n + u)`` and space ``O(n + u)``, where ``u`` is the number of
        distinct values.
    """
    if k <= 0 or not nums:
        return []

    frequencies = Counter(nums)
    buckets: list[list[int]] = [[] for _ in range(len(nums) + 1)]
    for number, count in frequencies.items():
        buckets[count].append(number)

    result: list[int] = []
    for count in range(len(nums), 0, -1):
        for number in buckets[count]:
            result.append(number)
            if len(result) == min(k, len(frequencies)):
                return result
    return result


if __name__ == "__main__":
    assert set(top_k_frequent([1, 1, 1, 2, 2, 3], 2)) == {1, 2}
    assert top_k_frequent([], 1) == []
    print("top_k_frequent examples passed")
