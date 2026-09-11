"""Compatibility import for the Arrays & Hashing Top K solution.

Problem statement:
    Given an integer list and ``k``, return the ``k`` most frequent values in
    any order.  Under the LeetCode contract, ``k`` is positive and does not
    exceed the distinct-value count.

The original location is retained for callers that already import from the
Heap group.  The implementation now lives with the frequency-bucket pattern
so the repository does not maintain two versions of the same problem.
"""

from Problems.ArraysHashing.Frequency.top_k_frequent_elements import (
    top_k_frequent,
)

__all__ = ["top_k_frequent"]


if __name__ == "__main__":
    result = top_k_frequent([1, 1, 1, 2, 2, 3], 2)
    assert set(result) == {1, 2}
    print(f"top_k_frequent example passed: {result}")
