# Pattern identification: rank values by bounded counts -> canonical frequency buckets;
# reuse ArraysHashing.Frequency.top_k_frequent_elements, not a second heap solution;
# bucket c holds exactly frequency-c values, emitted in descending frequency order.
"""Compatibility import for the Arrays & Hashing Top K solution.

Problem statement:
    Given an integer list and ``k``, return the ``k`` most frequent values in
    any order.  Under the LeetCode contract, ``k`` is positive and does not
    exceed the distinct-value count.

The original location is retained for callers that already import from the
Heap group.  The implementation now lives with the frequency-bucket pattern
so the repository does not maintain two versions of the same problem.
"""

# 1. Output: The re-exported function returns up to k distinct values with the highest occurrence counts.
# 2. Structure: Unsorted integers may repeat; occurrence counts cannot exceed the input length n.
# 3. Constraints: The helper returns [] for empty/non-positive k and all values for oversized k; O(n + u) time/space.
# 4. Choice: Counts fit indices 0..n, so reuse the helper's count groups instead of sorting or maintaining a heap.
# Count u distinct values, place each in its count's group, and scan largest counts first until enough are selected.
# 5. Why it works: Each count group contains exactly that frequency, so earlier selected groups outrank later ones.
# This legacy module only re-exports the function; it does not implement a separate heap algorithm.
from Problems.ArraysHashing.Frequency.top_k_frequent_elements import (
    top_k_frequent,
)

__all__ = ["top_k_frequent"]


if __name__ == "__main__":
    result = top_k_frequent([1, 1, 1, 2, 2, 3], 2)
    assert set(result) == {1, 2}
    print(f"top_k_frequent example passed: {result}")
