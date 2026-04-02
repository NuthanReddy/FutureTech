# -----------------------------------------------------------------------
# Top K Frequent Elements
#
# Problem:
#   Given an integer array and an integer k, return the k most frequent
#   elements.  The answer may be returned in any order.
#
# Approach:
#   1. Count element frequencies with a dictionary.
#   2. Maintain a MinHeap of size k holding (frequency, element) tuples.
#      Because the MinHeap pops the smallest frequency first, once the
#      heap exceeds size k we pop, leaving the k largest frequencies.
#
# Time complexity:  O(N log k) — one heap push per unique element, each
#                   costing O(log k).
# Space complexity: O(N) for the frequency map, O(k) for the heap.
# -----------------------------------------------------------------------

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.Heap import MinHeap


def top_k_frequent(nums: list[int], k: int) -> list[int]:
    """Return the *k* most frequent elements in *nums*.

    Args:
        nums: List of integers (may contain duplicates).
        k:    Number of top-frequent elements to return (1 ≤ k ≤ distinct count).

    Returns:
        A list of *k* elements ordered by decreasing frequency.

    Examples:
        >>> sorted(top_k_frequent([1, 1, 1, 2, 2, 3], 2))
        [1, 2]
    """
    if not nums or k <= 0:
        return []

    # Step 1 — frequency count
    freq: dict[int, int] = {}
    for num in nums:
        freq[num] = freq.get(num, 0) + 1

    # Step 2 — maintain a min-heap of size k
    heap = MinHeap()
    for element, count in freq.items():
        heap.push((count, element))
        if len(heap) > k:
            heap.pop()  # discard the least frequent

    # Step 3 — extract results (most frequent last in a min-heap)
    result: list[int] = []
    while heap:
        count, element = heap.pop()
        result.append(element)

    # Reverse so highest frequency comes first
    result.reverse()
    return result


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------

if __name__ == "__main__":
    # Typical case
    result = top_k_frequent([1, 1, 1, 2, 2, 3], 2)
    assert set(result[:2]) == {1, 2}
    print(f"top_k_frequent([1,1,1,2,2,3], 2) = {result}")

    # k equals distinct count — return all
    result = top_k_frequent([1, 2, 3], 3)
    assert set(result) == {1, 2, 3}
    print(f"top_k_frequent([1,2,3], 3) = {result}")

    # Single element
    assert top_k_frequent([1], 1) == [1]
    print("Single element: OK")

    # All same element
    assert top_k_frequent([5, 5, 5, 5], 1) == [5]
    print("All same element: OK")

    # Empty input
    assert top_k_frequent([], 1) == []
    print("Empty input: OK")

    # k = 0
    assert top_k_frequent([1, 2, 3], 0) == []
    print("k=0: OK")

    # Negative numbers
    result = top_k_frequent([-1, -1, -2, -2, -2, 3], 1)
    assert result == [-2]
    print(f"Negative numbers: top = {result}")

    # Larger example
    nums = [4, 1, -1, 2, -1, 2, 3, 4, 4, 4]
    result = top_k_frequent(nums, 2)
    assert set(result[:2]) == {4, -1} or set(result[:2]) == {4, 2}
    # 4 appears 4 times; -1 and 2 each appear 2 times — either is valid for 2nd
    print(f"Larger example: top_k={result}")

    print("\nAll tests passed!")
