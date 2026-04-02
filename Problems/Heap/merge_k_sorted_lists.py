# -----------------------------------------------------------------------
# Merge K Sorted Lists
#
# Problem:
#   Given k sorted lists, merge them into one sorted list.
#
# Approach:
#   Use a MinHeap to always extract the smallest element across all lists.
#   Push (value, list_index, element_index) tuples so the heap orders by
#   value first.  After popping the minimum, advance in that list and push
#   the next element.
#
# Time complexity:  O(N log k)  — each of the N elements is pushed/popped
#                   once, and heap operations cost O(log k).
# Space complexity: O(k) — the heap holds at most k elements at any time.
# -----------------------------------------------------------------------

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.Heap import MinHeap


def merge_k_sorted_lists(lists: list[list[int]]) -> list[int]:
    """Merge *k* sorted lists into a single sorted list.

    Args:
        lists: A list of individually sorted (ascending) integer lists.

    Returns:
        A single sorted list containing all elements.

    Examples:
        >>> merge_k_sorted_lists([[1, 4, 5], [1, 3, 4], [2, 6]])
        [1, 1, 2, 3, 4, 4, 5, 6]
    """
    heap = MinHeap()
    result: list[int] = []

    # Seed the heap with the first element of each non-empty list.
    for list_idx, lst in enumerate(lists):
        if lst:
            # (value, list_index, element_index)
            heap.push((lst[0], list_idx, 0))

    while heap:
        value, list_idx, elem_idx = heap.pop()
        result.append(value)

        next_idx = elem_idx + 1
        if next_idx < len(lists[list_idx]):
            heap.push((lists[list_idx][next_idx], list_idx, next_idx))

    return result


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------

if __name__ == "__main__":
    # Typical case
    lists = [[1, 4, 5], [1, 3, 4], [2, 6]]
    assert merge_k_sorted_lists(lists) == [1, 1, 2, 3, 4, 4, 5, 6]
    print(f"merge({lists}) = {merge_k_sorted_lists(lists)}")

    # Single list
    assert merge_k_sorted_lists([[1, 2, 3]]) == [1, 2, 3]
    print("Single list: OK")

    # Empty input
    assert merge_k_sorted_lists([]) == []
    print("Empty input: OK")

    # All empty lists
    assert merge_k_sorted_lists([[], [], []]) == []
    print("All empty lists: OK")

    # Mix of empty and non-empty
    assert merge_k_sorted_lists([[], [1], [], [0, 2]]) == [0, 1, 2]
    print("Mix of empty and non-empty: OK")

    # Lists with negative numbers
    assert merge_k_sorted_lists([[-5, -1, 3], [-2, 0, 4]]) == [-5, -2, -1, 0, 3, 4]
    print("Negative numbers: OK")

    # Lists with duplicates
    assert merge_k_sorted_lists([[1, 1], [1, 1]]) == [1, 1, 1, 1]
    print("Duplicates: OK")

    # Single element lists
    assert merge_k_sorted_lists([[5], [3], [8], [1]]) == [1, 3, 5, 8]
    print("Single element lists: OK")

    print("\nAll tests passed!")
