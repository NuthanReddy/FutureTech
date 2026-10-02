# LRU Cache — LeetCode 146
# Pattern identification: recency eviction with O(1) lookup -> DataStructures.LRUCache;
# reuse its map/list implementation; one node per key stays in recency order,
# while this adapter only translates KeyError misses to -1.
#
# Problem:
#   Design a data structure that follows the constraints of a Least Recently
#   Used (LRU) cache.
#
#   Implement the following operations:
#     • get(key)        → Return the value if key exists, otherwise -1.
#     • put(key, value) → Update the value if key exists.  Otherwise, add the
#                         key-value pair.  If the number of keys exceeds the
#                         capacity, evict the least recently used key.
#
#   Both operations must run in O(1) average time.
#
# Approach:
#   Reuse the LRUCache from DataStructures/LRUCache.py which is already backed
#   by a doubly-linked list + hash map.  Wrap it in a thin adapter that returns
#   -1 on cache miss (matching the LeetCode interface).
#
# Complexity:
#   Time:  O(1) per get / put
#   Space: O(capacity)

import sys
import os
from typing import Any, List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.LRUCache import LRUCache


# 1. Output: get returns a stored value or -1; put evicts the least recently used key when full.
# 2. Structure: The reused cache maps keys to nodes with links to both neighbors, ordered by last use.
# 3. Constraints: Require positive capacity; average O(1) get/put and O(capacity) storage.
# 4. Choice: Fast lookup plus oldest-use eviction needs a dictionary and linked order, already supplied by LRUCache.
# Delegate get/put to that implementation and translate only its missing-key exception to -1.
# 5. Why it works: The underlying cache moves touched keys to the recent end and evicts the opposite end.
# The adapter changes only the miss result, not the one-node-per-key recency order.
class LRUCacheLeetCode:
    """LeetCode-style LRU Cache backed by DataStructures.LRUCache.

    Returns -1 on cache miss instead of raising ``KeyError``.

    Examples:
        >>> c = LRUCacheLeetCode(2)
        >>> c.put(1, 1); c.put(2, 2)
        >>> c.get(1)
        1
        >>> c.put(3, 3)  # evicts key 2
        >>> c.get(2)
        -1
    """

    def __init__(self, capacity: int) -> None:
        """Initialize with a positive *capacity*."""
        self._cache = LRUCache(capacity)

    def get(self, key: Any) -> Any:
        """Return the value for *key*, or -1 if not present.  O(1)."""
        try:
            return self._cache.get(key)
        except KeyError:
            return -1

    def put(self, key: Any, value: Any) -> None:
        """Insert or update *key*.  Evicts LRU entry when full.  O(1)."""
        self._cache.put(key, value)


def process_operations(
    capacity: int,
    ops: List[str],
    args: List[List[int]],
) -> List[Any]:
    """Simulate a LeetCode-style operation sequence.

    Args:
        capacity: Cache capacity.
        ops: List of operation names ("LRUCache", "put", "get").
        args: Corresponding arguments for each operation.

    Returns:
        A list of results (None for constructor/put, int for get).

    Examples:
        >>> process_operations(
        ...     2,
        ...     ["LRUCache", "put", "put", "get", "put", "get", "put", "get", "get", "get"],
        ...     [[2], [1, 1], [2, 2], [1], [3, 3], [2], [4, 4], [1], [3], [4]],
        ... )
        [None, None, None, 1, None, -1, None, -1, 3, 4]
    """
    cache: LRUCacheLeetCode | None = None
    results: List[Any] = []

    for op, arg in zip(ops, args):
        if op == "LRUCache":
            cache = LRUCacheLeetCode(arg[0])
            results.append(None)
        elif op == "put":
            assert cache is not None
            cache.put(arg[0], arg[1])
            results.append(None)
        elif op == "get":
            assert cache is not None
            results.append(cache.get(arg[0]))
        else:
            raise ValueError(f"unknown operation: {op!r}")

    return results


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _run_tests() -> None:
    """Verify correctness with several test scenarios."""

    # --- LeetCode example ---
    ops = ["LRUCache", "put", "put", "get", "put", "get", "put", "get", "get", "get"]
    args = [[2], [1, 1], [2, 2], [1], [3, 3], [2], [4, 4], [1], [3], [4]]
    expected = [None, None, None, 1, None, -1, None, -1, 3, 4]
    assert process_operations(2, ops, args) == expected, "LeetCode example failed"

    # --- Capacity 1: every put evicts the previous ---
    cache = LRUCacheLeetCode(1)
    cache.put(1, 10)
    assert cache.get(1) == 10
    cache.put(2, 20)          # evicts key 1
    assert cache.get(1) == -1
    assert cache.get(2) == 20

    # --- Update existing key (no eviction) ---
    cache = LRUCacheLeetCode(2)
    cache.put(1, 1)
    cache.put(2, 2)
    cache.put(1, 10)          # update, not insert → no eviction
    assert cache.get(1) == 10
    assert cache.get(2) == 2  # key 2 still present

    # --- get() refreshes recency ---
    cache = LRUCacheLeetCode(2)
    cache.put(1, 1)
    cache.put(2, 2)
    cache.get(1)              # touch key 1 → key 2 is now LRU
    cache.put(3, 3)           # evicts key 2
    assert cache.get(2) == -1
    assert cache.get(1) == 1
    assert cache.get(3) == 3

    # --- Larger capacity stress test ---
    cache = LRUCacheLeetCode(3)
    for i in range(10):
        cache.put(i, i * 100)
    # Only the last 3 keys (7, 8, 9) should survive.
    for i in range(7):
        assert cache.get(i) == -1, f"key {i} should have been evicted"
    for i in range(7, 10):
        assert cache.get(i) == i * 100, f"key {i} missing"

    # --- Edge: get on empty cache ---
    cache = LRUCacheLeetCode(5)
    assert cache.get(999) == -1

    print("All lru_cache_problem tests passed ✓")


if __name__ == "__main__":
    _run_tests()
