"""LeetCode 146: LRU Cache.

Problem statement:
    Input: a positive ``capacity``, followed by ``get(key)`` and
    ``put(key, value)`` operations.
    Output: ``get`` returns the stored value or ``-1`` for a missing key;
    ``put`` inserts or updates a key and evicts the least-recently-used entry
    when capacity is exceeded.
    Constraints: capacity is 1 through 3,000; at most 200,000 operations are
    performed; keys and values are conventionally in 0 through 10,000; each
    operation must run in O(1) average time.

A hash map locates entries in O(1), while a doubly linked list records
least-to-most-recent usage without linear scans.
"""

from __future__ import annotations

from typing import Optional


class _CacheNode:
    """Internal key/value node in the recency list."""

    __slots__ = ("key", "value", "previous", "next")

    def __init__(self, key: int = 0, value: int = 0) -> None:
        self.key = key
        self.value = value
        self.previous: Optional["_CacheNode"] = None
        self.next: Optional["_CacheNode"] = None


class LRUCache:
    """Fixed-capacity integer cache with O(1) ``get`` and ``put``."""

    def __init__(self, capacity: int):
        if capacity < 1:
            raise ValueError("capacity must be at least 1")

        self._capacity = capacity
        self._nodes: dict[int, _CacheNode] = {}
        self._least_recent = _CacheNode()
        self._most_recent = _CacheNode()
        self._least_recent.next = self._most_recent
        self._most_recent.previous = self._least_recent

    def get(self, key: int) -> int:
        """Return a value and refresh recency, or ``-1`` on a miss."""
        node = self._nodes.get(key)
        if node is None:
            return -1

        self._remove(node)
        self._append_most_recent(node)
        return node.value

    def put(self, key: int, value: int) -> None:
        """Insert or update an entry, evicting the LRU entry if needed."""
        existing = self._nodes.get(key)
        if existing is not None:
            existing.value = value
            self._remove(existing)
            self._append_most_recent(existing)
            return

        node = _CacheNode(key, value)
        self._nodes[key] = node
        self._append_most_recent(node)

        if len(self._nodes) > self._capacity:
            evicted = self._least_recent.next
            self._remove(evicted)  # type: ignore[arg-type]
            del self._nodes[evicted.key]  # type: ignore[union-attr]

    def __len__(self) -> int:
        return len(self._nodes)

    def keys_mru_to_lru(self) -> list[int]:
        """Expose recency order for demos and focused tests."""
        keys: list[int] = []
        current = self._most_recent.previous
        while current is not self._least_recent:
            keys.append(current.key)  # type: ignore[union-attr]
            current = current.previous  # type: ignore[union-attr]
        return keys

    def _remove(self, node: _CacheNode) -> None:
        previous = node.previous
        next_node = node.next
        if previous is None or next_node is None:
            raise RuntimeError("cache node is not linked")
        previous.next = next_node
        next_node.previous = previous
        node.previous = None
        node.next = None

    def _append_most_recent(self, node: _CacheNode) -> None:
        previous = self._most_recent.previous
        if previous is None:
            raise RuntimeError("cache sentinel is not linked")
        previous.next = node
        node.previous = previous
        node.next = self._most_recent
        self._most_recent.previous = node


if __name__ == "__main__":
    cache = LRUCache(2)
    cache.put(1, 1)
    cache.put(2, 2)
    assert cache.get(1) == 1
    cache.put(3, 3)
    assert cache.get(2) == -1
    print("LRU Cache smoke test passed.")
