"""LRU (Least Recently Used) Cache.

All ``get``, ``put``, and ``delete`` operations run in O(1) time using a
doubly linked list for recency ordering and a hash map for fast key lookup.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class _Node:
    """Internal doubly-linked-list node storing a key/value pair."""

    __slots__ = ("key", "value", "prev", "next")

    def __init__(self, key: Any, value: Any) -> None:
        self.key = key
        self.value = value
        self.prev: Optional[_Node] = None
        self.next: Optional[_Node] = None


class LRUCache:
    """Least-Recently-Used cache with configurable capacity.

    Internally uses sentinel head/tail nodes so edge-case list manipulation
    is avoided.

    Args:
        capacity: Maximum number of key/value pairs to store. Must be >= 1.
    """

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self._capacity = capacity
        self._map: Dict[Any, _Node] = {}

        # Sentinel nodes — never hold real data.
        self._head = _Node(None, None)
        self._tail = _Node(None, None)
        self._head.next = self._tail
        self._tail.prev = self._head

    # ---- public API ----

    def get(self, key: Any) -> Any:
        """Return the value for *key* and mark it as most-recently used.  O(1).

        Raises ``KeyError`` if the key is not present.
        """
        if key not in self._map:
            raise KeyError(key)
        node = self._map[key]
        self._move_to_front(node)
        return node.value

    def put(self, key: Any, value: Any) -> None:
        """Insert or update *key* with *value*.  O(1).

        If the cache is at capacity, the least-recently-used entry is evicted.
        """
        if key in self._map:
            node = self._map[key]
            node.value = value
            self._move_to_front(node)
            return

        if len(self._map) >= self._capacity:
            self._evict()

        node = _Node(key, value)
        self._map[key] = node
        self._add_to_front(node)

    def delete(self, key: Any) -> Any:
        """Remove *key* and return its value.  O(1).

        Raises ``KeyError`` if the key is not present.
        """
        if key not in self._map:
            raise KeyError(key)
        node = self._map.pop(key)
        self._detach(node)
        return node.value

    # ---- dunder helpers ----

    def __len__(self) -> int:
        return len(self._map)

    def __contains__(self, key: Any) -> bool:
        return key in self._map

    def __repr__(self) -> str:
        items = []
        node = self._head.next
        while node is not self._tail:
            items.append(f"{node.key!r}: {node.value!r}")
            node = node.next
        return f"LRUCache([{', '.join(items)}], capacity={self._capacity})"

    # ---- internal linked-list helpers (all O(1)) ----

    def _add_to_front(self, node: _Node) -> None:
        """Insert *node* right after the head sentinel (most-recent)."""
        nxt = self._head.next
        self._head.next = node
        node.prev = self._head
        node.next = nxt
        nxt.prev = node

    def _detach(self, node: _Node) -> None:
        """Remove *node* from the list without deallocating it."""
        node.prev.next = node.next
        node.next.prev = node.prev

    def _move_to_front(self, node: _Node) -> None:
        """Detach *node* and re-insert it at the front (most-recent)."""
        self._detach(node)
        self._add_to_front(node)

    def _evict(self) -> None:
        """Remove the least-recently-used node (just before the tail sentinel)."""
        lru = self._tail.prev
        if lru is self._head:
            return  # nothing to evict
        self._detach(lru)
        del self._map[lru.key]


if __name__ == "__main__":
    cache = LRUCache(capacity=3)

    cache.put("a", 1)
    cache.put("b", 2)
    cache.put("c", 3)
    print(cache)  # a, b, c present — c is most recent

    print(f"get('a') = {cache.get('a')}")  # 1 — 'a' becomes most recent
    print(cache)

    cache.put("d", 4)  # evicts 'b' (least recently used)
    print(f"after put('d', 4): {cache}")
    print(f"'b' in cache: {'b' in cache}")  # False

    cache.put("a", 99)  # update existing key
    print(f"get('a') = {cache.get('a')}")  # 99

    print(f"delete('c') = {cache.delete('c')}")
    print(f"len = {len(cache)}")
    print(cache)
