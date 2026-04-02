# LFU Cache — LeetCode 460
#
# Problem:
#   Design and implement a data structure for a Least Frequently Used (LFU)
#   cache.
#
#   Implement the following operations:
#     • get(key)        → Return the value if key exists, otherwise -1.
#     • put(key, value) → Insert or update.  When at capacity, evict the least
#                         frequently used key.  Ties are broken by evicting the
#                         least recently used among the least frequent.
#
#   Both operations must run in O(1) time.
#
# Approach:
#   Inspired by the doubly-linked-list + hashmap pattern from
#   DataStructures/LRUCache.py, we extend it to track frequencies:
#
#     1. A hash map  key → Node  for O(1) key lookup.
#     2. A hash map  freq → DoublyLinkedList  so each frequency level has its
#        own recency list (most recent at front, LRU at tail).
#     3. A variable *min_freq* tracking the current minimum frequency.
#
#   On get(key):
#     - Look up the node, increase its frequency, move it to the new freq list.
#     - If the old freq list is now empty and was min_freq, bump min_freq.
#
#   On put(key, value):
#     - If key exists, update + touch (same as get logic).
#     - Otherwise, if at capacity, evict the LRU node from freq_map[min_freq].
#     - Insert a new node with freq=1 and set min_freq=1.
#
# Complexity:
#   Time:  O(1) per get / put
#   Space: O(capacity)

from __future__ import annotations

import sys
import os
from typing import Any, Dict, Optional

# Import to demonstrate awareness of the existing LRU pattern.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from DataStructures.LRUCache import LRUCache as _LRUCacheRef  # noqa: F401


# ---------------------------------------------------------------------------
# Internal node & doubly-linked list (same pattern as DataStructures/LRUCache)
# ---------------------------------------------------------------------------

class _Node:
    """Doubly-linked-list node with key, value, and access frequency."""

    __slots__ = ("key", "value", "freq", "prev", "next")

    def __init__(self, key: Any, value: Any) -> None:
        self.key = key
        self.value = value
        self.freq: int = 1
        self.prev: Optional[_Node] = None
        self.next: Optional[_Node] = None


class _DoublyLinkedList:
    """Sentinel-based doubly-linked list (most recent at head, LRU at tail)."""

    def __init__(self) -> None:
        self.head = _Node(None, None)
        self.tail = _Node(None, None)
        self.head.next = self.tail
        self.tail.prev = self.head
        self._size = 0

    def __len__(self) -> int:
        return self._size

    def add_to_front(self, node: _Node) -> None:
        """Insert *node* right after the head sentinel."""
        nxt = self.head.next
        self.head.next = node
        node.prev = self.head
        node.next = nxt
        nxt.prev = node
        self._size += 1

    def remove(self, node: _Node) -> None:
        """Detach *node* from the list."""
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = None
        node.next = None
        self._size -= 1

    def pop_lru(self) -> Optional[_Node]:
        """Remove and return the tail (LRU) node, or None if empty."""
        if self._size == 0:
            return None
        node = self.tail.prev
        self.remove(node)
        return node


# ---------------------------------------------------------------------------
# LFU Cache
# ---------------------------------------------------------------------------

class LFUCache:
    """Least-Frequently-Used cache with O(1) get and put.

    Uses the same doubly-linked-list + hashmap approach as
    ``DataStructures.LRUCache``, extended with per-frequency lists.

    Examples:
        >>> c = LFUCache(2)
        >>> c.put(1, 1); c.put(2, 2)
        >>> c.get(1)
        1
        >>> c.put(3, 3)   # evicts key 2 (freq 1, LRU)
        >>> c.get(2)
        -1
        >>> c.get(3)
        3
    """

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._min_freq: int = 0
        self._key_map: Dict[Any, _Node] = {}
        self._freq_map: Dict[int, _DoublyLinkedList] = {}

    # ----- public API -----

    def get(self, key: Any) -> Any:
        """Return value for *key* or -1 if absent.  O(1)."""
        if key not in self._key_map:
            return -1
        node = self._key_map[key]
        self._touch(node)
        return node.value

    def put(self, key: Any, value: Any) -> None:
        """Insert or update *key*.  Evicts LFU (then LRU) when full.  O(1)."""
        if self._capacity <= 0:
            return

        if key in self._key_map:
            node = self._key_map[key]
            node.value = value
            self._touch(node)
            return

        if len(self._key_map) >= self._capacity:
            self._evict()

        new_node = _Node(key, value)
        self._key_map[key] = new_node
        self._freq_map.setdefault(1, _DoublyLinkedList()).add_to_front(new_node)
        self._min_freq = 1

    # ----- internals -----

    def _touch(self, node: _Node) -> None:
        """Increase *node*'s frequency and migrate it to the new freq list."""
        old_freq = node.freq
        old_list = self._freq_map[old_freq]
        old_list.remove(node)

        if len(old_list) == 0 and old_freq == self._min_freq:
            self._min_freq += 1

        node.freq += 1
        self._freq_map.setdefault(node.freq, _DoublyLinkedList()).add_to_front(node)

    def _evict(self) -> None:
        """Remove the LFU (ties: LRU) entry."""
        dll = self._freq_map.get(self._min_freq)
        if dll is None:
            return
        victim = dll.pop_lru()
        if victim is not None:
            del self._key_map[victim.key]


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _run_tests() -> None:
    """Verify correctness with several test scenarios."""

    # --- LeetCode example 1 ---
    cache = LFUCache(2)
    cache.put(1, 1)
    cache.put(2, 2)
    assert cache.get(1) == 1        # freq(1)=2
    cache.put(3, 3)                  # evicts key 2 (freq 1, LRU)
    assert cache.get(2) == -1
    assert cache.get(3) == 3        # freq(3)=2
    cache.put(4, 4)                  # evicts key 1 (freq 2) vs key 3 (freq 2) → tie → LRU is key 1
    assert cache.get(1) == -1
    assert cache.get(3) == 3
    assert cache.get(4) == 4

    # --- Capacity 0 (degenerate) ---
    cache = LFUCache(0)
    cache.put(0, 0)
    assert cache.get(0) == -1

    # --- Capacity 1 ---
    cache = LFUCache(1)
    cache.put(1, 10)
    assert cache.get(1) == 10
    cache.put(2, 20)                 # evicts key 1
    assert cache.get(1) == -1
    assert cache.get(2) == 20

    # --- Update existing key (should not evict) ---
    cache = LFUCache(2)
    cache.put(1, 1)
    cache.put(2, 2)
    cache.put(1, 10)                 # update key 1 → freq goes up
    cache.put(3, 3)                  # evicts key 2 (lowest freq)
    assert cache.get(1) == 10
    assert cache.get(2) == -1
    assert cache.get(3) == 3

    # --- Frequency ordering matters ---
    cache = LFUCache(3)
    cache.put(1, 1)
    cache.put(2, 2)
    cache.put(3, 3)
    cache.get(1)                     # freq: 1→2
    cache.get(1)                     # freq: 1→3
    cache.get(2)                     # freq: 2→2
    # freqs: 1→3, 2→2, 3→1
    cache.put(4, 4)                  # evicts key 3 (lowest freq=1)
    assert cache.get(3) == -1
    assert cache.get(4) == 4
    assert cache.get(1) == 1
    assert cache.get(2) == 2

    # --- Tie-breaking by LRU within same frequency ---
    cache = LFUCache(2)
    cache.put(1, 1)                  # freq 1
    cache.put(2, 2)                  # freq 1
    # Both have freq=1.  Key 1 was inserted first → it's LRU among freq=1.
    cache.put(3, 3)                  # evicts key 1
    assert cache.get(1) == -1
    assert cache.get(2) == 2
    assert cache.get(3) == 3

    # --- Stress: sequential insertions ---
    cache = LFUCache(3)
    for i in range(20):
        cache.put(i, i * 10)
    # Only last 3 keys should survive.
    for i in range(17):
        assert cache.get(i) == -1
    for i in range(17, 20):
        assert cache.get(i) == i * 10

    print("All lfu_cache tests passed ✓")


if __name__ == "__main__":
    _run_tests()
