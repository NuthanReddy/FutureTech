"""Skip List implementation.

A probabilistic data structure that allows expected O(log n) search, insert,
and delete within an ordered sequence of elements.  It achieves this by
maintaining multiple layers of linked lists where each higher layer acts as
an "express lane" for the layers below.

Reference: Pugh, W. (1990). "Skip lists: a probabilistic alternative to
balanced trees."
"""

from __future__ import annotations

import random
from typing import Any, Generator, Optional


class _Node:
    """Internal node used by SkipList.

    Attributes:
        key: The sort key (None for the header sentinel).
        value: The associated value.
        forward: List of forward pointers, one per level.
    """

    __slots__ = ("key", "value", "forward")

    def __init__(self, key: Any, value: Any, level: int) -> None:
        self.key = key
        self.value = value
        self.forward: list[Optional[_Node]] = [None] * (level + 1)

    def __repr__(self) -> str:
        return f"_Node({self.key!r}, levels={len(self.forward)})"


class SkipList:
    """Skip List with expected O(log n) search, insert, and delete.

    Args:
        max_level: Maximum number of levels (0-indexed). A value of 16
            supports up to ~2^16 elements efficiently.
        p: Probability used when generating random levels. Default 0.5.

    Example:
        >>> sl = SkipList()
        >>> for k in [3, 1, 4, 1, 5]:
        ...     sl.insert(k, k * 10)
        >>> 4 in sl
        True
        >>> sl.search(3)
        30
        >>> len(sl)
        4
    """

    def __init__(self, max_level: int = 16, p: float = 0.5) -> None:
        self._max_level = max_level
        self._p = p
        self._level = 0  # Current highest level in use.
        # Header sentinel — key is None, never compared.
        self._header = _Node(key=None, value=None, level=max_level)
        self._size = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _random_level(self) -> int:
        """Generate a random level for a new node.  Expected O(1)."""
        lvl = 0
        while random.random() < self._p and lvl < self._max_level:
            lvl += 1
        return lvl

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def search(self, key: Any) -> Optional[Any]:
        """Return the value associated with *key*, or None.  Expected O(log n)."""
        current = self._header
        for i in range(self._level, -1, -1):
            while current.forward[i] is not None and current.forward[i].key < key:
                current = current.forward[i]
        current = current.forward[0]
        if current is not None and current.key == key:
            return current.value
        return None

    def insert(self, key: Any, value: Any = None) -> None:
        """Insert a key-value pair.  Expected O(log n).

        If the key already exists its value is updated.
        """
        update: list[Optional[_Node]] = [None] * (self._max_level + 1)
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] is not None and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        current = current.forward[0]

        if current is not None and current.key == key:
            # Key already present — update value.
            current.value = value
            return

        new_level = self._random_level()

        if new_level > self._level:
            for i in range(self._level + 1, new_level + 1):
                update[i] = self._header
            self._level = new_level

        new_node = _Node(key=key, value=value, level=new_level)

        for i in range(new_level + 1):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node

        self._size += 1

    def delete(self, key: Any) -> bool:
        """Delete the node with the given *key*.  Expected O(log n).

        Returns True if the key was found and removed, False otherwise.
        """
        update: list[Optional[_Node]] = [None] * (self._max_level + 1)
        current = self._header

        for i in range(self._level, -1, -1):
            while current.forward[i] is not None and current.forward[i].key < key:
                current = current.forward[i]
            update[i] = current

        target = current.forward[0]

        if target is None or target.key != key:
            return False

        for i in range(self._level + 1):
            if update[i].forward[i] is not target:
                break
            update[i].forward[i] = target.forward[i]

        # Lower the list level if the top layers are now empty.
        while self._level > 0 and self._header.forward[self._level] is None:
            self._level -= 1

        self._size -= 1
        return True

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self._size

    def __contains__(self, key: Any) -> bool:
        return self.search(key) is not None

    def __iter__(self) -> Generator[tuple[Any, Any], None, None]:
        """Iterate over (key, value) pairs in sorted order.  O(n)."""
        node = self._header.forward[0]
        while node is not None:
            yield (node.key, node.value)
            node = node.forward[0]

    def __repr__(self) -> str:
        items = ", ".join(f"{k!r}: {v!r}" for k, v in self)
        return f"SkipList({{{items}}})"


# ------------------------------------------------------------------
# Demo
# ------------------------------------------------------------------

if __name__ == "__main__":
    sl = SkipList()

    print("=== Skip List Demo ===\n")

    # Insert keys
    keys = [3, 6, 7, 9, 12, 19, 17, 26, 21, 25]
    print(f"Inserting keys: {keys}")
    for k in keys:
        sl.insert(k, k * 10)

    print(f"Size      : {len(sl)}")
    print(f"Sorted    : {list(sl)}")
    print(f"Search 19 : {sl.search(19)}")
    print(f"12 in list: {12 in sl}")
    print(f"99 in list: {99 in sl}")
    print(f"repr      : {sl!r}")

    # Delete a few keys
    for k in [3, 19, 25]:
        removed = sl.delete(k)
        print(f"\nDeleted {k}: {removed}")
        print(f"  size={len(sl)}, sorted={list(sl)}")

    print(f"\nDelete non-existent 999: {sl.delete(999)}")
    print(f"Final: {sl!r}")

    # Demonstrate duplicate key update
    sl.insert(7, 777)
    print(f"\nAfter insert(7, 777), search(7) = {sl.search(7)}")
    print(f"Size unchanged: {len(sl)}")
