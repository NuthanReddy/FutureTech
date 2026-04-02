from __future__ import annotations

from typing import Any, Optional, List, Tuple, Iterator


class _BPlusLeaf:
    """Leaf node in a B+ Tree. Stores key-value pairs and a forward pointer."""

    def __init__(self) -> None:
        self.keys: List[Any] = []
        self.values: List[Any] = []
        self.next_leaf: Optional[_BPlusLeaf] = None

    def __repr__(self) -> str:
        return f"_BPlusLeaf(keys={self.keys})"


class _BPlusInternal:
    """Internal (routing) node in a B+ Tree. Stores only keys."""

    def __init__(self) -> None:
        self.keys: List[Any] = []
        self.children: List[_BPlusInternal | _BPlusLeaf] = []

    def __repr__(self) -> str:
        return f"_BPlusInternal(keys={self.keys})"


class BPlusTree:
    """B+ Tree — a B-Tree variant optimised for range queries.

    Internal nodes store only routing keys; all data resides in the leaf
    level.  Leaves are linked left-to-right for efficient sequential scans.

    *order* is the maximum number of children an internal node may have
    (equivalently, a leaf may hold at most ``order - 1`` key-value pairs).

    Complexity (where *n* is the number of stored keys):
      - Search:       O(log_order(n))
      - Insert:       O(order · log_order(n))
      - Delete:       O(order · log_order(n))
      - Range query:  O(log_order(n) + k)  where *k* is the result size
    """

    def __init__(self, order: int = 4) -> None:
        if order < 3:
            raise ValueError("order must be >= 3")
        self._order: int = order
        self._root: _BPlusLeaf | _BPlusInternal = _BPlusLeaf()
        self._size: int = 0
        self._head: _BPlusLeaf = self._root  # leftmost leaf

    # ---- public API ----

    def search(self, key: Any) -> Optional[Any]:
        """Return the value mapped to *key*, or ``None`` if absent.

        Time: O(log_order(n))
        """
        leaf = self._find_leaf(key)
        for i, k in enumerate(leaf.keys):
            if k == key:
                return leaf.values[i]
        return None

    def insert(self, key: Any, value: Any = None) -> None:
        """Insert *key* (with optional *value*). Updates value if key exists.

        Time: O(order · log_order(n))
        """
        leaf = self._find_leaf(key)

        # Update in-place if key already present.
        for i, k in enumerate(leaf.keys):
            if k == key:
                leaf.values[i] = value
                return

        # Insert into the leaf in sorted position.
        idx = self._bisect(leaf.keys, key)
        leaf.keys.insert(idx, key)
        leaf.values.insert(idx, value)
        self._size += 1

        # Split if overflow.
        if len(leaf.keys) >= self._order:
            self._split_leaf(leaf)

    def delete(self, key: Any) -> bool:
        """Remove *key* from the tree. Return ``True`` if found.

        Time: O(order · log_order(n))
        """
        leaf = self._find_leaf(key)
        for i, k in enumerate(leaf.keys):
            if k == key:
                leaf.keys.pop(i)
                leaf.values.pop(i)
                self._size -= 1
                self._fix_after_delete(leaf)
                return True
        return False

    def range_query(self, low: Any, high: Any) -> List[Tuple[Any, Any]]:
        """Return all (key, value) pairs with ``low <= key <= high``.

        Time: O(log_order(n) + k) where *k* is the number of results.
        """
        result: List[Tuple[Any, Any]] = []
        leaf = self._find_leaf(low)
        while leaf is not None:
            for i, k in enumerate(leaf.keys):
                if k > high:
                    return result
                if k >= low:
                    result.append((k, leaf.values[i]))
            leaf = leaf.next_leaf
        return result

    # ---- dunder helpers ----

    def __len__(self) -> int:
        return self._size

    def __contains__(self, key: Any) -> bool:
        return self.search(key) is not None

    def __iter__(self) -> Iterator[Tuple[Any, Any]]:
        """Iterate over all (key, value) pairs in sorted order."""
        leaf = self._head
        while leaf is not None:
            for i, k in enumerate(leaf.keys):
                yield k, leaf.values[i]
            leaf = leaf.next_leaf

    def __repr__(self) -> str:
        keys = [k for k, _ in self]
        return f"BPlusTree(order={self._order}, size={self._size}, keys={keys})"

    # ---- internal helpers ----

    def _find_leaf(self, key: Any) -> _BPlusLeaf:
        """Walk from root to the leaf that should contain *key*."""
        node = self._root
        while isinstance(node, _BPlusInternal):
            # Use upper-bound so that keys equal to a routing key go right.
            i = self._bisect_right(node.keys, key)
            node = node.children[i]
        return node

    @staticmethod
    def _bisect(keys: List[Any], key: Any) -> int:
        """Return the leftmost index where *key* could be inserted (lower bound)."""
        lo, hi = 0, len(keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if keys[mid] < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    @staticmethod
    def _bisect_right(keys: List[Any], key: Any) -> int:
        """Return the rightmost index where *key* could be inserted (upper bound)."""
        lo, hi = 0, len(keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if keys[mid] <= key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    # ---- split helpers ----

    def _split_leaf(self, leaf: _BPlusLeaf) -> None:
        """Split an overflowing leaf and propagate upward if needed."""
        mid = len(leaf.keys) // 2
        new_leaf = _BPlusLeaf()
        new_leaf.keys = leaf.keys[mid:]
        new_leaf.values = leaf.values[mid:]
        new_leaf.next_leaf = leaf.next_leaf
        leaf.keys = leaf.keys[:mid]
        leaf.values = leaf.values[:mid]
        leaf.next_leaf = new_leaf

        promote_key = new_leaf.keys[0]
        self._insert_into_parent(leaf, promote_key, new_leaf)

    def _split_internal(self, node: _BPlusInternal) -> None:
        """Split an overflowing internal node and propagate upward."""
        mid = len(node.keys) // 2
        promote_key = node.keys[mid]

        new_node = _BPlusInternal()
        new_node.keys = node.keys[mid + 1:]
        new_node.children = node.children[mid + 1:]

        node.keys = node.keys[:mid]
        node.children = node.children[: mid + 1]

        self._insert_into_parent(node, promote_key, new_node)

    def _insert_into_parent(
        self,
        left: _BPlusLeaf | _BPlusInternal,
        key: Any,
        right: _BPlusLeaf | _BPlusInternal,
    ) -> None:
        """Insert *key* as a separator between *left* and *right* in the parent."""
        if left is self._root:
            new_root = _BPlusInternal()
            new_root.keys = [key]
            new_root.children = [left, right]
            self._root = new_root
            return

        parent = self._find_parent(self._root, left)
        idx = self._bisect(parent.keys, key)
        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, right)

        if len(parent.keys) >= self._order:
            self._split_internal(parent)

    def _find_parent(
        self, current: _BPlusInternal | _BPlusLeaf, child: _BPlusInternal | _BPlusLeaf
    ) -> Optional[_BPlusInternal]:
        """Return the parent internal node of *child*."""
        if isinstance(current, _BPlusLeaf):
            return None
        for c in current.children:
            if c is child:
                return current
            result = self._find_parent(c, child)
            if result is not None:
                return result
        return None

    # ---- deletion / rebalancing helpers ----

    def _fix_after_delete(self, leaf: _BPlusLeaf) -> None:
        """Rebalance after a deletion from *leaf*."""
        if leaf is self._root:
            return
        min_keys = (self._order - 1) // 2
        if len(leaf.keys) >= min_keys:
            # Update routing keys — the leaf min may have changed.
            self._update_routing_key(leaf)
            return
        parent = self._find_parent(self._root, leaf)
        if parent is None:
            return
        idx = parent.children.index(leaf)

        # Try borrowing from left sibling.
        if idx > 0:
            left_sib = parent.children[idx - 1]
            if isinstance(left_sib, _BPlusLeaf) and len(left_sib.keys) > min_keys:
                leaf.keys.insert(0, left_sib.keys.pop())
                leaf.values.insert(0, left_sib.values.pop())
                parent.keys[idx - 1] = leaf.keys[0]
                return

        # Try borrowing from right sibling.
        if idx < len(parent.children) - 1:
            right_sib = parent.children[idx + 1]
            if isinstance(right_sib, _BPlusLeaf) and len(right_sib.keys) > min_keys:
                leaf.keys.append(right_sib.keys.pop(0))
                leaf.values.append(right_sib.values.pop(0))
                parent.keys[idx] = right_sib.keys[0]
                return

        # Merge with a sibling.
        if idx > 0:
            self._merge_leaves(parent, idx - 1)
        else:
            self._merge_leaves(parent, idx)

        # Propagate fix upward if parent underflows.
        self._fix_internal(parent)

    def _merge_leaves(self, parent: _BPlusInternal, idx: int) -> None:
        """Merge leaf at *idx* with leaf at *idx + 1* under *parent*."""
        left: _BPlusLeaf = parent.children[idx]
        right: _BPlusLeaf = parent.children[idx + 1]
        left.keys.extend(right.keys)
        left.values.extend(right.values)
        left.next_leaf = right.next_leaf
        parent.keys.pop(idx)
        parent.children.pop(idx + 1)

    def _fix_internal(self, node: _BPlusInternal) -> None:
        """Rebalance an internal node that may have underflowed."""
        if node is self._root:
            if len(node.keys) == 0 and len(node.children) == 1:
                self._root = node.children[0]
                # Refresh _head when the root becomes a leaf.
                self._refresh_head()
            return

        min_keys = (self._order - 1) // 2
        if len(node.keys) >= min_keys:
            return

        parent = self._find_parent(self._root, node)
        if parent is None:
            return
        idx = parent.children.index(node)

        # Borrow from left internal sibling.
        if idx > 0:
            left_sib = parent.children[idx - 1]
            if isinstance(left_sib, _BPlusInternal) and len(left_sib.keys) > min_keys:
                node.keys.insert(0, parent.keys[idx - 1])
                node.children.insert(0, left_sib.children.pop())
                parent.keys[idx - 1] = left_sib.keys.pop()
                return

        # Borrow from right internal sibling.
        if idx < len(parent.children) - 1:
            right_sib = parent.children[idx + 1]
            if isinstance(right_sib, _BPlusInternal) and len(right_sib.keys) > min_keys:
                node.keys.append(parent.keys[idx])
                node.children.append(right_sib.children.pop(0))
                parent.keys[idx] = right_sib.keys.pop(0)
                return

        # Merge with a sibling.
        if idx > 0:
            self._merge_internals(parent, idx - 1)
        else:
            self._merge_internals(parent, idx)

        self._fix_internal(parent)

    def _merge_internals(self, parent: _BPlusInternal, idx: int) -> None:
        """Merge internal nodes at *idx* and *idx + 1* under *parent*."""
        left: _BPlusInternal = parent.children[idx]
        right: _BPlusInternal = parent.children[idx + 1]
        left.keys.append(parent.keys.pop(idx))
        left.keys.extend(right.keys)
        left.children.extend(right.children)
        parent.children.pop(idx + 1)

    def _update_routing_key(self, leaf: _BPlusLeaf) -> None:
        """After leaf keys change, update the first routing key that references it."""
        parent = self._find_parent(self._root, leaf)
        if parent is None:
            return
        idx = parent.children.index(leaf)
        if idx > 0 and len(leaf.keys) > 0:
            parent.keys[idx - 1] = leaf.keys[0]

    def _refresh_head(self) -> None:
        """Walk to the leftmost leaf and update ``_head``."""
        node = self._root
        while isinstance(node, _BPlusInternal):
            node = node.children[0]
        self._head = node


if __name__ == "__main__":
    bpt = BPlusTree(order=4)
    for v in [10, 20, 5, 6, 12, 30, 7, 17, 3, 8]:
        bpt.insert(v, f"val_{v}")

    print(bpt)
    print(f"len = {len(bpt)}")
    print(f"search 12 = {bpt.search(12)}")
    print(f"7 in bpt = {7 in bpt}")
    print(f"99 in bpt = {99 in bpt}")

    print(f"range [5, 12] = {bpt.range_query(5, 12)}")
    print(f"all pairs: {list(bpt)}")

    bpt.delete(6)
    bpt.delete(30)
    print(f"after deleting 6, 30: {bpt}")
