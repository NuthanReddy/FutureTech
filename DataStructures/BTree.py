from __future__ import annotations

from typing import Any, Optional, List, Tuple


class _BTreeNode:
    """Internal node for a B-Tree.

    Each node holds up to ``2t - 1`` keys and ``2t`` children.
    """

    def __init__(self, is_leaf: bool = True) -> None:
        self.keys: List[Any] = []
        self.values: List[Any] = []
        self.children: List[_BTreeNode] = []
        self.is_leaf: bool = is_leaf

    def __repr__(self) -> str:
        return f"_BTreeNode(keys={self.keys}, leaf={self.is_leaf})"


class BTree:
    """B-Tree (balanced multi-way search tree).

    A B-Tree of minimum degree *t* guarantees:
      - Every node (except root) has at least ``t - 1`` keys.
      - Every node has at most ``2t - 1`` keys.
      - All leaves are at the same depth.

    Complexity (where *n* is the number of stored keys):
      - Search:  O(t · log_t(n))
      - Insert:  O(t · log_t(n))
      - Delete:  O(t · log_t(n))
    """

    def __init__(self, t: int = 2) -> None:
        if t < 2:
            raise ValueError("minimum degree t must be >= 2")
        self._t: int = t
        self._root: _BTreeNode = _BTreeNode(is_leaf=True)
        self._size: int = 0

    # ---- public API ----

    def search(self, key: Any) -> Optional[Any]:
        """Return the value mapped to *key*, or ``None`` if absent.

        Time: O(t · log_t(n))
        """
        node, idx = self._search(self._root, key)
        if node is None:
            return None
        return node.values[idx]

    def insert(self, key: Any, value: Any = None) -> None:
        """Insert *key* (with optional *value*) into the tree.

        If the key already exists its value is updated in-place.

        Time: O(t · log_t(n))
        """
        # Update existing key if present.
        node, idx = self._search(self._root, key)
        if node is not None:
            node.values[idx] = value
            return

        root = self._root
        if len(root.keys) == 2 * self._t - 1:
            new_root = _BTreeNode(is_leaf=False)
            new_root.children.append(root)
            self._split_child(new_root, 0)
            self._root = new_root
        self._insert_nonfull(self._root, key, value)
        self._size += 1

    def delete(self, key: Any) -> bool:
        """Remove *key* from the tree. Return ``True`` if found, ``False`` otherwise.

        Time: O(t · log_t(n))
        """
        if not self._delete(self._root, key):
            return False
        # Shrink root if it became empty.
        if len(self._root.keys) == 0 and not self._root.is_leaf:
            self._root = self._root.children[0]
        self._size -= 1
        return True

    def inorder(self) -> List[Tuple[Any, Any]]:
        """Return all (key, value) pairs in sorted order.

        Time: O(n)
        """
        result: List[Tuple[Any, Any]] = []
        self._inorder(self._root, result)
        return result

    # ---- dunder helpers ----

    def __len__(self) -> int:
        return self._size

    def __contains__(self, key: Any) -> bool:
        node, _ = self._search(self._root, key)
        return node is not None

    def __repr__(self) -> str:
        keys = [k for k, _ in self.inorder()]
        return f"BTree(t={self._t}, size={self._size}, keys={keys})"

    # ---- internal helpers ----

    def _search(self, node: _BTreeNode, key: Any) -> Tuple[Optional[_BTreeNode], int]:
        """Return (node, index) if *key* is found, else (None, -1)."""
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        if i < len(node.keys) and key == node.keys[i]:
            return node, i
        if node.is_leaf:
            return None, -1
        return self._search(node.children[i], key)

    def _split_child(self, parent: _BTreeNode, idx: int) -> None:
        """Split the full child ``parent.children[idx]`` around its median."""
        t = self._t
        full = parent.children[idx]
        sibling = _BTreeNode(is_leaf=full.is_leaf)

        # Median key/value move up into the parent.
        parent.keys.insert(idx, full.keys[t - 1])
        parent.values.insert(idx, full.values[t - 1])
        parent.children.insert(idx + 1, sibling)

        # Right half goes to sibling.
        sibling.keys = full.keys[t:]
        sibling.values = full.values[t:]
        if not full.is_leaf:
            sibling.children = full.children[t:]

        # Left half stays in original node.
        full.keys = full.keys[: t - 1]
        full.values = full.values[: t - 1]
        if not full.is_leaf:
            full.children = full.children[:t]

    def _insert_nonfull(self, node: _BTreeNode, key: Any, value: Any) -> None:
        """Insert into a node guaranteed to have room."""
        i = len(node.keys) - 1
        if node.is_leaf:
            # Find position and insert directly.
            node.keys.append(None)
            node.values.append(None)
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            node.keys[i + 1] = key
            node.values[i + 1] = value
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            if len(node.children[i].keys) == 2 * self._t - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            self._insert_nonfull(node.children[i], key, value)

    # ---- deletion helpers ----

    def _delete(self, node: _BTreeNode, key: Any) -> bool:
        """Recursively delete *key* from the subtree rooted at *node*."""
        t = self._t
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1

        # Case 1: key is in this node.
        if i < len(node.keys) and key == node.keys[i]:
            if node.is_leaf:
                node.keys.pop(i)
                node.values.pop(i)
                return True
            return self._delete_internal(node, i)

        # Key is not in this node.
        if node.is_leaf:
            return False

        # Ensure the child we descend into has at least t keys.
        if len(node.children[i].keys) < t:
            self._fill(node, i)
            # After fill the structure may have changed; re-locate key.
            if i > len(node.keys):
                i -= 1
            # If nodes were merged, key may now be in current node.
            if i < len(node.keys) and key == node.keys[i]:
                if node.is_leaf:
                    node.keys.pop(i)
                    node.values.pop(i)
                    return True
                return self._delete_internal(node, i)
            # Recalculate child index after merge.
            ci = 0
            while ci < len(node.keys) and key > node.keys[ci]:
                ci += 1
            if ci < len(node.keys) and key == node.keys[ci]:
                if node.is_leaf:
                    node.keys.pop(ci)
                    node.values.pop(ci)
                    return True
                return self._delete_internal(node, ci)
            return self._delete(node.children[ci], key)

        return self._delete(node.children[i], key)

    def _delete_internal(self, node: _BTreeNode, idx: int) -> bool:
        """Delete key at *idx* from an internal *node*."""
        t = self._t
        # Case 2a: predecessor child has >= t keys.
        if len(node.children[idx].keys) >= t:
            pred_node = node.children[idx]
            while not pred_node.is_leaf:
                pred_node = pred_node.children[-1]
            pred_key = pred_node.keys[-1]
            pred_val = pred_node.values[-1]
            node.keys[idx] = pred_key
            node.values[idx] = pred_val
            return self._delete(node.children[idx], pred_key)
        # Case 2b: successor child has >= t keys.
        if len(node.children[idx + 1].keys) >= t:
            succ_node = node.children[idx + 1]
            while not succ_node.is_leaf:
                succ_node = succ_node.children[0]
            succ_key = succ_node.keys[0]
            succ_val = succ_node.values[0]
            node.keys[idx] = succ_key
            node.values[idx] = succ_val
            return self._delete(node.children[idx + 1], succ_key)
        # Case 2c: merge children — save the key before merge moves it.
        target_key = node.keys[idx]
        self._merge(node, idx)
        return self._delete(node.children[idx], target_key)

    def _fill(self, node: _BTreeNode, idx: int) -> None:
        """Ensure ``node.children[idx]`` has at least *t* keys."""
        t = self._t
        # Try borrowing from left sibling.
        if idx > 0 and len(node.children[idx - 1].keys) >= t:
            self._borrow_from_left(node, idx)
        # Try borrowing from right sibling.
        elif idx < len(node.children) - 1 and len(node.children[idx + 1].keys) >= t:
            self._borrow_from_right(node, idx)
        # Merge with a sibling.
        else:
            if idx < len(node.children) - 1:
                self._merge(node, idx)
            else:
                self._merge(node, idx - 1)

    def _borrow_from_left(self, node: _BTreeNode, idx: int) -> None:
        child = node.children[idx]
        left = node.children[idx - 1]
        child.keys.insert(0, node.keys[idx - 1])
        child.values.insert(0, node.values[idx - 1])
        node.keys[idx - 1] = left.keys.pop()
        node.values[idx - 1] = left.values.pop()
        if not left.is_leaf:
            child.children.insert(0, left.children.pop())

    def _borrow_from_right(self, node: _BTreeNode, idx: int) -> None:
        child = node.children[idx]
        right = node.children[idx + 1]
        child.keys.append(node.keys[idx])
        child.values.append(node.values[idx])
        node.keys[idx] = right.keys.pop(0)
        node.values[idx] = right.values.pop(0)
        if not right.is_leaf:
            child.children.append(right.children.pop(0))

    def _merge(self, node: _BTreeNode, idx: int) -> None:
        """Merge ``node.children[idx]`` and ``node.children[idx+1]``."""
        left = node.children[idx]
        right = node.children[idx + 1]

        left.keys.append(node.keys.pop(idx))
        left.values.append(node.values.pop(idx))
        left.keys.extend(right.keys)
        left.values.extend(right.values)
        if not left.is_leaf:
            left.children.extend(right.children)

        node.children.pop(idx + 1)

    def _inorder(self, node: _BTreeNode, result: List[Tuple[Any, Any]]) -> None:
        for i, key in enumerate(node.keys):
            if not node.is_leaf:
                self._inorder(node.children[i], result)
            result.append((key, node.values[i]))
        if not node.is_leaf:
            self._inorder(node.children[-1], result)


if __name__ == "__main__":
    bt = BTree(t=2)
    for v in [10, 20, 5, 6, 12, 30, 7, 17]:
        bt.insert(v, f"val_{v}")

    print(bt)                         # sorted keys
    print(f"len = {len(bt)}")         # 8
    print(f"search 12 = {bt.search(12)}")
    print(f"7 in bt = {7 in bt}")
    print(f"99 in bt = {99 in bt}")

    bt.delete(6)
    bt.delete(30)
    print(f"after deleting 6, 30: {bt}")
    print(f"inorder: {bt.inorder()}")
