from __future__ import annotations

from typing import Any, Generator, List, Optional


class _AVLNode:
    """Internal AVL tree node."""

    __slots__ = ("val", "left", "right", "height", "size")

    def __init__(self, val: Any) -> None:
        self.val: Any = val
        self.left: Optional[_AVLNode] = None
        self.right: Optional[_AVLNode] = None
        self.height: int = 1
        self.size: int = 1


class SortedSet:
    """A set that maintains elements in sorted order using an AVL tree.

    All main operations (add, remove, contains, floor, ceiling) run in
    O(log n) time.  Iteration is O(n).
    """

    def __init__(self) -> None:
        self._root: Optional[_AVLNode] = None

    # ------------------------------------------------------------------
    # AVL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _height(node: Optional[_AVLNode]) -> int:
        return node.height if node else 0

    @staticmethod
    def _size(node: Optional[_AVLNode]) -> int:
        return node.size if node else 0

    def _update(self, node: _AVLNode) -> None:
        node.height = 1 + max(self._height(node.left), self._height(node.right))
        node.size = 1 + self._size(node.left) + self._size(node.right)

    def _balance_factor(self, node: Optional[_AVLNode]) -> int:
        return self._height(node.left) - self._height(node.right) if node else 0

    def _rotate_right(self, z: _AVLNode) -> _AVLNode:
        y = z.left
        assert y is not None
        z.left = y.right
        y.right = z
        self._update(z)
        self._update(y)
        return y

    def _rotate_left(self, z: _AVLNode) -> _AVLNode:
        y = z.right
        assert y is not None
        z.right = y.left
        y.left = z
        self._update(z)
        self._update(y)
        return y

    def _rebalance(self, node: _AVLNode) -> _AVLNode:
        self._update(node)
        bf = self._balance_factor(node)

        if bf > 1:
            assert node.left is not None
            if self._balance_factor(node.left) < 0:
                node.left = self._rotate_left(node.left)
            return self._rotate_right(node)

        if bf < -1:
            assert node.right is not None
            if self._balance_factor(node.right) > 0:
                node.right = self._rotate_right(node.right)
            return self._rotate_left(node)

        return node

    # ------------------------------------------------------------------
    # Internal insert / delete
    # ------------------------------------------------------------------

    def _insert(self, node: Optional[_AVLNode], val: Any) -> tuple[_AVLNode, bool]:
        """Insert *val* and return (new_root, inserted).

        Time complexity: O(log n)
        """
        if node is None:
            return _AVLNode(val), True

        if val < node.val:
            node.left, inserted = self._insert(node.left, val)
        elif val > node.val:
            node.right, inserted = self._insert(node.right, val)
        else:
            return node, False  # duplicate

        return self._rebalance(node), inserted

    def _min_node(self, node: _AVLNode) -> _AVLNode:
        while node.left is not None:
            node = node.left
        return node

    def _delete(self, node: Optional[_AVLNode], val: Any) -> tuple[Optional[_AVLNode], bool]:
        """Delete *val* and return (new_root, deleted).

        Time complexity: O(log n)
        """
        if node is None:
            return None, False

        deleted: bool
        if val < node.val:
            node.left, deleted = self._delete(node.left, val)
        elif val > node.val:
            node.right, deleted = self._delete(node.right, val)
        else:
            deleted = True
            if node.left is None:
                return node.right, True
            if node.right is None:
                return node.left, True
            successor = self._min_node(node.right)
            node.val = successor.val
            node.right, _ = self._delete(node.right, successor.val)

        return self._rebalance(node), deleted

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(self, val: Any) -> None:
        """Add *val* to the set.  No-op if already present.

        Time complexity: O(log n)
        """
        self._root, _ = self._insert(self._root, val)

    def remove(self, val: Any) -> None:
        """Remove *val* from the set.  Raises KeyError if not found.

        Time complexity: O(log n)
        """
        self._root, deleted = self._delete(self._root, val)
        if not deleted:
            raise KeyError(val)

    def discard(self, val: Any) -> None:
        """Remove *val* if present; otherwise do nothing.

        Time complexity: O(log n)
        """
        self._root, _ = self._delete(self._root, val)

    def __contains__(self, val: Any) -> bool:
        """Return True if *val* is in the set.

        Time complexity: O(log n)
        """
        node = self._root
        while node is not None:
            if val < node.val:
                node = node.left
            elif val > node.val:
                node = node.right
            else:
                return True
        return False

    def __len__(self) -> int:
        """Return the number of elements.

        Time complexity: O(1)
        """
        return self._size(self._root)

    def _inorder(self, node: Optional[_AVLNode]) -> Generator[Any, None, None]:
        if node is not None:
            yield from self._inorder(node.left)
            yield node.val
            yield from self._inorder(node.right)

    def __iter__(self) -> Generator[Any, None, None]:
        """Iterate over elements in sorted order.

        Time complexity: O(n)
        """
        yield from self._inorder(self._root)

    def __repr__(self) -> str:
        return f"SortedSet({list(self)})"

    def min(self) -> Any:
        """Return the smallest element.

        Time complexity: O(log n)
        Raises ValueError if the set is empty.
        """
        if self._root is None:
            raise ValueError("min() on empty SortedSet")
        return self._min_node(self._root).val

    def max(self) -> Any:
        """Return the largest element.

        Time complexity: O(log n)
        Raises ValueError if the set is empty.
        """
        if self._root is None:
            raise ValueError("max() on empty SortedSet")
        node = self._root
        while node.right is not None:
            node = node.right
        return node.val

    def floor(self, val: Any) -> Optional[Any]:
        """Return the greatest element <= *val*, or None.

        Time complexity: O(log n)
        """
        result: Optional[Any] = None
        node = self._root
        while node is not None:
            if val == node.val:
                return node.val
            elif val < node.val:
                node = node.left
            else:
                result = node.val
                node = node.right
        return result

    def ceiling(self, val: Any) -> Optional[Any]:
        """Return the smallest element >= *val*, or None.

        Time complexity: O(log n)
        """
        result: Optional[Any] = None
        node = self._root
        while node is not None:
            if val == node.val:
                return node.val
            elif val > node.val:
                node = node.right
            else:
                result = node.val
                node = node.left
        return result

    def range_query(self, low: Any, high: Any) -> List[Any]:
        """Return all elements in [low, high] in sorted order.

        Time complexity: O(log n + k) where k is the number of results.
        """
        results: List[Any] = []
        self._range_collect(self._root, low, high, results)
        return results

    def _range_collect(
        self,
        node: Optional[_AVLNode],
        low: Any,
        high: Any,
        results: List[Any],
    ) -> None:
        if node is None:
            return
        if low < node.val:
            self._range_collect(node.left, low, high, results)
        if low <= node.val <= high:
            results.append(node.val)
        if node.val < high:
            self._range_collect(node.right, low, high, results)


if __name__ == "__main__":
    ss = SortedSet()
    for v in [5, 3, 8, 1, 4, 7, 10, 2]:
        ss.add(v)

    print("SortedSet:", ss)
    print("len:", len(ss))
    print("5 in ss:", 5 in ss)
    print("6 in ss:", 6 in ss)
    print("min:", ss.min())
    print("max:", ss.max())
    print("floor(6):", ss.floor(6))
    print("ceiling(6):", ss.ceiling(6))
    print("range_query(3, 7):", ss.range_query(3, 7))

    ss.remove(5)
    print("After removing 5:", ss)
    ss.discard(99)  # no error
    print("After discarding 99:", ss)
