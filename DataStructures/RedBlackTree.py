"""Red-Black Tree implementation.

A self-balancing binary search tree where each node has a color (RED or BLACK).
Guarantees O(log n) time for insert, delete, and search operations by
maintaining the following invariants:

1. Every node is either RED or BLACK.
2. The root is BLACK.
3. Every NIL leaf is BLACK.
4. If a node is RED, both its children are BLACK.
5. All paths from a node to its descendant NIL leaves contain the same
   number of BLACK nodes.
"""

from __future__ import annotations

from typing import Any, Generator, Optional

RED = True
BLACK = False


class _Node:
    """Internal node used by RedBlackTree."""

    __slots__ = ("key", "value", "color", "left", "right", "parent")

    def __init__(
        self,
        key: Any,
        value: Any = None,
        color: bool = RED,
        left: Optional[_Node] = None,
        right: Optional[_Node] = None,
        parent: Optional[_Node] = None,
    ) -> None:
        self.key = key
        self.value = value
        self.color = color
        self.left = left
        self.right = right
        self.parent = parent

    def __repr__(self) -> str:
        color_str = "R" if self.color == RED else "B"
        return f"_Node({self.key!r}, {color_str})"


class RedBlackTree:
    """Red-Black Tree (self-balancing BST).

    All major operations run in O(log n) time.

    Example:
        >>> tree = RedBlackTree()
        >>> for k in [10, 20, 30, 15, 25]:
        ...     tree.insert(k, k)
        >>> 15 in tree
        True
        >>> tree.search(30)
        30
        >>> len(tree)
        5
    """

    def __init__(self) -> None:
        # Sentinel NIL node shared by all leaves and the root's parent.
        self._NIL: _Node = _Node(key=None, value=None, color=BLACK)
        self._root: _Node = self._NIL
        self._size: int = 0

    # ------------------------------------------------------------------
    # Rotations
    # ------------------------------------------------------------------

    def _left_rotate(self, x: _Node) -> None:
        """Left rotation around *x*.  O(1)."""
        y = x.right
        x.right = y.left
        if y.left is not self._NIL:
            y.left.parent = x
        y.parent = x.parent
        if x.parent is self._NIL:
            self._root = y
        elif x is x.parent.left:
            x.parent.left = y
        else:
            x.parent.right = y
        y.left = x
        x.parent = y

    def _right_rotate(self, y: _Node) -> None:
        """Right rotation around *y*.  O(1)."""
        x = y.left
        y.left = x.right
        if x.right is not self._NIL:
            x.right.parent = y
        x.parent = y.parent
        if y.parent is self._NIL:
            self._root = x
        elif y is y.parent.right:
            y.parent.right = x
        else:
            y.parent.left = x
        x.right = y
        y.parent = x

    # ------------------------------------------------------------------
    # Insertion
    # ------------------------------------------------------------------

    def insert(self, key: Any, value: Any = None) -> None:
        """Insert a key-value pair.  O(log n).

        If the key already exists its value is updated.
        """
        # Walk down to find the insertion point.
        parent = self._NIL
        current = self._root
        while current is not self._NIL:
            parent = current
            if key < current.key:
                current = current.left
            elif key > current.key:
                current = current.right
            else:
                # Key exists — update value.
                current.value = value
                return

        new_node = _Node(
            key=key,
            value=value,
            color=RED,
            left=self._NIL,
            right=self._NIL,
            parent=parent,
        )

        if parent is self._NIL:
            self._root = new_node
        elif key < parent.key:
            parent.left = new_node
        else:
            parent.right = new_node

        self._size += 1
        self._insert_fixup(new_node)

    def _insert_fixup(self, z: _Node) -> None:
        """Restore Red-Black properties after insertion.  O(log n)."""
        while z.parent.color == RED:
            if z.parent is z.parent.parent.left:
                uncle = z.parent.parent.right
                if uncle.color == RED:
                    # Case 1: uncle is red — recolor.
                    z.parent.color = BLACK
                    uncle.color = BLACK
                    z.parent.parent.color = RED
                    z = z.parent.parent
                else:
                    if z is z.parent.right:
                        # Case 2: z is a right child — left rotate.
                        z = z.parent
                        self._left_rotate(z)
                    # Case 3: z is a left child — right rotate.
                    z.parent.color = BLACK
                    z.parent.parent.color = RED
                    self._right_rotate(z.parent.parent)
            else:
                # Mirror cases with left/right swapped.
                uncle = z.parent.parent.left
                if uncle.color == RED:
                    z.parent.color = BLACK
                    uncle.color = BLACK
                    z.parent.parent.color = RED
                    z = z.parent.parent
                else:
                    if z is z.parent.left:
                        z = z.parent
                        self._right_rotate(z)
                    z.parent.color = BLACK
                    z.parent.parent.color = RED
                    self._left_rotate(z.parent.parent)
        self._root.color = BLACK

    # ------------------------------------------------------------------
    # Deletion
    # ------------------------------------------------------------------

    def _transplant(self, u: _Node, v: _Node) -> None:
        """Replace subtree rooted at *u* with subtree rooted at *v*."""
        if u.parent is self._NIL:
            self._root = v
        elif u is u.parent.left:
            u.parent.left = v
        else:
            u.parent.right = v
        v.parent = u.parent

    def _tree_minimum(self, node: _Node) -> _Node:
        """Return the node with the smallest key in the subtree."""
        while node.left is not self._NIL:
            node = node.left
        return node

    def _tree_maximum(self, node: _Node) -> _Node:
        """Return the node with the largest key in the subtree."""
        while node.right is not self._NIL:
            node = node.right
        return node

    def delete(self, key: Any) -> bool:
        """Delete the node with the given *key*.  O(log n).

        Returns True if the key was found and removed, False otherwise.
        """
        z = self._find_node(key)
        if z is self._NIL:
            return False

        y = z
        y_original_color = y.color

        if z.left is self._NIL:
            x = z.right
            self._transplant(z, z.right)
        elif z.right is self._NIL:
            x = z.left
            self._transplant(z, z.left)
        else:
            y = self._tree_minimum(z.right)
            y_original_color = y.color
            x = y.right
            if y.parent is z:
                x.parent = y
            else:
                self._transplant(y, y.right)
                y.right = z.right
                y.right.parent = y
            self._transplant(z, y)
            y.left = z.left
            y.left.parent = y
            y.color = z.color

        if y_original_color == BLACK:
            self._delete_fixup(x)

        self._size -= 1
        return True

    def _delete_fixup(self, x: _Node) -> None:
        """Restore Red-Black properties after deletion.  O(log n)."""
        while x is not self._root and x.color == BLACK:
            if x is x.parent.left:
                w = x.parent.right
                if w.color == RED:
                    # Case 1
                    w.color = BLACK
                    x.parent.color = RED
                    self._left_rotate(x.parent)
                    w = x.parent.right
                if w.left.color == BLACK and w.right.color == BLACK:
                    # Case 2
                    w.color = RED
                    x = x.parent
                else:
                    if w.right.color == BLACK:
                        # Case 3
                        w.left.color = BLACK
                        w.color = RED
                        self._right_rotate(w)
                        w = x.parent.right
                    # Case 4
                    w.color = x.parent.color
                    x.parent.color = BLACK
                    w.right.color = BLACK
                    self._left_rotate(x.parent)
                    x = self._root
            else:
                # Mirror cases.
                w = x.parent.left
                if w.color == RED:
                    w.color = BLACK
                    x.parent.color = RED
                    self._right_rotate(x.parent)
                    w = x.parent.left
                if w.right.color == BLACK and w.left.color == BLACK:
                    w.color = RED
                    x = x.parent
                else:
                    if w.left.color == BLACK:
                        w.right.color = BLACK
                        w.color = RED
                        self._left_rotate(w)
                        w = x.parent.left
                    w.color = x.parent.color
                    x.parent.color = BLACK
                    w.left.color = BLACK
                    self._right_rotate(x.parent)
                    x = self._root
        x.color = BLACK

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    def _find_node(self, key: Any) -> _Node:
        """Return the node matching *key*, or the NIL sentinel."""
        node = self._root
        while node is not self._NIL:
            if key < node.key:
                node = node.left
            elif key > node.key:
                node = node.right
            else:
                return node
        return self._NIL

    def search(self, key: Any) -> Optional[Any]:
        """Return the value associated with *key*, or None.  O(log n)."""
        node = self._find_node(key)
        if node is self._NIL:
            return None
        return node.value

    def minimum(self) -> Optional[Any]:
        """Return the smallest key in the tree, or None if empty.  O(log n)."""
        if self._root is self._NIL:
            return None
        return self._tree_minimum(self._root).key

    def maximum(self) -> Optional[Any]:
        """Return the largest key in the tree, or None if empty.  O(log n)."""
        if self._root is self._NIL:
            return None
        return self._tree_maximum(self._root).key

    def inorder(self) -> list[tuple[Any, Any]]:
        """Return an in-order list of (key, value) pairs.  O(n)."""
        result: list[tuple[Any, Any]] = []
        self._inorder_walk(self._root, result)
        return result

    def _inorder_walk(
        self, node: _Node, result: list[tuple[Any, Any]]
    ) -> None:
        if node is not self._NIL:
            self._inorder_walk(node.left, result)
            result.append((node.key, node.value))
            self._inorder_walk(node.right, result)

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __len__(self) -> int:
        return self._size

    def __contains__(self, key: Any) -> bool:
        return self._find_node(key) is not self._NIL

    def __repr__(self) -> str:
        items = ", ".join(f"{k!r}: {v!r}" for k, v in self.inorder())
        return f"RedBlackTree({{{items}}})"


# ------------------------------------------------------------------
# Demo
# ------------------------------------------------------------------

if __name__ == "__main__":
    tree = RedBlackTree()

    print("=== Red-Black Tree Demo ===\n")

    # Insert keys
    keys = [20, 15, 25, 10, 5, 1, 30, 35, 40, 18]
    print(f"Inserting keys: {keys}")
    for k in keys:
        tree.insert(k, k * 10)

    print(f"Tree size : {len(tree)}")
    print(f"In-order  : {tree.inorder()}")
    print(f"Min key   : {tree.minimum()}")
    print(f"Max key   : {tree.maximum()}")
    print(f"Search 25 : {tree.search(25)}")
    print(f"15 in tree: {15 in tree}")
    print(f"99 in tree: {99 in tree}")
    print(f"repr      : {tree!r}")

    # Delete a few keys
    for k in [1, 20, 40]:
        removed = tree.delete(k)
        print(f"\nDeleted {k}: {removed}")
        print(f"  size={len(tree)}, in-order={tree.inorder()}")

    print(f"\nDelete non-existent 999: {tree.delete(999)}")
    print(f"Final tree: {tree!r}")
