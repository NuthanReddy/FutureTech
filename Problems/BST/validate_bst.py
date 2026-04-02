# Validate Binary Search Tree (LeetCode 98)
#
# DS used: BST (DataStructures/MyBinarySearchTree.py)
#
# Problem:
# Given the root of a binary tree, determine if it is a valid BST.
# A valid BST means:
#   - Left subtree contains only nodes with keys < node's key
#   - Right subtree contains only nodes with keys > node's key
#   - Both subtrees must also be valid BSTs
#
# Example:
#   Input:    2        Input:    5
#            / \                / \
#           1   3              1   4
#                                 / \
#                                3   6
#   Output: True       Output: False (3 < 5 but in right subtree)
#
# Why BST?
# This problem tests the core BST invariant.  A common mistake is checking
# only parent-child relationships; the correct approach passes valid ranges
# (lower, upper) down the recursion.
#
# Time:  O(n) — visit each node once
# Space: O(h) — recursion stack, h = tree height

from __future__ import annotations

import math
from typing import List, Optional


class TreeNode:
    def __init__(self, val: int = 0, left: Optional["TreeNode"] = None,
                 right: Optional["TreeNode"] = None) -> None:
        self.val = val
        self.left = left
        self.right = right


def is_valid_bst(root: Optional[TreeNode]) -> bool:
    """Return True if *root* is a valid binary search tree."""

    def _validate(node: Optional[TreeNode], low: float, high: float) -> bool:
        if node is None:
            return True
        if not (low < node.val < high):
            return False
        return (_validate(node.left, low, node.val)
                and _validate(node.right, node.val, high))

    return _validate(root, -math.inf, math.inf)


# ---------- helper: build tree from level-order list ----------

def build_tree(values: List[Optional[int]]) -> Optional[TreeNode]:
    """Build a binary tree from a level-order list (None = missing node)."""
    if not values or values[0] is None:
        return None
    root = TreeNode(values[0])
    queue = [root]
    i = 1
    while i < len(values):
        node = queue.pop(0)
        if i < len(values) and values[i] is not None:
            node.left = TreeNode(values[i])
            queue.append(node.left)
        i += 1
        if i < len(values) and values[i] is not None:
            node.right = TreeNode(values[i])
            queue.append(node.right)
        i += 1
    return root


if __name__ == "__main__":
    # Valid BST: [2, 1, 3]
    print(is_valid_bst(build_tree([2, 1, 3])))       # True

    # Invalid BST: [5, 1, 4, None, None, 3, 6]
    print(is_valid_bst(build_tree([5, 1, 4, None, None, 3, 6])))  # False

    # Single node
    print(is_valid_bst(build_tree([1])))               # True

    # Empty tree
    print(is_valid_bst(None))                          # True

