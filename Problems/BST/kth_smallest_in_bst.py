# Kth Smallest Element in a BST (LeetCode 230)
#
# DS used: BST (DataStructures/MyBinarySearchTree.py)
#
# Problem:
# Given the root of a BST and an integer k, return the kth smallest value
# (1-indexed) among all node values.
#
# Example:
#   Input: root = [3,1,4,null,2], k = 1  →  Output: 1
#
# Why BST?
# An in-order traversal of a BST visits nodes in sorted order.
# We simply stop at the kth visited node.
#
# Time:  O(H + k) where H = height (average O(log n + k))
# Space: O(H) recursion stack

from __future__ import annotations

from typing import List, Optional


class TreeNode:
    def __init__(self, val: int = 0, left: Optional["TreeNode"] = None,
                 right: Optional["TreeNode"] = None) -> None:
        self.val = val
        self.left = left
        self.right = right


def kth_smallest(root: Optional[TreeNode], k: int) -> int:
    """Return the kth smallest value in the BST (1-indexed).

    Pattern identification: rank query on a BST -> early-stop iterative inorder;
    popped nodes are visited in sorted order, so visit k supplies the answer.
    """
    # 1. Output: Return the 1-indexed kth smallest BST value.
    # 2. Structure: BST ordering makes left-node-right traversal visit values in sorted order.
    # 3. Constraints: k is assumed positive; exhausting the tree before visit k raises ValueError.
    # 4. Choice: Push each left spine, pop and count nodes, then continue into the right subtree.
    # 5. Why it works: Each pop is the next sorted value, so the kth pop has the requested rank.
    #    Early stopping costs O(h + k) time and O(h) stack space, where h is tree height.
    stack: list[TreeNode] = []
    current = root
    count = 0

    while stack or current:
        while current:
            stack.append(current)
            current = current.left
        current = stack.pop()
        count += 1
        if count == k:
            return current.val
        current = current.right

    raise ValueError(f"k={k} is larger than the number of nodes")


def build_tree(values: List[Optional[int]]) -> Optional[TreeNode]:
    """Build a binary tree from a level-order list."""
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
    tree1 = build_tree([3, 1, 4, None, 2])
    print(kth_smallest(tree1, 1))  # 1

    tree2 = build_tree([5, 3, 6, 2, 4, None, None, 1])
    print(kth_smallest(tree2, 3))  # 3

    print(kth_smallest(TreeNode(42), 1))  # 42
