"""LeetCode Top Interview 150 tree problems, grouped by traversal pattern.

The module intentionally shares one ``TreeNode`` type across all problems.
LeetCode presents each exercise in isolation, but a shared node makes the
relationships between DFS, BFS, reconstruction, and BST traversal easier to
study and avoids repeating test/build helpers.

Unless a function explicitly documents a stronger precondition, ``None`` is
treated as an empty tree.  Recursive solutions use ``O(h)`` call-stack space,
where ``h`` is the tree height; on a completely skewed tree, ``h == n``.

## Problem statements

The following concise statements preserve the interview exercise represented
by each function.  Inputs use ``TreeNode`` unless stated otherwise.

* **Maximum Depth of Binary Tree** — Input: a binary tree. Output: the number
  of nodes on its longest root-to-leaf path. Constraint: an empty tree has
  depth zero.
* **Same Tree** — Input: two binary trees. Output: whether they have identical
  structure and equal values at every corresponding position.
* **Invert Binary Tree** — Input: a binary tree. Output: the same root after
  swapping every node's left and right subtrees in place.
* **Symmetric Tree** — Input: a binary tree. Output: whether it is a mirror of
  itself around its root.
* **Construct from Preorder and Inorder** — Input: preorder and inorder
  traversals of a binary tree. Output: the reconstructed root. Constraint:
  values are unique and both traversals describe the same tree.
* **Construct from Inorder and Postorder** — Input: inorder and postorder
  traversals. Output: the reconstructed root. Constraint: values are unique
  and both traversals describe the same tree.
* **Populating Next Right Pointers II** — Input: any binary tree. Output: the
  root after each node's ``next`` pointer links to the next node on its level,
  or ``None`` at the level's end.
* **Flatten Binary Tree to Linked List** — Input: a binary tree. Output: the
  same tree rewired in place as a preorder right-only linked list; every left
  pointer must be ``None``.
* **Path Sum** — Input: a binary tree and an integer target. Output: whether
  some root-to-leaf path has values summing to the target.
* **Sum Root to Leaf Numbers** — Input: a digit-valued binary tree. Output:
  the sum of numbers formed by every root-to-leaf digit path.
* **Binary Tree Maximum Path Sum** — Input: a non-empty binary tree in the
  original problem. Output: the greatest sum of any path whose edges connect
  adjacent nodes and which may start and end anywhere.
* **Count Complete Tree Nodes** — Input: a complete binary tree. Output: its
  number of nodes. Constraint: every level except possibly the last is full,
  and the last is filled from left to right.
* **Lowest Common Ancestor of a Binary Tree** — Input: a binary tree and two
  existing node references. Output: the lowest node whose subtree contains
  both references; identity, not only value, distinguishes nodes.
* **Binary Tree Right Side View** — Input: a binary tree. Output: the values
  visible when looking from the right, one value per depth.
* **Average of Levels in Binary Tree** — Input: a binary tree. Output: the
  arithmetic mean of node values at each depth, in top-down order.
* **Binary Tree Zigzag Level Order Traversal** — Input: a binary tree. Output:
  level-order values, alternating left-to-right and right-to-left directions.
* **Minimum Absolute Difference in BST** — Input: a BST with at least two
  nodes. Output: the minimum absolute difference between any two node values.
* **Kth Smallest Element in a BST** — Input: a BST and 1-indexed ``k``.
  Output: the kth value in sorted order. Constraint: ``1 <= k <= node count``.
* **Validate Binary Search Tree** — Input: a binary tree. Output: whether every
  node satisfies strict BST ordering relative to all of its ancestors.
* **Binary Search Tree Iterator** — Input: a BST. Output: an object exposing
  sorted ``next`` values and ``has_next`` without materializing all values.
  Constraint: iterator storage should be ``O(h)``.
"""

from __future__ import annotations

from collections import deque
from typing import Iterable, Iterator, Optional


class TreeNode:
    """Binary-tree node used by both ordinary tree and ``next``-pointer tasks."""

    def __init__(
        self,
        val: int = 0,
        left: Optional["TreeNode"] = None,
        right: Optional["TreeNode"] = None,
        next: Optional["TreeNode"] = None,
    ) -> None:
        self.val = val
        self.left = left
        self.right = right
        self.next = next


def build_tree_level_order(values: Iterable[Optional[int]]) -> Optional[TreeNode]:
    """Build a tree from LeetCode-style level-order values.

    ``None`` means that a child is absent.  Extra values after the queue of
    possible parents is exhausted are rejected because they describe nodes
    that cannot be connected to the tree.

    Time: O(n)
    Space: O(w), where w is the maximum tree width
    """

    items = list(values)
    if not items:
        return None
    if items[0] is None:
        if any(value is not None for value in items[1:]):
            raise ValueError("non-empty nodes cannot follow an empty root")
        return None

    root = TreeNode(items[0])
    parents: deque[TreeNode] = deque([root])
    index = 1

    while parents and index < len(items):
        parent = parents.popleft()

        if items[index] is not None:
            parent.left = TreeNode(items[index])
            parents.append(parent.left)
        index += 1

        if index < len(items) and items[index] is not None:
            parent.right = TreeNode(items[index])
            parents.append(parent.right)
        index += 1

    if any(value is not None for value in items[index:]):
        raise ValueError("level-order input contains unreachable nodes")
    return root


def level_order_values(root: Optional[TreeNode]) -> list[Optional[int]]:
    """Serialize a tree to compact level order; useful for examples and tests."""

    if root is None:
        return []

    result: list[Optional[int]] = []
    queue: deque[Optional[TreeNode]] = deque([root])
    while queue:
        node = queue.popleft()
        if node is None:
            result.append(None)
            continue
        result.append(node.val)
        queue.append(node.left)
        queue.append(node.right)

    # Missing children beyond the final real node carry no information.
    while result and result[-1] is None:
        result.pop()
    return result


# ---------------------------------------------------------------------------
# Pattern 1: structural DFS
# ---------------------------------------------------------------------------


def max_depth(root: Optional[TreeNode]) -> int:
    """Return the number of nodes on the longest root-to-leaf path.

    Recursion mirrors the definition of tree height: a node contributes one
    plus the larger child height.  An iterative level-order traversal is a
    useful alternative when a skewed tree may exceed Python's recursion limit.

    Time: O(n), because every node is visited once
    Space: O(h) recursion stack
    """

    if root is None:
        return 0
    return 1 + max(max_depth(root.left), max_depth(root.right))


def is_same_tree(
    first: Optional[TreeNode], second: Optional[TreeNode]
) -> bool:
    """Return whether two trees have identical structure and values.

    Invariant: each recursive call compares nodes occupying the same logical
    position.  One missing node is therefore a structural mismatch; two
    missing nodes complete that branch successfully.

    Time: O(n), for n nodes in the smaller matching prefix
    Space: O(h)
    """

    if first is None or second is None:
        return first is second
    return (
        first.val == second.val
        and is_same_tree(first.left, second.left)
        and is_same_tree(first.right, second.right)
    )


def invert_tree(root: Optional[TreeNode]) -> Optional[TreeNode]:
    """Mirror a tree in-place and return its root.

    The iterative breadth-first form avoids recursion-depth failures and makes
    the mutation invariant explicit: once a node leaves the queue, its direct
    children have been swapped exactly once.  Recursive postorder is shorter
    but uses the same O(h) stack risk as ``max_depth``.

    Time: O(n)
    Space: O(w), where w is maximum width
    """

    if root is None:
        return None

    queue: deque[TreeNode] = deque([root])
    while queue:
        node = queue.popleft()
        node.left, node.right = node.right, node.left
        if node.left is not None:
            queue.append(node.left)
        if node.right is not None:
            queue.append(node.right)
    return root


def is_symmetric(root: Optional[TreeNode]) -> bool:
    """Return whether the tree is a mirror of itself.

    Queue entries are mirror pairs.  Their values must match, and their
    children are enqueued in crossed order: outer with outer, inner with inner.
    This iterative approach avoids deep recursion while preserving the mirror
    invariant directly.

    Time: O(n)
    Space: O(w)
    """

    if root is None:
        return True

    pairs: deque[tuple[Optional[TreeNode], Optional[TreeNode]]] = deque(
        [(root.left, root.right)]
    )
    while pairs:
        left, right = pairs.popleft()
        if left is None or right is None:
            if left is not right:
                return False
            continue
        if left.val != right.val:
            return False
        pairs.append((left.left, right.right))
        pairs.append((left.right, right.left))
    return True


# ---------------------------------------------------------------------------
# Pattern 2: traversal order as construction or mutation
# ---------------------------------------------------------------------------


def build_tree_preorder_inorder(
    preorder: list[int], inorder: list[int]
) -> Optional[TreeNode]:
    """Reconstruct a tree from preorder and inorder traversals.

    Preorder chooses the next root.  Its index in inorder divides the current
    subtree into left and right ranges.  The shared preorder cursor advances
    once per node, avoiding copied slices and their accidental O(n^2) cost.

    LeetCode guarantees unique values.  This implementation validates that
    requirement because duplicate values make the reconstruction ambiguous.

    Time: O(n)
    Space: O(n) for the index map plus O(h) recursion stack
    """

    _validate_reconstruction_inputs(preorder, inorder)
    if not preorder:
        return None

    inorder_index = {value: index for index, value in enumerate(inorder)}
    preorder_index = 0

    def build(left: int, right: int) -> Optional[TreeNode]:
        nonlocal preorder_index
        if left > right:
            return None

        root_value = preorder[preorder_index]
        preorder_index += 1
        split = inorder_index[root_value]
        if not left <= split <= right:
            raise ValueError("preorder and inorder describe different trees")

        root = TreeNode(root_value)
        # Preorder is root-left-right, so the left subtree must consume its
        # values before construction proceeds to the right subtree.
        root.left = build(left, split - 1)
        root.right = build(split + 1, right)
        return root

    return build(0, len(inorder) - 1)


def build_tree_inorder_postorder(
    inorder: list[int], postorder: list[int]
) -> Optional[TreeNode]:
    """Reconstruct a tree from inorder and postorder traversals.

    Reading postorder backward produces root-right-left.  Therefore the right
    subtree must be built first; reversing that order is a common bug.

    Time: O(n)
    Space: O(n) for the index map plus O(h) recursion stack
    """

    _validate_reconstruction_inputs(inorder, postorder)
    if not inorder:
        return None

    inorder_index = {value: index for index, value in enumerate(inorder)}
    postorder_index = len(postorder) - 1

    def build(left: int, right: int) -> Optional[TreeNode]:
        nonlocal postorder_index
        if left > right:
            return None

        root_value = postorder[postorder_index]
        postorder_index -= 1
        split = inorder_index[root_value]
        if not left <= split <= right:
            raise ValueError("inorder and postorder describe different trees")

        root = TreeNode(root_value)
        root.right = build(split + 1, right)
        root.left = build(left, split - 1)
        return root

    return build(0, len(inorder) - 1)


def _validate_reconstruction_inputs(
    first: list[int], second: list[int]
) -> None:
    """Validate the common unique-value traversal preconditions."""

    if len(first) != len(second):
        raise ValueError("traversals must have equal lengths")
    if len(set(first)) != len(first) or len(set(second)) != len(second):
        raise ValueError("reconstruction requires unique node values")
    if set(first) != set(second):
        raise ValueError("traversals must contain the same values")


def connect_next(root: Optional[TreeNode]) -> Optional[TreeNode]:
    """Populate each node's ``next`` pointer without an auxiliary BFS queue.

    ``level_start`` begins a level whose ``next`` chain is already available.
    A dummy node and ``tail`` build the next level from left to right.  The
    invariant after processing each parent is that ``dummy.next ... tail`` is
    exactly the discovered prefix of the following level.

    Compared with ordinary BFS, this keeps extra space constant by reusing the
    output pointers as the current-level queue.

    Time: O(n)
    Extra space: O(1)
    """

    if root is None:
        return None

    # Do not rely on a caller supplying a pristine root.
    root.next = None
    level_start: Optional[TreeNode] = root
    while level_start is not None:
        dummy = TreeNode()
        tail = dummy
        current: Optional[TreeNode] = level_start

        while current is not None:
            for child in (current.left, current.right):
                if child is not None:
                    tail.next = child
                    tail = child
            current = current.next

        # Explicit termination also clears stale next pointers on reused trees.
        tail.next = None
        level_start = dummy.next
    return root


def flatten(root: Optional[TreeNode]) -> None:
    """Flatten a tree in-place into a right-only preorder linked list.

    The stack stores nodes still to visit.  Pushing right before left ensures
    left is popped first, matching preorder.  ``previous`` is always the tail
    of the already-flattened prefix, so linking it to the current node extends
    that prefix by exactly one.

    A Morris-style predecessor splice can achieve O(1) extra space, but the
    stack version is easier to verify and remains O(h) on balanced trees.

    Time: O(n)
    Space: O(h); this becomes O(n) for a maximally skewed tree
    """

    if root is None:
        return

    stack = [root]
    previous: Optional[TreeNode] = None
    while stack:
        node = stack.pop()
        if node.right is not None:
            stack.append(node.right)
        if node.left is not None:
            stack.append(node.left)

        if previous is not None:
            previous.left = None
            previous.right = node
        previous = node

    # Ensure a reused node cannot retain links beyond the flattened sequence.
    assert previous is not None
    previous.left = None
    previous.right = None


# ---------------------------------------------------------------------------
# Pattern 3: root-to-leaf state and postorder tree DP
# ---------------------------------------------------------------------------


def has_path_sum(root: Optional[TreeNode], target_sum: int) -> bool:
    """Return whether any root-to-leaf path sums to ``target_sum``.

    Subtracting each node value keeps recursive state local.  The leaf check is
    essential: reaching the target at an internal node is not a valid answer.

    Time: O(n)
    Space: O(h)
    """

    if root is None:
        return False
    remaining = target_sum - root.val
    if root.left is None and root.right is None:
        return remaining == 0
    return has_path_sum(root.left, remaining) or has_path_sum(root.right, remaining)


def sum_numbers(root: Optional[TreeNode]) -> int:
    """Sum all numbers represented by root-to-leaf digit paths.

    Invariant: ``prefix`` is the integer represented by nodes strictly above
    the current node.  Appending the current digit is ``prefix * 10 + value``.

    Time: O(n)
    Space: O(h)
    """

    def visit(node: Optional[TreeNode], prefix: int) -> int:
        if node is None:
            return 0
        current = prefix * 10 + node.val
        if node.left is None and node.right is None:
            return current
        return visit(node.left, current) + visit(node.right, current)

    return visit(root, 0)


def max_path_sum(root: Optional[TreeNode]) -> int:
    """Return the maximum sum of a path between any two nodes.

    Postorder separates two concepts:

    * the value returned to a parent is a one-branch gain, because a parent
      path cannot fork through both of this node's children;
    * the global candidate may use both non-negative child gains and therefore
      form a complete left-node-right path.

    Negative gains are discarded with zero, but the node itself is never
    discarded.  Consequently an all-negative tree correctly returns its least
    negative value.  Empty input is handled as zero for library convenience;
    the LeetCode problem guarantees a non-empty tree.

    Time: O(n)
    Space: O(h)
    """

    if root is None:
        return 0

    best = float("-inf")

    def gain(node: Optional[TreeNode]) -> int:
        nonlocal best
        if node is None:
            return 0

        left_gain = max(gain(node.left), 0)
        right_gain = max(gain(node.right), 0)
        best = max(best, node.val + left_gain + right_gain)
        return node.val + max(left_gain, right_gain)

    gain(root)
    return int(best)


# ---------------------------------------------------------------------------
# Pattern 4: structural shortcuts and ancestor recursion
# ---------------------------------------------------------------------------


def count_complete_tree_nodes(root: Optional[TreeNode]) -> int:
    """Count nodes in a *complete* binary tree faster than a full traversal.

    In a complete tree, equal leftmost and rightmost heights prove the subtree
    is perfect, so its size is ``2**height - 1``.  Otherwise recurse into both
    children.  The completeness precondition matters: equal boundary heights
    do not prove perfection for an arbitrary sparse tree.

    Time: O(log^2 n) for a complete tree
    Space: O(log n) recursion stack
    """

    def left_height(node: Optional[TreeNode]) -> int:
        height = 0
        while node is not None:
            height += 1
            node = node.left
        return height

    def right_height(node: Optional[TreeNode]) -> int:
        height = 0
        while node is not None:
            height += 1
            node = node.right
        return height

    if root is None:
        return 0
    left = left_height(root)
    right = right_height(root)
    if left == right:
        return (1 << left) - 1
    return 1 + count_complete_tree_nodes(root.left) + count_complete_tree_nodes(
        root.right
    )


def lowest_common_ancestor(
    root: Optional[TreeNode], first: TreeNode, second: TreeNode
) -> Optional[TreeNode]:
    """Return the lowest node whose subtree contains both target nodes.

    Identity, not value, identifies targets; duplicate values are therefore
    safe.  If left and right each return a target, the current node is their
    split point.  If only one side returns a node, propagate it upward.

    LeetCode guarantees both targets exist.  With a missing target, this common
    concise formulation returns the one that is present rather than ``None``.

    Time: O(n)
    Space: O(h)
    """

    if root is None or root is first or root is second:
        return root

    left_match = lowest_common_ancestor(root.left, first, second)
    right_match = lowest_common_ancestor(root.right, first, second)
    if left_match is not None and right_match is not None:
        return root
    return left_match if left_match is not None else right_match


# ---------------------------------------------------------------------------
# Pattern 5: level-order BFS
# ---------------------------------------------------------------------------


def right_side_view(root: Optional[TreeNode]) -> list[int]:
    """Return the final visible node at each level.

    BFS makes level boundaries explicit.  Because children are enqueued
    left-to-right, the last node removed in each level is the right-side view.

    Time: O(n)
    Space: O(w)
    """

    if root is None:
        return []

    result: list[int] = []
    queue: deque[TreeNode] = deque([root])
    while queue:
        level_size = len(queue)
        for index in range(level_size):
            node = queue.popleft()
            if node.left is not None:
                queue.append(node.left)
            if node.right is not None:
                queue.append(node.right)
            if index == level_size - 1:
                result.append(node.val)
    return result


def average_of_levels(root: Optional[TreeNode]) -> list[float]:
    """Return the arithmetic mean of values at each depth.

    Time: O(n)
    Space: O(w)
    """

    if root is None:
        return []

    averages: list[float] = []
    queue: deque[TreeNode] = deque([root])
    while queue:
        level_size = len(queue)
        level_sum = 0
        for _ in range(level_size):
            node = queue.popleft()
            level_sum += node.val
            if node.left is not None:
                queue.append(node.left)
            if node.right is not None:
                queue.append(node.right)
        averages.append(level_sum / level_size)
    return averages


def zigzag_level_order(root: Optional[TreeNode]) -> list[list[int]]:
    """Return level values with alternating left-to-right direction.

    Nodes are always traversed in ordinary BFS order; only placement in the
    current result row changes.  This avoids reversing whole levels afterward
    and, more importantly, avoids complicated alternating enqueue rules.

    Time: O(n)
    Space: O(w), excluding output
    """

    if root is None:
        return []

    levels: list[list[int]] = []
    queue: deque[TreeNode] = deque([root])
    left_to_right = True

    while queue:
        level_size = len(queue)
        level: deque[int] = deque()
        for _ in range(level_size):
            node = queue.popleft()
            if left_to_right:
                level.append(node.val)
            else:
                level.appendleft(node.val)
            if node.left is not None:
                queue.append(node.left)
            if node.right is not None:
                queue.append(node.right)
        levels.append(list(level))
        left_to_right = not left_to_right
    return levels


# ---------------------------------------------------------------------------
# Pattern 6: BST ordering and lazy inorder traversal
# ---------------------------------------------------------------------------


def get_minimum_difference(root: Optional[TreeNode]) -> int:
    """Return the minimum absolute difference between values in a BST.

    Inorder visits BST values in increasing order, so the global minimum must
    occur between adjacent visited values.  The iterative stack is preferable
    here because it exposes the previous-value invariant and avoids recursion.

    Raises:
        ValueError: if fewer than two nodes are present.

    Time: O(n)
    Space: O(h)
    """

    stack: list[TreeNode] = []
    current = root
    previous_value: Optional[int] = None
    minimum: Optional[int] = None

    while stack or current is not None:
        while current is not None:
            stack.append(current)
            current = current.left
        current = stack.pop()

        if previous_value is not None:
            difference = current.val - previous_value
            minimum = difference if minimum is None else min(minimum, difference)
        previous_value = current.val
        current = current.right

    if minimum is None:
        raise ValueError("at least two BST nodes are required")
    return minimum


def kth_smallest(root: Optional[TreeNode], k: int) -> int:
    """Return the 1-indexed kth smallest BST value using iterative inorder.

    The stack contains the path of ancestors whose value/right subtree has not
    yet been processed.  Stopping after k visits can be much cheaper than
    materializing all n values.

    Time: O(h + k)
    Space: O(h)
    """

    if k <= 0:
        raise ValueError("k must be positive")

    stack: list[TreeNode] = []
    current = root
    visited = 0
    while stack or current is not None:
        while current is not None:
            stack.append(current)
            current = current.left
        current = stack.pop()
        visited += 1
        if visited == k:
            return current.val
        current = current.right

    raise ValueError("k is larger than the number of nodes")


def is_valid_bst(root: Optional[TreeNode]) -> bool:
    """Return whether every node obeys all ancestor-imposed BST bounds.

    Checking only a node against its parent is insufficient: a deep node can
    violate the root's range.  Each recursive call therefore carries the open
    interval allowed by every ancestor.  ``None`` bounds avoid assumptions
    about the magnitude or type range of integer values.

    Time: O(n)
    Space: O(h)
    """

    def validate(
        node: Optional[TreeNode],
        lower: Optional[int],
        upper: Optional[int],
    ) -> bool:
        if node is None:
            return True
        if lower is not None and node.val <= lower:
            return False
        if upper is not None and node.val >= upper:
            return False
        return validate(node.left, lower, node.val) and validate(
            node.right, node.val, upper
        )

    return validate(root, None, None)


class BSTIterator(Iterator[int]):
    """Lazy inorder iterator using O(h) memory instead of storing all values.

    The stack is maintained so its top is always the next smallest unreturned
    node.  After returning a node, pushing the left spine of its right subtree
    restores that invariant.  Each node is pushed and popped once, giving
    amortized O(1) ``next`` calls even though one call may descend O(h).
    """

    def __init__(self, root: Optional[TreeNode]) -> None:
        self._stack: list[TreeNode] = []
        self._push_left_spine(root)

    def _push_left_spine(self, node: Optional[TreeNode]) -> None:
        while node is not None:
            self._stack.append(node)
            node = node.left

    def next(self) -> int:
        """Return the next smallest value, or raise ``StopIteration``."""

        if not self._stack:
            raise StopIteration
        node = self._stack.pop()
        self._push_left_spine(node.right)
        return node.val

    def has_next(self) -> bool:
        """Return whether another value is available."""

        return bool(self._stack)

    def __next__(self) -> int:
        return self.next()


if __name__ == "__main__":
    example = build_tree_level_order([3, 9, 20, None, None, 15, 7])
    print("level order:", level_order_values(example))
    print("maximum depth:", max_depth(example))
    print("right-side view:", right_side_view(example))
    print("level averages:", average_of_levels(example))
