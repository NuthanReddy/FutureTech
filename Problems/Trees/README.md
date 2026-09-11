# Trees and Tree Traversal

These solutions cover all 20 tree-related exercises in LeetCode's **Top
Interview 150**. They are grouped by the reusable decision being practiced,
not merely by LeetCode's nearby headings.

## Problem statements

Each implementation below includes the same statement in its module
docstring. Inputs, required outputs, and the principal known constraints are
listed here so the README can be used independently as a study guide.

1. **Maximum Depth of Binary Tree** — Input: a binary tree. Output: the number
   of nodes on its longest root-to-leaf path. An empty tree has depth `0`.
2. **Same Tree** — Input: two binary trees. Output: whether structure and
   corresponding values are identical.
3. **Invert Binary Tree** — Input: a binary tree. Output: the root after every
   node's left and right subtrees are swapped in place.
4. **Symmetric Tree** — Input: a binary tree. Output: whether it is a mirror of
   itself around its root.
5. **Construct from Preorder and Inorder** — Input: preorder and inorder
   traversals. Output: the reconstructed root. Values are unique and the
   traversals are assumed to describe one tree.
6. **Construct from Inorder and Postorder** — Input: inorder and postorder
   traversals. Output: the reconstructed root. Values are unique and the
   traversals are assumed consistent.
7. **Populating Next Right Pointers II** — Input: any binary tree. Output: the
   root with each `next` pointer connected to the next node on that level, or
   `None` at the end.
8. **Flatten Binary Tree to Linked List** — Input: a binary tree. Output: the
   same tree rewired in place as a preorder, right-only linked list with all
   left pointers cleared.
9. **Path Sum** — Input: a binary tree and integer target. Output: whether a
   root-to-leaf path sums to the target; an internal matching prefix is not
   sufficient.
10. **Sum Root to Leaf Numbers** — Input: a digit-valued binary tree. Output:
    the sum of the numbers represented by all root-to-leaf paths.
11. **Binary Tree Maximum Path Sum** — Input: a non-empty binary tree. Output:
    the maximum sum of a connected path that may start and end at any nodes.
12. **Count Complete Tree Nodes** — Input: a complete binary tree. Output: its
    node count. All earlier levels are full and the last level is left-filled.
13. **Lowest Common Ancestor of a Binary Tree** — Input: a binary tree and two
    existing node references. Output: their lowest common ancestor; node
    identity matters when values repeat.
14. **Binary Tree Right Side View** — Input: a binary tree. Output: the value
    visible at the rightmost position of every depth, top down.
15. **Average of Levels in Binary Tree** — Input: a binary tree. Output: the
    arithmetic mean of values at each depth, top down.
16. **Binary Tree Zigzag Level Order Traversal** — Input: a binary tree.
    Output: level-order rows alternating left-to-right and right-to-left.
17. **Minimum Absolute Difference in BST** — Input: a BST with at least two
    nodes. Output: the minimum absolute difference between any two values.
18. **Kth Smallest Element in a BST** — Input: a BST and 1-indexed `k`.
    Output: the kth value in sorted order, with `1 <= k <= node count`.
19. **Validate Binary Search Tree** — Input: a binary tree. Output: whether
    every node obeys strict ordering against all ancestor-imposed bounds.
20. **Binary Search Tree Iterator** — Input: a BST. Output: a lazy iterator
    exposing sorted `next` values and `has_next`; storage should be `O(h)`.

## Pattern map

| Pattern | Problem | Primary choice | Time | Extra space |
| --- | --- | --- | ---: | ---: |
| Structural DFS | Maximum Depth of Binary Tree | recursive height | O(n) | O(h) |
| Structural DFS | Same Tree | recursive paired nodes | O(n) | O(h) |
| Structural DFS | Invert Binary Tree | iterative BFS mutation | O(n) | O(w) |
| Structural DFS | Symmetric Tree | iterative mirror pairs | O(n) | O(w) |
| Traversal reconstruction | Construct from Preorder and Inorder | indexed recursive ranges | O(n) | O(n) |
| Traversal reconstruction | Construct from Inorder and Postorder | indexed recursive ranges | O(n) | O(n) |
| Pointer threading | Populating Next Right Pointers II | reuse current `next` chain | O(n) | O(1) |
| Preorder mutation | Flatten Binary Tree to Linked List | explicit DFS stack | O(n) | O(h), O(n) worst |
| Root-to-leaf state | Path Sum | subtract remaining target | O(n) | O(h) |
| Root-to-leaf state | Sum Root to Leaf Numbers | carry numeric prefix | O(n) | O(h) |
| Postorder tree DP | Binary Tree Maximum Path Sum | return one branch, record two | O(n) | O(h) |
| Complete-tree shortcut | Count Complete Tree Nodes | compare boundary heights | O(log² n) | O(log n) |
| Ancestor recursion | Lowest Common Ancestor | merge subtree matches | O(n) | O(h) |
| Level-order BFS | Binary Tree Right Side View | final node per level | O(n) | O(w) |
| Level-order BFS | Average of Levels | fixed-size level batches | O(n) | O(w) |
| Level-order BFS | Zigzag Level Order Traversal | alternating deque placement | O(n) | O(w) |
| BST inorder | Minimum Absolute Difference in BST | adjacent sorted values | O(n) | O(h) |
| BST inorder | Kth Smallest Element in a BST | early-stop inorder | O(h + k) | O(h) |
| BST bounds | Validate Binary Search Tree | ancestor range propagation | O(n) | O(h) |
| Lazy BST inorder | Binary Search Tree Iterator | stored left spine | O(1) amortized next | O(h) |

Here, `h` is tree height and `w` is maximum width. A balanced tree has
`h = O(log n)`; a skewed tree has `h = O(n)`.

## Recursive versus iterative choices

- **Recursion** is clearest when a parent answer is composed from child
  answers: height, path gain, reconstruction ranges, and ancestor matching.
- **Iteration** is preferred when traversal state itself is the lesson:
  level boundaries, mirror pairs, lazy inorder, and in-place breadth-first
  mutation.
- Recursive Python code has a practical depth limit. For adversarial skewed
  trees with thousands of nodes, translate structural DFS to an explicit
  stack even when both versions have the same asymptotic `O(h)` space.
- `connect_next` is a deliberate exception to ordinary BFS. It uses the
  pointers being produced to traverse a level, which reduces queue space from
  `O(w)` to `O(1)`.
- `flatten` uses a stack because its preorder invariant is easy to inspect.
  A Morris/predecessor-splicing solution removes the stack but is easier to
  corrupt through incorrect pointer rewiring.

## Important invariants and edge cases

- Reconstruction assumes **unique values**, as the original problems do.
  Length, membership, and duplicate mismatches raise `ValueError`.
- Path Sum accepts only a **root-to-leaf** completion; matching an internal
  prefix is not enough.
- Maximum Path Sum returns only one branch to a parent, while its global
  candidate may join two branches.
- Count Complete Tree Nodes relies on the tree being **complete**. Its
  equal-height shortcut is not valid for arbitrary sparse trees.
- BST validation carries bounds from every ancestor. Parent-only comparisons
  miss violations such as `[5, 1, 6, None, None, 3, 7]`.
- Empty trees produce the natural empty/zero/true result where the problem
  permits it. Operations requiring data (`kth_smallest` and minimum
  difference) raise `ValueError` for invalid requests.
- Lowest Common Ancestor compares node identity, so duplicate values are safe.
  The concise implementation follows LeetCode's guarantee that both targets
  exist.

## Existing overlap and repository critique

`Problems/BST/validate_bst.py` and `Problems/BST/kth_smallest_in_bst.py`
already solve LeetCode 98 and 230. They are useful standalone scripts, but
each defines a separate `TreeNode` and level-order builder. This Top150 module
keeps those files unchanged and centralizes a reusable node, stricter invalid
input behavior, shared helpers, and pattern-level documentation.

The overlap is intentional:

- use `Problems/BST/` for isolated, script-first study;
- use `Problems/Trees/` to compare the complete interview set and reuse
  trees across exercises.

The wider repository also contains implementation-focused trees under
`DataStructures/` and tests for B-trees, red-black trees, Fenwick trees, and
segment trees. Those structures solve different storage/range-query concerns;
they are not substitutes for binary-tree traversal exercises.

Coverage gaps are now mainly outside Top Interview 150: serialization,
duplicate-key BST policies, iterative postorder templates, trie problems, and
balanced-tree rotations. They should remain separate additions rather than be
mixed into this interview-list module.

## Run

From the repository root:

```powershell
python -m Problems.Trees.solution
uv run pytest tests/test_top150_trees.py
```
