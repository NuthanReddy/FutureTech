# Binary Search Tree Problems

Standalone solutions using BST ordering rather than arbitrary-tree traversal.

## Pattern identification steps

1. Recognize sorted-order rank queries or a request to verify strict BST ordering.
2. Choose iterative inorder for rank; choose bounded DFS for validation.
3. Keep popped inorder values sorted, or carry the open interval allowed by every ancestor.
4. Check limits: rank needs a valid BST and 1 <= k <= node count; parent-only checks miss violations, duplicates fail strict validation, and skewed recursion can overflow.

| Problem / file | Identification cue | Chosen pattern |
| --- | --- | --- |
| Kth Smallest — `kth_smallest_in_bst.py` | kth value in sorted BST order | Early-stop iterative inorder |
| Validate BST — `validate_bst.py` | All descendants must obey ancestor ordering | DFS with strict lower/upper bounds |
