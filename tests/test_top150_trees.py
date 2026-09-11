"""Focused tests for the Top Interview 150 tree pattern collection."""

import pytest

from Problems.Trees.solution import (
    BSTIterator,
    TreeNode,
    average_of_levels,
    build_tree_inorder_postorder,
    build_tree_level_order,
    build_tree_preorder_inorder,
    connect_next,
    count_complete_tree_nodes,
    flatten,
    get_minimum_difference,
    has_path_sum,
    invert_tree,
    is_same_tree,
    is_symmetric,
    is_valid_bst,
    kth_smallest,
    level_order_values,
    lowest_common_ancestor,
    max_depth,
    max_path_sum,
    right_side_view,
    sum_numbers,
    zigzag_level_order,
)


def test_level_order_helpers_and_empty_inputs() -> None:
    root = build_tree_level_order([1, 2, 3, None, 4])
    assert level_order_values(root) == [1, 2, 3, None, 4]
    assert build_tree_level_order([]) is None
    assert level_order_values(None) == []

    with pytest.raises(ValueError):
        build_tree_level_order([None, 1])


def test_structural_dfs_problems() -> None:
    root = build_tree_level_order([3, 9, 20, None, None, 15, 7])
    copy = build_tree_level_order([3, 9, 20, None, None, 15, 7])
    different = build_tree_level_order([3, 9, 20, None, None, 7, 15])

    assert max_depth(root) == 3
    assert max_depth(None) == 0
    assert is_same_tree(root, copy)
    assert not is_same_tree(root, different)

    assert level_order_values(invert_tree(copy)) == [3, 20, 9, 7, 15]
    assert invert_tree(None) is None
    assert is_symmetric(build_tree_level_order([1, 2, 2, 3, 4, 4, 3]))
    assert not is_symmetric(build_tree_level_order([1, 2, 2, None, 3, None, 3]))
    assert is_symmetric(None)


def test_reconstruct_tree_from_preorder_and_inorder() -> None:
    root = build_tree_preorder_inorder(
        [3, 9, 20, 15, 7],
        [9, 3, 15, 20, 7],
    )
    assert level_order_values(root) == [3, 9, 20, None, None, 15, 7]
    assert build_tree_preorder_inorder([], []) is None


def test_reconstruct_tree_from_inorder_and_postorder() -> None:
    root = build_tree_inorder_postorder(
        [9, 3, 15, 20, 7],
        [9, 15, 7, 20, 3],
    )
    assert level_order_values(root) == [3, 9, 20, None, None, 15, 7]
    assert build_tree_inorder_postorder([], []) is None


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ([1], []),
        ([1, 1], [1, 1]),
        ([1, 2], [1, 3]),
        ([1, 2, 3], [3, 1, 2]),
    ],
)
def test_reconstruction_rejects_invalid_traversals(
    first: list[int], second: list[int]
) -> None:
    with pytest.raises(ValueError):
        build_tree_preorder_inorder(first, second)


def test_connect_next_right_pointers_on_sparse_tree() -> None:
    root = build_tree_level_order([1, 2, 3, 4, 5, None, 7])
    assert root is not None

    connect_next(root)

    assert root.next is None
    assert root.left is not None and root.right is not None
    assert root.left.next is root.right
    assert root.left.left is not None and root.left.right is not None
    assert root.right.right is not None
    assert root.left.left.next is root.left.right
    assert root.left.right.next is root.right.right
    assert root.right.right.next is None


def test_flatten_uses_preorder_and_clears_left_links() -> None:
    root = build_tree_level_order([1, 2, 5, 3, 4, None, 6])
    flatten(root)

    values = []
    current = root
    while current is not None:
        assert current.left is None
        values.append(current.val)
        current = current.right
    assert values == [1, 2, 3, 4, 5, 6]
    assert flatten(None) is None


def test_root_to_leaf_and_postorder_path_problems() -> None:
    path_tree = build_tree_level_order(
        [5, 4, 8, 11, None, 13, 4, 7, 2, None, None, None, 1]
    )
    assert has_path_sum(path_tree, 22)
    assert not has_path_sum(path_tree, 5)
    assert not has_path_sum(None, 0)

    assert sum_numbers(build_tree_level_order([4, 9, 0, 5, 1])) == 1026
    assert sum_numbers(None) == 0

    assert max_path_sum(build_tree_level_order([-10, 9, 20, None, None, 15, 7])) == 42
    assert max_path_sum(build_tree_level_order([-3])) == -3
    assert max_path_sum(None) == 0


def test_complete_node_count_and_lowest_common_ancestor() -> None:
    complete = build_tree_level_order([1, 2, 3, 4, 5, 6])
    assert count_complete_tree_nodes(complete) == 6
    assert count_complete_tree_nodes(None) == 0

    root = build_tree_level_order([3, 5, 1, 6, 2, 0, 8, None, None, 7, 4])
    assert root is not None and root.left is not None and root.right is not None
    assert lowest_common_ancestor(root, root.left, root.right) is root
    assert root.left.right is not None and root.left.right.right is not None
    assert (
        lowest_common_ancestor(root, root.left, root.left.right.right) is root.left
    )

    duplicate_root = TreeNode(1, TreeNode(2), TreeNode(2))
    assert (
        lowest_common_ancestor(
            duplicate_root, duplicate_root.left, duplicate_root.right
        )
        is duplicate_root
    )


def test_level_order_bfs_views() -> None:
    root = build_tree_level_order([3, 9, 20, None, None, 15, 7])
    assert right_side_view(root) == [3, 20, 7]
    assert average_of_levels(root) == pytest.approx([3.0, 14.5, 11.0])
    assert zigzag_level_order(root) == [[3], [20, 9], [15, 7]]

    assert right_side_view(None) == []
    assert average_of_levels(None) == []
    assert zigzag_level_order(None) == []


def test_bst_inorder_and_bounds_problems() -> None:
    bst = build_tree_level_order([4, 2, 6, 1, 3])
    assert get_minimum_difference(bst) == 1
    assert kth_smallest(bst, 1) == 1
    assert kth_smallest(bst, 5) == 6
    assert is_valid_bst(bst)

    # The value 3 is in the root's right subtree and violates the lower bound 5.
    invalid = build_tree_level_order([5, 1, 6, None, None, 3, 7])
    assert not is_valid_bst(invalid)
    assert is_valid_bst(None)

    with pytest.raises(ValueError):
        get_minimum_difference(TreeNode(1))
    with pytest.raises(ValueError):
        kth_smallest(bst, 0)
    with pytest.raises(ValueError):
        kth_smallest(bst, 6)


def test_bst_iterator_is_lazy_and_python_iterable() -> None:
    root = build_tree_level_order([7, 3, 15, None, None, 9, 20])
    iterator = BSTIterator(root)

    assert iterator.has_next()
    assert iterator.next() == 3
    assert list(iterator) == [7, 9, 15, 20]
    assert not iterator.has_next()
    with pytest.raises(StopIteration):
        iterator.next()
