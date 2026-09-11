"""Targeted tests for the Top Interview 150 linked-list section."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from Problems.LinkedList.arithmetic_and_merge.add_two_numbers import (
    add_two_numbers,
)
from Problems.LinkedList.arithmetic_and_merge.merge_two_sorted_lists import (
    merge_two_lists,
)
from Problems.LinkedList.cache_design.lru_cache import LRUCache
from Problems.LinkedList.fast_slow_pointers.remove_nth_node_from_end import (
    remove_nth_from_end,
)
from Problems.LinkedList.fast_slow_pointers.linked_list_cycle import has_cycle
from Problems.LinkedList.fast_slow_pointers.rotate_list import rotate_right
from Problems.LinkedList.filtering_and_partitioning.partition_list import (
    partition,
)
from Problems.LinkedList.filtering_and_partitioning import (
    remove_duplicates_sorted_list_ii,
)
from Problems.LinkedList.hash_map_cloning.copy_list_with_random_pointer import (
    copy_random_list,
)
from Problems.LinkedList.helpers import (
    ListNode,
    RandomListNode,
    build_linked_list,
    build_random_list,
    linked_list_to_list,
    random_list_to_spec,
)
from Problems.LinkedList.in_place_reversal.reverse_linked_list_ii import (
    reverse_between,
)
from Problems.LinkedList.in_place_reversal.reverse_nodes_in_k_group import (
    reverse_k_group,
)

delete_duplicates = remove_duplicates_sorted_list_ii.delete_duplicates


def values(head: ListNode | None) -> list[int]:
    return linked_list_to_list(head)


class TestAddTwoNumbers:
    def test_different_lengths_and_final_carry(self) -> None:
        first = build_linked_list([9, 9, 9])
        second = build_linked_list([1])

        assert values(add_two_numbers(first, second)) == [0, 0, 0, 1]

    def test_empty_operands(self) -> None:
        assert add_two_numbers(None, None) is None


class TestMergeTwoSortedLists:
    def test_merges_duplicates_and_negative_values(self) -> None:
        first = build_linked_list([-3, 1, 4])
        second = build_linked_list([-2, 1, 5])

        assert values(merge_two_lists(first, second)) == [-3, -2, 1, 1, 4, 5]

    def test_empty_list(self) -> None:
        remaining = build_linked_list([1, 2])
        assert merge_two_lists(None, remaining) is remaining


class TestCopyRandomList:
    def test_deep_copy_preserves_identity_edges(self) -> None:
        original = build_random_list([7, 7, 3], [2, 0, 1])

        copied = copy_random_list(original)

        assert random_list_to_spec(copied) == ([7, 7, 3], [2, 0, 1])
        original_node = original
        copied_node = copied
        while original_node is not None and copied_node is not None:
            assert copied_node is not original_node
            if copied_node.random is not None:
                assert copied_node.random is not original_node.random
            original_node = original_node.next
            copied_node = copied_node.next

        copied.val = 99  # type: ignore[union-attr]
        assert original.val == 7  # type: ignore[union-attr]

    def test_empty_and_external_random_target(self) -> None:
        assert copy_random_list(None) is None

        head = RandomListNode(1)
        head.random = RandomListNode(2)
        with pytest.raises(ValueError, match="outside"):
            copy_random_list(head)


class TestReverseLinkedListII:
    def test_reverses_middle_and_entire_list(self) -> None:
        middle = build_linked_list([1, 2, 3, 4, 5])
        entire = build_linked_list([1, 2, 3])

        assert values(reverse_between(middle, 2, 4)) == [1, 4, 3, 2, 5]
        assert values(reverse_between(entire, 1, 3)) == [3, 2, 1]

    def test_rejects_out_of_range_without_mutating(self) -> None:
        head = build_linked_list([1, 2])
        with pytest.raises(ValueError, match="outside"):
            reverse_between(head, 1, 3)
        assert values(head) == [1, 2]


class TestReverseNodesInKGroup:
    def test_reverses_complete_groups_only(self) -> None:
        head = build_linked_list([1, 2, 3, 4, 5])
        assert values(reverse_k_group(head, 3)) == [3, 2, 1, 4, 5]

    def test_identity_case_and_invalid_k(self) -> None:
        head = build_linked_list([1, 2])
        assert reverse_k_group(head, 1) is head
        with pytest.raises(ValueError, match="at least"):
            reverse_k_group(head, 0)


class TestRemoveNthNodeFromEnd:
    def test_removes_middle_and_head(self) -> None:
        middle = build_linked_list([1, 2, 3, 4, 5])
        head = build_linked_list([1, 2])

        assert values(remove_nth_from_end(middle, 2)) == [1, 2, 3, 5]
        assert values(remove_nth_from_end(head, 2)) == [2]

    def test_single_node_and_invalid_distance(self) -> None:
        assert remove_nth_from_end(ListNode(1), 1) is None
        with pytest.raises(ValueError, match="larger"):
            remove_nth_from_end(build_linked_list([1]), 2)


class TestLinkedListCycle:
    def test_detects_self_and_later_cycle(self) -> None:
        self_cycle = ListNode(1)
        self_cycle.next = self_cycle

        head = build_linked_list([3, 2, 0, -4])
        assert head is not None and head.next is not None
        tail = head
        while tail.next is not None:
            tail = tail.next
        tail.next = head.next

        assert has_cycle(self_cycle)
        assert has_cycle(head)

    def test_rejects_empty_and_acyclic_lists(self) -> None:
        assert not has_cycle(None)
        assert not has_cycle(build_linked_list([1, 2, 3]))


class TestRemoveDuplicatesSortedListII:
    def test_removes_duplicate_runs_at_boundaries(self) -> None:
        head = build_linked_list([1, 1, 2, 3, 4, 4])
        assert values(delete_duplicates(head)) == [2, 3]

    def test_all_duplicates_and_no_duplicates(self) -> None:
        assert delete_duplicates(build_linked_list([2, 2, 2])) is None
        assert values(delete_duplicates(build_linked_list([1, 2, 3]))) == [
            1,
            2,
            3,
        ]


class TestRotateList:
    def test_rotates_with_count_larger_than_length(self) -> None:
        head = build_linked_list([0, 1, 2])
        assert values(rotate_right(head, 4)) == [2, 0, 1]

    def test_empty_zero_and_negative_rotation(self) -> None:
        assert rotate_right(None, 10) is None
        head = build_linked_list([1, 2])
        assert rotate_right(head, 0) is head
        with pytest.raises(ValueError, match="non-negative"):
            rotate_right(head, -1)


class TestPartitionList:
    def test_stable_partition(self) -> None:
        head = build_linked_list([1, 4, 3, 2, 5, 2])
        assert values(partition(head, 3)) == [1, 2, 2, 4, 3, 5]

    def test_all_nodes_on_one_side(self) -> None:
        below = build_linked_list([1, 2])
        above = build_linked_list([3, 4])
        assert values(partition(below, 5)) == [1, 2]
        assert values(partition(above, 3)) == [3, 4]


class TestLRUCache:
    def test_leetcode_sequence_and_get_refresh(self) -> None:
        cache = LRUCache(2)
        cache.put(1, 1)
        cache.put(2, 2)
        assert cache.get(1) == 1
        cache.put(3, 3)
        assert cache.get(2) == -1
        cache.put(4, 4)
        assert cache.get(1) == -1
        assert cache.get(3) == 3
        assert cache.get(4) == 4

    def test_update_capacity_one_and_invalid_capacity(self) -> None:
        cache = LRUCache(1)
        cache.put(1, 1)
        cache.put(1, 10)
        assert len(cache) == 1
        assert cache.get(1) == 10
        cache.put(2, 20)
        assert cache.get(1) == -1
        assert cache.keys_mru_to_lru() == [2]

        with pytest.raises(ValueError, match="capacity"):
            LRUCache(0)
