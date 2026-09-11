"""LeetCode 86: Partition List.

Problem statement:
    Input: ``head`` of a singly linked list and pivot value ``x``.
    Output: a stable partition where all nodes with values below ``x`` precede
    all remaining nodes, preserving order within both partitions.
    Constraints: the list has 0 through 200 nodes and node/pivot values are
    conventionally in -100 through 100.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def partition(head: Optional[ListNode], x: int) -> Optional[ListNode]:
    """Stable-partition nodes into values below ``x`` and values at least ``x``.

    Two output chains preserve relative order without allocating data nodes.

    Time: O(n); extra space: O(1).
    """
    before_dummy = ListNode()
    before_tail = before_dummy
    after_dummy = ListNode()
    after_tail = after_dummy
    current = head

    while current is not None:
        next_node = current.next
        current.next = None
        if current.val < x:
            before_tail.next = current
            before_tail = current
        else:
            after_tail.next = current
            after_tail = current
        current = next_node

    before_tail.next = after_dummy.next
    return before_dummy.next


class Solution:
    """LeetCode-compatible wrapper."""

    def partition(
        self,
        head: Optional[ListNode],
        x: int,
    ) -> Optional[ListNode]:
        return partition(head, x)


if __name__ == "__main__":
    sample = build_linked_list([1, 4, 3, 2, 5, 2])
    assert linked_list_to_list(partition(sample, 3)) == [1, 2, 2, 4, 3, 5]
    print("Partition List smoke test passed.")
