"""LeetCode 25: Reverse Nodes in k-Group.

Problem statement:
    Input: ``head`` of a singly linked list and positive group size ``k``.
    Output: the list with every complete consecutive group of ``k`` nodes
    reversed; a final shorter group remains in its original order.
    Constraints: the list has 1 through 5,000 nodes, ``1 <= k <= n``, and
    values are conventionally in 0 through 1,000.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def reverse_k_group(
    head: Optional[ListNode],
    k: int,
) -> Optional[ListNode]:
    """Reverse each complete group of ``k`` nodes in place.

    An incomplete final group is preserved. The group look-ahead prevents any
    partial mutation before confirming that a full group exists.

    Time: O(n); extra space: O(1).
    """
    if k < 1:
        raise ValueError("k must be at least 1")
    if k == 1 or head is None:
        return head

    dummy = ListNode(next=head)
    group_before = dummy

    while True:
        kth = group_before
        for _ in range(k):
            kth = kth.next
            if kth is None:
                return dummy.next

        group_after = kth.next
        previous = group_after
        current = group_before.next

        while current is not group_after:
            next_node = current.next  # type: ignore[union-attr]
            current.next = previous  # type: ignore[union-attr]
            previous = current
            current = next_node

        old_group_head = group_before.next
        group_before.next = kth
        group_before = old_group_head  # type: ignore[assignment]


class Solution:
    """LeetCode-compatible wrapper."""

    def reverseKGroup(
        self,
        head: Optional[ListNode],
        k: int,
    ) -> Optional[ListNode]:
        return reverse_k_group(head, k)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 4, 5])
    assert linked_list_to_list(reverse_k_group(sample, 2)) == [2, 1, 4, 3, 5]
    print("Reverse Nodes in k-Group smoke test passed.")
