"""LeetCode 19: Remove Nth Node From End of List.

Problem statement:
    Input: ``head`` of a singly linked list and positive ``n``.
    Output: the list after deleting its ``n``th node measured from the end.
    Constraints: the list has 1 through 30 nodes, ``1 <= n <= list length``,
    and node values are conventionally in 0 through 100.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def remove_nth_from_end(
    head: Optional[ListNode],
    n: int,
) -> Optional[ListNode]:
    """Remove the ``n``th node from the end using a fixed pointer gap.

    Time: O(length); extra space: O(1).
    """
    if n < 1:
        raise ValueError("n must be at least 1")

    dummy = ListNode(next=head)
    fast: Optional[ListNode] = dummy
    for _ in range(n):
        fast = fast.next
        if fast is None:
            raise ValueError("n is larger than the list length")

    slow = dummy
    while fast.next is not None:
        fast = fast.next
        slow = slow.next  # type: ignore[assignment]

    target = slow.next
    slow.next = target.next  # type: ignore[union-attr]
    return dummy.next


class Solution:
    """LeetCode-compatible wrapper."""

    def removeNthFromEnd(
        self,
        head: Optional[ListNode],
        n: int,
    ) -> Optional[ListNode]:
        return remove_nth_from_end(head, n)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 4, 5])
    assert linked_list_to_list(remove_nth_from_end(sample, 2)) == [1, 2, 3, 5]
    print("Remove Nth Node From End smoke test passed.")
