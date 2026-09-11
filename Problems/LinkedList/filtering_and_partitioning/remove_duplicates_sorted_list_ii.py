"""LeetCode 82: Remove Duplicates from Sorted List II.

Problem statement:
    Input: ``head`` of a singly linked list sorted in nondecreasing order.
    Output: the list containing only values that appeared exactly once; every
    node belonging to a duplicate-value run is removed.
    Constraints: the list has 0 through 300 nodes and values are conventionally
    in -100 through 100.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def delete_duplicates(head: Optional[ListNode]) -> Optional[ListNode]:
    """Remove every value that appears more than once in a sorted list.

    The sentinel lets the predecessor remain valid even when a duplicate run
    starts at the original head.

    Time: O(n); extra space: O(1).
    """
    dummy = ListNode(next=head)
    previous_unique = dummy
    current = head

    while current is not None:
        if current.next is not None and current.val == current.next.val:
            duplicate_value = current.val
            while current is not None and current.val == duplicate_value:
                current = current.next
            previous_unique.next = current
        else:
            previous_unique = current
            current = current.next

    return dummy.next


class Solution:
    """LeetCode-compatible wrapper."""

    def deleteDuplicates(
        self,
        head: Optional[ListNode],
    ) -> Optional[ListNode]:
        return delete_duplicates(head)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 3, 4, 4, 5])
    assert linked_list_to_list(delete_duplicates(sample)) == [1, 2, 5]
    print("Remove Duplicates from Sorted List II smoke test passed.")
