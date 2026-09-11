"""LeetCode 21: Merge Two Sorted Lists.

Problem statement:
    Inputs: ``first`` and ``second``, two singly linked lists sorted in
    nondecreasing order.
    Output: one nondecreasing linked list containing every input node.
    Constraints: either list may be empty; each list has at most 50 nodes and
    node values are conventionally in -100 through 100.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def merge_two_lists(
    first: Optional[ListNode],
    second: Optional[ListNode],
) -> Optional[ListNode]:
    """Merge two ascending lists by relinking their existing nodes.

    A copying alternative is simpler when inputs must remain unchanged, but
    in-place relinking gives O(1) auxiliary space.

    Time: O(m + n); extra space: O(1).
    """
    dummy = ListNode()
    tail = dummy

    while first is not None and second is not None:
        if first.val <= second.val:
            tail.next = first
            first = first.next
        else:
            tail.next = second
            second = second.next
        tail = tail.next

    tail.next = first if first is not None else second
    return dummy.next


class Solution:
    """LeetCode-compatible wrapper."""

    def mergeTwoLists(
        self,
        list1: Optional[ListNode],
        list2: Optional[ListNode],
    ) -> Optional[ListNode]:
        return merge_two_lists(list1, list2)


if __name__ == "__main__":
    left = build_linked_list([1, 2, 4])
    right = build_linked_list([1, 3, 4])
    assert linked_list_to_list(merge_two_lists(left, right)) == [
        1,
        1,
        2,
        3,
        4,
        4,
    ]
    print("Merge Two Sorted Lists smoke test passed.")
