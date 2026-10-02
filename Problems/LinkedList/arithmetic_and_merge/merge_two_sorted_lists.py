# Pattern identification: two sorted chains -> sentinel-and-tail merge;
# the emitted prefix is sorted, and each head is its chain's next candidate.
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
    # 1. Output: Return one ascending list containing all nodes from both inputs.
    # 2. Structure: Each input is already sorted; its current node is its smallest remaining value.
    # 3. Constraints: Inputs may be empty; relinking is allowed. O(m + n) time and O(1) extra space.
    # 4. Choice: Sorted inputs make only their two heads candidates; attach the smaller at tail and advance that input.
    # 5. Why it works: The smaller head is the smallest unused value, so every attachment keeps order.
    # Once one input ends, the other's entire remaining chain is already sorted.
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
        # 1. Output: Return the merged ascending chain through this compatibility entry point.
        # 2. Structure: list1 and list2 are sorted chains of nodes joined by next links.
        # 3. Constraints: Either may be empty; existing nodes are reused in O(m + n) time.
        # 4. Choice: Delegate to merge_two_lists, which attaches the smaller remaining head.
        # 5. Why it works: The helper keeps the attached prefix sorted and loses no input nodes.
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
