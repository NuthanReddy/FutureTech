# Pattern identification: remove every repeated value in sorted input -> skip equal runs;
# the retained prefix contains only singleton runs, with a sentinel predecessor.
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
    # 1. Output: Return only nodes whose values appear exactly once, not one copy of each repeated value.
    # 2. Structure: Sorted values put all copies of a value in one consecutive run.
    # 3. Constraints: Assume sorted input; an empty list returns None. O(n) time and O(1) extra space.
    # 4. Choice: Equal values are adjacent, so skip each whole repeated run using the last retained node's next link.
    # A singleton advances the retained node; no separate frequency dictionary is needed.
    # 5. Why it works: The retained prefix contains only singleton runs; no later copy can exist in sorted input.
    # A placeholder before head lets us discard a repeated run at the start as well.
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
        # 1. Output: Return the chain after removing every value that occurred more than once.
        # 2. Structure: Sorted nodes group equal values into adjacent runs.
        # 3. Constraints: Empty input is valid; the helper takes O(n) time and O(1) extra space.
        # 4. Choice: Call delete_duplicates to skip repeated runs while linking singleton runs.
        # 5. Why it works: Each complete run is examined before deciding whether any node may remain.
        return delete_duplicates(head)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 3, 4, 4, 5])
    assert linked_list_to_list(delete_duplicates(sample)) == [1, 2, 5]
    print("Remove Duplicates from Sorted List II smoke test passed.")
