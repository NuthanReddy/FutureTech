# Pattern identification: deletion measured from the end -> sentinel + fixed gap;
# fast stays n links ahead, leaving slow at the target's predecessor at the tail.
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
    # 1. Output: Return the head after removing the nth node counted backward from the end.
    # 2. Structure: A singly linked list only lets us move forward through next links.
    # 3. Constraints: Require 1 <= n <= length; invalid n raises ValueError. O(length) time, O(1) space.
    # 4. Choice: We cannot walk backward from the tail, so start fast n links ahead and move both references together.
    # Start at a placeholder head so slow can precede even the first real node.
    # 5. Why it works: Their n-link gap stays fixed; when fast is at the tail, slow precedes the target.
    # Skipping slow.next removes exactly that node, even when it is the original head.
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
        # 1. Output: Return the list with its nth node from the end removed.
        # 2. Structure: head starts a forward-only chain; n measures a position from the tail.
        # 3. Constraints: The helper checks n against the length; O(length) time and O(1) extra space.
        # 4. Choice: Delegate to remove_nth_from_end and its two references kept n links apart.
        # 5. Why it works: The gap leaves the trailing reference immediately before the node to skip.
        return remove_nth_from_end(head, n)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 4, 5])
    assert linked_list_to_list(remove_nth_from_end(sample, 2)) == [1, 2, 3, 5]
    print("Remove Nth Node From End smoke test passed.")
