"""LeetCode 141: Linked List Cycle.

Problem statement:
    Input: ``head`` of a singly linked list that may have a ``next`` pointer
    leading back to an earlier node.
    Output: ``True`` when the list contains a cycle; otherwise ``False``.
    Constraints: the list has 0 through 10,000 nodes, values are commonly in
    -100,000 through 100,000, and the list must not be modified.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import ListNode, build_linked_list


def has_cycle(head: Optional[ListNode]) -> bool:
    """Return whether ``head`` contains a cycle in its ``next`` pointers.

    Floyd's tortoise-and-hare algorithm uses two traversal speeds. In an
    acyclic list the fast pointer reaches the end; in a cycle it eventually
    catches the slow pointer. A visited-node set is simpler to explain, but it
    needs O(n) extra space.

    Time: O(n); extra space: O(1).
    """
    slow = head
    fast = head

    while fast is not None and fast.next is not None:
        slow = slow.next  # type: ignore[assignment]
        fast = fast.next.next
        if slow is fast:
            return True

    return False


class Solution:
    """LeetCode-compatible wrapper."""

    def hasCycle(self, head: Optional[ListNode]) -> bool:
        return has_cycle(head)


if __name__ == "__main__":
    sample = build_linked_list([3, 2, 0, -4])
    assert sample is not None and sample.next is not None
    tail = sample
    while tail.next is not None:
        tail = tail.next
    tail.next = sample.next
    assert has_cycle(sample)
    print("Linked List Cycle smoke test passed.")
