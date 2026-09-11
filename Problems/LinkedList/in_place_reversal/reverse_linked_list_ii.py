"""LeetCode 92: Reverse Linked List II.

Problem statement:
    Input: ``head`` of a singly linked list and one-based inclusive positions
    ``left`` and ``right``.
    Output: the original list with only nodes from ``left`` through ``right``
    reversed.
    Constraints: the list has 1 through 500 nodes; ``1 <= left <= right <= n``;
    values are conventionally in -500 through 500.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def reverse_between(
    head: Optional[ListNode],
    left: int,
    right: int,
) -> Optional[ListNode]:
    """Reverse the inclusive one-based range ``left..right`` in place.

    Head-insertion keeps the node before the range fixed and repeatedly moves
    the next range node to the front.

    Time: O(n); extra space: O(1).
    """
    if left < 1 or right < left:
        raise ValueError("require 1 <= left <= right")

    length = 0
    current = head
    while current is not None:
        length += 1
        current = current.next
    if right > length:
        raise ValueError("right position is outside the list")
    if left == right:
        return head

    dummy = ListNode(next=head)
    before_range = dummy
    for _ in range(left - 1):
        before_range = before_range.next  # type: ignore[assignment]

    range_tail = before_range.next
    for _ in range(right - left):
        moving = range_tail.next  # type: ignore[union-attr]
        range_tail.next = moving.next  # type: ignore[union-attr]
        moving.next = before_range.next  # type: ignore[union-attr]
        before_range.next = moving

    return dummy.next


class Solution:
    """LeetCode-compatible wrapper."""

    def reverseBetween(
        self,
        head: Optional[ListNode],
        left: int,
        right: int,
    ) -> Optional[ListNode]:
        return reverse_between(head, left, right)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 4, 5])
    assert linked_list_to_list(reverse_between(sample, 2, 4)) == [1, 4, 3, 2, 5]
    print("Reverse Linked List II smoke test passed.")
