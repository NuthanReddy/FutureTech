# Pattern identification: reverse one bounded range -> sentinel-based head insertion;
# the predecessor stays fixed and the original range head remains the range tail.
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
    # 1. Output: Return the list with only the inclusive positions left through right reversed.
    # 2. Structure: Positions are one-based in a forward chain; reversing means changing next links.
    # 3. Constraints: Validate 1 <= left <= right <= length before changing links; O(n) time, O(1) space.
    # 4. Choice: Only one bounded section changes, so hold before_range fixed and move each next range node to its front.
    # range_tail remains the original first node, avoiding a search for where to reconnect the section.
    # 5. Why it works: range_tail remains the original first node; each move extends the reversed prefix.
    # Nodes outside the range keep their order, and a one-position range needs no rewiring.
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
        # Unlink moving from its old place before inserting it at the range's front.
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
        # 1. Output: Return the list with the selected positions reversed.
        # 2. Structure: left and right bound a one-based section of the next-linked chain.
        # 3. Constraints: The helper validates the range; O(n) time and O(1) extra space.
        # 4. Choice: Delegate to reverse_between, which moves successive range nodes to its front.
        # 5. Why it works: Each move grows the reversed section while leaving the outside chain connected.
        return reverse_between(head, left, right)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 4, 5])
    assert linked_list_to_list(reverse_between(sample, 2, 4)) == [1, 4, 3, 2, 5]
    print("Reverse Linked List II smoke test passed.")
