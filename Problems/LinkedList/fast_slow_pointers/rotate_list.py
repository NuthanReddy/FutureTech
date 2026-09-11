"""LeetCode 61: Rotate List.

Problem statement:
    Input: ``head`` of a singly linked list and non-negative rotation count
    ``k``.
    Output: the list after moving the final ``k`` nodes to the front, with
    rotations reduced modulo the list length.
    Constraints: the list has 0 through 500 nodes, values are conventionally
    in -100 through 100, and ``0 <= k <= 2 * 10^9``.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def rotate_right(
    head: Optional[ListNode],
    k: int,
) -> Optional[ListNode]:
    """Rotate a list right by ``k`` positions.

    Temporarily closing the list into a cycle makes rotation a single cut at
    the new tail.

    Time: O(n); extra space: O(1).
    """
    if k < 0:
        raise ValueError("k must be non-negative")
    if head is None or head.next is None or k == 0:
        return head

    length = 1
    old_tail = head
    while old_tail.next is not None:
        old_tail = old_tail.next
        length += 1

    shift = k % length
    if shift == 0:
        return head

    old_tail.next = head
    steps_to_new_tail = length - shift - 1
    new_tail = head
    for _ in range(steps_to_new_tail):
        new_tail = new_tail.next  # type: ignore[assignment]

    new_head = new_tail.next
    new_tail.next = None
    return new_head


class Solution:
    """LeetCode-compatible wrapper."""

    def rotateRight(
        self,
        head: Optional[ListNode],
        k: int,
    ) -> Optional[ListNode]:
        return rotate_right(head, k)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 4, 5])
    assert linked_list_to_list(rotate_right(sample, 2)) == [4, 5, 1, 2, 3]
    print("Rotate List smoke test passed.")
