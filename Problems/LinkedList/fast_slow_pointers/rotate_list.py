# Pattern identification: cyclic shift by possibly large k -> measure, modulo, ring cut;
# the ring preserves node order; cutting after length - shift nodes restores a list.
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
    # 1. Output: Return the chain after moving its last k positions to the front.
    # 2. Structure: Nodes form a forward chain; we can count its length and remember its tail.
    # 3. Constraints: k must be non-negative; empty/single-node lists stay unchanged. O(n) time, O(1) space.
    # 4. Choice: Rotation keeps circular node order, so reduce k modulo length, join tail to head, and cut at the new tail.
    # 5. Why it works: Whole-length rotations change nothing; the temporary ring preserves every node's order.
    # Cutting after length - shift nodes moves exactly the final shift nodes to the front.
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
        # 1. Output: Return the head after k right rotations.
        # 2. Structure: head names the first node of a next-linked chain.
        # 3. Constraints: Non-negative k may exceed the length; O(n) time and O(1) extra space.
        # 4. Choice: Delegate to rotate_right, which reduces k and cuts a temporary ring.
        # 5. Why it works: The helper's cut preserves order and makes the old suffix the new prefix.
        return rotate_right(head, k)


if __name__ == "__main__":
    sample = build_linked_list([1, 2, 3, 4, 5])
    assert linked_list_to_list(rotate_right(sample, 2)) == [4, 5, 1, 2, 3]
    print("Rotate List smoke test passed.")
