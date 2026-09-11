"""LeetCode 2: Add Two Numbers.

Problem statement:
    Inputs: ``first`` and ``second``, non-empty linked lists whose nodes store
    one decimal digit in reverse order.
    Output: a newly allocated reverse-order linked list representing their sum.
    Constraints: each digit is 0 through 9; the lists have at most 100 nodes.

Digits are stored least-significant first. Carry propagation is therefore a
single forward pass, just like column addition performed from right to left.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def add_two_numbers(
    first: Optional[ListNode],
    second: Optional[ListNode],
) -> Optional[ListNode]:
    """Return the sum as a newly allocated reverse-order digit list.

    Time: O(max(m, n)); extra space: O(max(m, n)) for the result.
    """
    dummy = ListNode()
    tail = dummy
    carry = 0

    while first is not None or second is not None or carry:
        first_digit = first.val if first is not None else 0
        second_digit = second.val if second is not None else 0
        carry, digit = divmod(first_digit + second_digit + carry, 10)

        tail.next = ListNode(digit)
        tail = tail.next

        first = first.next if first is not None else None
        second = second.next if second is not None else None

    return dummy.next


class Solution:
    """LeetCode-compatible wrapper."""

    def addTwoNumbers(
        self,
        l1: Optional[ListNode],
        l2: Optional[ListNode],
    ) -> Optional[ListNode]:
        return add_two_numbers(l1, l2)


if __name__ == "__main__":
    left = build_linked_list([2, 4, 3])
    right = build_linked_list([5, 6, 4])
    assert linked_list_to_list(add_two_numbers(left, right)) == [7, 0, 8]
    print("Add Two Numbers smoke test passed.")
