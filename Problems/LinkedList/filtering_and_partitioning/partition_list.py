# Pattern identification: pivot split with stable order -> two sentinel output chains;
# each detached node enters exactly one chain, in its original encounter order.
"""LeetCode 86: Partition List.

Problem statement:
    Input: ``head`` of a singly linked list and pivot value ``x``.
    Output: a stable partition where all nodes with values below ``x`` precede
    all remaining nodes, preserving order within both partitions.
    Constraints: the list has 0 through 200 nodes and node/pivot values are
    conventionally in -100 through 100.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    ListNode,
    build_linked_list,
    linked_list_to_list,
)


def partition(head: Optional[ListNode], x: int) -> Optional[ListNode]:
    """Stable-partition nodes into values below ``x`` and values at least ``x``.

    Two output chains preserve relative order without allocating data nodes.

    Time: O(n); extra space: O(1).
    """
    # 1. Output: Return nodes below x followed by nodes at least x, preserving order in each group.
    # 2. Structure: A forward chain can be split by changing next links rather than copying values.
    # 3. Constraints: Empty input is valid; reuse nodes in O(n) time and O(1) extra space.
    # 4. Choice: Order must stay unchanged within each group, so append to two tails rather than sorting the nodes.
    # Save the next node before detaching current; append to its group, then join the finished chains.
    # 5. Why it works: Each visited node enters exactly one group at its end, so neither group changes order.
    # Detaching before appending prevents old links from crossing groups or creating a loop.
    before_dummy = ListNode()
    before_tail = before_dummy
    after_dummy = ListNode()
    after_tail = after_dummy
    current = head

    while current is not None:
        next_node = current.next
        current.next = None
        if current.val < x:
            before_tail.next = current
            before_tail = current
        else:
            after_tail.next = current
            after_tail = current
        current = next_node

    before_tail.next = after_dummy.next
    return before_dummy.next


class Solution:
    """LeetCode-compatible wrapper."""

    def partition(
        self,
        head: Optional[ListNode],
        x: int,
    ) -> Optional[ListNode]:
        # 1. Output: Return a stable split with values below x first.
        # 2. Structure: head starts a chain of nodes; x selects which output group each node joins.
        # 3. Constraints: Nodes are reused; the helper uses O(n) time and O(1) extra space.
        # 4. Choice: Call partition, which appends nodes to two separate chains and joins them.
        # 5. Why it works: Appending in encounter order preserves the order within each group.
        return partition(head, x)


if __name__ == "__main__":
    sample = build_linked_list([1, 4, 3, 2, 5, 2])
    assert linked_list_to_list(partition(sample, 3)) == [1, 2, 2, 4, 3, 5]
    print("Partition List smoke test passed.")
