# Pattern identification: arbitrary cross-links need a deep copy -> two-pass identity map;
# each original owns one clone before next/random edges are wired to clones only.
"""LeetCode 138: Copy List with Random Pointer.

Problem statement:
    Input: ``head`` of a linked list whose nodes have ``next`` and ``random``
    pointers, where ``random`` is null or references a node in the same list.
    Output: the head of a deep copy that preserves both pointer relationships.
    Constraints: the list has 0 through 1,000 nodes and values are commonly
    constrained to -10,000 through 10,000.
"""

from __future__ import annotations

from typing import Optional

from Problems.LinkedList.helpers import (
    RandomListNode,
    build_random_list,
    random_list_to_spec,
)


def copy_random_list(
    head: Optional[RandomListNode],
) -> Optional[RandomListNode]:
    """Deep-copy both ``next`` and ``random`` edges.

    The identity map is easier to audit than the O(1)-space interleaving
    technique. The latter is useful under strict memory constraints but
    temporarily mutates the input list.

    Time: O(n); extra space: O(n).
    """
    # 1. Output: Return a new list with the same values, next links, and random links, sharing no nodes.
    # 2. Structure: next walks the chain; random may name any node in that chain or None.
    # 3. Constraints: Reject next-link cycles and outside random targets; O(n) time and O(n) extra space.
    # 4. Choice: Random targets may not be copied yet, so first map every original to a copy, then translate both links.
    # 5. Why it works: Every original has exactly one copy before links are set, even for backward/self links.
    # Translated links point only to copies; empty input needs no copy and returns None.
    if head is None:
        return None

    copies: dict[RandomListNode, RandomListNode] = {}
    current = head
    while current is not None:
        if current in copies:
            raise ValueError("cycle detected in next pointers")
        copies[current] = RandomListNode(current.val)
        current = current.next

    current = head
    while current is not None:
        clone = copies[current]
        clone.next = copies.get(current.next)
        if current.random is not None and current.random not in copies:
            raise ValueError("random pointer targets a node outside the list")
        clone.random = copies.get(current.random)
        current = current.next

    return copies[head]


class Solution:
    """LeetCode-compatible wrapper."""

    def copyRandomList(
        self,
        head: Optional[RandomListNode],
    ) -> Optional[RandomListNode]:
        # 1. Output: Return a deep copy through the LeetCode-compatible entry point.
        # 2. Structure: Each node has a next link and an optional arbitrary random link.
        # 3. Constraints: The helper validates link targets; O(n) time and O(n) additional storage.
        # 4. Choice: Call copy_random_list to allocate copies first and translate references second.
        # 5. Why it works: One copy per original preserves shared targets without sharing original nodes.
        return copy_random_list(head)


if __name__ == "__main__":
    sample = build_random_list(
        [7, 13, 11, 10, 1],
        [None, 0, 4, 2, 0],
    )
    copied = copy_random_list(sample)
    assert random_list_to_spec(copied) == (
        [7, 13, 11, 10, 1],
        [None, 0, 4, 2, 0],
    )
    print("Copy List with Random Pointer smoke test passed.")
