"""Reusable node types and conversion helpers for linked-list problems."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence


@dataclass(eq=False, slots=True)
class ListNode:
    """Node for a singly linked list."""

    val: int = 0
    next: Optional["ListNode"] = None


@dataclass(eq=False, slots=True)
class RandomListNode:
    """Node with both ``next`` and arbitrary ``random`` pointers."""

    val: int = 0
    next: Optional["RandomListNode"] = None
    random: Optional["RandomListNode"] = None


def build_linked_list(values: Iterable[int]) -> Optional[ListNode]:
    """Build a singly linked list while preserving input order."""
    dummy = ListNode()
    tail = dummy

    for value in values:
        tail.next = ListNode(value)
        tail = tail.next

    return dummy.next


def linked_list_to_list(head: Optional[ListNode]) -> list[int]:
    """Return node values and reject accidental cycles."""
    values: list[int] = []
    seen: set[int] = set()
    current = head

    while current is not None:
        identity = id(current)
        if identity in seen:
            raise ValueError("cycle detected in singly linked list")
        seen.add(identity)
        values.append(current.val)
        current = current.next

    return values


def build_random_list(
    values: Sequence[int],
    random_indices: Sequence[Optional[int]],
) -> Optional[RandomListNode]:
    """Build a random-pointer list from values and zero-based targets.

    ``None`` represents a null random pointer.
    """
    if len(values) != len(random_indices):
        raise ValueError("values and random_indices must have equal lengths")
    if not values:
        return None

    nodes = [RandomListNode(value) for value in values]
    for index in range(len(nodes) - 1):
        nodes[index].next = nodes[index + 1]

    for node, random_index in zip(nodes, random_indices):
        if random_index is None:
            continue
        if not 0 <= random_index < len(nodes):
            raise ValueError("random index is outside the list")
        node.random = nodes[random_index]

    return nodes[0]


def random_list_to_spec(
    head: Optional[RandomListNode],
) -> tuple[list[int], list[Optional[int]]]:
    """Serialize a random-pointer list into values and random indices."""
    nodes: list[RandomListNode] = []
    positions: dict[RandomListNode, int] = {}
    current = head

    while current is not None:
        if current in positions:
            raise ValueError("cycle detected in next pointers")
        positions[current] = len(nodes)
        nodes.append(current)
        current = current.next

    random_indices: list[Optional[int]] = []
    for node in nodes:
        if node.random is None:
            random_indices.append(None)
        elif node.random in positions:
            random_indices.append(positions[node.random])
        else:
            raise ValueError("random pointer targets a node outside the list")

    return [node.val for node in nodes], random_indices
