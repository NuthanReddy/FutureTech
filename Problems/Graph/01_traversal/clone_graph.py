"""Clone Graph (LeetCode 133).

Problem: Given a reference to a node in a connected undirected graph, create
a deep copy with the same values and adjacency relationships.  Output: the
cloned start node, or ``None`` for an empty graph.  Constraints: node values
are integers, neighbors may contain cycles, and each original node is copied
exactly once.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass(eq=False)
class Node:
    val: int = 0
    neighbors: List["Node"] = field(default_factory=list)


def clone_graph(node: Optional[Node]) -> Optional[Node]:
    """Deep-copy a connected graph, including cycles and self-loops."""
    if node is None:
        return None
    # The map is the key invariant: one original identity has one clone.
    copies: Dict[Node, Node] = {node: Node(node.val)}
    queue = deque([node])
    while queue:
        current = queue.popleft()
        for neighbor in current.neighbors:
            if neighbor not in copies:
                copies[neighbor] = Node(neighbor.val)
                queue.append(neighbor)
            copies[current].neighbors.append(copies[neighbor])
    return copies[node]
