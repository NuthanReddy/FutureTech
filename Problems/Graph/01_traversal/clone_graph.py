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
    """Deep-copy a connected graph, including cycles and self-loops.

    Pattern identification: cyclic adjacency copy -> BFS with identity map;
    each original has exactly one clone, created before exploring neighbors.
    """
    # 1. Output: Return a deep-copied start node with the same values and neighbor links.
    # 2. Structure: Neighbor references form a graph that can contain cycles and self-loops.
    # 3. Constraints: Cycles can lead back to copied nodes, so remember copies by identity, not value.
    #    Copy only the reachable component; None returns None without allocating nodes.
    # 4. Choice: BFS with an original-to-copy dictionary; create each copy before linking neighbors.
    # 5. Why it works: One copy per original identity preserves sharing and prevents cyclic revisits.
    #    Each node and neighbor link is processed once: O(V + E) time and space including copies.
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
