"""Graph Valid Tree (LeetCode 261).

Problem: Given ``n`` vertices numbered ``0`` through ``n-1`` and undirected
edges, determine whether they form one connected acyclic tree.  Output: a
boolean.  Constraints: a valid tree must contain exactly ``n - 1`` edges;
duplicate edges and cycles make the answer false.
"""

from typing import List


def valid_tree(n: int, edges: List[List[int]]) -> bool:
    """A graph is a tree iff it has n-1 edges and no union finds a cycle."""
    if len(edges) != n - 1:
        return False
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra == rb:
            return False
        parent[rb] = ra
    # n-1 acyclic edges over n vertices implies connectivity.
    return True
