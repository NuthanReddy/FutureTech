"""Number of Connected Components in an Undirected Graph (LeetCode 323).

Problem: Given ``n`` vertices numbered ``0`` through ``n-1`` and undirected
edges, count maximal sets of mutually reachable vertices.  Output: one
integer component count.  Constraints: edges may be disconnected and the
graph may have isolated vertices.
"""

from typing import List


def count_components(n: int, edges: List[List[int]]) -> int:
    """Count components while merging each undirected edge."""
    parent = list(range(n))
    components = n

    def find(x: int) -> int:
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    for a, b in edges:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra
            components -= 1
    return components
