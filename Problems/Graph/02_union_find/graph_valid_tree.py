"""Graph Valid Tree (LeetCode 261).

Problem: Given ``n`` vertices numbered ``0`` through ``n-1`` and undirected
edges, determine whether they form one connected acyclic tree.  Output: a
boolean.  Constraints: a valid tree must contain exactly ``n - 1`` edges;
duplicate edges and cycles make the answer false.
"""

from typing import List


def valid_tree(n: int, edges: List[List[int]]) -> bool:
    """A graph is a tree iff it has n-1 edges and no union finds a cycle.

    Pattern identification: undirected tree test -> edge count plus Union-Find;
    accepted edges join distinct roots, preserving an acyclic forest.
    """
    # 1. Output: Return whether the undirected edges form one connected, cycle-free tree.
    # 2. Structure: Only group membership and cycles matter, not routes; a tree has n-1 joining edges.
    # 3. Constraints: Endpoints must be in 0..n-1; duplicate or self-loop edges cannot be accepted.
    # 4. Choice: Check the edge count, then use parent links to find group representatives and join them.
    #    Shorten parent chains while searching; equal representatives reject an edge as a cycle.
    # 5. Why it works: Equal roots expose a cycle; n-1 cycle-free joins leave exactly one component.
    #    Parent storage is O(n); this version compresses paths but does not balance unions.
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
