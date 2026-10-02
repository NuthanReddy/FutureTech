"""Number of Connected Components in an Undirected Graph (LeetCode 323).

Problem: Given ``n`` vertices numbered ``0`` through ``n-1`` and undirected
edges, count maximal sets of mutually reachable vertices.  Output: one
integer component count.  Constraints: edges may be disconnected and the
graph may have isolated vertices.
"""

from typing import List


def count_components(n: int, edges: List[List[int]]) -> int:
    """Count components while merging each undirected edge.

    Pattern identification: count undirected groups without routes -> Union-Find;
    decrease the component count only when two distinct roots merge.
    """
    # 1. Output: Return the number of connected components, including isolated vertices.
    # 2. Structure: Each undirected edge joins reachable groups; we need their count, not actual paths.
    # 3. Constraints: Endpoints are in 0..n-1; repeated edges must not reduce the count twice.
    # 4. Choice: Start with n groups; follow and shorten parent links to find each endpoint's representative.
    #    Join different representatives and decrease the group count only then.
    # 5. Why it works: Joining separate groups removes exactly one component; equal roots change nothing.
    #    Parent storage is O(n); without balanced unions a find may follow a long chain.
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
