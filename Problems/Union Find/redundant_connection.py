# Redundant Connection (LeetCode 684)
#
# DS used: Disjoint Set / Union-Find (DataStructures/DisjointSet.py)
#
# Problem:
# A tree is a connected graph with no cycles.  You are given a graph that
# started as a tree with n nodes (1..n), with one additional edge added.
# Return the edge that, if removed, would make the graph a valid tree.
# If there are multiple answers, return the one that occurs *last* in the input.
#
# Example:
#   edges = [[1,2],[1,3],[2,3]]
#   Output: [2,3]   — removing it leaves a valid tree
#
# Why Union-Find?
# Process edges one by one.  The first edge whose two endpoints are already in
# the same component is the redundant edge (it creates a cycle).
#
# Time:  O(n · α(n)) ≈ O(n)
# Space: O(n)

from typing import List


class _UnionFind:
    def __init__(self, n: int) -> None:
        self.parent = list(range(n + 1))  # 1-indexed
        self.size = [1] * (n + 1)

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> bool:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False  # already connected → cycle!
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]
        return True


def find_redundant_connection(edges: List[List[int]]) -> List[int]:
    """Return the last edge that creates a cycle."""
    n = len(edges)
    uf = _UnionFind(n)
    for u, v in edges:
        if not uf.union(u, v):
            return [u, v]
    return []  # should not reach here for valid input


if __name__ == "__main__":
    print(find_redundant_connection([[1, 2], [1, 3], [2, 3]]))
    # Output: [2, 3]

    print(find_redundant_connection([[1, 2], [2, 3], [3, 4], [1, 4], [1, 5]]))
    # Output: [1, 4]

