# Number of Provinces (LeetCode 547)
#
# DS used: Disjoint Set / Union-Find (DataStructures/DisjointSet.py)
#
# Problem:
# There are n cities. Some of them are connected directly, some indirectly,
# and some are not connected at all.  A "province" is a group of directly or
# indirectly connected cities.
# Given an n×n adjacency matrix `is_connected`, return the number of provinces.
#
# Example:
#   is_connected = [[1,1,0],
#                   [1,1,0],
#                   [0,0,1]]
#   Output: 2   (cities 0-1 form one province, city 2 another)
#
# Why Union-Find?
# Each edge merges two sets.  After processing all edges the number of
# distinct roots == number of provinces.  Union by size + path compression
# gives near-O(1) per operation.
#
# Time:  O(n² · α(n)) ≈ O(n²)
# Space: O(n)

from typing import List


class _UnionFind:
    """Lightweight Union-Find with path compression and union by size."""

    def __init__(self, n: int) -> None:
        self.parent = list(range(n))
        self.size = [1] * n
        self.components = n

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path halving
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> bool:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        # union by size
        if self.size[ra] < self.size[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        self.size[ra] += self.size[rb]
        self.components -= 1
        return True


def find_circle_num(is_connected: List[List[int]]) -> int:
    """Return the number of provinces (connected components)."""
    n = len(is_connected)
    uf = _UnionFind(n)
    for i in range(n):
        for j in range(i + 1, n):
            if is_connected[i][j] == 1:
                uf.union(i, j)
    return uf.components


if __name__ == "__main__":
    # Example 1 — two provinces
    grid1 = [[1, 1, 0],
             [1, 1, 0],
             [0, 0, 1]]
    print(find_circle_num(grid1))  # 2

    # Example 2 — three provinces (no connections)
    grid2 = [[1, 0, 0],
             [0, 1, 0],
             [0, 0, 1]]
    print(find_circle_num(grid2))  # 3

    # Example 3 — single province
    grid3 = [[1, 1, 1],
             [1, 1, 1],
             [1, 1, 1]]
    print(find_circle_num(grid3))  # 1

