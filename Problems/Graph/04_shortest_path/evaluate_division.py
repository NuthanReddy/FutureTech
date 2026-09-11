"""Evaluate Division (LeetCode 399).

Problem: Given equations such as ``a / b = 2.0``, evaluate independent
division queries.  Output: one float per query, or ``-1.0`` when a variable
is unknown or the variables are disconnected.  Constraints: variables are
strings and equation values are positive; equations can form cycles.
"""

from collections import defaultdict
from typing import Dict, List, Tuple


class _WeightedUnionFind:
    """Store ``weight[x] = x / parent[x]`` for ratio queries."""

    def __init__(self) -> None:
        self.parent: Dict[str, str] = {}
        self.weight: Dict[str, float] = {}

    def add(self, x: str) -> None:
        if x not in self.parent:
            self.parent[x] = x
            self.weight[x] = 1.0

    def find(self, x: str) -> Tuple[str, float]:
        if self.parent[x] == x:
            return x, 1.0
        parent, ratio_to_root = self.find(self.parent[x])
        self.weight[x] *= ratio_to_root
        self.parent[x] = parent
        return self.parent[x], self.weight[x]

    def union(self, numerator: str, denominator: str, value: float) -> None:
        self.add(numerator)
        self.add(denominator)
        root_num, num_to_root = self.find(numerator)
        root_den, den_to_root = self.find(denominator)
        if root_num == root_den:
            return
        # Need root_num/root_den = value * den_to_root/num_to_root.
        self.parent[root_num] = root_den
        self.weight[root_num] = value * den_to_root / num_to_root

    def ratio(self, numerator: str, denominator: str) -> float:
        if numerator not in self.parent or denominator not in self.parent:
            return -1.0
        root_num, num_to_root = self.find(numerator)
        root_den, den_to_root = self.find(denominator)
        return -1.0 if root_num != root_den else num_to_root / den_to_root


def calc_equation(
    equations: List[List[str]], values: List[float], queries: List[List[str]]
) -> List[float]:
    """Evaluate all division queries in near-constant amortized time."""
    uf = _WeightedUnionFind()
    for (a, b), value in zip(equations, values):
        uf.union(a, b, value)
    return [uf.ratio(a, b) for a, b in queries]
