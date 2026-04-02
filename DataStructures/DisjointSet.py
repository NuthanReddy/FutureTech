from __future__ import annotations

from typing import Dict, Hashable


class DisjointSet:
    """Union-Find with path compression and union by size."""

    def __init__(self) -> None:
        self._parent: Dict[Hashable, Hashable] = {}
        self._size: Dict[Hashable, int] = {}

    def make_set(self, item: Hashable) -> None:
        if item not in self._parent:
            self._parent[item] = item
            self._size[item] = 1

    def find(self, item: Hashable) -> Hashable:
        if item not in self._parent:
            raise KeyError(f"Unknown item: {item}")
        if self._parent[item] != item:
            self._parent[item] = self.find(self._parent[item])
        return self._parent[item]

    def union(self, a: Hashable, b: Hashable) -> bool:
        self.make_set(a)
        self.make_set(b)

        root_a = self.find(a)
        root_b = self.find(b)
        if root_a == root_b:
            return False

        if self._size[root_a] < self._size[root_b]:
            root_a, root_b = root_b, root_a

        self._parent[root_b] = root_a
        self._size[root_a] += self._size[root_b]
        return True

    def connected(self, a: Hashable, b: Hashable) -> bool:
        if a not in self._parent or b not in self._parent:
            return False
        return self.find(a) == self.find(b)

    def component_size(self, item: Hashable) -> int:
        return self._size[self.find(item)]


if __name__ == "__main__":
    ds = DisjointSet()
    ds.union("A", "B")
    ds.union("B", "C")
    print(ds.connected("A", "C"))
    print(ds.component_size("A"))

