from __future__ import annotations

from typing import Iterable, List


class FenwickTree:
    """Binary Indexed Tree for point updates and prefix/range sums."""

    def __init__(self, nums: Iterable[int] | None = None) -> None:
        self._values: List[int] = []
        self._tree: List[int] = [0]
        if nums is not None:
            self.build(nums)

    def build(self, nums: Iterable[int]) -> None:
        self._values = list(nums)
        self._tree = [0] * (len(self._values) + 1)
        for idx, value in enumerate(self._values):
            self._add(idx + 1, value)

    def update(self, index: int, delta: int) -> None:
        self._validate_index(index)
        self._values[index] += delta
        self._add(index + 1, delta)

    def prefix_sum(self, index: int) -> int:
        if index < 0:
            return 0
        self._validate_index(index)
        tree_index = index + 1
        result = 0
        while tree_index > 0:
            result += self._tree[tree_index]
            tree_index -= tree_index & -tree_index
        return result

    def range_sum(self, left: int, right: int) -> int:
        if left > right:
            raise ValueError("left must be <= right")
        self._validate_index(left)
        self._validate_index(right)
        return self.prefix_sum(right) - self.prefix_sum(left - 1)

    def _add(self, tree_index: int, delta: int) -> None:
        while tree_index < len(self._tree):
            self._tree[tree_index] += delta
            tree_index += tree_index & -tree_index

    def _validate_index(self, index: int) -> None:
        if index < 0 or index >= len(self._values):
            raise IndexError("index out of range")


if __name__ == "__main__":
    ft = FenwickTree([2, 1, 3, 4, 5])
    print(ft.range_sum(1, 3))
    ft.update(2, 2)
    print(ft.range_sum(1, 3))

