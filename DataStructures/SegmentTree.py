from __future__ import annotations

from typing import Callable, List, Optional


class SegmentTree:
    """Segment Tree for efficient range queries and point updates.

    Supports configurable merge operations (sum, min, max, etc.) passed
    as a callable at construction time.

    Time complexity:
        build  — O(n)
        update — O(log n)
        query  — O(log n)
    Space complexity: O(n)
    """

    def __init__(
        self,
        data: Optional[List[int]] = None,
        merge: Callable[[int, int], int] = lambda a, b: a + b,
        identity: int = 0,
    ) -> None:
        """Create a SegmentTree.

        Args:
            data: Initial list of values.  If None the tree is empty.
            merge: Associative binary function used to combine segments
                   (default: addition).
            identity: Identity element for *merge* (0 for sum, float('inf')
                      for min, etc.).
        """
        self._merge = merge
        self._identity = identity
        self._n: int = 0
        self._tree: List[int] = []
        self._data: List[int] = []
        if data is not None:
            self.build(data)

    def build(self, data: List[int]) -> None:
        """Build the segment tree from *data*.

        Time complexity: O(n)
        """
        self._data = list(data)
        self._n = len(self._data)
        self._tree = [self._identity] * (4 * self._n)
        if self._n > 0:
            self._build(1, 0, self._n - 1)

    def _build(self, node: int, start: int, end: int) -> None:
        if start == end:
            self._tree[node] = self._data[start]
            return
        mid = (start + end) // 2
        self._build(2 * node, start, mid)
        self._build(2 * node + 1, mid + 1, end)
        self._tree[node] = self._merge(self._tree[2 * node], self._tree[2 * node + 1])

    def update(self, index: int, value: int) -> None:
        """Set the element at *index* to *value*.

        Time complexity: O(log n)
        """
        if index < 0 or index >= self._n:
            raise IndexError(f"index {index} out of range [0, {self._n})")
        self._data[index] = value
        self._update(1, 0, self._n - 1, index, value)

    def _update(self, node: int, start: int, end: int, index: int, value: int) -> None:
        if start == end:
            self._tree[node] = value
            return
        mid = (start + end) // 2
        if index <= mid:
            self._update(2 * node, start, mid, index, value)
        else:
            self._update(2 * node + 1, mid + 1, end, index, value)
        self._tree[node] = self._merge(self._tree[2 * node], self._tree[2 * node + 1])

    def query(self, left: int, right: int) -> int:
        """Query the aggregate over [left, right] (inclusive).

        Time complexity: O(log n)
        """
        if left < 0 or right >= self._n or left > right:
            raise ValueError(
                f"invalid range [{left}, {right}] for array of size {self._n}"
            )
        return self._query(1, 0, self._n - 1, left, right)

    def _query(self, node: int, start: int, end: int, left: int, right: int) -> int:
        if left > end or right < start:
            return self._identity
        if left <= start and end <= right:
            return self._tree[node]
        mid = (start + end) // 2
        left_val = self._query(2 * node, start, mid, left, right)
        right_val = self._query(2 * node + 1, mid + 1, end, left, right)
        return self._merge(left_val, right_val)

    def __repr__(self) -> str:
        return f"SegmentTree(data={self._data}, merge={self._merge.__name__ if hasattr(self._merge, '__name__') else '...'})"

    def __len__(self) -> int:
        return self._n


if __name__ == "__main__":
    # --- Sum segment tree ---
    nums = [1, 3, 5, 7, 9, 11]
    st_sum = SegmentTree(nums)
    print("Sum tree:", st_sum)
    print(f"sum([1..3]) = {st_sum.query(1, 3)}")  # 3+5+7 = 15
    st_sum.update(2, 10)
    print(f"After update index 2 -> 10: sum([1..3]) = {st_sum.query(1, 3)}")  # 3+10+7 = 20

    # --- Min segment tree ---
    st_min = SegmentTree(nums, merge=min, identity=float("inf"))
    print(f"\nMin tree: {st_min}")
    print(f"min([0..4]) = {st_min.query(0, 4)}")  # 1

    # --- Max segment tree ---
    st_max = SegmentTree(nums, merge=max, identity=float("-inf"))
    print(f"\nMax tree: {st_max}")
    print(f"max([2..5]) = {st_max.query(2, 5)}")  # 11
