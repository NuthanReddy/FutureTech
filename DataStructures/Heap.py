"""MinHeap and MaxHeap — array-based binary heap implementations.

No use of the ``heapq`` standard-library module; the heap property is
maintained manually via ``_sift_up`` and ``_sift_down``.
"""

from __future__ import annotations

from typing import Any, List, Optional


class MinHeap:
    """Array-based min-heap.

    The smallest element is always at index 0.

    Parent / child index relations (0-based):
        parent(i)      = (i - 1) // 2
        left_child(i)  = 2 * i + 1
        right_child(i) = 2 * i + 2
    """

    def __init__(self) -> None:
        self._data: List[Any] = []

    # ---- public API ----

    def push(self, value: Any) -> None:
        """Insert *value* into the heap.  O(log n)."""
        self._data.append(value)
        self._sift_up(len(self._data) - 1)

    def pop(self) -> Any:
        """Remove and return the minimum element.  O(log n).

        Raises ``IndexError`` if the heap is empty.
        """
        if not self._data:
            raise IndexError("pop from empty heap")
        self._swap(0, len(self._data) - 1)
        value = self._data.pop()
        if self._data:
            self._sift_down(0)
        return value

    def peek(self) -> Any:
        """Return the minimum element without removing it.  O(1).

        Raises ``IndexError`` if the heap is empty.
        """
        if not self._data:
            raise IndexError("peek at empty heap")
        return self._data[0]

    def heapify(self, items: List[Any]) -> None:
        """Build the heap in-place from *items*.  O(n).

        Any existing contents are replaced.
        """
        self._data = list(items)
        # Sift down from the last non-leaf node to the root.
        for i in range(len(self._data) // 2 - 1, -1, -1):
            self._sift_down(i)

    # ---- dunder helpers ----

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return len(self._data) > 0

    def __repr__(self) -> str:
        return f"MinHeap({self._data})"

    # ---- internal helpers ----

    def _swap(self, i: int, j: int) -> None:
        self._data[i], self._data[j] = self._data[j], self._data[i]

    def _sift_up(self, idx: int) -> None:
        """Restore the heap property by moving *idx* upward.  O(log n)."""
        while idx > 0:
            parent = (idx - 1) // 2
            if self._data[idx] < self._data[parent]:
                self._swap(idx, parent)
                idx = parent
            else:
                break

    def _sift_down(self, idx: int) -> None:
        """Restore the heap property by moving *idx* downward.  O(log n)."""
        size = len(self._data)
        while True:
            smallest = idx
            left = 2 * idx + 1
            right = 2 * idx + 2

            if left < size and self._data[left] < self._data[smallest]:
                smallest = left
            if right < size and self._data[right] < self._data[smallest]:
                smallest = right

            if smallest != idx:
                self._swap(idx, smallest)
                idx = smallest
            else:
                break


class MaxHeap:
    """Array-based max-heap.

    The largest element is always at index 0.
    Identical structure to ``MinHeap`` but with reversed comparisons.
    """

    def __init__(self) -> None:
        self._data: List[Any] = []

    # ---- public API ----

    def push(self, value: Any) -> None:
        """Insert *value* into the heap.  O(log n)."""
        self._data.append(value)
        self._sift_up(len(self._data) - 1)

    def pop(self) -> Any:
        """Remove and return the maximum element.  O(log n).

        Raises ``IndexError`` if the heap is empty.
        """
        if not self._data:
            raise IndexError("pop from empty heap")
        self._swap(0, len(self._data) - 1)
        value = self._data.pop()
        if self._data:
            self._sift_down(0)
        return value

    def peek(self) -> Any:
        """Return the maximum element without removing it.  O(1).

        Raises ``IndexError`` if the heap is empty.
        """
        if not self._data:
            raise IndexError("peek at empty heap")
        return self._data[0]

    def heapify(self, items: List[Any]) -> None:
        """Build the heap in-place from *items*.  O(n).

        Any existing contents are replaced.
        """
        self._data = list(items)
        for i in range(len(self._data) // 2 - 1, -1, -1):
            self._sift_down(i)

    # ---- dunder helpers ----

    def __len__(self) -> int:
        return len(self._data)

    def __bool__(self) -> bool:
        return len(self._data) > 0

    def __repr__(self) -> str:
        return f"MaxHeap({self._data})"

    # ---- internal helpers ----

    def _swap(self, i: int, j: int) -> None:
        self._data[i], self._data[j] = self._data[j], self._data[i]

    def _sift_up(self, idx: int) -> None:
        """Restore the heap property by moving *idx* upward.  O(log n)."""
        while idx > 0:
            parent = (idx - 1) // 2
            if self._data[idx] > self._data[parent]:
                self._swap(idx, parent)
                idx = parent
            else:
                break

    def _sift_down(self, idx: int) -> None:
        """Restore the heap property by moving *idx* downward.  O(log n)."""
        size = len(self._data)
        while True:
            largest = idx
            left = 2 * idx + 1
            right = 2 * idx + 2

            if left < size and self._data[left] > self._data[largest]:
                largest = left
            if right < size and self._data[right] > self._data[largest]:
                largest = right

            if largest != idx:
                self._swap(idx, largest)
                idx = largest
            else:
                break


if __name__ == "__main__":
    print("=== MinHeap ===")
    minh = MinHeap()
    for v in [5, 3, 8, 1, 2, 7]:
        minh.push(v)
    print(minh)                       # MinHeap([1, 2, 7, 5, 3, 8])
    print(f"peek: {minh.peek()}")     # 1
    print(f"pop:  {minh.pop()}")      # 1
    print(f"pop:  {minh.pop()}")      # 2
    print(minh)

    minh2 = MinHeap()
    minh2.heapify([9, 4, 6, 1, 3])
    print(f"\nheapified: {minh2}")
    while minh2:
        print(minh2.pop(), end=" ")
    print()

    print("\n=== MaxHeap ===")
    maxh = MaxHeap()
    for v in [5, 3, 8, 1, 2, 7]:
        maxh.push(v)
    print(maxh)
    print(f"peek: {maxh.peek()}")     # 8
    print(f"pop:  {maxh.pop()}")      # 8
    print(f"pop:  {maxh.pop()}")      # 7
    print(maxh)

    maxh2 = MaxHeap()
    maxh2.heapify([9, 4, 6, 1, 3])
    print(f"\nheapified: {maxh2}")
    while maxh2:
        print(maxh2.pop(), end=" ")
    print()