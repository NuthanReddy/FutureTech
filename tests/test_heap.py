import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.Heap import MinHeap, MaxHeap


# --- MinHeap ---

class TestMinHeap:
    def test_push_pop_order(self) -> None:
        h = MinHeap()
        for v in [5, 3, 8, 1, 4]:
            h.push(v)
        assert [h.pop() for _ in range(5)] == [1, 3, 4, 5, 8]

    def test_peek_returns_min_without_removing(self) -> None:
        h = MinHeap()
        h.push(10)
        h.push(2)
        assert h.peek() == 2
        assert len(h) == 2

    def test_heapify(self) -> None:
        h = MinHeap()
        h.heapify([9, 7, 5, 3, 1])
        assert h.pop() == 1
        assert h.pop() == 3

    def test_empty_pop_raises(self) -> None:
        with pytest.raises(IndexError):
            MinHeap().pop()

    def test_empty_peek_raises(self) -> None:
        with pytest.raises(IndexError):
            MinHeap().peek()

    def test_len(self) -> None:
        h = MinHeap()
        assert len(h) == 0
        h.push(1)
        assert len(h) == 1

    def test_bool(self) -> None:
        h = MinHeap()
        assert not h
        h.push(42)
        assert h

    def test_single_element(self) -> None:
        h = MinHeap()
        h.push(7)
        assert h.peek() == 7
        assert h.pop() == 7
        assert len(h) == 0

    def test_duplicate_values(self) -> None:
        h = MinHeap()
        for v in [3, 1, 3, 1, 2]:
            h.push(v)
        assert [h.pop() for _ in range(5)] == [1, 1, 2, 3, 3]


# --- MaxHeap ---

class TestMaxHeap:
    def test_push_pop_order(self) -> None:
        h = MaxHeap()
        for v in [5, 3, 8, 1, 4]:
            h.push(v)
        assert [h.pop() for _ in range(5)] == [8, 5, 4, 3, 1]

    def test_peek_returns_max_without_removing(self) -> None:
        h = MaxHeap()
        h.push(2)
        h.push(10)
        assert h.peek() == 10
        assert len(h) == 2

    def test_heapify(self) -> None:
        h = MaxHeap()
        h.heapify([1, 3, 5, 7, 9])
        assert h.pop() == 9
        assert h.pop() == 7

    def test_empty_pop_raises(self) -> None:
        with pytest.raises(IndexError):
            MaxHeap().pop()

    def test_empty_peek_raises(self) -> None:
        with pytest.raises(IndexError):
            MaxHeap().peek()

    def test_len_and_bool(self) -> None:
        h = MaxHeap()
        assert not h
        h.push(1)
        assert h
        assert len(h) == 1

    def test_repr(self) -> None:
        h = MaxHeap()
        h.push(5)
        assert "MaxHeap" in repr(h)
