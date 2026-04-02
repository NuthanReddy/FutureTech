import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.BPlusTree import BPlusTree


class TestBPlusTree:
    def test_insert_and_search(self) -> None:
        bpt = BPlusTree(order=4)
        bpt.insert(10, "ten")
        bpt.insert(20, "twenty")
        bpt.insert(5, "five")
        assert bpt.search(10) == "ten"
        assert bpt.search(20) == "twenty"
        assert bpt.search(5) == "five"

    def test_search_missing(self) -> None:
        bpt = BPlusTree(order=4)
        bpt.insert(1, "one")
        assert bpt.search(99) is None

    def test_delete(self) -> None:
        bpt = BPlusTree(order=4)
        for k in [10, 20, 5, 15, 25]:
            bpt.insert(k, str(k))
        assert bpt.delete(10) is True
        assert bpt.search(10) is None
        assert bpt.search(20) == "20"

    def test_delete_nonexistent(self) -> None:
        bpt = BPlusTree(order=4)
        bpt.insert(1)
        assert bpt.delete(100) is False

    def test_range_query(self) -> None:
        bpt = BPlusTree(order=4)
        for k in range(1, 21):
            bpt.insert(k, k * 10)
        result = bpt.range_query(5, 10)
        keys = [k for k, v in result]
        assert keys == [5, 6, 7, 8, 9, 10]
        values = [v for k, v in result]
        assert values == [50, 60, 70, 80, 90, 100]

    def test_range_query_no_match(self) -> None:
        bpt = BPlusTree(order=4)
        for k in [1, 2, 3]:
            bpt.insert(k)
        assert bpt.range_query(10, 20) == []

    def test_iteration_sorted(self) -> None:
        bpt = BPlusTree(order=4)
        keys = [30, 10, 50, 20, 40]
        for k in keys:
            bpt.insert(k, str(k))
        result = list(bpt)
        assert [k for k, v in result] == sorted(keys)

    def test_len(self) -> None:
        bpt = BPlusTree(order=4)
        assert len(bpt) == 0
        for k in range(5):
            bpt.insert(k)
        assert len(bpt) == 5
        bpt.delete(0)
        assert len(bpt) == 4

    def test_contains(self) -> None:
        bpt = BPlusTree(order=4)
        bpt.insert(42, "answer")
        assert 42 in bpt
        assert 0 not in bpt

    def test_invalid_order_raises(self) -> None:
        with pytest.raises(ValueError):
            BPlusTree(order=2)

    def test_insert_duplicate_updates(self) -> None:
        bpt = BPlusTree(order=4)
        bpt.insert(10, "old")
        bpt.insert(10, "new")
        assert bpt.search(10) == "new"
        assert len(bpt) == 1

    def test_many_inserts(self) -> None:
        bpt = BPlusTree(order=4)
        for k in range(100):
            bpt.insert(k, k * 2)
        assert len(bpt) == 100
        for k in range(100):
            assert bpt.search(k) == k * 2

    def test_delete_all(self) -> None:
        bpt = BPlusTree(order=4)
        for k in range(10):
            bpt.insert(k, str(k))
        for k in range(10):
            assert bpt.delete(k) is True
        assert len(bpt) == 0
