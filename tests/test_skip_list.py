import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.SkipList import SkipList


class TestSkipList:
    def test_insert_and_search(self) -> None:
        sl = SkipList()
        sl.insert(10, "ten")
        sl.insert(5, "five")
        sl.insert(20, "twenty")
        assert sl.search(10) == "ten"
        assert sl.search(5) == "five"
        assert sl.search(20) == "twenty"

    def test_search_nonexistent_returns_none(self) -> None:
        sl = SkipList()
        sl.insert(1, "one")
        assert sl.search(99) is None

    def test_delete(self) -> None:
        sl = SkipList()
        sl.insert(10, "ten")
        sl.insert(20, "twenty")
        assert sl.delete(10) is True
        assert sl.search(10) is None
        assert len(sl) == 1

    def test_delete_nonexistent(self) -> None:
        sl = SkipList()
        sl.insert(1)
        assert sl.delete(999) is False

    def test_len(self) -> None:
        sl = SkipList()
        assert len(sl) == 0
        sl.insert(1)
        sl.insert(2)
        assert len(sl) == 2
        sl.delete(1)
        assert len(sl) == 1

    def test_contains(self) -> None:
        sl = SkipList()
        sl.insert(42, "answer")
        assert 42 in sl
        assert 0 not in sl

    def test_iteration_yields_sorted_keys(self) -> None:
        sl = SkipList()
        keys = [30, 10, 50, 20, 40]
        for k in keys:
            sl.insert(k, str(k))
        result = list(sl)
        assert [k for k, v in result] == sorted(keys)

    def test_insert_duplicate_updates_value(self) -> None:
        sl = SkipList()
        sl.insert(5, "old")
        sl.insert(5, "new")
        assert sl.search(5) == "new"
        assert len(sl) == 1

    def test_large_insert_and_search(self) -> None:
        sl = SkipList()
        for i in range(200):
            sl.insert(i, i * 2)
        assert len(sl) == 200
        for i in range(200):
            assert sl.search(i) == i * 2

    def test_repr(self) -> None:
        sl = SkipList()
        sl.insert(1, "one")
        assert "SkipList" in repr(sl)
