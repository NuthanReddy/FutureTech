import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.SortedSet import SortedSet


class TestSortedSet:
    def test_add_and_contains(self) -> None:
        ss = SortedSet()
        ss.add(5)
        ss.add(3)
        ss.add(8)
        assert 5 in ss
        assert 3 in ss
        assert 8 in ss
        assert 99 not in ss

    def test_add_duplicate_ignored(self) -> None:
        ss = SortedSet()
        ss.add(5)
        ss.add(5)
        assert len(ss) == 1

    def test_remove(self) -> None:
        ss = SortedSet()
        ss.add(10)
        ss.add(20)
        ss.remove(10)
        assert 10 not in ss
        assert len(ss) == 1

    def test_remove_missing_raises(self) -> None:
        ss = SortedSet()
        with pytest.raises(KeyError):
            ss.remove(1)

    def test_discard_missing_no_error(self) -> None:
        ss = SortedSet()
        ss.discard(1)  # should not raise

    def test_min_max(self) -> None:
        ss = SortedSet()
        for v in [20, 10, 30, 5, 25]:
            ss.add(v)
        assert ss.min() == 5
        assert ss.max() == 30

    def test_min_max_empty_raises(self) -> None:
        ss = SortedSet()
        with pytest.raises(ValueError):
            ss.min()
        with pytest.raises(ValueError):
            ss.max()

    def test_floor(self) -> None:
        ss = SortedSet()
        for v in [10, 20, 30]:
            ss.add(v)
        assert ss.floor(25) == 20
        assert ss.floor(30) == 30
        assert ss.floor(5) is None

    def test_ceiling(self) -> None:
        ss = SortedSet()
        for v in [10, 20, 30]:
            ss.add(v)
        assert ss.ceiling(15) == 20
        assert ss.ceiling(10) == 10
        assert ss.ceiling(35) is None

    def test_range_query(self) -> None:
        ss = SortedSet()
        for v in range(1, 11):
            ss.add(v)
        result = ss.range_query(3, 7)
        assert result == [3, 4, 5, 6, 7]

    def test_range_query_no_match(self) -> None:
        ss = SortedSet()
        for v in [1, 2, 3]:
            ss.add(v)
        assert ss.range_query(10, 20) == []

    def test_iteration_sorted(self) -> None:
        ss = SortedSet()
        items = [50, 30, 10, 40, 20]
        for v in items:
            ss.add(v)
        assert list(ss) == sorted(items)

    def test_len(self) -> None:
        ss = SortedSet()
        assert len(ss) == 0
        ss.add(1)
        ss.add(2)
        ss.add(3)
        assert len(ss) == 3
        ss.remove(2)
        assert len(ss) == 2

    def test_repr(self) -> None:
        ss = SortedSet()
        ss.add(1)
        assert "SortedSet" in repr(ss)
