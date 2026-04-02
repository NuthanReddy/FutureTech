import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.SegmentTree import SegmentTree


class TestSegmentTreeSum:
    def test_build_and_query_sum(self) -> None:
        st = SegmentTree([1, 2, 3, 4, 5])
        assert st.query(0, 4) == 15  # 1+2+3+4+5
        assert st.query(1, 3) == 9   # 2+3+4
        assert st.query(0, 0) == 1
        assert st.query(4, 4) == 5

    def test_point_update_sum(self) -> None:
        st = SegmentTree([1, 2, 3, 4, 5])
        st.update(2, 10)  # [1, 2, 10, 4, 5]
        assert st.query(0, 4) == 22
        assert st.query(2, 2) == 10
        assert st.query(1, 3) == 16  # 2+10+4

    def test_update_out_of_range_raises(self) -> None:
        st = SegmentTree([1, 2, 3])
        with pytest.raises(IndexError):
            st.update(5, 10)

    def test_query_invalid_range_raises(self) -> None:
        st = SegmentTree([1, 2, 3])
        with pytest.raises(ValueError):
            st.query(2, 0)

    def test_len(self) -> None:
        st = SegmentTree([1, 2, 3, 4])
        assert len(st) == 4


class TestSegmentTreeMin:
    def test_build_and_query_min(self) -> None:
        st = SegmentTree([5, 2, 8, 1, 9], merge=min, identity=float("inf"))
        assert st.query(0, 4) == 1
        assert st.query(0, 2) == 2
        assert st.query(3, 4) == 1

    def test_update_and_requery_min(self) -> None:
        st = SegmentTree([5, 2, 8, 1, 9], merge=min, identity=float("inf"))
        st.update(3, 100)  # [5, 2, 8, 100, 9]
        assert st.query(0, 4) == 2
        assert st.query(3, 4) == 9


class TestSegmentTreeMax:
    def test_build_and_query_max(self) -> None:
        st = SegmentTree([5, 2, 8, 1, 9], merge=max, identity=float("-inf"))
        assert st.query(0, 4) == 9
        assert st.query(0, 2) == 8
        assert st.query(3, 4) == 9

    def test_update_and_requery_max(self) -> None:
        st = SegmentTree([5, 2, 8, 1, 9], merge=max, identity=float("-inf"))
        st.update(4, 0)  # [5, 2, 8, 1, 0]
        assert st.query(0, 4) == 8
        assert st.query(3, 4) == 1


class TestSegmentTreeBuild:
    def test_build_replaces_data(self) -> None:
        st = SegmentTree([1, 2, 3])
        assert st.query(0, 2) == 6
        st.build([10, 20])
        assert len(st) == 2
        assert st.query(0, 1) == 30

    def test_single_element(self) -> None:
        st = SegmentTree([42])
        assert st.query(0, 0) == 42
        st.update(0, 7)
        assert st.query(0, 0) == 7
