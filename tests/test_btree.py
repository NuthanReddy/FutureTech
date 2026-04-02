import sys
import os
import random
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.BTree import BTree


class TestBTree:
    def test_insert_and_search(self) -> None:
        bt = BTree(t=2)
        bt.insert(10, "ten")
        bt.insert(20, "twenty")
        bt.insert(5, "five")
        assert bt.search(10) == "ten"
        assert bt.search(20) == "twenty"
        assert bt.search(5) == "five"

    def test_search_missing(self) -> None:
        bt = BTree(t=2)
        bt.insert(1, "one")
        assert bt.search(99) is None

    def test_delete_leaf(self) -> None:
        bt = BTree(t=2)
        for k in [10, 20, 5, 6, 12, 30]:
            bt.insert(k, str(k))
        assert bt.delete(6) is True
        assert bt.search(6) is None
        assert bt.search(10) == "10"

    def test_delete_internal_node(self) -> None:
        bt = BTree(t=2)
        for k in range(1, 11):
            bt.insert(k, str(k))
        # delete a key likely to be in an internal node
        assert bt.delete(5) is True
        assert bt.search(5) is None
        # remaining keys intact
        for k in range(1, 11):
            if k != 5:
                assert bt.search(k) == str(k)

    def test_delete_with_merge(self) -> None:
        bt = BTree(t=2)
        for k in [1, 2, 3, 4, 5]:
            bt.insert(k, str(k))
        # delete enough to trigger merge
        bt.delete(1)
        bt.delete(2)
        bt.delete(3)
        assert bt.search(4) == "4"
        assert bt.search(5) == "5"

    def test_delete_nonexistent(self) -> None:
        bt = BTree(t=2)
        bt.insert(1)
        assert bt.delete(100) is False

    def test_inorder_sorted(self) -> None:
        bt = BTree(t=3)
        keys = [50, 30, 70, 20, 40, 60, 80, 10, 25]
        for k in keys:
            bt.insert(k, str(k))
        result = bt.inorder()
        assert [k for k, v in result] == sorted(keys)

    def test_len(self) -> None:
        bt = BTree(t=2)
        assert len(bt) == 0
        bt.insert(1)
        bt.insert(2)
        assert len(bt) == 2
        bt.delete(1)
        assert len(bt) == 1

    def test_contains(self) -> None:
        bt = BTree(t=2)
        bt.insert(42)
        assert 42 in bt
        assert 0 not in bt

    def test_invalid_t_raises(self) -> None:
        with pytest.raises(ValueError):
            BTree(t=1)

    def test_stress_100_random_keys(self) -> None:
        bt = BTree(t=3)
        keys = random.sample(range(1000), 100)
        for k in keys:
            bt.insert(k, k * 10)
        assert len(bt) == 100
        for k in keys:
            assert bt.search(k) == k * 10
        # inorder is sorted
        result = bt.inorder()
        sorted_keys = [k for k, v in result]
        assert sorted_keys == sorted(sorted_keys)

    def test_insert_duplicate_updates(self) -> None:
        bt = BTree(t=2)
        bt.insert(10, "old")
        bt.insert(10, "new")
        assert bt.search(10) == "new"

    def test_repr(self) -> None:
        bt = BTree(t=2)
        bt.insert(1)
        assert "BTree" in repr(bt)
