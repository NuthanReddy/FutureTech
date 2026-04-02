import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.RedBlackTree import RedBlackTree


class TestRedBlackTree:
    def test_insert_and_search(self) -> None:
        rbt = RedBlackTree()
        rbt.insert(10, "ten")
        rbt.insert(5, "five")
        rbt.insert(15, "fifteen")
        assert rbt.search(10) == "ten"
        assert rbt.search(5) == "five"
        assert rbt.search(15) == "fifteen"

    def test_search_missing_returns_none(self) -> None:
        rbt = RedBlackTree()
        rbt.insert(1)
        assert rbt.search(99) is None

    def test_delete_and_verify(self) -> None:
        rbt = RedBlackTree()
        for k in [10, 5, 15, 3, 7]:
            rbt.insert(k, str(k))
        assert rbt.delete(5) is True
        assert rbt.search(5) is None
        assert 5 not in rbt
        # other keys still present
        assert rbt.search(10) == "10"
        assert rbt.search(3) == "3"

    def test_delete_nonexistent_returns_false(self) -> None:
        rbt = RedBlackTree()
        rbt.insert(1)
        assert rbt.delete(999) is False

    def test_minimum_and_maximum(self) -> None:
        rbt = RedBlackTree()
        for k in [20, 10, 30, 5, 15]:
            rbt.insert(k)
        assert rbt.minimum() == 5
        assert rbt.maximum() == 30

    def test_minimum_maximum_empty(self) -> None:
        rbt = RedBlackTree()
        assert rbt.minimum() is None
        assert rbt.maximum() is None

    def test_inorder_sorted(self) -> None:
        rbt = RedBlackTree()
        keys = [50, 30, 70, 20, 40, 60, 80]
        for k in keys:
            rbt.insert(k, str(k))
        result = rbt.inorder()
        assert [k for k, v in result] == sorted(keys)

    def test_len(self) -> None:
        rbt = RedBlackTree()
        assert len(rbt) == 0
        rbt.insert(1)
        rbt.insert(2)
        assert len(rbt) == 2
        rbt.delete(1)
        assert len(rbt) == 1

    def test_contains(self) -> None:
        rbt = RedBlackTree()
        rbt.insert(42, "answer")
        assert 42 in rbt
        assert 0 not in rbt

    def test_insert_duplicate_updates_value(self) -> None:
        rbt = RedBlackTree()
        rbt.insert(10, "original")
        rbt.insert(10, "updated")
        assert rbt.search(10) == "updated"
        assert len(rbt) == 1

    def test_delete_root(self) -> None:
        rbt = RedBlackTree()
        rbt.insert(10)
        assert rbt.delete(10) is True
        assert len(rbt) == 0
        assert rbt.search(10) is None

    def test_many_inserts_and_deletes(self) -> None:
        rbt = RedBlackTree()
        for i in range(50):
            rbt.insert(i, i * 10)
        assert len(rbt) == 50
        for i in range(0, 50, 2):
            rbt.delete(i)
        assert len(rbt) == 25
        for i in range(1, 50, 2):
            assert rbt.search(i) == i * 10
