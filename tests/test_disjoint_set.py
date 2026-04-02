import pytest

from DataStructures.DisjointSet import DisjointSet


def test_union_and_connected() -> None:
    ds = DisjointSet()
    ds.union(1, 2)
    ds.union(2, 3)

    assert ds.connected(1, 3) is True
    assert ds.connected(1, 4) is False


def test_component_size() -> None:
    ds = DisjointSet()
    ds.union("a", "b")
    ds.union("b", "c")

    assert ds.component_size("a") == 3


def test_find_unknown_raises() -> None:
    ds = DisjointSet()
    with pytest.raises(KeyError):
        ds.find("missing")

