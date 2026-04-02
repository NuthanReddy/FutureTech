import pytest

from DataStructures.FenwickTree import FenwickTree


def test_prefix_and_range_sum() -> None:
    ft = FenwickTree([1, 2, 3, 4, 5])

    assert ft.prefix_sum(2) == 6
    assert ft.range_sum(1, 3) == 9


def test_update_changes_totals() -> None:
    ft = FenwickTree([5, 1, 2])
    ft.update(1, 4)

    assert ft.prefix_sum(1) == 10
    assert ft.range_sum(0, 2) == 12


def test_invalid_indices_raise() -> None:
    ft = FenwickTree([3, 1])
    with pytest.raises(IndexError):
        ft.prefix_sum(5)
    with pytest.raises(IndexError):
        ft.update(-1, 1)
    with pytest.raises(ValueError):
        ft.range_sum(1, 0)

