"""Targeted tests for the Top Interview 150 binary-search solutions."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from Problems.BinarySearch.solution import (
    find_median_sorted_arrays,
    find_min_rotated,
    find_peak_element,
    search_insert,
    search_matrix,
    search_range,
    search_rotated,
)


@pytest.mark.parametrize(
    ("nums", "target", "expected"),
    [
        ([1, 3, 5, 6], 5, 2),
        ([1, 3, 5, 6], 2, 1),
        ([1, 3, 5, 6], 7, 4),
        ([1, 3, 5, 6], 0, 0),
        ([], 4, 0),
    ],
)
def test_search_insert(nums: list[int], target: int, expected: int) -> None:
    assert search_insert(nums, target) == expected


def test_search_matrix_finds_and_rejects_values() -> None:
    matrix = [
        [1, 3, 5, 7],
        [10, 11, 16, 20],
        [23, 30, 34, 60],
    ]

    assert search_matrix(matrix, 3) is True
    assert search_matrix(matrix, 13) is False


@pytest.mark.parametrize("matrix", [[], [[]]])
def test_search_matrix_handles_empty_shapes(matrix: list[list[int]]) -> None:
    assert search_matrix(matrix, 1) is False


def test_find_peak_element_returns_a_valid_peak() -> None:
    nums = [1, 2, 1, 3, 5, 6, 4]
    peak_index = find_peak_element(nums)
    left_neighbor = nums[peak_index - 1] if peak_index > 0 else float("-inf")
    right_neighbor = (
        nums[peak_index + 1] if peak_index + 1 < len(nums) else float("-inf")
    )

    assert nums[peak_index] > left_neighbor
    assert nums[peak_index] > right_neighbor


def test_find_peak_element_handles_single_value() -> None:
    assert find_peak_element([9]) == 0


def test_find_peak_element_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match="at least one"):
        find_peak_element([])


@pytest.mark.parametrize(
    ("nums", "target", "expected"),
    [
        ([4, 5, 6, 7, 0, 1, 2], 0, 4),
        ([4, 5, 6, 7, 0, 1, 2], 3, -1),
        ([1], 1, 0),
        ([1], 0, -1),
        ([], 5, -1),
        ([5, 1, 3], 5, 0),
    ],
)
def test_search_rotated(nums: list[int], target: int, expected: int) -> None:
    assert search_rotated(nums, target) == expected


@pytest.mark.parametrize(
    ("nums", "target", "expected"),
    [
        ([5, 7, 7, 8, 8, 10], 8, [3, 4]),
        ([5, 7, 7, 8, 8, 10], 6, [-1, -1]),
        ([], 0, [-1, -1]),
        ([2, 2], 2, [0, 1]),
        ([1, 2, 3], 1, [0, 0]),
        ([1, 2, 3], 3, [2, 2]),
    ],
)
def test_search_range(
    nums: list[int],
    target: int,
    expected: list[int],
) -> None:
    assert search_range(nums, target) == expected


@pytest.mark.parametrize(
    ("nums", "expected"),
    [
        ([3, 4, 5, 1, 2], 1),
        ([4, 5, 6, 7, 0, 1, 2], 0),
        ([11, 13, 15, 17], 11),
        ([2, 1], 1),
        ([1], 1),
    ],
)
def test_find_min_rotated(nums: list[int], expected: int) -> None:
    assert find_min_rotated(nums) == expected


def test_find_min_rotated_rejects_empty_input() -> None:
    with pytest.raises(ValueError, match="at least one"):
        find_min_rotated([])


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [
        ([1, 3], [2], 2.0),
        ([1, 2], [3, 4], 2.5),
        ([], [1], 1.0),
        ([2], [], 2.0),
        ([0, 0], [0, 0], 0.0),
        ([1, 2, 7], [3, 4, 5, 6], 4.0),
    ],
)
def test_find_median_sorted_arrays(
    first: list[int],
    second: list[int],
    expected: float,
) -> None:
    assert find_median_sorted_arrays(first, second) == expected


def test_find_median_sorted_arrays_rejects_two_empty_inputs() -> None:
    with pytest.raises(ValueError, match="at least one"):
        find_median_sorted_arrays([], [])
