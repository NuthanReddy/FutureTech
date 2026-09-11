"""Focused tests for the Top Interview 150 Arrays & Hashing group."""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from Problems.ArraysHashing.Frequency.top_k_frequent_elements import (
    top_k_frequent,
)
from Problems.ArraysHashing.Grouping.group_anagrams import group_anagrams
from Problems.ArraysHashing.HashMap.contains_duplicate import contains_duplicate
from Problems.ArraysHashing.HashMap.two_sum import two_sum
from Problems.ArraysHashing.HashMap.valid_anagram import is_anagram
from Problems.ArraysHashing.Matrix.valid_sudoku import is_valid_sudoku
from Problems.ArraysHashing.Prefix.product_of_array_except_self import (
    product_except_self,
)
from Problems.ArraysHashing.Set.longest_consecutive_sequence import (
    longest_consecutive,
)
from Problems.Heap.top_k_frequent_elements import (
    top_k_frequent as heap_group_top_k_frequent,
)


def test_contains_duplicate_handles_duplicates_and_empty_input() -> None:
    assert contains_duplicate([1, 2, 3, 1])
    assert not contains_duplicate([1, 2, 3, 4])
    assert not contains_duplicate([])


def test_two_sum_handles_duplicate_values_and_missing_pair() -> None:
    assert two_sum([2, 7, 11, 15], 9) == [0, 1]
    assert two_sum([3, 3], 6) == [0, 1]
    assert two_sum([1, 2, 4], 8) == []


def test_valid_anagram_is_case_sensitive_and_counts_multiplicity() -> None:
    assert is_anagram("anagram", "nagaram")
    assert not is_anagram("rat", "car")
    assert not is_anagram("aab", "abb")
    assert not is_anagram("a", "A")


def test_group_anagrams_uses_frequency_equivalence_classes() -> None:
    groups = group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"])
    assert {frozenset(group) for group in groups} == {
        frozenset({"eat", "tea", "ate"}),
        frozenset({"tan", "nat"}),
        frozenset({"bat"}),
    }
    assert group_anagrams([]) == []


def test_top_k_frequent_and_legacy_heap_import_share_one_implementation() -> None:
    expected_values = {1, 2}
    assert set(top_k_frequent([1, 1, 1, 2, 2, 3], 2)) == expected_values
    assert set(heap_group_top_k_frequent([1, 1, 1, 2, 2, 3], 2)) == expected_values
    assert top_k_frequent([1, 2], 5) == [1, 2]
    assert top_k_frequent([1, 2], 0) == []


def test_product_except_self_handles_zero_without_division() -> None:
    assert product_except_self([1, 2, 3, 4]) == [24, 12, 8, 6]
    assert product_except_self([-1, 1, 0, -3, 3]) == [0, 0, 9, 0, 0]
    assert product_except_self([]) == []


def test_valid_sudoku_checks_rows_columns_boxes_and_shape() -> None:
    valid_board = [
        ["5", "3", ".", ".", "7", ".", ".", ".", "."],
        ["6", ".", ".", "1", "9", "5", ".", ".", "."],
        [".", "9", "8", ".", ".", ".", ".", "6", "."],
        ["8", ".", ".", ".", "6", ".", ".", ".", "3"],
        ["4", ".", ".", "8", ".", "3", ".", ".", "1"],
        ["7", ".", ".", ".", "2", ".", ".", ".", "6"],
        [".", "6", ".", ".", ".", ".", "2", "8", "."],
        [".", ".", ".", "4", "1", "9", ".", ".", "5"],
        [".", ".", ".", ".", "8", ".", ".", "7", "9"],
    ]
    assert is_valid_sudoku(valid_board)

    invalid_column = [row[:] for row in valid_board]
    invalid_column[0][0] = "6"
    assert not is_valid_sudoku(invalid_column)
    assert not is_valid_sudoku([["."]])


def test_longest_consecutive_ignores_duplicates_and_unsorted_order() -> None:
    assert longest_consecutive([100, 4, 200, 1, 3, 2]) == 4
    assert longest_consecutive([0, 3, 7, 2, 5, 8, 4, 6, 0, 1]) == 9
    assert longest_consecutive([]) == 0
