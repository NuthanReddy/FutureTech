import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from Problems.Backtracking.combination_sum import combination_sum
from Problems.Backtracking.combinations import combine
from Problems.Backtracking.generate_parentheses import generate_parenthesis
from Problems.Backtracking.letter_combinations_phone import letter_combinations
from Problems.Backtracking.n_queens_ii import total_n_queens
from Problems.Backtracking.permutations import permute
from Problems.Backtracking.word_search import exist
from Problems.Trie.design_add_search_words import WordDictionary
from Problems.Trie.implement_trie import Trie
from Problems.Trie.word_search_ii import find_words


def test_letter_combinations_phone() -> None:
    assert letter_combinations("") == []
    assert letter_combinations("23") == [
        "ad",
        "ae",
        "af",
        "bd",
        "be",
        "bf",
        "cd",
        "ce",
        "cf",
    ]


def test_combine_prunes_and_handles_edges() -> None:
    assert combine(4, 2) == [
        [1, 2],
        [1, 3],
        [1, 4],
        [2, 3],
        [2, 4],
        [3, 4],
    ]
    assert combine(3, 0) == [[]]
    assert combine(2, 3) == []


def test_permute_distinct_values() -> None:
    assert sorted(permute([1, 2, 3])) == sorted(
        [
            [1, 2, 3],
            [1, 3, 2],
            [2, 1, 3],
            [2, 3, 1],
            [3, 1, 2],
            [3, 2, 1],
        ]
    )


def test_combination_sum_reuses_candidates_without_duplicate_orderings() -> None:
    assert combination_sum([2, 3, 6, 7], 7) == [[2, 2, 3], [7]]
    assert combination_sum([2, 3, 5], 8) == [[2, 2, 2, 2], [2, 3, 3], [3, 5]]


def test_generate_parenthesis_and_n_queens() -> None:
    assert generate_parenthesis(3) == [
        "((()))",
        "(()())",
        "(())()",
        "()(())",
        "()()()",
    ]
    assert total_n_queens(1) == 1
    assert total_n_queens(4) == 2


def test_word_search_restores_board() -> None:
    board = [
        ["A", "B", "C", "E"],
        ["S", "F", "C", "S"],
        ["A", "D", "E", "E"],
    ]

    assert exist(board, "ABCCED") is True
    assert exist(board, "SEE") is True
    assert exist(board, "ABCB") is False
    assert board == [
        ["A", "B", "C", "E"],
        ["S", "F", "C", "S"],
        ["A", "D", "E", "E"],
    ]


def test_trie_operations() -> None:
    trie = Trie()
    trie.insert("apple")

    assert trie.search("apple") is True
    assert trie.search("app") is False
    assert trie.starts_with("app") is True
    assert trie.startsWith("app") is True

    trie.insert("app")
    assert trie.search("app") is True


def test_word_dictionary_wildcards() -> None:
    dictionary = WordDictionary()
    dictionary.add_word("bad")
    dictionary.add_word("dad")
    dictionary.addWord("mad")

    assert dictionary.search("pad") is False
    assert dictionary.search("bad") is True
    assert dictionary.search(".ad") is True
    assert dictionary.search("b..") is True
    assert dictionary.search("..") is False


def test_word_search_ii_uses_trie_prefix_pruning() -> None:
    board = [
        ["o", "a", "a", "n"],
        ["e", "t", "a", "e"],
        ["i", "h", "k", "r"],
        ["i", "f", "l", "v"],
    ]

    assert sorted(find_words(board, ["oath", "pea", "eat", "rain"])) == [
        "eat",
        "oath",
    ]
    assert board == [
        ["o", "a", "a", "n"],
        ["e", "t", "a", "e"],
        ["i", "h", "k", "r"],
        ["i", "f", "l", "v"],
    ]
