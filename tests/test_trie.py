import pytest

from DataStructures.Trie import Trie


def test_insert_search_and_starts_with() -> None:
    trie = Trie()
    trie.insert("cat")
    trie.insert("car")

    assert trie.search("cat") is True
    assert trie.search("cap") is False
    assert trie.starts_with("ca") is True
    assert trie.starts_with("do") is False


def test_count_prefix_with_duplicates() -> None:
    trie = Trie()
    trie.insert("car")
    trie.insert("car")
    trie.insert("cart")
    trie.insert("cat")

    assert trie.count_prefix("car") == 3
    assert trie.count_prefix("ca") == 4
    assert trie.count_prefix("") == 4


def test_delete_existing_and_missing_word() -> None:
    trie = Trie()
    trie.insert("dog")
    trie.insert("door")

    assert trie.delete("dog") is True
    assert trie.search("dog") is False
    assert trie.search("door") is True
    assert trie.delete("dog") is False


def test_invalid_input_raises() -> None:
    trie = Trie()
    with pytest.raises(ValueError):
        trie.insert("")

