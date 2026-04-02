from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


@dataclass
class _TrieNode:
    children: Dict[str, "_TrieNode"] = field(default_factory=dict)
    end_count: int = 0
    pass_count: int = 0


class Trie:
    """Prefix tree supporting insert/search/prefix counts and deletion."""

    def __init__(self) -> None:
        self._root = _TrieNode()
        self._size = 0

    def insert(self, word: str) -> None:
        self._validate_word(word)
        node = self._root
        for ch in word:
            node = node.children.setdefault(ch, _TrieNode())
            node.pass_count += 1
        node.end_count += 1
        self._size += 1

    def search(self, word: str) -> bool:
        self._validate_word(word)
        node = self._get_node(word)
        return node is not None and node.end_count > 0

    def starts_with(self, prefix: str) -> bool:
        if prefix == "":
            return True
        self._validate_word(prefix)
        return self._get_node(prefix) is not None

    def count_prefix(self, prefix: str) -> int:
        if prefix == "":
            return self._size
        self._validate_word(prefix)
        node = self._get_node(prefix)
        return 0 if node is None else node.pass_count

    def delete(self, word: str) -> bool:
        self._validate_word(word)
        node = self._root
        path = []

        for ch in word:
            if ch not in node.children:
                return False
            path.append((node, ch))
            node = node.children[ch]

        if node.end_count == 0:
            return False

        node.end_count -= 1
        self._size -= 1

        # Update pass counts in reverse order and prune empty branches.
        for parent, ch in reversed(path):
            child = parent.children[ch]
            child.pass_count -= 1
            if child.pass_count == 0 and child.end_count == 0 and not child.children:
                del parent.children[ch]
        return True

    def _get_node(self, text: str) -> _TrieNode | None:
        node = self._root
        for ch in text:
            if ch not in node.children:
                return None
            node = node.children[ch]
        return node

    @staticmethod
    def _validate_word(word: str) -> None:
        if not isinstance(word, str) or word == "":
            raise ValueError("word/prefix must be a non-empty string")


if __name__ == "__main__":
    trie = Trie()
    trie.insert("cat")
    trie.insert("car")
    trie.insert("cart")
    print(trie.search("car"))
    print(trie.starts_with("ca"))
    print(trie.count_prefix("car"))

