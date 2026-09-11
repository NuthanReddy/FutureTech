"""Top Interview 150: Design Add and Search Words Data Structure.

Problem statement:
    Input: a stream of addWord(word) and search(pattern) operations. In search
    patterns, "." matches exactly one character.
    Required output: addWord stores a lowercase word; search returns whether
    any stored word matches the full pattern.
    Key constraints: words are lowercase, search patterns contain lowercase
    letters or ".", word length is up to 25, and there are up to 10^4
    operations in the canonical prompt.

Invariant:
    During wildcard DFS, ``node`` represents every dictionary word prefix that
    matches ``pattern[:index]`` along the current branch. When index reaches the
    pattern length, a match exists only if the node is a complete word.

Alternative:
    Bucket words by length and compare patterns character-by-character. That is
    often fine for small data, but a trie avoids scanning unrelated prefixes
    and shares memory between common prefixes.

Complexity:
    add_word: O(L)
    search: O(L) without wildcards; O(26^W * L) worst case with W wildcards.
    Space: O(total inserted characters + L recursion depth).
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class _Node:
    children: dict[str, "_Node"] = field(default_factory=dict)
    is_word: bool = False


class WordDictionary:
    """Trie-backed dictionary with single-character wildcard search."""

    def __init__(self) -> None:
        self.root = _Node()

    def add_word(self, word: str) -> None:
        """Add ``word`` to the dictionary."""
        node = self.root
        for char in word:
            node = node.children.setdefault(char, _Node())
        node.is_word = True

    def addWord(self, word: str) -> None:  # noqa: N802
        """LeetCode-compatible alias for ``add_word``."""
        self.add_word(word)

    def search(self, pattern: str) -> bool:
        """Return True when ``pattern`` matches a stored word."""

        def dfs(node: _Node, index: int) -> bool:
            if index == len(pattern):
                return node.is_word

            char = pattern[index]
            if char == ".":
                return any(dfs(child, index + 1) for child in node.children.values())

            child = node.children.get(char)
            return child is not None and dfs(child, index + 1)

        return dfs(self.root, 0)


if __name__ == "__main__":
    words = WordDictionary()
    words.add_word("bad")
    words.add_word("dad")
    words.add_word("mad")
    print(words.search(".ad"))
