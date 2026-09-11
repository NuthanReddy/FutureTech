"""Top Interview 150: Implement Trie (Prefix Tree).

Problem statement:
    Input: a stream of insert(word), search(word), and startsWith(prefix)
    operations.
    Required output: insert stores a word; search returns whether the exact
    word was inserted; startsWith returns whether any inserted word has the
    given prefix.
    Key constraints: lowercase English strings, 1 <= len(word/prefix) <= 2000,
    and up to 3 * 10^4 operations in the canonical prompt.

Backtracking is not needed for basic trie operations: each character makes a
single deterministic transition.

Invariant:
    Every node represents the prefix formed by the path from the root to that
    node. ``is_word`` is true only when that prefix was inserted as a complete
    word.

Alternatives:
    A set can answer exact word lookup, and a sorted list can answer prefix
    queries with binary search. A trie is preferable when prefix operations are
    frequent and many words share prefixes.

Complexity:
    Time: O(L) for insert/search/startsWith, where L is the input length.
    Space: O(total inserted characters) in the worst case.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class TrieNode:
    children: dict[str, "TrieNode"] = field(default_factory=dict)
    is_word: bool = False


class Trie:
    """A prefix tree supporting insert, exact search, and prefix search."""

    def __init__(self) -> None:
        self.root = TrieNode()

    def insert(self, word: str) -> None:
        """Insert ``word`` into the trie."""
        node = self.root
        for char in word:
            node = node.children.setdefault(char, TrieNode())
        node.is_word = True

    def search(self, word: str) -> bool:
        """Return True if ``word`` was inserted as a complete word."""
        node = self._find_node(word)
        return node is not None and node.is_word

    def starts_with(self, prefix: str) -> bool:
        """Return True if any inserted word starts with ``prefix``."""
        return self._find_node(prefix) is not None

    # LeetCode uses camelCase. Keep an alias so the practice API matches both
    # Python style and the original prompt.
    def startsWith(self, prefix: str) -> bool:  # noqa: N802
        """LeetCode-compatible alias for ``starts_with``."""
        return self.starts_with(prefix)

    def _find_node(self, text: str) -> TrieNode | None:
        node = self.root
        for char in text:
            if char not in node.children:
                return None
            node = node.children[char]
        return node


if __name__ == "__main__":
    trie = Trie()
    trie.insert("apple")
    print(trie.search("apple"))
    print(trie.search("app"))
    print(trie.starts_with("app"))
