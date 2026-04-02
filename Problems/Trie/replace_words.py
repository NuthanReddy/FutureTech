# Replace Words (LeetCode 648)
#
# DS used: Trie (DataStructures/Trie.py)
#
# Problem:
# Given a dictionary of root words and a sentence, replace every derivative
# word in the sentence with the *shortest* root that is its prefix.
#
# Example:
#   roots = ["cat", "bat", "rat"]
#   sentence = "the cattle was rattled by the battery"
#   Output: "the cat was rat by the bat"
#
# Why Trie?
# A trie lets us walk each word character-by-character and stop as soon as we
# hit a node marked as end-of-word → O(L) per word where L is root length,
# much faster than checking every root with str.startswith().
#
# Time:  O(R·K + W·L)  — R roots of avg length K, W words of avg length L
# Space: O(R·K)         — trie storage

from __future__ import annotations

from typing import Dict, List


class _TrieNode:
    __slots__ = ("children", "is_end")

    def __init__(self) -> None:
        self.children: Dict[str, _TrieNode] = {}
        self.is_end: bool = False


def replace_words(roots: List[str], sentence: str) -> str:
    """Replace every word in *sentence* with its shortest matching root."""
    # Build trie from root words
    trie_root = _TrieNode()
    for word in roots:
        node = trie_root
        for ch in word:
            if ch not in node.children:
                node.children[ch] = _TrieNode()
            node = node.children[ch]
        node.is_end = True

    def _shortest_root(word: str) -> str:
        """Walk the trie; return the shortest root prefix or the word itself."""
        node = trie_root
        for i, ch in enumerate(word):
            if ch not in node.children:
                break
            node = node.children[ch]
            if node.is_end:
                return word[: i + 1]
        return word

    return " ".join(_shortest_root(w) for w in sentence.split())


if __name__ == "__main__":
    roots = ["cat", "bat", "rat"]
    sentence = "the cattle was rattled by the battery"
    print(replace_words(roots, sentence))
    # Output: "the cat was rat by the bat"

    # Edge: root is the full word
    print(replace_words(["a", "aa", "aaa"], "aadsfasf adfadfasdfasf bbbb"))
    # Output: "a a bbbb"

