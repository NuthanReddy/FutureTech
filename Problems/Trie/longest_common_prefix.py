# Longest Common Prefix using Trie (LeetCode 14 — Trie approach)
#
# DS used: Trie (DataStructures/Trie.py)
#
# Problem:
# Given an array of strings, find the longest common prefix among all strings.
#
# Example:
#   strs = ["flower", "flow", "flight"]
#   Output: "fl"
#
# Why Trie?
# Insert every string, then walk from the root while each node has exactly one
# child and is not marked as a word end.  The path so far IS the common prefix.
# This generalises well when we also need prefix-count queries later.
#
# Time:  O(S) total characters across all strings
# Space: O(S) trie nodes

from __future__ import annotations

from typing import Dict, List


class _TrieNode:
    __slots__ = ("children", "is_end")

    def __init__(self) -> None:
        self.children: Dict[str, _TrieNode] = {}
        self.is_end: bool = False


def longest_common_prefix(strs: List[str]) -> str:
    """Return the longest common prefix of all strings using a Trie."""
    if not strs:
        return ""

    # Build trie
    root = _TrieNode()
    for word in strs:
        if word == "":
            return ""  # empty string means prefix is ""
        node = root
        for ch in word:
            if ch not in node.children:
                node.children[ch] = _TrieNode()
            node = node.children[ch]
        node.is_end = True

    # Walk while there is a single unbranching path
    prefix: list[str] = []
    node = root
    while len(node.children) == 1 and not node.is_end:
        ch = next(iter(node.children))
        prefix.append(ch)
        node = node.children[ch]

    return "".join(prefix)


if __name__ == "__main__":
    print(longest_common_prefix(["flower", "flow", "flight"]))  # "fl"
    print(longest_common_prefix(["dog", "racecar", "car"]))     # ""
    print(longest_common_prefix(["interspecies", "interstellar", "interstate"]))  # "inters"
    print(longest_common_prefix(["a"]))                          # "a"
    print(longest_common_prefix([""]))                           # ""

