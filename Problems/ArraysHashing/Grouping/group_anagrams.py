"""Group Anagrams - canonical representation pattern.

Problem statement:
    Given a list of lowercase English words, group all words that are
    anagrams of one another and return the groups in any order.  Anagrams
    contain the same letters with the same multiplicities, but may have a
    different order.  An empty list produces an empty list of groups.
"""

from __future__ import annotations


def group_anagrams(words: list[str]) -> list[list[str]]:
    """Group anagrams while preserving input order within each group.

    A fixed 26-count tuple is the canonical key for lowercase English words,
    which is the LeetCode constraint.  Words with the same tuple have exactly
    the same frequency for every letter, so they belong to the same bucket.
    ``groups`` is a dictionary from that key to the output bucket.

    Sorting each word is simpler to write but costs ``O(k log k)`` per word.
    Counting costs ``O(k)`` and makes the grouping invariant visible.  The
    implementation intentionally does not sort the result: group order is not
    part of the problem contract, while retaining insertion order keeps
    examples predictable and avoids unnecessary work.

    Complexity:
        For ``n`` words of maximum length ``k``: time ``O(nk)``, space
        ``O(nk)`` including the returned groups.
    """
    groups: dict[tuple[int, ...], list[str]] = {}
    for word in words:
        counts = [0] * 26
        for character in word:
            counts[ord(character) - ord("a")] += 1
        key = tuple(counts)
        groups.setdefault(key, []).append(word)
    return list(groups.values())


if __name__ == "__main__":
    result = group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"])
    assert {frozenset(group) for group in result} == {
        frozenset({"eat", "tea", "ate"}),
        frozenset({"tan", "nat"}),
        frozenset({"bat"}),
    }
    print("group_anagrams examples passed")
