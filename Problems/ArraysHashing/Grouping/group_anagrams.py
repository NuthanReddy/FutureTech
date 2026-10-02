"""Group Anagrams - canonical representation pattern.

Problem statement:
    Given a list of lowercase English words, group all words that are
    anagrams of one another and return the groups in any order.  Anagrams
    contain the same letters with the same multiplicities, but may have a
    different order.  An empty list produces an empty list of groups.
"""

from __future__ import annotations

from collections import Counter, defaultdict


def group_anagrams(words: list[str]) -> list[list[str]]:
    """Group anagrams while preserving input order within each group.

    Pattern identification: group words regardless of letter order -> count keys;
    each bucket contains only words with identical character multiplicities.

    A frozenset of character-count pairs is the canonical key.  Words with the
    same key have exactly the same frequency for every character, so they
    belong to the same bucket.  ``groups`` is a dictionary from that key to the
    output bucket.

    ``Counter`` makes the frequency invariant explicit, while ``frozenset``
    makes the unordered character-count pairs hashable.  The implementation
    intentionally does not sort the result: group order is not part of the
    problem contract, while retaining insertion order keeps examples
    predictable.

    Complexity:
        For ``n`` words of maximum length ``k``: time ``O(nk)``, space
        ``O(nk)`` including the returned groups.
    """
    # 1. Output: Return groups of anagram words, retaining input order within each group.
    # 2. Structure: Letter order can change, but each letter's number of copies must stay the same.
    # 3. Constraints: Treat characters literally; repeated and empty words are valid, as is an empty list.
    # 4. Choice: Count each word's letters; a frozenset makes those unordered counts a shared dictionary key.
    #    Append the word to that key's group instead of comparing it with every earlier word.
    # 5. Why it works: Two keys agree exactly when all character counts agree, so buckets are anagram groups.
    #    Average time is O(nk) for n words of maximum length k; keys use O(nk) extra space.
    groups: defaultdict[frozenset[tuple[str, int]], list[str]] = defaultdict(
        list
    )
    for word in words:
        key = frozenset(Counter(word).items())
        groups[key].append(word)
    return list(groups.values())


if __name__ == "__main__":
    result = group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"])
    assert {frozenset(group) for group in result} == {
        frozenset({"eat", "tea", "ate"}),
        frozenset({"tan", "nat"}),
        frozenset({"bat"}),
    }
    print("group_anagrams examples passed")
