"""Valid Anagram - frequency-map comparison pattern.

Problem statement:
    Given strings ``first`` and ``second``, return whether ``second`` is an
    an anagram of ``first``: it must use exactly the same characters with the
    same multiplicities, in any order.  Characters are treated literally and
    case-sensitively; an empty string is valid input.
"""

from __future__ import annotations


def is_anagram(first: str, second: str) -> bool:
    """Return whether *first* and *second* are anagrams.

    Pattern identification: equal character multiplicities, order irrelevant -> counts;
    each count tracks unmatched occurrences from first and must never go negative.

    Counting rather than sorting makes the condition explicit: two strings
    are anagrams exactly when every character has equal frequency.  The
    length check is an inexpensive early rejection and also makes the final
    comparison easier to reason about.

    This function treats characters literally, matching LeetCode's
    case-sensitive input contract; it does not silently normalize case or
    whitespace.  Sorting is a valid alternative, but costs ``O(n log n)``
    instead of the frequency-map solution's average ``O(n)``.

    Complexity:
        Time ``O(n)`` average, space ``O(u)`` for ``u`` distinct characters.
    """
    # 1. Output: Return whether the strings contain exactly the same characters with the same counts.
    # 2. Structure: Order does not matter, but copies do: a set alone cannot distinguish "aab" from "abb".
    # 3. Constraints: Matching is case-sensitive and literal; unequal lengths fail, two empty strings pass.
    # 4. Choice: Count first's characters, then subtract each character in second and reject missing/excess copies.
    # 5. Why it works: Equal lengths and no negative count mean every counted copy was consumed exactly once.
    #    Average O(n) time and O(u) extra space for u distinct characters.
    if len(first) != len(second):
        return False

    frequencies: dict[str, int] = {}
    for character in first:
        frequencies[character] = frequencies.get(character, 0) + 1

    for character in second:
        if character not in frequencies:
            return False
        frequencies[character] -= 1
        if frequencies[character] < 0:
            return False

    return True


if __name__ == "__main__":
    assert is_anagram("anagram", "nagaram")
    assert not is_anagram("rat", "car")
    print("is_anagram examples passed")
