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
