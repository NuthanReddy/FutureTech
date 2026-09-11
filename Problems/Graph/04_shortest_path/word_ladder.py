"""Word Ladder (LeetCode 127).

Problem: Transform ``begin_word`` into ``end_word`` by changing one character
at a time; every intermediate word must be in ``word_list``.  Output: the
number of words in the shortest sequence, including both endpoints, or ``0``
if none exists.  Constraints: words have equal length and use lowercase
letters.
"""

from collections import defaultdict, deque
from typing import List


def ladder_length(begin_word: str, end_word: str, word_list: List[str]) -> int:
    """Return the number of words in the shortest valid transformation."""
    words = set(word_list)
    if end_word not in words:
        return 0
    patterns = defaultdict(list)
    for word in words | {begin_word}:
        for i in range(len(word)):
            patterns[word[:i] + "*" + word[i + 1:]].append(word)
    queue = deque([(begin_word, 1)])
    seen = {begin_word}
    while queue:
        word, distance = queue.popleft()
        if word == end_word:
            return distance
        for i in range(len(word)):
            for neighbor in patterns[word[:i] + "*" + word[i + 1:]]:
                if neighbor not in seen:
                    seen.add(neighbor)
                    queue.append((neighbor, distance + 1))
    return 0
