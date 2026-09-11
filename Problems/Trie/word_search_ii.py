"""Top Interview 150: Word Search II.

Problem statement:
    Input: a rectangular lowercase character grid ``board`` and a list of
    lowercase dictionary ``words``.
    Required output: all dictionary words that can be formed by adjacent
    horizontal or vertical board cells without reusing a cell in one word path.
    Key constraints: the canonical prompt uses boards up to 12 x 12, words of
    length up to 10, and up to 3 * 10^4 words. The board is restored before
    the function returns.

Trie/backtracking invariant:
    The board path spells the same prefix as the path from the trie root to
    ``node``. If the next board character is not a trie child, no dictionary
    word can be completed from that path, so the branch stops immediately.

Alternative:
    Run single-word Word Search for each word. That repeats most grid work and
    ignores shared prefixes. A trie merges those prefixes and searches the
    board once.

Complexity:
    Time: O(rows * cols * 4^L) worst case, where L is the longest word, but the
    trie usually prunes far earlier.
    Space: O(total dictionary characters + L recursion depth), excluding the
    returned words.
"""

from __future__ import annotations

from dataclasses import dataclass, field


VISITED = "#"
DIRECTIONS = ((1, 0), (-1, 0), (0, 1), (0, -1))


@dataclass(slots=True)
class _TrieNode:
    children: dict[str, "_TrieNode"] = field(default_factory=dict)
    word: str | None = None


def find_words(board: list[list[str]], words: list[str]) -> list[str]:
    """Return all words from ``words`` that appear in ``board``."""
    if not board or not board[0] or not words:
        return []

    root = _TrieNode()
    for word in set(words):
        node = root
        for char in word:
            node = node.children.setdefault(char, _TrieNode())
        node.word = word

    rows = len(board)
    cols = len(board[0])
    found: list[str] = []

    def dfs(row: int, col: int, parent: _TrieNode) -> None:
        char = board[row][col]
        node = parent.children.get(char)
        if node is None:
            return

        if node.word is not None:
            found.append(node.word)
            node.word = None

        board[row][col] = VISITED
        for row_delta, col_delta in DIRECTIONS:
            next_row = row + row_delta
            next_col = col + col_delta
            if (
                0 <= next_row < rows
                and 0 <= next_col < cols
                and board[next_row][next_col] != VISITED
            ):
                dfs(next_row, next_col, node)
        board[row][col] = char

        # Once a trie leaf has no remaining word, later starts cannot benefit
        # from keeping it. This is safe because all words under it are found or
        # impossible from the explored prefix.
        if not node.children and node.word is None:
            del parent.children[char]

    for row in range(rows):
        for col in range(cols):
            if board[row][col] in root.children:
                dfs(row, col, root)

    return found


if __name__ == "__main__":
    sample_board = [
        ["o", "a", "a", "n"],
        ["e", "t", "a", "e"],
        ["i", "h", "k", "r"],
        ["i", "f", "l", "v"],
    ]
    print(find_words(sample_board, ["oath", "pea", "eat", "rain"]))
