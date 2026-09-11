# Trie Problems

A trie stores strings by prefix. Each edge consumes one character, and a node
marks whether the path so far is a complete word. This folder contains both
general trie exercises and Top Interview 150 trie problems.

## Implemented coverage

| Problem | File | Core pattern | Time | Space |
|---|---|---|---|---|
| Implement Trie | `implement_trie.py` | Prefix tree operations | `O(L)` per operation | `O(total characters)` |
| Design Add and Search Words Data Structure | `design_add_search_words.py` | Trie plus wildcard DFS | `O(26^wildcards * L)` worst case | `O(total characters + L)` |
| Word Search II | `word_search_ii.py` | Trie-guided grid backtracking | `O(m*n*4^L)` worst case, pruned by prefixes | `O(total dictionary chars + L)` |
| Longest Common Prefix | `longest_common_prefix.py` | Single-child prefix walk | `O(total characters)` | `O(total characters)` |
| Replace Words | `replace_words.py` | Shortest root prefix lookup | `O(total root chars + sentence chars)` | `O(total root chars)` |

## Problem statements

### Implement Trie

- **Input:** A stream of operations: `insert(word)`, `search(word)`, and
  `startsWith(prefix)` / `starts_with(prefix)`.
- **Required output:** `insert` stores a word; `search` returns whether the
  exact word was inserted; `startsWith` returns whether any inserted word has
  the given prefix.
- **Key constraints:** Canonical prompt uses lowercase English strings with
  `1 <= len(word/prefix) <= 2000` and up to `3 * 10^4` operations.

### Design Add and Search Words Data Structure

- **Input:** A stream of operations: `addWord(word)` / `add_word(word)` and
  `search(pattern)`, where `.` in `pattern` matches exactly one character.
- **Required output:** `addWord` stores a lowercase word; `search` returns
  whether any stored word matches the full pattern.
- **Key constraints:** Canonical prompt uses lowercase words, patterns
  containing lowercase letters or `.`, word length up to 25, and up to `10^4`
  operations.

### Word Search II

- **Input:** A rectangular lowercase character grid `board` and a list of
  lowercase dictionary `words`.
- **Required output:** All dictionary words that can be formed by adjacent
  horizontal or vertical board cells without reusing a cell in one word path.
- **Key constraints:** Canonical prompt uses boards up to `12 x 12`, words of
  length up to 10, and up to `3 * 10^4` words. The board is restored before
  the function returns.

### Longest Common Prefix

- **Input:** A list `strs` of strings.
- **Required output:** The longest prefix shared by every string in the list,
  or `""` when there is no common prefix.
- **Key constraints:** Canonical prompt uses `1 <= len(strs) <= 200` and
  `0 <= len(strs[i]) <= 200`; strings contain lowercase English letters when
  non-empty.

### Replace Words

- **Input:** A dictionary of root words and a sentence made of words separated
  by single spaces.
- **Required output:** A sentence where every derivative word is replaced by
  the shortest dictionary root that is its prefix; words with no root prefix
  stay unchanged.
- **Key constraints:** Canonical prompt uses lowercase English words, up to
  `1000` roots, root length up to `100`, and sentence length up to `10^6`.

## Critique and overlap notes

- A trie is most valuable when many strings share prefixes. For a single lookup
  in a small list, a set or direct string comparison is simpler and often
  faster in practice.
- Wildcard search turns the trie into a DFS problem. The invariant is that the
  current node represents all words matching the consumed pattern prefix.
- Word Search II is the strongest overlap with Backtracking. Plain Word Search
  checks one target word, so grid DFS is enough. Word Search II checks many
  words; a trie prevents exploring board paths that are not prefixes of any
  remaining word.
- Pruning completed or exhausted trie nodes matters. The Word Search II
  implementation deletes leaf branches after they are fully explored so later
  board starts do less work.
- Existing `longest_common_prefix.py` and `replace_words.py` intentionally keep
  their own small node classes. For interview practice that is useful because
  each file is standalone; production code would likely share a node/helper to
  avoid duplication.
