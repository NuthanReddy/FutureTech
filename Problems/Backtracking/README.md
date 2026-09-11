# Top Interview 150: Backtracking

Backtracking problems are depth-first searches over a decision tree. The
important invariant is that the partial answer is always valid before recursion
continues; each recursive call makes one choice, explores it, and then undoes
that choice before trying the next candidate.

## Implemented coverage

| Problem | File | Core pattern | Time | Space |
|---|---|---|---|---|
| Letter Combinations of a Phone Number | `letter_combinations_phone.py` | Fixed-depth product search | `O(4^n * n)` | `O(n)` recursion, excluding output |
| Combinations | `combinations.py` | Choose/skip with pruning | `O(C(n, k) * k)` | `O(k)` recursion, excluding output |
| Permutations | `permutations.py` | Used-set ordering | `O(n! * n)` | `O(n)` recursion, excluding output |
| Combination Sum | `combination_sum.py` | Reuse candidates from current index | Exponential in target/candidates | `O(target / min(candidates))` recursion |
| Generate Parentheses | `generate_parentheses.py` | Constrained binary choices | Catalan count `O(C_n * n)` | `O(n)` recursion, excluding output |
| N-Queens II | `n_queens_ii.py` | Constraint sets for columns/diagonals | `O(n!)` upper bound | `O(n)` |
| Word Search | `word_search.py` | Grid DFS with in-place visited marks | `O(m*n*4^L)` | `O(L)` recursion |

## Problem statements

### Letter Combinations of a Phone Number

- **Input:** A string `digits` containing only keypad digits `2` through `9`.
- **Required output:** All possible strings formed by choosing one mapped
  letter for each digit, in digit order. Return `[]` for an empty input.
- **Key constraints:** LeetCode's Top 150 version uses `0 <= len(digits) <= 4`;
  digits `0` and `1` have no letter mapping and are rejected by this module.

### Combinations

- **Input:** Two integers `n` and `k`.
- **Required output:** Every size-`k` combination chosen from integers `1..n`,
  with each combination in increasing order and no duplicate combinations.
- **Key constraints:** Canonical prompt uses `1 <= n <= 20` and `1 <= k <= n`.
  This module also handles `k == 0` as `[[]]` and `k > n` as `[]`.

### Permutations

- **Input:** A list `nums` of distinct integers.
- **Required output:** Every possible ordering of all values in `nums`.
- **Key constraints:** Canonical prompt uses `1 <= len(nums) <= 6`; all values
  are distinct, so no duplicate-skipping logic is needed.

### Combination Sum

- **Input:** A list of positive candidate integers and a non-negative `target`.
- **Required output:** Unique combinations whose values sum to `target`; each
  candidate may be reused unlimited times.
- **Key constraints:** Canonical prompt gives distinct positive candidates and
  positive target. This module deduplicates candidates defensively and rejects
  non-positive candidates because they break the decreasing-target invariant.

### Generate Parentheses

- **Input:** An integer `n`, the number of parentheses pairs.
- **Required output:** All well-formed parentheses strings containing exactly
  `n` opening and `n` closing parentheses.
- **Key constraints:** Canonical prompt uses `1 <= n <= 8`. This module also
  accepts `n == 0`, returning `[""]`.

### N-Queens II

- **Input:** An integer `n`, the side length of an `n x n` chessboard.
- **Required output:** The count of distinct ways to place `n` queens so that
  no two queens share a row, column, or diagonal.
- **Key constraints:** Canonical prompt uses `1 <= n <= 9`; the implementation
  supports `n == 0` as one empty placement.

### Word Search

- **Input:** A rectangular character grid `board` and a string `word`.
- **Required output:** `True` if `word` can be formed by adjacent horizontal or
  vertical cells without reusing a cell in the same path; otherwise `False`.
- **Key constraints:** Canonical prompt uses small boards (`m, n <= 6`) and a
  non-empty word. The board is restored before the function returns.

## Pattern critiques and decision notes

- Backtracking is not a data structure; it is a disciplined way to enumerate
  legal states. If a problem asks for all answers, the output size usually
  dominates and no dynamic programming table can avoid listing them.
- Pruning should be tied to a clear invariant. For example, `combine` stops
  early when there are not enough remaining numbers to fill the path, while
  `generate_parentheses` never creates a prefix with more closing than opening
  parentheses.
- Sorting is useful only when it supports pruning or deterministic output. In
  `combination_sum`, sorted candidates let the DFS break once a candidate is
  larger than the remaining target.
- The Word Search family overlaps with Trie. A single-word search is cleaner as
  grid backtracking; multi-word search should switch to Trie prefix pruning
  (`Problems/Trie/word_search_ii.py`) so dead prefixes stop immediately.
- In-place mutation is acceptable when the undo step is adjacent and symmetric.
  The grid search temporarily marks a cell as visited and restores it before
  returning; that keeps the memory bound small without leaking state across
  branches.

## General template

```python
def dfs(state):
    if complete(state):
        record_answer()
        return

    for choice in legal_choices(state):
        make(choice)
        dfs(next_state)
        undo(choice)
```

The `make` and `undo` steps are the invariant boundary: every loop iteration
must see the same state it would have seen if earlier branches had never run.
