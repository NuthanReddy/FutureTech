# Arrays & Hashing

This group covers the eight LeetCode Top Interview 150 problems commonly
organized as the **Arrays & Hashing** foundation: `Contains Duplicate`,
`Two Sum`, `Valid Anagram`, `Group Anagrams`, `Top K Frequent Elements`,
`Product of Array Except Self`, `Valid Sudoku`, and `Longest Consecutive
Sequence`.  Each problem is executable on its own and exposes a small
snake-case function for pytest.

## Problem statements

| Problem | Input | Required output |
| --- | --- | --- |
| Contains Duplicate | Integer array `nums` | Whether any value appears at least twice |
| Two Sum | Integer array `nums` and integer `target` | Indices of two distinct values whose sum is `target`, or no-pair result |
| Valid Anagram | Strings `s` and `t` | Whether `t` contains exactly the same character counts as `s` |
| Group Anagrams | Array of strings | The strings grouped into anagram-equivalent groups |
| Top K Frequent Elements | Integer array `nums` and integer `k` | The `k` values with highest frequency |
| Product of Array Except Self | Integer array `nums` | An output array where each position is the product of all other values |
| Valid Sudoku | Partially filled 9x9 board | Whether the current board obeys row, column, and 3x3-box rules |
| Longest Consecutive Sequence | Unsorted integer array `nums` | Length of the longest run of consecutive integers |

The solution docstrings record the known constraints and edge cases for each
problem. The implementations use the function signatures expected by the
focused tests and do not mutate caller-owned input unless explicitly stated.

## Pattern identification steps

1. Look for repeated values, complements, equal counts, or order-independent groups.
2. Choose membership sets, index/count maps, canonical keys, or frequency buckets.
3. For all-except-self products, choose prefix/suffix accumulation instead of hashing.
4. State what each key or accumulator represents; check before inserting when indices must differ.
5. Do not use unordered state for adjacency/order constraints; do not use count buckets for unbounded streams.

## Pattern map

| Pattern | Problems / identification cue | State and invariant | Complexity |
| --- | --- | --- | --- |
| Membership map/set | [Contains Duplicate](HashMap/contains_duplicate.py): repeated value; [Two Sum](HashMap/two_sum.py): target complement, original indices | State describes exactly the processed prefix; a hit proves the required relation. | `O(n)` average time, `O(n)` space |
| Character frequencies | [Valid Anagram](HashMap/valid_anagram.py): same characters with multiplicity, order irrelevant | Counts track unmatched characters from the first string. | `O(n)` time, `O(u)` space |
| Canonical keys | [Group Anagrams](Grouping/group_anagrams.py): partition words by equal multiplicities | Equal keys mean equal character multiplicities, so one dictionary bucket is one equivalence class. | `O(nk)` time, `O(nk)` output-inclusive space |
| Frequency buckets | [Top K Frequent Elements](Frequency/top_k_frequent_elements.py): rank values by bounded occurrence counts | Bucket `c` contains precisely values occurring `c` times; scanning downward yields highest frequencies first. | `O(n + u)` time, `O(n + u)` space |
| Prefix/suffix products | [Product of Array Except Self](Prefix/product_of_array_except_self.py): exclude each index without division | Each output is left product times right product; neither pass includes the current item. | `O(n)` time, `O(1)` auxiliary space |
| Region validation | [Valid Sudoku](Matrix/valid_sudoku.py): uniqueness in overlapping regions | Each row, column, and box set contains only values already seen in that region. | `O(1)` on 9x9, `O(r*c)` generally |
| Set starts | [Longest Consecutive Sequence](Set/longest_consecutive_sequence.py): consecutive values, not adjacent positions | Only values without predecessors start scans, so each sequence is traversed once. | `O(n)` average time, `O(n)` space |

## Problem statements

| Problem | Inputs and required output | Key constraints |
| --- | --- | --- |
| Contains Duplicate | Integer list `nums`; return whether any value occurs at least twice. | Empty and negative-valued lists are valid; target is linear average time. |
| Two Sum | Integer list `nums` and integer `target`; return indices of two distinct values that sum to `target`, or `[]` if none exist. | Do not reuse one index; LeetCode guarantees one answer, while this implementation handles no-match input. |
| Valid Anagram | Strings `first` and `second`; return whether they have equal character multiplicities. | Character comparison is literal and case-sensitive; empty strings are valid. |
| Group Anagrams | List of lowercase English words; return all anagram groups in any order. | Words in a group have identical letter counts; empty input returns `[]`. |
| Top K Frequent Elements | Integer list `nums` and `k`; return the `k` most frequent values in any order. | LeetCode uses `1 <= k <= distinct values` and guarantees a unique answer; the helper also handles invalid/oversized `k` safely. |
| Product of Array Except Self | Integer list `nums`; return `answer` where `answer[i]` is the product of every value except `nums[i]`. | Division is forbidden, products fit the specified range, and the intended running time is `O(n)`. |
| Valid Sudoku | Partially filled 9-by-9 board of digits and `"."`; return whether all filled cells obey row, column, and box uniqueness. | It need not be solvable; the function returns `False` for malformed board dimensions. |
| Longest Consecutive Sequence | Unsorted integer list `nums`; return the longest consecutive-integer run length. | Input order is irrelevant, duplicates do not extend runs, and the expected time is average `O(n)`. |

## Why these patterns fit

These problems are less about exotic data structures than about choosing the
smallest state that makes the next decision constant-time: a set for
membership, a map for index/count lookup, a canonical key for equivalence, or
an accumulation invariant for products.  The implementations favor
single-pass or near-single-pass solutions and keep the LeetCode input/output
shape visible rather than introducing classes that do not add value.

## Alternatives and critique

- Sorting is useful when ordered output or deterministic traversal is needed,
  but it is rejected for duplicate detection, anagrams, and consecutive runs
  because it costs `O(n log n)` and can mutate input.
- `Counter` is used only where it improves the frequency-bucket explanation.
  The anagram solution uses an explicit dictionary so the decrement invariant
  is visible to learners.
- A heap is a good general solution for Top K when the stream is too large to
  bucket, with `O(u log k)` time.  The existing
  `Problems/Heap/top_k_frequent_elements.py` was a direct overlap, so it now
  re-exports the bucket implementation here instead of maintaining divergent
  logic.  For the bounded LeetCode array, buckets are simpler and asymptotically
  faster.
- Division for Product of Array Except Self is shorter but fails around zero
  values and violates the problem constraint; prefix/suffix products avoid
  both issues.
- Sudoku uses three families of sets rather than encoding cells into one
  large set.  The separate regions make the invariant auditable and prevent
  accidentally checking only rows or only boxes.
- `frozenset` is an immutable, hashable set.  It is useful when comparing
  unordered groups, such as verifying Group Anagrams results without depending
  on the order of groups or words.

## Running examples and tests

Every solution has a small `__main__` example.  The focused pytest suite is:

```text
uv run pytest tests/test_arrays_hashing.py
```
