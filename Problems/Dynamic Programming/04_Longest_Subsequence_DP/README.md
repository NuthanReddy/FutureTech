# Longest Subsequence DP

## Pattern: How to Identify

Use this pattern when:
- order matters, but elements do not need to be contiguous
- you ask for longest/maximum valid chain
- current choice depends on previous chosen element

Typical clues:
- "longest increasing subsequence"
- "longest common subsequence"
- "best chain under ordering rule"

## State Shape

For one sequence, use `dp[i]` for a subsequence ending at `i`, or `(index, previous_index)` for include/skip recursion. For two sequences, use `dp[i][j]` for prefixes `A[:i]` and `B[:j]`.

## Base DP Values

For LIS example:
- from any single element, minimum subsequence length is `1`

Memoized DFS base:
- if index reaches end, remaining LIS length is `0`

Tabulation base:
- initialize `dp[i] = 1` for all `i`

## Recurrence (Carry Forward)

Memoized state `(index, prev_index)`:
- skip current element
- take current element if it keeps sequence increasing

`best(index, prev) = max(skip, 1 + best(index+1, index))` when allowed

Tabulation state `dp[i]`:
- LIS ending at `i`
- check all `j < i` and carry forward best valid chain

`dp[i] = max(dp[i], dp[j] + 1)` if `nums[j] < nums[i]`

## Break / Termination Condition

Memoization ends at `index == n`.
Tabulation ends after processing all indices.

## Complexity

For the implemented quadratic LIS:

- Time: `O(n^2)`
- Space: `O(n)`

Two-sequence prefix problems such as LCS and edit distance generally use `O(nm)` time and space, with space reducible to `O(min(n, m))`.

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - explicit decision flow with previous index state
  - easy to reason about include/exclude logic
  - larger state table for `(index, prev)`
- Tabulation:
  - compact O(n^2) iterative solution
  - straightforward for learning LIS transitions
  - you must design update order correctly

## Implemented Top Interview 150 problems

### Edit Distance

**Problem statement:** Given two strings, return the minimum number of
insertions, deletions, and substitutions required to transform the first into
the second.

- **Input:** strings `source` and `target` (standard lengths up to 500).
- **Output:** non-negative minimum edit count.

`dp[i][j]` compares prefixes of lengths `i` and `j`. Equal characters carry
the diagonal state unchanged; unequal characters choose the cheapest of
insert, delete, and replace. Empty-prefix initialization is not optional:
only insertions or deletions can transform an empty prefix.

The memoized version uses suffix states and is often easier to derive. The
tabulated version uses prefix states and is easier to inspect and reduce to
`O(min(n, m))` space.

## Example Problem

**Longest Increasing Subsequence (LIS)**

**Problem statement:** Given an integer array, return the length of the
longest strictly increasing subsequence; chosen elements do not need to be
contiguous.

- **Input:** integer array `nums`.
- **Output:** LIS length; an empty array produces `0`.

See `solution.py` and `demo.py`.

## Related Problems

1. Longest Common Subsequence
2. Edit Distance
3. Longest Common Substring
4. Wildcard Matching
5. Russian Doll Envelopes / nested increasing chains

## Comparison

LIS has one sequence coordinate plus a previous-choice constraint, while Edit
Distance needs two coordinates because both strings contribute independent
prefixes. The quadratic LIS table is intentionally retained for clarity; the
patience-sorting optimization can reduce LIS to `O(n log n)` but stores
tails rather than the full “best ending at index” state.
