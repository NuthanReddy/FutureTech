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

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - explicit decision flow with previous index state
  - easy to reason about include/exclude logic
  - larger state table for `(index, prev)`
- Tabulation:
  - compact O(n^2) iterative solution
  - straightforward for learning LIS transitions
  - you must design update order correctly

## Example Problem

**Longest Increasing Subsequence (LIS)**

Given an integer array, return the length of the longest strictly increasing subsequence.

See `solution.py` and `demo.py`.

