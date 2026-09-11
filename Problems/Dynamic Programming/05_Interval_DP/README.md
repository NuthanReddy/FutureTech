# Interval DP

## Pattern: How to Identify

Use Interval DP when:
- state is a subarray/subsequence range `[i, j]`
- you split a range at pivot `k`
- combining two solved sub-intervals gives full interval answer

Typical clues:
- "minimum cost to combine/merge"
- "parenthesize expression"
- palindrome partition over ranges

## State Shape

`dp[left][right]` represents the answer for the inclusive interval `[left, right]`. The transition normally tries every split point `k`, so both resulting intervals are shorter than the current one.

## Base DP Values

For Matrix Chain Multiplication example:
- one matrix alone needs no multiplication cost
- so `dp[i][i] = 0`

Memoized DFS base:
- if `i == j`, return `0`

Tabulation base:
- diagonal is zero because chain length 1 has zero cost

## Recurrence (Carry Forward)

For interval `[i, j]`, try every split `k` in `[i, j-1]`:

`dp[i][j] = min(dp[i][k] + dp[k+1][j] + cost_to_multiply_two_parts)`

For matrix chain with dimensions `dims`:

`cost = dims[i] * dims[k+1] * dims[j+1]`

## Break / Termination Condition

Memoization ends when interval shrinks to single matrix (`i == j`).
Tabulation ends after filling intervals by increasing chain length.

## Complexity

For `n` items and a split point at every interval:

- Time: `O(n^3)`
- Space: `O(n^2)`

Some specialized interval recurrences or optimizations improve the cubic time, but the increasing-length fill order remains the standard tabulation strategy.

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - very close to recursive divide-and-conquer idea
  - computes only visited intervals
  - recursion overhead
- Tabulation:
  - deterministic fill order by interval size
  - avoids recursion stack limits
  - requires careful loop nesting by length, start, split

## Example Problem

**Matrix Chain Multiplication (Minimum Cost)**

Given matrix dimensions array, find minimum scalar multiplications needed to multiply the full chain.

See `solution.py` and `demo.py`.

## Related Problems

1. Burst Balloons
2. Palindrome Partitioning
3. Optimal Binary Search Tree
4. Minimum Cost to Merge Stones
5. Boolean Parenthesization
