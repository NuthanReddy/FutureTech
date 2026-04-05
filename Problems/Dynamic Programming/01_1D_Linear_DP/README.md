# 1D Linear DP

## Pattern: How to Identify

Use 1D Linear DP when:
- the input is a sequence (array/string)
- each answer at index `i` depends on nearby earlier/later indices
- you can move linearly from one end to the other

Typical clues:
- "max/min up to index i"
- "pick or skip current item"
- transitions like `i -> i+1`, `i -> i+2`

## Base DP Values

For this example (House Robber):
- `dp[n] = 0` (no houses left)
- `dp[n+1] = 0` (safe padding for `i+2`)

For memoized DFS:
- if `i >= n`, return `0`

## Recurrence (Carry Forward)

At house `i`:
- skip current house: `dp[i+1]`
- rob current house: `nums[i] + dp[i+2]`

So:

`dp[i] = max(dp[i+1], nums[i] + dp[i+2])`

This carries best value from the future states back to current state.

## Break / Termination Condition

Memoization terminates when index exits the array (`i >= n`).
Tabulation terminates when loop has processed all indices.

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - natural for recursion and "pick/skip" thinking
  - computes only reachable states
  - recursion overhead and call stack usage
- Tabulation:
  - iterative, no recursion depth risk
  - often easier to optimize memory
  - you must design loop order carefully

## Example Problem

**House Robber**

Given non-negative integers where each value is money in a house, find the maximum amount you can rob without robbing two adjacent houses.

See `solution.py` for both implementations and `demo.py` for a runnable example.

