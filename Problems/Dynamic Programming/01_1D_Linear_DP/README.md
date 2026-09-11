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

## State Shape

Typical states are `dp[i] = answer from index i onward`, `dp[i] = answer for the first i items`, or `dp[i] = answer ending at i`. Add a small status dimension when the next choice depends on local history.

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

## Complexity

For `n` positions and a constant number of transitions:

- Time: `O(n)`
- Space: `O(n)` for the full table, often reducible to `O(1)` or `O(k)` when only the previous `k` states are required

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - natural for recursion and "pick/skip" thinking
  - computes only reachable states
  - recursion overhead and call stack usage
- Tabulation:
  - iterative, no recursion depth risk
  - often easier to optimize memory
  - you must design loop order carefully

## Implemented Top Interview 150 problems

### Climbing Stairs

**Problem statement:** Given `n` stairs, count distinct ways to reach the top
when each move climbs either one or two stairs.

- **Input:** integer `n` (`1 <= n <= 45` in the standard problem).
- **Output:** number of distinct routes.

`ways(i)` counts paths from stair `i` to the top. The terminal state is one
completed path, and the transition adds one-step and two-step choices.
Tabulation reduces the two required predecessor values to `O(1)` space.

### Word Break

**Problem statement:** Given a string and a dictionary of non-empty words,
decide whether the string can be segmented into dictionary words.

- **Input:** string `text` and word list `words`.
- **Output:** `True` if a complete segmentation exists; otherwise `False`.

`can_break(i)` asks whether the suffix beginning at `i` is segmentable. The
prefix-table version instead marks reachable string boundaries. Both are
`O(n * w * L)` in the straightforward implementation (`w` dictionary words,
`L` word-comparison cost); a trie can reduce repeated prefix checks when the
dictionary is large.

## Example Problem

**House Robber**

**Problem statement:** Given non-negative amounts in a row of houses, maximize
the amount robbed without robbing two adjacent houses.

- **Input:** integer array `nums` (standard constraint: up to 100 houses).
- **Output:** maximum obtainable amount.

See `solution.py` for both implementations and `demo.py` for a runnable example.

## Related Problems

1. Fibonacci / Tribonacci
2. Climbing Stairs
3. Decode Ways
4. Maximum Subarray
5. Jump Game and minimum jumps

The same take/skip reasoning also appears in House Robber, while Word Break
shows how the pattern generalizes from numeric arrays to string boundaries.
