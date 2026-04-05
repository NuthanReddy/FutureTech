# Knapsack / Subset DP

## Pattern: How to Identify

Use this pattern when:
- each item can be taken or skipped
- choices are constrained by capacity/target sum
- objective is max value, exact sum, or count of ways

Typical clues:
- "0/1 knapsack"
- "subset sum"
- "partition equal subset"

## Base DP Values

For 0/1 Knapsack example:
- if no items left, best value is `0`
- if capacity is `0`, best value is `0`

Memoized DFS base:
- `index == n` or `remaining_capacity == 0` -> `0`

Tabulation base:
- first row and first column are `0`

## Recurrence (Carry Forward)

At item `i`:
- skip item: value from next item with same capacity
- take item (if weight fits): `value[i] + next state with reduced capacity`

`dp[i][cap] = max(dp[i+1][cap], value[i] + dp[i+1][cap-weight[i]])`

## Break / Termination Condition

Memoization stops at end of items or zero capacity.
Tabulation stops after all items and capacities are processed.

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - natural for decision tree (take/skip)
  - can avoid unreachable states
  - recursion overhead
- Tabulation:
  - predictable loops and no recursion stack
  - easy to inspect table states for learning
  - may fill more states than needed

## Example Problem

**0/1 Knapsack**

Given item weights and values, maximize total value without exceeding bag capacity. Each item can be chosen at most once.

See `solution.py` and `demo.py`.

