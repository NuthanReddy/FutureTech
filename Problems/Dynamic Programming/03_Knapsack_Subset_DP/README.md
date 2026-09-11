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

## State Shape

The general state is `dp[i][capacity]` or `dp[i][target]`: the best/count/feasibility result using items from index `i` with the remaining resource. A one-dimensional table is possible when loop order encodes whether an item may be reused.

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

## Complexity

For `n` items and capacity/target `W`:

- Time: `O(nW)`
- Space: `O(nW)` for a two-dimensional table, reducible to `O(W)`
- This is pseudo-polynomial because the numeric capacity `W` affects the running time

For one-dimensional tabulation, iterate capacity downward for 0/1 items and upward for unbounded items.

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - natural for decision tree (take/skip)
  - can avoid unreachable states
  - recursion overhead
- Tabulation:
  - predictable loops and no recursion stack
  - easy to inspect table states for learning
  - may fill more states than needed

## Implemented Top Interview 150 problems

### Coin Change

**Problem statement:** Given coin denominations and unlimited copies of each,
return the fewest coins needed to form `amount`, or `-1` if it cannot be
formed.

- **Input:** positive `coins` and non-negative integer `amount` (standard
  constraint: `amount <= 10_000`).
- **Output:** minimum coin count, or `-1`.

This is **unbounded** knapsack: after choosing a coin, the same coin remains
available. The forward amount transition (`current - coin`) intentionally
permits reuse. Impossible states start at a sentinel larger than any possible
answer and are mapped to `-1`.

### Partition Equal Subset Sum

**Problem statement:** Given positive integers, decide whether they can be
partitioned into two subsets having the same sum.

- **Input:** integer array `nums` (standard version has at most 200 values).
- **Output:** boolean feasibility result.

The equal split first reduces to reaching `sum(nums) / 2`. Its one-dimensional
table is a 0/1 knapsack feasibility table, so targets must be visited in
descending order to prevent using one number multiple times.

## Example Problem

**0/1 Knapsack**

**Problem statement:** Given item weights, values, and a bag capacity, choose
each item at most once to maximize total value without exceeding capacity.

- **Input:** parallel `weights` and `values` arrays plus non-negative
  `capacity`.
- **Output:** maximum total value.

See `solution.py` and `demo.py`.

## Related Problems

1. Subset Sum
2. Partition Equal Subset Sum
3. Coin Change (minimum coins)
4. Coin Change II (number of combinations)
5. Rod Cutting / Unbounded Knapsack

## Comparison

Coin Change minimizes a count and uses an “unbounded” forward update;
Partition Equal Subset Sum tracks reachability and uses a “0/1” backward
update. Both fit the same capacity-shaped family, but their loop directions
are a correctness requirement, not merely an optimization.
