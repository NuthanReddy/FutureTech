# 2D Grid DP

## Pattern: How to Identify

Use 2D Grid DP when:
- state is a cell `(row, col)` in a matrix
- movement options are fixed (for example right/down)
- each cell answer depends on neighbor cells

Typical clues:
- "minimum path sum"
- "count ways in a grid"
- blocked cells or weighted cells

## Base DP Values

For this example (Minimum Path Sum):
- destination cell contributes its own value
- out-of-bounds states are invalid and treated as infinity

For memoized DFS:
- if `(r, c)` is outside grid, return `inf`
- if `(r, c)` is destination, return `grid[r][c]`

For tabulation:
- initialize `dp[0][0] = grid[0][0]`

## Recurrence (Carry Forward)

Each cell can be reached from top or left:

`dp[r][c] = grid[r][c] + min(dp[r-1][c], dp[r][c-1])`

Memoized version computes from current cell to destination:

`best(r, c) = grid[r][c] + min(best(r+1, c), best(r, c+1))`

## Break / Termination Condition

Memoization ends when recursion reaches destination or boundary.
Tabulation ends after filling all rows and columns.

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - expressive when transitions are directional from current cell
  - easy to include boundary checks in recursion
  - recursion depth can be large for big grids
- Tabulation:
  - iterative and stack-safe
  - often faster in Python due to less call overhead
  - needs careful initialization of first row/column

## Example Problem

**Minimum Path Sum**

Given a grid of non-negative integers, move only right or down from top-left to bottom-right and return the minimum path sum.

See `solution.py` and `demo.py`.

