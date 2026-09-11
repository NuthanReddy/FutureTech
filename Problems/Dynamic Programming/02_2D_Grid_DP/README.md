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

## State Shape

`dp[r][c]` represents the answer for cell `(r, c)`, either from the start to that cell or from that cell to the destination. Choose one direction and keep the recurrence consistent.

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

## Complexity

For an `R x C` grid with a constant number of moves:

- Time: `O(RC)`
- Space: `O(RC)`, reducible to `O(C)` with a rolling row when only the previous row is needed

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - expressive when transitions are directional from current cell
  - easy to include boundary checks in recursion
  - recursion depth can be large for big grids
- Tabulation:
  - iterative and stack-safe
  - often faster in Python due to less call overhead
  - needs careful initialization of first row/column

## Implemented Top Interview 150 problems

### Unique Paths

**Problem statement:** Given an empty `rows x cols` grid, count paths from the
top-left to the bottom-right using only right and down moves.

- **Input:** positive integers `rows` and `cols` (commonly at most 100).
- **Output:** number of valid paths.

The coordinate recurrence is a counting version of the same grid DAG:
`paths(r,c) = paths(r+1,c) + paths(r,c+1)`. Unlike Minimum Path Sum there is
no cell cost, so the tabulated solution can use one rolling row and `O(C)`
space.

### Minimum Path Sum

**Problem statement:** Given an `m x n` grid of non-negative integers, move
only right or down from the top-left to the bottom-right and return the
smallest possible sum of visited cells.

- **Input:** non-empty `grid` (standard bounds are up to `200 x 200`).
- **Output:** minimum path sum.

Given a grid of non-negative integers, move only right or down from top-left to bottom-right and return the minimum path sum.

See `solution.py` and `demo.py`.

## Comparison

Memoized DFS is closest to the “move toward the destination” proof and
naturally handles boundaries with sentinel values. Tabulation is stack-safe,
and the Unique Paths implementation demonstrates the stronger optimization:
when only the previous row is needed, keep one row rather than a full matrix.

## Related Problems

1. Unique Paths
2. Unique Paths with Obstacles
3. Maximum Path Sum
4. Minimum Falling Path Sum
5. Dungeon Game
