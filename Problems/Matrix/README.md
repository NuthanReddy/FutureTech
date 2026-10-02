# Matrix

## Pattern identification steps

1. Identify whether the grid needs traversal, geometric transformation, region validation, or simultaneous updates.
2. Choose shrinking boundaries for spirals, transpose/reverse for square rotation, sets for uniqueness.
3. For in-place updates, retain original facts using row/column markers or separate old/new bits.
4. State what remains unvisited or unchanged; guard collapsed boundaries and preserve marker metadata.
5. Do not rotate rectangular grids with square transpose logic or overwrite states still needed by neighbors.

| Problem | Problem statement (input → required output) | Identification cue / core idea | Time | Extra space |
| --- | --- | --- | ---: | ---: |
| Valid Sudoku (`solution.py:is_valid_sudoku`) | 9x9 partial board → whether rows, columns, and 3x3 boxes are valid | overlapping uniqueness constraints -> region sets | O(1) (fixed 9x9) | O(1) |
| Spiral Matrix (`solution.py:spiral_order`) | `m x n` matrix → values in clockwise spiral order | peel outer rings -> four shrinking boundaries | O(mn) | O(1) besides output/slice workspace |
| [Spiral Matrix (legacy)](leetcode_54_spiral_matrix.py) | `m x n` matrix → clockwise traversal | same shrinking boundaries; guard single remaining row/column | O(mn) | O(1) besides output |
| Rotate Image (`solution.py:rotate`) | `n x n` matrix → same matrix rotated 90° clockwise in-place | square coordinate rotation -> transpose + row reverse | O(n²) | O(1) |
| Set Matrix Zeroes (`solution.py:set_zeroes`) | matrix → zero every row/column containing an original zero, in-place | preserve original zero locations -> first row/column markers | O(mn) | O(n) if first row is replaced |
| Game of Life (`solution.py:game_of_life`) | binary board → one generation under eight-neighbor Conway rules, in-place | simultaneous updates -> pack old and new bits in each cell | O(mn) | O(1) |

## Inputs and constraints

Matrices are rectangular with dimensions `m x n`; empty matrices are handled
where meaningful. Rotation requires a square matrix. Sudoku is exactly 9x9
with digits `1`–`9` or `"."`. Game of Life cells are `0` or `1`. Traversal
returns a new list; the other matrix operations mutate their input and return
`None`.

The in-place techniques are not merely micro-optimizations: they preserve the
problem's requirement to avoid a second matrix.  Marker metadata is handled
before mutating the first row/column, and Game of Life reads only the low bit
until every next state has been encoded.
