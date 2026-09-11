# Matrix

| Problem | Problem statement (input → required output) | Core idea | Time | Extra space |
| --- | --- | ---: | ---: |
| Valid Sudoku | 9x9 partial board → whether rows, columns, and 3x3 boxes are valid | three sets per cell | O(1) (fixed 9x9) | O(1) |
| Spiral Matrix | `m x n` matrix → values in clockwise spiral order | four shrinking boundaries | O(mn) | O(1) besides output |
| Rotate Image | `n x n` matrix → same matrix rotated 90° clockwise in-place | transpose + row reverse | O(n²) | O(1) |
| Set Matrix Zeroes | matrix → zero every row/column containing an original zero, in-place | first row/column markers | O(mn) | O(1) |
| Game of Life | binary board → one generation under eight-neighbor Conway rules, in-place | pack old and new bits in each cell | O(mn) | O(1) |

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
