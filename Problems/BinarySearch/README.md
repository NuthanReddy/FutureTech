# Binary Search

These are the seven problems in the **Binary Search** section of LeetCode's
Top Interview 150 study plan.  The implementations emphasize the invariant
that makes each boundary update correct rather than treating binary search as
one memorized template.

## Problem statements and complexities

### Search Insert Position

Given a sorted integer sequence `nums` and an integer `target`, return the
index of `target` if it exists.  If it does not exist, return the index where
`target` should be inserted so the sequence remains sorted.

| Inputs | Required output | Key constraints | Pattern | Time | Extra space |
| --- | --- | --- | --- | ---: | ---: |
| `nums: Sequence[int]`, `target: int` | insertion/search index as `int` | `nums` is ascending; target runtime is `O(log n)` | first true / lower bound | `O(log n)` | `O(1)` |

### Search a 2D Matrix

Given an `m x n` integer matrix and an integer `target`, return whether
`target` appears in the matrix.  Each row is sorted left to right, and each
row's first value is greater than the previous row's last value.

| Inputs | Required output | Key constraints | Pattern | Time | Extra space |
| --- | --- | --- | --- | ---: | ---: |
| `matrix: Sequence[Sequence[int]]`, `target: int` | `True` if found, otherwise `False` | row-major global sorted order; target runtime is `O(log(mn))` | exact search over virtual flattened indices | `O(log(mn))` | `O(1)` |

### Find Peak Element

Given a non-empty integer sequence `nums`, return the index of any peak
element.  A peak is strictly greater than its immediate neighbors; values
outside the array are treated as negative infinity, so endpoints can be peaks.

| Inputs | Required output | Key constraints | Pattern | Time | Extra space |
| --- | --- | --- | --- | ---: | ---: |
| `nums: Sequence[int]` | index of any peak as `int` | adjacent values differ; target runtime is `O(log n)` | binary search on a local slope | `O(log n)` | `O(1)` |

### Search in Rotated Sorted Array

Given a distinct ascending integer sequence that has been rotated at an
unknown pivot and an integer `target`, return the index of `target` if present;
otherwise return `-1`.

| Inputs | Required output | Key constraints | Pattern | Time | Extra space |
| --- | --- | --- | --- | ---: | ---: |
| `nums: Sequence[int]`, `target: int` | found index as `int`, or `-1` | values are distinct; target runtime is `O(log n)` | exact search after identifying the sorted half | `O(log n)` | `O(1)` |

### Find First and Last Position

Given a sorted integer sequence `nums` that may contain duplicates and an
integer `target`, return `[first_index, last_index]` for the target's range.
Return `[-1, -1]` if the target does not appear.

| Inputs | Required output | Key constraints | Pattern | Time | Extra space |
| --- | --- | --- | --- | ---: | ---: |
| `nums: Sequence[int]`, `target: int` | two-item list `[first, last]`, or `[-1, -1]` | `nums` is non-decreasing; target runtime is `O(log n)` | lower bound + upper bound | `O(log n)` | `O(1)` |

### Find Minimum in Rotated Sorted Array

Given a non-empty distinct ascending integer sequence that has been rotated at
an unknown pivot, return the minimum value in the sequence.

| Inputs | Required output | Key constraints | Pattern | Time | Extra space |
| --- | --- | --- | --- | ---: | ---: |
| `nums: Sequence[int]` | minimum value as `int` | values are distinct; target runtime is `O(log n)` | pivot/minimum search | `O(log n)` | `O(1)` |

### Median of Two Sorted Arrays

Given two individually sorted integer sequences, return the median of their
combined sorted values.  If the combined length is even, return the average of
the two middle values as a float.

| Inputs | Required output | Key constraints | Pattern | Time | Extra space |
| --- | --- | --- | --- | ---: | ---: |
| `first: Sequence[int]`, `second: Sequence[int]` | median as `float` | at least one input is non-empty; both inputs are non-decreasing; target runtime is `O(log(min(m, n)))` | binary search for a valid two-array partition | `O(log(min(m, n)))` | `O(1)` |

## Choosing a binary-search pattern

### 1. Exact-value search with inclusive bounds

Use an inclusive interval, `[left, right]`, when a matching midpoint can be
returned immediately.  Continue while `left <= right`; discarding the
midpoint uses `middle + 1` or `middle - 1`.

This is the clearest fit for:

- a normal sorted array;
- the virtual flattened array in **Search a 2D Matrix**;
- **Search in Rotated Sorted Array**, after determining which half is sorted.

### 2. Boundary search with half-open bounds

Use `[left, right)` for insertion points and duplicate boundaries.  Continue
while `left < right`, and keep `middle` with `right = middle` when it might be
the answer.

- **Lower bound:** first value `>= target`.
- **Upper bound:** first value `> target`.

Search Insert Position is exactly lower bound.  First/last position combines
`lower_bound(target)` and `upper_bound(target) - 1`.

Python's `bisect_left` and `bisect_right` implement these operations, but the
local helpers are explicit so their invariants and edge cases remain visible.

### 3. Search over a monotonic decision

Sometimes the answer is not an exact target.  Instead, a comparison tells us
which side is guaranteed to contain an answer:

- a rising or falling slope leads to a peak;
- comparing the midpoint with the right endpoint locates a rotation pivot;
- comparing cross-partition boundary values adjusts the median partition.

The essential question is: **what statement remains true about the retained
interval after this update?**  That statement is the loop invariant.

## Boundary alternatives

| Choice | Best use | Main risk |
| --- | --- | --- |
| Inclusive `[left, right]` | exact match | forgetting `left <= right` or `±1` updates |
| Half-open `[left, right)` | first/last valid position | mixing it with inclusive updates |
| Recursive search | educational divide-and-conquer | `O(log n)` call-stack space |
| Linear scan | tiny or unsorted input | loses the required logarithmic complexity |
| `bisect` module | production boundary lookup | hides the pattern when studying |

## Existing coverage and overlap

The older `Problems/` folders already include related ideas, but they solve
different patterns:

| Existing area | Relationship to this folder | Why keep this folder separate |
| --- | --- | --- |
| `Problems/BST/validate_bst.py` | Uses binary-search-tree ordering constraints | Tree validation is a recursive range problem, not array boundary search |
| `Problems/SortedSet/sliding_window_median.py` | Maintains sorted order to read medians | Window maintenance is data-structure driven; Top 150 median requires partition search |
| `Problems/Dynamic Programming/05_Interval_DP/README.md` | Mentions Optimal Binary Search Tree | That is interval DP over roots, not logarithmic lookup |
| `Problems/Trees/` | Covers BST inorder and bounds patterns | Tree-specific binary-search properties stay with tree traversal problems |

Keeping the Top 150 binary-search set here makes the array/search-space
invariants easy to compare side by side without moving or rewriting the older
standalone exercises.

## Edge-case policy

- Empty exact-search inputs return the normal "not found" result.
- Empty Search Insert Position returns index `0`.
- Empty matrix input returns `False`.
- Peak/minimum queries on empty input raise `ValueError` because no valid
  answer exists.
- Median accepts one empty array, but two empty arrays raise `ValueError`.
- Rotated-array solutions rely on the Top Interview 150 constraint that input
  values are distinct.

## Quick example

From the repository root:

```powershell
uv run python -c "from Problems.BinarySearch import search_range; print(search_range([5, 7, 7, 8, 8, 10], 8))"
```

Expected output:

```text
[3, 4]
```
