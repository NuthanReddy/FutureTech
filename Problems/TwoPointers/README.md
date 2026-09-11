# Top Interview 150 — Two Pointers

These solutions cover the six Two Pointers problems in LeetCode's Top Interview
150: **Valid Palindrome**, **Is Subsequence**, **Two Sum II**, **3Sum**,
**Container With Most Water**, and **Trapping Rain Water**.

## Problem statements

* **Valid Palindrome** — Input: string `s`. Output: whether its
  case-insensitive letters and digits form the same sequence forwards and
  backwards after ignoring punctuation and spaces.
* **Is Subsequence** — Input: strings `s` and `t`. Output: whether `s` can be
  formed by deleting characters from `t`, without reordering the remainder.
* **Two Sum II** — Input: non-decreasing integer array `numbers` and integer
  `target`. Output: the distinct one-based indices of the pair summing to
  `target`, or `[]` if no pair exists.
* **3Sum** — Input: integer array `nums`. Output: all unique triples of values
  whose sum is zero; duplicate triples must not be returned.
* **Container With Most Water** — Input: non-negative line heights. Output: the
  largest area between two lines, where area is width times the shorter height.
* **Trapping Rain Water** — Input: non-negative bar heights. Output: total water
  units trapped between the bars after rainfall.

The arrays may be empty; the usual Top 150 constraints are large, so linear
solutions are expected for the pair/boundary problems and O(n²) is the target
for 3Sum (rather than brute-force O(n³)).

## Decision guide

Use two pointers when the input is ordered, or when a left/right boundary
defines a shrinking search space. The key question is: *which discarded region
can never contain a better answer?*

| Problem | Invariant / decision | Time | Extra space |
| --- | --- | ---: | ---: |
| Valid Palindrome | equal normalized characters remain possible | O(n) | O(1) |
| Is Subsequence | prefix of `s` has been matched in `t` | O(|t|) | O(1) |
| Two Sum II | sorted pair sum is compared with target | O(n) | O(1) |
| 3Sum | sorted anchor plus inward pair search | O(n²) | O(1) besides output |
| Container With Most Water | move the shorter wall; the taller wall cannot help | O(n) | O(1) |
| Trapping Rain Water | smaller running boundary determines that side's water | O(n) | O(1) |

## Alternatives and critique

* Hashing solves unsorted Two Sum in O(n), but does not exploit the sorted-input
  contract and uses O(n) memory.
* A hash set can detect 3Sum candidates, but duplicate control is harder and
  worst-case time remains O(n²); sorting makes both uniqueness and pointer
  movement explicit.
* Expanding around every pair for water problems is quadratic. Prefix
  maxima make trapping rain water O(n) but consume O(n) memory; the two-pointer
  version keeps the same reasoning while reducing memory to O(1).
* Building a filtered copy for Valid Palindrome is readable, but the in-place
  pointers avoid an allocation and demonstrate the normalization invariant.

Run the examples with:

```text
python Problems/TwoPointers/solution.py
```
