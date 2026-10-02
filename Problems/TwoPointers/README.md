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

## Pattern identification steps

1. Look for ordered pair searches, symmetric comparisons, or order-preserving matching.
2. Choose inward pointers for pairs/boundaries; choose forward cursors for subsequences.
3. Prove each pointer move discards only impossible or non-improving candidates.
4. For triples, sort and fix an anchor before searching the remaining pair; skip duplicates.
5. Do not use sum-directed pointer moves on unsorted input; use hashing if original indices must survive.

| Problem | Identification cue | Invariant / decision | Time | Extra space |
| --- | --- | --- | ---: | ---: |
| Valid Palindrome (`solution.py:isPalindrome`) | compare normalized ends | equal normalized characters remain possible | O(n) | O(1) |
| Is Subsequence (`solution.py:isSubsequence`) | delete without reordering | prefix of `s` has been matched in `t` | O(|t|) | O(1) |
| Two Sum II (`solution.py:twoSum`) | sorted pair sum, original positions | sorted pair sum is compared with target | O(n) | O(1) |
| 3Sum (`solution.py:threeSum`) | unique zero-sum triples | sorted anchor plus inward pair search | O(n²) | O(1) besides output/sort workspace |
| Container With Most Water (`solution.py:maxArea`) | area limited by shorter endpoint | move the shorter wall; the taller wall cannot help | O(n) | O(1) |
| Trapping Rain Water (`solution.py:trap`) | per-bar water bounded on both sides | smaller running boundary determines that side's water | O(n) | O(1) |
| [Trapping Rain Water (legacy)](leetcode_42_trapping_rain_water.py) | rising bar closes a basin | non-increasing stack of indices; popped bottoms are bounded by surviving left wall | O(n) | O(n) |

The legacy rain-water script uses a monotonic stack, not two pointers.

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
