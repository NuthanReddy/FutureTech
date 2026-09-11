# Top Interview 150 — Sliding Window

This folder covers the six Top Interview 150 sliding-window problems:
**Minimum Size Subarray Sum**, **Longest Substring Without Repeating
Characters**, **Longest Repeating Character Replacement**, **Permutation in
String**, **Sliding Window Maximum**, and **Minimum Window Substring**.

## Problem statements

* **Minimum Size Subarray Sum** — Input: positive integer `target` and an array
  `nums` of positive integers. Output: the minimum length of a contiguous
  subarray whose sum is at least `target`, or `0` if none exists.
* **Longest Substring Without Repeating Characters** — Input: string `s`.
  Output: the length of the longest contiguous substring with no repeated
  character.
* **Longest Repeating Character Replacement** — Input: uppercase string `s`
  and non-negative integer `k`. Output: the longest substring that can be made
  entirely one repeated letter using at most `k` replacements.
* **Permutation in String** — Input: strings `s1` and `s2`. Output: whether
  `s2` contains a contiguous substring that is a permutation of `s1`.
* **Sliding Window Maximum** — Input: integer array `nums` and positive window
  size `k`. Output: the maximum of each contiguous length-`k` window, in order.
* **Minimum Window Substring** — Input: strings `s` and `t`. Output: the
  shortest substring of `s` containing every character in `t`, including
  duplicate occurrences, or `""` when impossible.

These problems assume contiguous windows. The positive-number constraint is
essential for the first problem; without it, shrinking the left edge is not
monotonic. Typical input sizes require O(n) sliding-window methods.

## Decision guide

Maintain a contiguous `[left, right]` interval. Expand `right` to include new
data, then move `left` until the window invariant is valid again. Because each
boundary moves only forward, the total work is linear (apart from explicit
counter comparisons).

| Problem | Window state / invariant | Time | Extra space |
| --- | --- | ---: | ---: |
| Minimum Size Subarray Sum | positive sum is at least target while shrinking | O(n) | O(1) |
| Longest Substring | no character appears twice | O(n) | O(min(n, alphabet)) |
| Character Replacement | window length minus max frequency is at most k | O(n) | O(alphabet) |
| Permutation in String | fixed-size frequency vectors/counters match | O(n) | O(alphabet) |
| Sliding Window Maximum | deque is decreasing and stores live indices | O(n) | O(k) |
| Minimum Window Substring | required counts are all satisfied | O(|s| + |t|) | O(alphabet) |

## Alternatives and critique

* Minimum Size Subarray Sum requires positive numbers. With negatives, the
  shrink argument fails; use prefix sums plus a monotonic deque instead.
* A set is enough for Longest Substring, but last-seen indices jump `left`
  directly and avoid repeatedly deleting characters.
* Character Replacement keeps a non-decreasing `max_frequency`. It may be
  stale after shrinking, but that only delays shrinking and never overstates a
  final answer; recomputing it is correct but can make the solution quadratic.
* Permutation in String uses `Counter` for readable general Unicode input.
  For lowercase English-only constraints, two arrays of length 26 are faster
  and avoid hashing.
* A heap solves Sliding Window Maximum in O(n log k). The monotonic deque is
  O(n) because every index enters and leaves once.
* Minimum Window Substring is a variable-size *minimum* window: record before
  removing the left character. Brute-force substring checks repeat counting and
  are quadratic or worse.

Run the examples with:

```text
python Problems/SlidingWindow/top_interview_150.py
```
