# Sliding Window

## Pattern identification steps

1. Look for a contiguous substring/subarray with a fixed size or adjustable constraint.
2. Choose a rolling sum/count for fixed windows; choose forward boundaries when shrinking restores feasibility.
3. Maintain state for exactly the current window; use monotonic index deques for extrema.
4. Record longest windows after repair, shortest covering windows before removing required data.
5. Do not use sum-based shrinking with negative values or windows for non-contiguous subsequences.

## Problem map

| Problem | Identification cue |
| --- | --- |
| Minimum Size Subarray Sum (`top_interview_150.py:minSubArrayLen`) | Positive values and sum threshold -> expand sum, shrink while covered. |
| Longest Substring Without Repeating Characters (`top_interview_150.py:lengthOfLongestSubstring`) | Contiguous unique characters -> jump left past the last duplicate. |
| Longest Repeating Character Replacement (`top_interview_150.py:characterReplacement`) | At most k replacements -> counts plus historical maximum frequency for length-only search. |
| Permutation in String (`top_interview_150.py:checkInclusion`) | Anagram substring -> fixed window of len(s1), compare multiplicities. |
| Sliding Window Maximum (`top_interview_150.py:maxSlidingWindow`) | Maximum of every length-k interval -> decreasing deque of live indices. |
| Minimum Window Substring (`top_interview_150.py:minWindow`) | Shortest coverage including duplicates -> deficit counts, shrink while covered. |
| [Longest K-Stable Subarray](longest_k_stable_subarray.py) | Longest interval with max - min <= k (k >= 0) -> two monotonic deques. |
| [Maximum K-Subarray Sum](max_sub_array_sum.py) | Maximum sum of exactly k adjacent values (1 <= k <= n) -> outgoing/incoming rolling sum. |

See [TopInterview150.md](TopInterview150.md) for existing problem statements,
invariants, complexity, alternatives, and example commands.
