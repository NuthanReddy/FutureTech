"""LeetCode Top Interview 150: Sliding Window.

The window methods expand on the right, restore their validity by moving the
left boundary, and update the answer only after the invariant is restored.

Problem statements
------------------
* Minimum Size Subarray Sum: given a positive-integer array ``nums`` and
  positive ``target``, return the minimum length of a contiguous subarray whose
  sum is at least ``target``; return 0 when none exists.
* Longest Substring Without Repeating Characters: given string ``s``, return
  the length of its longest contiguous substring with all distinct characters.
* Longest Repeating Character Replacement: given uppercase string ``s`` and
  integer ``k``, return the longest substring that can become all one letter
  after replacing at most ``k`` characters.
* Permutation in String: given strings ``s1`` and ``s2``, decide whether some
  contiguous substring of ``s2`` is a permutation of ``s1``.  Output is bool.
* Sliding Window Maximum: given integer array ``nums`` and positive window size
  ``k``, return the maximum value from each contiguous window of length ``k``.
* Minimum Window Substring: given strings ``s`` and ``t``, return the shortest
  substring of ``s`` containing every character of ``t`` with multiplicity, or
  ``""`` if no such substring exists.

Inputs are strings/arrays as described above; outputs are integers, booleans,
lists, or substrings. Standard constraints make O(n) or O(n log n) solutions
necessary; the first problem specifically relies on all array values being
positive.
"""

from collections import Counter, deque


class Solution:
    def minSubArrayLen(self, target: int, nums: list[int]) -> int:
        """Return the shortest positive-number subarray with sum >= target."""
        left = 0
        window_sum = 0
        best = len(nums) + 1
        for right, value in enumerate(nums):
            window_sum += value
            # Positivity is essential: removing from the left can only reduce
            # the sum, so this loop finds the shortest valid window ending here.
            while window_sum >= target:
                best = min(best, right - left + 1)
                window_sum -= nums[left]
                left += 1
        return 0 if best == len(nums) + 1 else best

    def lengthOfLongestSubstring(self, s: str) -> int:
        """Return the longest substring containing no repeated character."""
        last_seen: dict[str, int] = {}
        left = best = 0
        for right, character in enumerate(s):
            if character in last_seen:
                # Never move left backwards; an old occurrence may be outside
                # the current window already.
                left = max(left, last_seen[character] + 1)
            last_seen[character] = right
            best = max(best, right - left + 1)
        return best

    def characterReplacement(self, s: str, k: int) -> int:
        """Return the longest window fixable by replacing at most k characters."""
        counts: Counter[str] = Counter()
        left = best = max_frequency = 0
        for right, character in enumerate(s):
            counts[character] += 1
            max_frequency = max(max_frequency, counts[character])
            # Keep one most-common character and replace the rest.
            while right - left + 1 - max_frequency > k:
                counts[s[left]] -= 1
                left += 1
            best = max(best, right - left + 1)
        return best

    def checkInclusion(self, s1: str, s2: str) -> bool:
        """Return whether some permutation of s1 occurs as a substring of s2."""
        if len(s1) > len(s2):
            return False
        need = Counter(s1)
        window = Counter(s2[: len(s1)])
        if window == need:
            return True
        for right in range(len(s1), len(s2)):
            window[s2[right]] += 1
            outgoing = s2[right - len(s1)]
            window[outgoing] -= 1
            if window[outgoing] == 0:
                del window[outgoing]
            if window == need:
                return True
        return False

    def maxSlidingWindow(self, nums: list[int], k: int) -> list[int]:
        """Return each window maximum using a decreasing monotonic deque."""
        if not nums or k <= 0:
            return []
        candidates: deque[int] = deque()
        result: list[int] = []
        for right, value in enumerate(nums):
            while candidates and nums[candidates[-1]] <= value:
                candidates.pop()
            candidates.append(right)
            if candidates[0] <= right - k:
                candidates.popleft()
            if right >= k - 1:
                result.append(nums[candidates[0]])
        return result

    def minWindow(self, s: str, t: str) -> str:
        """Return the shortest substring of s containing all characters of t."""
        if not t or len(t) > len(s):
            return ""
        required = Counter(t)
        remaining = len(t)
        left = 0
        best_start, best_length = 0, len(s) + 1
        for right, character in enumerate(s):
            if required[character] > 0:
                remaining -= 1
            required[character] -= 1
            while remaining == 0:
                if right - left + 1 < best_length:
                    best_start, best_length = left, right - left + 1
                required[s[left]] += 1
                if required[s[left]] > 0:
                    remaining += 1
                left += 1
        return "" if best_length == len(s) + 1 else s[best_start : best_start + best_length]


if __name__ == "__main__":
    solver = Solution()
    assert solver.minSubArrayLen(7, [2, 3, 1, 2, 4, 3]) == 2
    assert solver.lengthOfLongestSubstring("abcabcbb") == 3
    assert solver.characterReplacement("AABABBA", 1) == 4
    assert solver.checkInclusion("ab", "eidbaooo")
    assert solver.maxSlidingWindow([1, 3, -1, -3, 5, 3, 6, 7], 3) == [3, 3, 5, 5, 6, 7]
    assert solver.minWindow("ADOBECODEBANC", "ABC") == "BANC"
    print("Sliding-window smoke tests passed.")
