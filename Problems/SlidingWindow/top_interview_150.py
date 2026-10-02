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
        """Return the shortest positive-number subarray with sum >= target.

        Pattern identification: positive values and minimum threshold length -> sum window;
        sum tracks [left, right]; record covered windows before shrinking loses coverage.
        """
        # 1. Output: Return the shortest contiguous length with sum >= target, or zero if none exists.
        # 2. Structure: We need adjacent values, and positivity makes left removal reduce the sum: expand, then try shrinking.
        # 3. Constraints: Require positive target and array values; negative values invalidate this shrinking rule.
        # 4. Choice: Add rightmost values to window_sum, record qualifying lengths, and shrink while still qualifying.
        # 5. Why it works: Once a left endpoint qualifies, keeping it for later right endpoints cannot make it shorter.
        #    Both boundaries advance at most n times: O(n) time and O(1) extra space.
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
        """Return the longest substring containing no repeated character.

        Pattern identification: longest contiguous unique characters -> last-seen jumps;
        left never retreats and the current window contains no duplicate.
        """
        # 1. Output: Return the maximum length of a substring with no repeated character.
        # 2. Structure: Characters must be adjacent; a repeated character tells us exactly how far to move the left boundary.
        # 3. Constraints: Treat characters literally; empty input returns zero and left must never retreat.
        # 4. Choice: Store last-seen indices and jump left past an in-window duplicate before recording length.
        # 5. Why it works: The retained window stays unique and is the longest unique suffix ending at right.
        #    Average O(n) time and O(u) extra space for u distinct characters.
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
        """Return the longest window fixable by replacing at most k characters.

        Pattern identification: length minus dominant count is replacement cost -> counts;
        historical max_frequency keeps length - max_frequency <= k after repair,
        without claiming every retained window is currently feasible.
        """
        # 1. Output: Return the longest substring made uniform by at most k character replacements.
        # 2. Structure: Keep the most common letter and replace the rest; length minus its count measures a window's cost.
        # 3. Constraints: Assume k >= 0 and uppercase letters; the saved maximum frequency may become stale.
        # 4. Choice: Update counts and the historical max_frequency; shrink when length - max_frequency > k.
        # 5. Why it works: A stale maximum can retain an invalid window, but cannot raise best past a feasible length.
        #    A larger record needs a newly achieved frequency; O(n) time and O(1) space for the fixed alphabet.
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
        """Return whether some permutation of s1 occurs as a substring of s2.

        Pattern identification: permutation substring -> fixed-size frequency window;
        counts describe exactly len(s1) consecutive characters; equality proves a match.
        """
        # 1. Output: Return whether s2 contains a contiguous permutation of s1.
        # 2. Structure: A permutation has the same counts and length, so test only length-len(s1) windows, not all substrings.
        # 3. Constraints: A longer s1 cannot fit; empty s1 matches the initially empty window.
        # 4. Choice: Compare Counters, then add the incoming character and remove the outgoing one each slide.
        # 5. Why it works: Counts always describe exactly len(s1) adjacent characters; equality proves a permutation.
        #    For m=len(s1), n=len(s2), u distinct characters: O(m+n*u) time, O(m+u) space including the initial slice.
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
        """Return each window maximum using a decreasing monotonic deque.

        Pattern identification: repeated fixed-window maxima -> monotonic index deque;
        indices are live and values decrease, so the front is the current maximum.
        """
        # 1. Output: Return the maximum value of each complete length-k window.
        # 2. Structure: Adjacent length-k windows overlap; a newer value at least as large also stays in future windows longer.
        # 3. Constraints: Empty input or k <= 0 returns []; k beyond the input yields no complete windows.
        # 4. Choice: Keep indices in a deque from largest value to smallest; remove weaker backs and expired fronts.
        #    Read the front for each complete window instead of rescanning all k values.
        # 5. Why it works: Removed weaker values expire sooner, so they can never beat their newer replacement.
        #    Each index enters/leaves once: O(n) time and O(min(n,k)) extra space, excluding output.
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
        """Return the shortest substring of s containing all characters of t.

        Pattern identification: minimum multiplicity-aware coverage -> deficit window;
        remaining counts missing occurrences; zero permits recording then shrinking.
        """
        # 1. Output: Return the shortest substring covering every character of t, including repeated copies.
        # 2. Structure: We need adjacent characters with all required copies; once covered, removing left characters tests shorter answers.
        # 3. Constraints: Match literally; empty t or impossible coverage returns "", and first equal-length tie wins.
        # 4. Choice: Decrease required counts on expansion; when remaining is zero, record and shrink until coverage fails.
        # 5. Why it works: remaining counts only missing copies, so zero certifies coverage before each left removal.
        #    Average O(len(s)+len(t)) time; the Counter stores O(u) distinct characters across both strings.
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
