"""LeetCode Top Interview 150: Two Pointers.

Each method is intentionally self-contained and keeps the invariant beside the
loop that maintains it.  Running this file executes a small smoke-test demo.

Problem statements
------------------
* Valid Palindrome: given a string ``s``, decide whether its letters and digits
  read identically forwards and backwards after case-folding and ignoring
  non-alphanumeric characters.  Input is a string; output is a boolean.
* Is Subsequence: given strings ``s`` and ``t``, decide whether deleting zero or
  more characters from ``t`` can produce ``s``.  Output is a boolean.
* Two Sum II: given a non-decreasing integer array ``numbers`` and integer
  ``target``, return the one-based indices of the unique pair summing to the
  target, or an empty list if absent.  The two indices must be distinct.
* 3Sum: given an integer array ``nums``, return every unique value-triple whose
  sum is zero.  Output is a list of triples; duplicate triples are forbidden.
* Container With Most Water: given non-negative heights, choose two distinct
  lines and maximize ``(right-left) * min(height[left], height[right])``.
  Output is the maximum area.
* Trapping Rain Water: given non-negative bar heights, return the total units
  of water trapped after raining.  Output is a non-negative integer.

The standard constraints are large enough that quadratic pair enumeration is
undesirable (typically ``n <= 10**5`` for linear problems and ``n <= 3000``
for 3Sum); all methods handle empty input where meaningful.
"""

from collections.abc import Sequence


class Solution:
    def isPalindrome(self, s: str) -> bool:
        """Return whether ``s`` is a palindrome after ignoring punctuation.

        Pattern identification: mirrored normalized characters -> inward pointers;
        all compared outer pairs match, leaving only the interior unchecked.
        """
        # 1. Output: Return whether letters and digits read the same from both ends, ignoring case.
        # 2. Structure: Only mirrored characters need comparing, so two ends can move inward without a reversed copy.
        # 3. Constraints: Use the implementation's isalnum()/lower() rules; empty or punctuation-only input passes.
        # 4. Choice: Move two pointers inward, skip non-alphanumeric characters, and reject a differing pair.
        # 5. Why it works: All discarded outer pairs matched, leaving only the unchecked interior.
        #    Each pointer moves in one direction: O(n) time and O(1) extra space.
        left, right = 0, len(s) - 1
        while left < right:
            # Non-alphanumeric characters do not participate in the comparison.
            while left < right and not s[left].isalnum():
                left += 1
            while left < right and not s[right].isalnum():
                right -= 1
            if s[left].lower() != s[right].lower():
                return False
            left += 1
            right -= 1
        return True

    def isSubsequence(self, s: str, t: str) -> bool:
        """Return whether ``s`` can be obtained by deleting characters from ``t``.

        Pattern identification: deletions preserve order -> forward matching cursor;
        s[:s_index] is matched within the processed prefix of t.
        """
        # 1. Output: Return whether deleting characters from t can leave s in its original order.
        # 2. Structure: We may skip t's characters but cannot reorder them, suggesting a forward cursor for each string.
        # 3. Constraints: Empty s always matches; repeated letters each need their own later match.
        # 4. Choice: Scan t once and advance s_index only when the next required character matches.
        # 5. Why it works: Taking the earliest available match leaves the most remaining characters for the suffix.
        #    s[:s_index] stays matched; O(len(t)) time and O(1) extra space.
        s_index = 0
        for character in t:
            # The invariant is that s[:s_index] has already been matched.
            if s_index < len(s) and s[s_index] == character:
                s_index += 1
        return s_index == len(s)

    def twoSum(self, numbers: Sequence[int], target: int) -> list[int]:
        """Find two sorted values and return their one-based indices.

        Pattern identification: sorted pair sum -> inward pointers;
        every possible remaining pair lies between left and right.
        """
        # 1. Output: Return distinct one-based indices summing to target, or [] if no pair exists.
        # 2. Structure: Sorted values make left moves increase sums and right moves decrease them, ruling out many pairs at once.
        # 3. Constraints: Do not mutate the sequence; one position cannot supply both values.
        # 4. Choice: Compare the endpoint sum; advance left if too small, or retreat right if too large.
        # 5. Why it works: A too-small left endpoint cannot pair with anything smaller than right, and vice versa.
        #    Each discarded endpoint is safe: O(n) time and O(1) extra space.
        left, right = 0, len(numbers) - 1
        while left < right:
            total = numbers[left] + numbers[right]
            if total == target:
                return [left + 1, right + 1]
            # Sorted order makes this safe: only one side can improve the sum.
            if total < target:
                left += 1
            else:
                right -= 1
        return []

    def threeSum(self, nums: list[int]) -> list[list[int]]:
        """Return unique triples whose sum is zero.

        Pattern identification: unique zero-sum triples -> sorted anchor plus pair search;
        pointer moves preserve remaining candidates; duplicate skips prevent repeats.
        """
        # 1. Output: Return every unique value-triple whose sum is zero.
        # 2. Structure: Fixing one value leaves two that must sum to its negative; sorting lets endpoints guide that search.
        # 3. Constraints: This method sorts nums in place; fewer than three values yield no triples.
        # 4. Choice: Fix each distinct anchor, move endpoint pointers by the sum, and skip duplicate matches.
        # 5. Why it works: Sorted sums justify each discarded endpoint; duplicate skips remove only repeated triples.
        #    O(n^2) time; Python sorting uses O(n) extra space, excluding returned triples.
        nums.sort()
        result: list[list[int]] = []
        for anchor in range(len(nums) - 2):
            if anchor and nums[anchor] == nums[anchor - 1]:
                continue
            if nums[anchor] > 0:
                break
            left, right = anchor + 1, len(nums) - 1
            while left < right:
                total = nums[anchor] + nums[left] + nums[right]
                if total == 0:
                    result.append([nums[anchor], nums[left], nums[right]])
                    left += 1
                    right -= 1
                    # Skip equal values so each value-triple is emitted once.
                    while left < right and nums[left] == nums[left - 1]:
                        left += 1
                    while left < right and nums[right] == nums[right + 1]:
                        right -= 1
                elif total < 0:
                    left += 1
                else:
                    right -= 1
        return result

    def maxArea(self, height: Sequence[int]) -> int:
        """Return the largest container area formed by two vertical lines.

        Pattern identification: width times shorter wall -> inward pointers;
        after recording area, the shorter endpoint cannot improve with narrower width.
        """
        # 1. Output: Return the largest area enclosed by two distinct lines.
        # 2. Structure: Moving inward reduces width; only replacing the shorter wall can improve the limiting height.
        # 3. Constraints: Heights are nonnegative; fewer than two lines return zero.
        # 4. Choice: Record the endpoint area, then move the shorter endpoint inward.
        # 5. Why it works: Keeping that shorter wall while narrowing the width cannot improve its recorded area.
        #    No better pair is discarded: O(n) time and O(1) extra space.
        left, right = 0, len(height) - 1
        best = 0
        while left < right:
            width = right - left
            best = max(best, width * min(height[left], height[right]))
            # Moving the taller side cannot increase the limiting height.
            if height[left] < height[right]:
                left += 1
            else:
                right -= 1
        return best

    def trap(self, height: Sequence[int]) -> int:
        """Compute trapped rain water in O(n) time and O(1) extra space.

        Pattern identification: water needs two enclosing maxima -> boundary pointers;
        the smaller running maximum fixes that side's water; processed bars are final.
        """
        # 1. Output: Return the sum of water trapped above all bars.
        # 2. Structure: Water needs walls on both sides; knowing the lower wall can settle one end without scanning every pair.
        # 3. Constraints: Heights are nonnegative; empty input returns zero without accessing a bar.
        # 4. Choice: Track both running maxima and process the side with the smaller maximum.
        # 5. Why it works: The opposite maximum covers this side's old maximum; a new taller bar contributes zero.
        #    Each bar is finalized once: O(n) time and O(1) extra space.
        left, right = 0, len(height) - 1
        left_max = right_max = 0
        water = 0
        while left <= right:
            # Process the side with the smaller known boundary: its water level
            # is already determined by that side's running maximum.
            if left_max <= right_max:
                left_max = max(left_max, height[left])
                water += left_max - height[left]
                left += 1
            else:
                right_max = max(right_max, height[right])
                water += right_max - height[right]
                right -= 1
        return water


if __name__ == "__main__":
    solver = Solution()
    assert solver.isPalindrome("A man, a plan, a canal: Panama")
    assert solver.isSubsequence("abc", "ahbgdc")
    assert solver.twoSum([2, 7, 11, 15], 9) == [1, 2]
    assert solver.threeSum([-1, 0, 1, 2, -1, -4]) == [[-1, -1, 2], [-1, 0, 1]]
    assert solver.maxArea([1, 8, 6, 2, 5, 4, 8, 3, 7]) == 49
    assert solver.trap([0, 1, 0, 2, 1, 0, 1, 3, 2, 1, 2, 1]) == 6
    print("Two-pointers smoke tests passed.")
