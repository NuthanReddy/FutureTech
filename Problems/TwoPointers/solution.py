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
        """Return whether ``s`` is a palindrome after ignoring punctuation."""
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
        """Return whether ``s`` can be obtained by deleting characters from ``t``."""
        s_index = 0
        for character in t:
            # The invariant is that s[:s_index] has already been matched.
            if s_index < len(s) and s[s_index] == character:
                s_index += 1
        return s_index == len(s)

    def twoSum(self, numbers: Sequence[int], target: int) -> list[int]:
        """Find two sorted values and return their one-based indices."""
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
        """Return unique triples whose sum is zero."""
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
        """Return the largest container area formed by two vertical lines."""
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
        """Compute trapped rain water in O(n) time and O(1) extra space."""
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
