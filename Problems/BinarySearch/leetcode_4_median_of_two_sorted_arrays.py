#
# @lc app=leetcode id=4 lang=python3
#
# [4] Median of Two Sorted Arrays
#

# @lc code=start
class Solution:
    def findMedianSortedArrays(self, nums1: List[int], nums2: List[int]) -> float:
        """Pattern identification: sorted streams, middle rank -> partial two-pointer merge ->
        each consumed value is the next smallest; this variant is linear, not binary search.
        """
        # 1. Output: Return the median, averaging the two middle values for even totals.
        # 2. Structure: Each input is sorted, so its next unread value is its smallest.
        # 3. Constraints: At least one array must be non-empty; assume both sorted.
        #    This partial merge takes O(m+n) time and O(1) space, not logarithmic time.
        # 4. Choice: Advance the pointer with the smaller next value until reaching
        #    the lower middle; for even totals average it with the next unread value.
        # 5. Why it works: Each consumed value is next in combined sorted order,
        #    so counting consumed values locates the required middle rank(s).
        if not nums1:
            if len(nums2) % 2 == 1:
                return nums2[len(nums2) // 2]
            else:
                return (nums2[len(nums2) // 2 - 1] + nums2[len(nums2) // 2]) / 2
        if not nums2:
            if len(nums1) % 2 == 1:
                return nums1[len(nums1) // 2]
            else:
                return (nums1[len(nums1) // 2 - 1] + nums1[len(nums1) // 2]) / 2
        odd = (len(nums1) + len(nums2)) % 2 == 1
        left_index = (len(nums1) + len(nums2) - 1) // 2
        i = j = 0
        while i + j <= left_index:
            if i < len(nums1) and (j >= len(nums2) or nums1[i] < nums2[j]):
                current = nums1[i]
                i += 1
            else:
                current = nums2[j]
                j += 1
        if odd:
            return current
        if i < len(nums1) and (j >= len(nums2) or nums1[i] < nums2[j]):
            next_val = nums1[i]
        else:
            next_val = nums2[j]
        return (current + next_val) / 2
        
# @lc code=end