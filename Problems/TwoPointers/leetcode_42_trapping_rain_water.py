#
# @lc app=leetcode id=42 lang=python3
#
# [42] Trapping Rain Water
#
# Pattern identification: a rising bar closes basins -> monotonic index stack;
# heights stay non-increasing; popped bottoms use surviving left and current right walls.

# @lc code=start
class Solution:
    def trap(self, height: List[int]) -> int:
        # 1. Output: Return the total water held between the bars.
        # 2. Structure: A taller incoming bar closes earlier low spots; the most recent low spot is resolved first.
        # 3. Constraints: Heights are nonnegative; an empty list or bars without two walls trap nothing.
        # 4. Choice: Use a stack of indices with heights from tall to short; pop low bottoms and add enclosed layers.
        # 5. Why it works: The surviving left wall and current right wall enclose each newly exposed layer.
        #    Each index is pushed/popped at most once: O(n) time and O(n) extra space.
        stack = []  # stores indices
        water = 0

        for i in range(len(height)):
            # While current bar is taller than the bar at stack top,
            # we found a bounded region — pop and calculate trapped water.
            while stack and height[i] > height[stack[-1]]:
                bottom = stack.pop()

                if not stack:
                    break  # no left boundary

                # Width between current bar and new stack top (left boundary)
                width = i - stack[-1] - 1
                # Height is bounded by the shorter of the two walls, minus the bottom
                bounded_height = min(height[i], height[stack[-1]]) - height[bottom]

                water += width * bounded_height

            stack.append(i)

        return water
# @lc code=end
