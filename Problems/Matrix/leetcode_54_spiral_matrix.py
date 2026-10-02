#
# @lc app=leetcode id=54 lang=python3
#
# [54] Spiral Matrix
#
# Pattern identification: clockwise outer-ring traversal -> four shrinking boundaries;
# the remaining rectangle holds unvisited cells; guard collapsed rows and columns.

# @lc code=start
class Solution:
    def spiralOrder(self, matrix: List[List[int]]) -> List[int]:
        # 1. Output: Return all matrix values in clockwise spiral order.
        # 2. Structure: This is a prescribed visiting order, not a path search; each outer ring leaves a smaller rectangle.
        # 3. Constraints: Assume rectangular rows; empty input returns [], and a single row/column is visited once.
        # 4. Choice: Walk top, right, bottom, and left edges, moving each boundary inward after visiting it.
        # 5. Why it works: Bounds enclose unvisited cells; checking crossed bounds prevents duplicate edge visits.
        #    O(rows*columns) time and O(1) extra space beyond output.
        output = []
        if not matrix:
            return output
        top, bottom = 0, len(matrix) - 1
        left, right = 0, len(matrix[0]) - 1
        while left <= right and top <= bottom:
            for i in range(left, right + 1):
                output.append(matrix[top][i])
            top += 1
            for i in range(top, bottom + 1):
                output.append(matrix[i][right])
            right -= 1
            # Check if there are more rows and columns to traverse 
            # before traversing the bottom row and left column
            if top <= bottom:
                for i in range(right, left - 1, -1):
                    output.append(matrix[bottom][i])
                bottom -= 1
            if left <= right:
                for i in range(bottom, top - 1, -1):
                    output.append(matrix[i][left])
                left += 1
        return output
# @lc code=end
