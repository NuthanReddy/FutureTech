# Pattern identification: contiguous area limited by shortest bar -> increasing index stack;
# a shorter bar closes popped spans; the remaining top bounds each span on the left.
"""Largest Rectangle in Histogram.

Problem: Given non-negative bar heights in a histogram, where each bar has
unit width, find the largest rectangle that can be formed from contiguous bars.

Input: ``heights``, a list of integer bar heights.
Output: the maximum rectangle area as an integer.
Constraints: heights may be empty and are non-negative; a rectangle must use
one or more contiguous bars.
"""

from __future__ import annotations


def largest_rectangle_area(heights: list[int]) -> int:
    """Return the largest rectangle area in *heights*.

    The stack stores indices with nondecreasing heights.  A shorter bar means
    that every taller bar popped now has found its first smaller bar on the
    right; the new stack top is its first smaller bar on the left.  A zero
    sentinel flushes the remaining bars without a special second loop.

    Complexity: O(n) time and O(n) space.
    """
    # 1. Output: Return the largest area under any consecutive group of histogram bars.
    # 2. Structure: Each bar has width one; a rectangle's height cannot exceed its shortest included bar.
    # 3. Constraints: Assume non-negative heights; empty input gives zero. O(n) time and O(n) space.
    # 4. Choice: A shorter bar blocks taller rectangles from extending right, so stack unfinished spans by height.
    # Keep indices with nondecreasing heights; pop taller bars at that blocker and compute their widths.
    # 5. Why it works: A pop finds the right limit; the remaining top limits how far that bar can extend left.
    # Equal bars eventually let the earlier copy cover the full span; a final zero closes positive heights.
    stack: list[int] = []
    best = 0
    for index in range(len(heights) + 1):
        current_height = heights[index] if index < len(heights) else 0
        while stack and current_height < heights[stack[-1]]:
            height = heights[stack.pop()]
            left_boundary = stack[-1] if stack else -1
            width = index - left_boundary - 1
            best = max(best, height * width)
        stack.append(index)
    return best


if __name__ == "__main__":
    assert largest_rectangle_area([2, 1, 5, 6, 2, 3]) == 10
    assert largest_rectangle_area([2, 4]) == 4
    assert largest_rectangle_area([]) == 0
    print("Largest Rectangle: all checks passed")
