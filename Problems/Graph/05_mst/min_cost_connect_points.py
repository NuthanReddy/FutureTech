"""Min Cost to Connect All Points (LeetCode 1584).

Problem: Given points in the plane, connect every point with edges whose cost
is Manhattan distance ``abs(dx) + abs(dy)``.  Output: the minimum total cost
of a connected network (an MST).  Constraints: every point is distinct and
the input can contain zero, one, or many points.
"""

from typing import List


def min_cost_connect_points(points: List[List[int]]) -> int:
    """Return the Manhattan-weight MST cost."""
    n = len(points)
    if n < 2:
        return 0
    best = [float("inf")] * n
    best[0] = 0
    used = [False] * n
    total = 0
    for _ in range(n):
        node = min((i for i in range(n) if not used[i]), key=best.__getitem__)
        used[node] = True
        total += best[node]
        for other in range(n):
            if not used[other]:
                cost = abs(points[node][0] - points[other][0]) + abs(points[node][1] - points[other][1])
                best[other] = min(best[other], cost)
    return int(total)
