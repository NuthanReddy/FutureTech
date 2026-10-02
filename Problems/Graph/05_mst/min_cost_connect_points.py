"""Min Cost to Connect All Points (LeetCode 1584).

Problem: Given points in the plane, connect every point with edges whose cost
is Manhattan distance ``abs(dx) + abs(dy)``.  Output: the minimum total cost
of a connected network (an MST).  Constraints: every point is distinct and
the input can contain zero, one, or many points.
"""

from typing import List


def min_cost_connect_points(points: List[List[int]]) -> int:
    """Return the Manhattan-weight MST cost.

    Pattern identification: cheapest network connecting all points -> dense Prim;
    best[v] is the cheapest edge from the growing tree to unused point v.
    """
    # 1. Output: Return the minimum total Manhattan cost of connecting every point.
    # 2. Structure: Connect all points, not just one route; any pair is allowed and extra cycles only add cost.
    # 3. Constraints: Zero or one point costs zero; avoid storing every pairwise edge.
    # 4. Choice: Store each outside point's cheapest link to the growing tree; take the smallest and update links.
    # 5. Why it works: Any connecting tree must cross from inside to outside; this cheapest crossing is safe.
    #    Add it to an optimum and remove another crossing on the resulting cycle: cost cannot increase.
    #    Dense Prim scans points each round: O(n^2) time and O(n) extra space.
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
