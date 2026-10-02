"""Cheapest Flights Within K Stops (LeetCode 787).

Problem: Given directed flights ``[from, to, price]``, find the cheapest route
from ``src`` to ``dst`` using at most ``k`` intermediate stops.  Output: the
minimum price, or ``-1`` if no permitted route exists.  Constraints: prices
are non-negative and at most ``k + 1`` flight edges may be used.
"""

from typing import List


def find_cheapest_price(n: int, flights: List[List[int]], src: int, dst: int, k: int) -> int:
    """Return the cheapest price using at most ``k`` intermediate stops.

    Pattern identification: cheapest route with an edge budget -> bounded
    Bellman-Ford; round r reads only costs achievable with at most r-1 edges.

    Each round adds at most one edge.  Reading from a copy prevents an update
    in the same round from accidentally using more than the allowed edges.
    """
    # 1. Output: Return the cheapest src-to-dst price with at most k stops, or -1.
    # 2. Structure: Both price and flight count matter; a cheapest unrestricted route may use too many stops.
    # 3. Constraints: Vertices are 0..n-1; a cheaper route is invalid if it exceeds the edge budget.
    # 4. Choice: Try improving costs through every flight for k+1 rounds, using the previous round's costs.
    #    A snapshot ensures each round adds at most one flight rather than chaining updates immediately.
    # 5. Why it works: After round r, distances are best costs using at most r edges; snapshots prevent extras.
    #    For E flights, time is O(n + (k+1)*(n+E)), including copies; extra space is O(n).
    inf = float("inf")
    distance = [inf] * n
    distance[src] = 0
    for _ in range(k + 1):
        previous = distance[:]
        for start, end, price in flights:
            if previous[start] != inf:
                distance[end] = min(distance[end], previous[start] + price)
    return -1 if distance[dst] == inf else int(distance[dst])
