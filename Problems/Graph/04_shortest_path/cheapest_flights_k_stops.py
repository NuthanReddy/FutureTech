"""Cheapest Flights Within K Stops (LeetCode 787).

Problem: Given directed flights ``[from, to, price]``, find the cheapest route
from ``src`` to ``dst`` using at most ``k`` intermediate stops.  Output: the
minimum price, or ``-1`` if no permitted route exists.  Constraints: prices
are non-negative and at most ``k + 1`` flight edges may be used.
"""

from typing import List


def find_cheapest_price(n: int, flights: List[List[int]], src: int, dst: int, k: int) -> int:
    """Return the cheapest price using at most ``k`` intermediate stops.

    Each round adds at most one edge.  Reading from a copy prevents an update
    in the same round from accidentally using more than the allowed edges.
    """
    inf = float("inf")
    distance = [inf] * n
    distance[src] = 0
    for _ in range(k + 1):
        previous = distance[:]
        for start, end, price in flights:
            if previous[start] != inf:
                distance[end] = min(distance[end], previous[start] + price)
    return -1 if distance[dst] == inf else int(distance[dst])
