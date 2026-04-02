# -----------------------------------------------------------------------
# Network Delay Time
#
# Problem:
#   You are given a network of n nodes (labelled 1 … n) and a list of
#   directed, weighted edges [source, target, time].  A signal is sent
#   from a given source node.  Return the minimum time for ALL nodes to
#   receive the signal, or -1 if it is impossible.
#
# Approach:
#   Build a weighted directed Graph and run Dijkstra's shortest_path from
#   the source to every other node.  The answer is the maximum shortest
#   distance.  If any node is unreachable the answer is -1.
#
# Time complexity:  O(V × (V + E) log V) — Dijkstra is called V-1 times.
#                   (Could be O((V+E) log V) with a single-source
#                   Dijkstra, but we reuse the Graph API as-is.)
# Space complexity: O(V + E) for the graph.
# -----------------------------------------------------------------------

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.Graph import Graph


def network_delay_time(
    times: list[list[int]], n: int, source: int
) -> int:
    """Return the time for a signal from *source* to reach all *n* nodes.

    Args:
        times:  List of [u, v, w] directed edges (u→v with weight w).
        n:      Number of nodes (labelled 1 … n).
        source: The node that initiates the signal.

    Returns:
        Minimum time for all nodes to receive the signal, or -1 if some
        node is unreachable.

    Examples:
        >>> network_delay_time([[2,1,1],[2,3,1],[3,4,1]], 4, 2)
        2
    """
    graph = Graph(directed=True)
    for node in range(1, n + 1):
        graph.add_vertex(node)
    for u, v, w in times:
        graph.add_edge(u, v, weight=float(w))

    max_dist = 0
    for target in range(1, n + 1):
        if target == source:
            continue
        dist, _ = graph.shortest_path(source, target)
        if dist is None:
            return -1
        max_dist = max(max_dist, int(dist))

    return max_dist


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------

if __name__ == "__main__":
    # Classic example
    times1 = [[2, 1, 1], [2, 3, 1], [3, 4, 1]]
    assert network_delay_time(times1, 4, 2) == 2
    print(f"Example 1: {network_delay_time(times1, 4, 2)}")

    # Unreachable node
    times2 = [[1, 2, 1]]
    assert network_delay_time(times2, 3, 1) == -1
    print(f"Unreachable node: {network_delay_time(times2, 3, 1)}")

    # Single node — no edges needed
    assert network_delay_time([], 1, 1) == 0
    print(f"Single node: {network_delay_time([], 1, 1)}")

    # Two nodes, direct edge
    times3 = [[1, 2, 5]]
    assert network_delay_time(times3, 2, 1) == 5
    print(f"Two nodes direct: {network_delay_time(times3, 2, 1)}")

    # Shorter path through intermediate node
    # 1→2 costs 10, but 1→3→2 costs 3+2=5
    times4 = [[1, 2, 10], [1, 3, 3], [3, 2, 2]]
    assert network_delay_time(times4, 3, 1) == 5
    print(f"Shorter via relay: {network_delay_time(times4, 3, 1)}")

    # All nodes reachable, find max delay
    # 1→2 (1), 1→3 (4), 2→3 (2)
    # Shortest: 1→2=1, 1→3=min(4, 1+2)=3 → max=3
    times5 = [[1, 2, 1], [1, 3, 4], [2, 3, 2]]
    assert network_delay_time(times5, 3, 1) == 3
    print(f"Max delay: {network_delay_time(times5, 3, 1)}")

    # Star topology
    times6 = [[1, 2, 3], [1, 3, 7], [1, 4, 1]]
    assert network_delay_time(times6, 4, 1) == 7
    print(f"Star topology: {network_delay_time(times6, 4, 1)}")

    # Source cannot reach itself scenario — two node, wrong direction
    times7 = [[2, 1, 1]]
    assert network_delay_time(times7, 2, 1) == -1
    print(f"Wrong direction: {network_delay_time(times7, 2, 1)}")

    print("\nAll tests passed!")
