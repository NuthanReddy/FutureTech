# -----------------------------------------------------------------------
# Shortest Path in a Maze
#
# Problem:
#   Given a 2D grid where 0 = open cell and 1 = wall, find the shortest
#   path from the top-left corner (0,0) to the bottom-right corner
#   (rows-1, cols-1).  Movement is allowed in four directions (up, down,
#   left, right).  Return the path length (number of steps), or -1 if the
#   destination is unreachable.
#
# Approach:
#   Build a Graph from the grid — each open cell is a vertex and each
#   pair of adjacent open cells forms an edge with weight 1.  Then use
#   BFS-equivalent shortest_path (Dijkstra with uniform weights) to find
#   the minimum-step path.
#
# Time complexity:  O(R × C × log(R × C)) — Dijkstra on the grid graph.
# Space complexity: O(R × C) for the graph.
# -----------------------------------------------------------------------

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.Graph import Graph

# Four cardinal directions
_DIRECTIONS = [(-1, 0), (1, 0), (0, -1), (0, 1)]


def shortest_path_maze(grid: list[list[int]]) -> int:
    """Return the shortest path length from top-left to bottom-right.

    Args:
        grid: 2D list of 0s (open) and 1s (walls).

    Returns:
        Number of steps in the shortest path, or -1 if unreachable.

    Examples:
        >>> shortest_path_maze([[0, 0, 0], [1, 1, 0], [0, 0, 0]])
        4
    """
    if not grid or not grid[0]:
        return -1

    rows, cols = len(grid), len(grid[0])

    # Start or end is a wall → impossible
    if grid[0][0] == 1 or grid[rows - 1][cols - 1] == 1:
        return -1

    # Trivial 1×1 grid
    if rows == 1 and cols == 1:
        return 0

    # Build graph from grid
    graph = Graph(directed=False)
    for r in range(rows):
        for c in range(cols):
            if grid[r][c] == 0:
                graph.add_vertex((r, c))
                for dr, dc in _DIRECTIONS:
                    nr, nc = r + dr, c + dc
                    if 0 <= nr < rows and 0 <= nc < cols and grid[nr][nc] == 0:
                        graph.add_edge((r, c), (nr, nc), weight=1.0)

    start = (0, 0)
    end = (rows - 1, cols - 1)

    dist, path = graph.shortest_path(start, end)
    if dist is None:
        return -1
    return int(dist)


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------

if __name__ == "__main__":
    # Simple 3×3 maze
    grid1 = [
        [0, 0, 0],
        [1, 1, 0],
        [0, 0, 0],
    ]
    assert shortest_path_maze(grid1) == 4
    print(f"3×3 maze: {shortest_path_maze(grid1)} steps")

    # No path available
    grid2 = [
        [0, 1],
        [1, 0],
    ]
    assert shortest_path_maze(grid2) == -1
    print(f"Blocked maze: {shortest_path_maze(grid2)}")

    # Straight line
    grid3 = [[0, 0, 0, 0, 0]]
    assert shortest_path_maze(grid3) == 4
    print(f"Single row: {shortest_path_maze(grid3)} steps")

    # Single cell
    grid4 = [[0]]
    assert shortest_path_maze(grid4) == 0
    print(f"1×1 grid: {shortest_path_maze(grid4)} steps")

    # Start is a wall
    grid5 = [[1, 0], [0, 0]]
    assert shortest_path_maze(grid5) == -1
    print(f"Start is wall: {shortest_path_maze(grid5)}")

    # End is a wall
    grid6 = [[0, 0], [0, 1]]
    assert shortest_path_maze(grid6) == -1
    print(f"End is wall: {shortest_path_maze(grid6)}")

    # Larger maze with winding path
    grid7 = [
        [0, 0, 1, 0, 0],
        [1, 0, 1, 0, 1],
        [0, 0, 0, 0, 0],
        [0, 1, 1, 1, 0],
        [0, 0, 0, 0, 0],
    ]
    result = shortest_path_maze(grid7)
    assert result == 8
    print(f"5×5 winding maze: {result} steps")

    # Empty grid
    assert shortest_path_maze([]) == -1
    assert shortest_path_maze([[]]) == -1
    print("Empty grid: OK")

    # All walls except start and end (disconnected)
    grid8 = [
        [0, 1, 1],
        [1, 1, 1],
        [1, 1, 0],
    ]
    assert shortest_path_maze(grid8) == -1
    print(f"Disconnected grid: {shortest_path_maze(grid8)}")

    print("\nAll tests passed!")
