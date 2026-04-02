# -----------------------------------------------------------------------
# Course Schedule (Cycle Detection + Topological Sort)
#
# Problem:
#   There are n courses labelled 0 … n-1.  Some have prerequisites given
#   as pairs [course, prerequisite].
#
#   Part 1 — can_finish(n, prerequisites) -> bool
#     Determine whether it is possible to finish all courses (i.e. the
#     prerequisite graph has NO cycle).
#
#   Part 2 — find_order(n, prerequisites) -> list[int]
#     Return a valid ordering in which to take the courses.  Return an
#     empty list if no valid ordering exists.
#
# Approach:
#   Build a directed Graph (prerequisite → course) and use:
#     - Graph.has_cycle()        for Part 1
#     - Graph.topological_sort() for Part 2
#
# Time complexity:  O(V + E) for both parts.
# Space complexity: O(V + E) for the adjacency list.
# -----------------------------------------------------------------------

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DataStructures.Graph import Graph


def can_finish(num_courses: int, prerequisites: list[list[int]]) -> bool:
    """Return True if all courses can be completed.

    Args:
        num_courses:   Total number of courses (0 … num_courses-1).
        prerequisites: List of [course, prerequisite] pairs.

    Returns:
        True when there is no cyclic dependency among courses.

    Examples:
        >>> can_finish(2, [[1, 0]])
        True
        >>> can_finish(2, [[1, 0], [0, 1]])
        False
    """
    graph = Graph(directed=True)
    for i in range(num_courses):
        graph.add_vertex(i)
    for course, prereq in prerequisites:
        graph.add_edge(prereq, course)

    return not graph.has_cycle()


def find_order(num_courses: int, prerequisites: list[list[int]]) -> list[int]:
    """Return a valid course ordering, or an empty list if impossible.

    Args:
        num_courses:   Total number of courses.
        prerequisites: List of [course, prerequisite] pairs.

    Returns:
        A list representing one valid topological ordering, or [] if a
        cycle makes completion impossible.

    Examples:
        >>> find_order(4, [[1, 0], [2, 0], [3, 1], [3, 2]])  # doctest: +SKIP
        [0, 1, 2, 3]  # or [0, 2, 1, 3]
    """
    graph = Graph(directed=True)
    for i in range(num_courses):
        graph.add_vertex(i)
    for course, prereq in prerequisites:
        graph.add_edge(prereq, course)

    try:
        return list(graph.topological_sort())
    except ValueError:
        return []


def _is_valid_order(
    order: list[int], num_courses: int, prerequisites: list[list[int]]
) -> bool:
    """Validate that *order* satisfies all prerequisites."""
    if len(order) != num_courses:
        return False
    position = {course: idx for idx, course in enumerate(order)}
    return all(position[prereq] < position[course] for course, prereq in prerequisites)


# -----------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------

if __name__ == "__main__":
    # --- can_finish tests ---

    # Simple chain: 0 → 1
    assert can_finish(2, [[1, 0]]) is True
    print("can_finish(2, [[1,0]]) = True: OK")

    # Cycle: 0 → 1 → 0
    assert can_finish(2, [[1, 0], [0, 1]]) is False
    print("can_finish(2, [[1,0],[0,1]]) = False: OK")

    # No prerequisites
    assert can_finish(3, []) is True
    print("can_finish(3, []) = True: OK")

    # Single course
    assert can_finish(1, []) is True
    print("can_finish(1, []) = True: OK")

    # Diamond shape — no cycle
    assert can_finish(4, [[1, 0], [2, 0], [3, 1], [3, 2]]) is True
    print("Diamond (no cycle): OK")

    # Longer cycle: 0→1→2→0
    assert can_finish(3, [[1, 0], [2, 1], [0, 2]]) is False
    print("Longer cycle: OK")

    # --- find_order tests ---

    # Simple chain
    order = find_order(2, [[1, 0]])
    assert order == [0, 1]
    print(f"find_order(2, [[1,0]]) = {order}")

    # Cycle → empty
    order = find_order(2, [[1, 0], [0, 1]])
    assert order == []
    print(f"find_order(2, [[1,0],[0,1]]) = {order}")

    # No prerequisites — any permutation is valid
    order = find_order(3, [])
    assert set(order) == {0, 1, 2}
    print(f"find_order(3, []) = {order}")

    # Diamond
    prereqs = [[1, 0], [2, 0], [3, 1], [3, 2]]
    order = find_order(4, prereqs)
    assert _is_valid_order(order, 4, prereqs)
    print(f"find_order(4, diamond) = {order}")

    # Single course, no prereqs
    order = find_order(1, [])
    assert order == [0]
    print(f"find_order(1, []) = {order}")

    print("\nAll tests passed!")
