from __future__ import annotations

import heapq
from collections import defaultdict, deque
from typing import Any, Dict, Hashable, List, Optional, Set, Tuple


class Graph:
    """Graph with adjacency-list representation.

    Supports directed/undirected, weighted edges, and standard graph
    algorithms: BFS, DFS, Dijkstra shortest path, cycle detection, and
    topological sort.

    Time complexities (V = vertices, E = edges):
        add_vertex      — O(1)
        add_edge        — O(1)
        remove_vertex   — O(V + E)
        remove_edge     — O(E)  worst-case scan of neighbour list
        bfs / dfs       — O(V + E)
        shortest_path   — O((V + E) log V)  Dijkstra with min-heap
        has_cycle       — O(V + E)
        topological_sort— O(V + E)
    """

    def __init__(self, directed: bool = True) -> None:
        """Create a graph.

        Args:
            directed: If True the graph is directed; otherwise undirected.
        """
        self._directed = directed
        # vertex -> list of (neighbour, weight)
        self._adj: Dict[Hashable, List[Tuple[Hashable, float]]] = defaultdict(list)
        self._vertices: Set[Hashable] = set()

    @property
    def directed(self) -> bool:
        return self._directed

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_vertex(self, v: Hashable) -> None:
        """Add vertex *v* (idempotent).

        Time complexity: O(1)
        """
        self._vertices.add(v)
        # ensure key exists in adjacency list
        _ = self._adj[v]

    def add_edge(self, u: Hashable, v: Hashable, weight: float = 1.0) -> None:
        """Add an edge from *u* to *v* with the given *weight*.

        Automatically adds both vertices if they don't exist yet.
        For undirected graphs the reverse edge is added as well.

        Time complexity: O(1)
        """
        self._vertices.add(u)
        self._vertices.add(v)
        self._adj[u].append((v, weight))
        if not self._directed:
            self._adj[v].append((u, weight))

    def remove_vertex(self, v: Hashable) -> None:
        """Remove vertex *v* and all edges touching it.

        Time complexity: O(V + E)
        Raises KeyError if the vertex does not exist.
        """
        if v not in self._vertices:
            raise KeyError(v)
        self._vertices.discard(v)
        del self._adj[v]
        for u in self._adj:
            self._adj[u] = [(nb, w) for nb, w in self._adj[u] if nb != v]

    def remove_edge(self, u: Hashable, v: Hashable) -> None:
        """Remove one edge from *u* to *v*.

        For undirected graphs the reverse edge is also removed.

        Time complexity: O(degree(u))
        Raises KeyError if the edge does not exist.
        """
        removed = False
        for i, (nb, _) in enumerate(self._adj.get(u, [])):
            if nb == v:
                self._adj[u].pop(i)
                removed = True
                break
        if not self._directed:
            for i, (nb, _) in enumerate(self._adj.get(v, [])):
                if nb == u:
                    self._adj[v].pop(i)
                    break
        if not removed:
            raise KeyError(f"edge ({u}, {v}) not found")

    # ------------------------------------------------------------------
    # Traversals
    # ------------------------------------------------------------------

    def bfs(self, start: Hashable) -> List[Hashable]:
        """Breadth-first traversal from *start*.

        Time complexity: O(V + E)
        """
        visited: Set[Hashable] = set()
        order: List[Hashable] = []
        queue: deque[Hashable] = deque([start])
        visited.add(start)

        while queue:
            node = queue.popleft()
            order.append(node)
            for nb, _ in self._adj[node]:
                if nb not in visited:
                    visited.add(nb)
                    queue.append(nb)
        return order

    def dfs(self, start: Hashable) -> List[Hashable]:
        """Depth-first traversal from *start* (iterative).

        Time complexity: O(V + E)
        """
        visited: Set[Hashable] = set()
        order: List[Hashable] = []
        stack: List[Hashable] = [start]

        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            order.append(node)
            # push neighbours in reverse so the first neighbour is visited first
            for nb, _ in reversed(self._adj[node]):
                if nb not in visited:
                    stack.append(nb)
        return order

    # ------------------------------------------------------------------
    # Shortest path (Dijkstra)
    # ------------------------------------------------------------------

    def shortest_path(
        self, start: Hashable, end: Hashable
    ) -> Tuple[Optional[float], List[Hashable]]:
        """Find the shortest (minimum-weight) path from *start* to *end*
        using Dijkstra's algorithm.

        Returns ``(distance, [path])`` or ``(None, [])`` if unreachable.

        Time complexity: O((V + E) log V)
        """
        dist: Dict[Hashable, float] = {start: 0.0}
        prev: Dict[Hashable, Optional[Hashable]] = {start: None}
        heap: List[Tuple[float, Any]] = [(0.0, start)]

        while heap:
            d, u = heapq.heappop(heap)
            if d > dist.get(u, float("inf")):
                continue
            if u == end:
                break
            for nb, w in self._adj[u]:
                nd = d + w
                if nd < dist.get(nb, float("inf")):
                    dist[nb] = nd
                    prev[nb] = u
                    heapq.heappush(heap, (nd, nb))

        if end not in dist:
            return None, []

        path: List[Hashable] = []
        cur: Optional[Hashable] = end
        while cur is not None:
            path.append(cur)
            cur = prev[cur]
        path.reverse()
        return dist[end], path

    # ------------------------------------------------------------------
    # Cycle detection
    # ------------------------------------------------------------------

    def has_cycle(self) -> bool:
        """Return True if the graph contains a cycle.

        For directed graphs uses DFS with white/gray/black colouring.
        For undirected graphs uses DFS parent tracking.

        Time complexity: O(V + E)
        """
        if self._directed:
            return self._has_cycle_directed()
        return self._has_cycle_undirected()

    def _has_cycle_directed(self) -> bool:
        WHITE, GRAY, BLACK = 0, 1, 2
        colour: Dict[Hashable, int] = {v: WHITE for v in self._vertices}

        def _dfs(v: Hashable) -> bool:
            colour[v] = GRAY
            for nb, _ in self._adj[v]:
                if colour[nb] == GRAY:
                    return True
                if colour[nb] == WHITE and _dfs(nb):
                    return True
            colour[v] = BLACK
            return False

        return any(colour[v] == WHITE and _dfs(v) for v in self._vertices)

    def _has_cycle_undirected(self) -> bool:
        visited: Set[Hashable] = set()

        def _dfs(v: Hashable, parent: Optional[Hashable]) -> bool:
            visited.add(v)
            for nb, _ in self._adj[v]:
                if nb not in visited:
                    if _dfs(nb, v):
                        return True
                elif nb != parent:
                    return True
            return False

        return any(v not in visited and _dfs(v, None) for v in self._vertices)

    # ------------------------------------------------------------------
    # Topological sort (Kahn's algorithm)
    # ------------------------------------------------------------------

    def topological_sort(self) -> List[Hashable]:
        """Return a topological ordering of vertices.

        Only valid for directed acyclic graphs (DAGs).

        Time complexity: O(V + E)
        Raises ValueError if the graph has a cycle.
        """
        if not self._directed:
            raise TypeError("topological sort is only defined for directed graphs")

        in_degree: Dict[Hashable, int] = {v: 0 for v in self._vertices}
        for u in self._vertices:
            for nb, _ in self._adj[u]:
                in_degree[nb] = in_degree.get(nb, 0) + 1

        queue: deque[Hashable] = deque(v for v in self._vertices if in_degree[v] == 0)
        order: List[Hashable] = []

        while queue:
            v = queue.popleft()
            order.append(v)
            for nb, _ in self._adj[v]:
                in_degree[nb] -= 1
                if in_degree[nb] == 0:
                    queue.append(nb)

        if len(order) != len(self._vertices):
            raise ValueError("graph contains a cycle — topological sort is not possible")
        return order

    # ------------------------------------------------------------------
    # Representation
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        kind = "Directed" if self._directed else "Undirected"
        return (
            f"Graph({kind}, vertices={len(self._vertices)}, "
            f"edges={sum(len(e) for e in self._adj.values())})"
        )


if __name__ == "__main__":
    # --- Directed graph demo ---
    g = Graph(directed=True)
    for u, v, w in [("A", "B", 4), ("A", "C", 2), ("B", "D", 3),
                     ("C", "B", 1), ("C", "D", 5), ("D", "E", 1)]:
        g.add_edge(u, v, w)

    print(g)
    print("BFS from A:", g.bfs("A"))
    print("DFS from A:", g.dfs("A"))

    dist, path = g.shortest_path("A", "E")
    print(f"Shortest A->E: distance={dist}, path={path}")

    print("Has cycle:", g.has_cycle())
    print("Topological sort:", g.topological_sort())

    # --- Undirected graph demo ---
    ug = Graph(directed=False)
    for u, v in [("X", "Y"), ("Y", "Z"), ("Z", "X")]:
        ug.add_edge(u, v)
    print(f"\n{ug}")
    print("Has cycle:", ug.has_cycle())

    # Remove edge and re-check
    ug.remove_edge("Z", "X")
    print("After removing Z-X, has cycle:", ug.has_cycle())
