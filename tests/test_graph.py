import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.Graph import Graph


class TestGraphBasic:
    def test_add_vertex(self) -> None:
        g = Graph()
        g.add_vertex("A")
        g.add_vertex("B")
        assert "Graph" in repr(g)

    def test_add_edge_directed(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B", 2.0)
        assert g.bfs("A") == ["A", "B"]
        # B -> A doesn't exist in directed graph
        assert g.bfs("B") == ["B"]

    def test_add_edge_undirected(self) -> None:
        g = Graph(directed=False)
        g.add_edge("A", "B", 1.0)
        assert "B" in g.bfs("A")
        assert "A" in g.bfs("B")

    def test_remove_vertex(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.add_edge("B", "C")
        g.remove_vertex("B")
        assert g.bfs("A") == ["A"]

    def test_remove_vertex_missing_raises(self) -> None:
        g = Graph()
        with pytest.raises(KeyError):
            g.remove_vertex("Z")

    def test_remove_edge(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.remove_edge("A", "B")
        assert g.bfs("A") == ["A"]

    def test_remove_edge_missing_raises(self) -> None:
        g = Graph(directed=True)
        g.add_vertex("A")
        with pytest.raises(KeyError):
            g.remove_edge("A", "B")


class TestGraphTraversal:
    def test_bfs(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.add_edge("A", "C")
        g.add_edge("B", "D")
        result = g.bfs("A")
        assert result[0] == "A"
        assert set(result) == {"A", "B", "C", "D"}

    def test_dfs(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.add_edge("A", "C")
        g.add_edge("B", "D")
        result = g.dfs("A")
        assert result[0] == "A"
        assert set(result) == {"A", "B", "C", "D"}


class TestShortestPath:
    def test_dijkstra(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B", 1)
        g.add_edge("B", "C", 2)
        g.add_edge("A", "C", 10)
        cost, path = g.shortest_path("A", "C")
        assert cost == 3
        assert path == ["A", "B", "C"]

    def test_dijkstra_no_path(self) -> None:
        g = Graph(directed=True)
        g.add_vertex("A")
        g.add_vertex("B")
        cost, path = g.shortest_path("A", "B")
        assert cost is None
        assert path == []

    def test_dijkstra_same_node(self) -> None:
        g = Graph(directed=True)
        g.add_vertex("A")
        cost, path = g.shortest_path("A", "A")
        assert cost == 0
        assert path == ["A"]


class TestCycleDetection:
    def test_has_cycle_directed(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.add_edge("B", "C")
        g.add_edge("C", "A")
        assert g.has_cycle() is True

    def test_no_cycle_directed(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.add_edge("B", "C")
        assert g.has_cycle() is False

    def test_has_cycle_undirected(self) -> None:
        g = Graph(directed=False)
        g.add_edge("A", "B")
        g.add_edge("B", "C")
        g.add_edge("C", "A")
        assert g.has_cycle() is True

    def test_no_cycle_undirected(self) -> None:
        g = Graph(directed=False)
        g.add_edge("A", "B")
        g.add_edge("B", "C")
        assert g.has_cycle() is False


class TestTopologicalSort:
    def test_topological_sort_dag(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.add_edge("A", "C")
        g.add_edge("B", "D")
        g.add_edge("C", "D")
        result = g.topological_sort()
        # A must come before B and C; B and C must come before D
        assert result.index("A") < result.index("B")
        assert result.index("A") < result.index("C")
        assert result.index("B") < result.index("D")
        assert result.index("C") < result.index("D")

    def test_topological_sort_cyclic_raises(self) -> None:
        g = Graph(directed=True)
        g.add_edge("A", "B")
        g.add_edge("B", "A")
        with pytest.raises(ValueError):
            g.topological_sort()

    def test_topological_sort_undirected_raises(self) -> None:
        g = Graph(directed=False)
        g.add_edge("A", "B")
        with pytest.raises(TypeError):
            g.topological_sort()
