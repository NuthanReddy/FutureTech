# Graph problems — Top Interview 150 patterns

This folder groups graph exercises by the invariant that makes the solution
simple, rather than by the order in which they appear on a problem list.

| Pattern | Problems in this folder | Why this approach wins |
| --- | --- | --- |
| Grid traversal | Islands, Surrounded Regions, Pacific Atlantic, Rotting Oranges | Each cell is visited once; BFS/DFS state is the component or frontier. |
| Graph traversal | Clone Graph | A `visited` map preserves one clone per original node and terminates cycles. |
| Union-Find | Graph Valid Tree, Connected Components | Connectivity queries are incremental and do not need explicit traversal paths. |
| Topological sort | Course Schedule I/II | A cycle is exactly the condition that prevents consuming every vertex. |
| Weighted shortest path | Evaluate Division, Network Delay Time | Dijkstra or multiplicative path accumulation matches edge semantics. |
| Unweighted shortest path | Word Ladder, Snakes and Ladders | Every move costs one, so BFS gives the first shortest answer. |
| Relaxation / bounded shortest path | Cheapest Flights Within K Stops | Bellman-Ford-style rounds enforce the stop limit; Dijkstra alone does not. |
| Minimum spanning tree | Min Cost to Connect All Points | Prim grows the cheapest cut edge and never needs all pair paths. |

## Problem statements

Every solution module in the grouped folders repeats its statement so it can
be studied independently. The input, required output, and principal
constraints are summarized here as well.

### Grid traversal

- **Number of Islands (`01_traversal/number_of_islands.py`)** — Input: a
  rectangular `0/1` grid. Output: the number of four-directionally connected
  land components. Constraint: diagonal cells do not connect.
- **Surrounded Regions (`01_traversal/surrounded_regions.py`)** — Input: an
  `X/O` board. Output: mutate it by changing every non-border-connected `O`
  region to `X`; return `None`. Constraint: adjacency is four-directional.
- **Pacific Atlantic Water Flow
  (`01_traversal/pacific_atlantic_water_flow.py`)** — Input: a rectangular
  height matrix. Output: coordinates that can flow to both the top/left and
  bottom/right borders. Constraint: flow moves orthogonally to equal/lower
  height.
- **Rotting Oranges (`01_traversal/rotting_oranges.py`)** — Input: a grid
  where `0` is empty, `1` fresh, and `2` rotten. Output: minimum minutes to
  rot all reachable fresh oranges, or `-1`. Constraint: infection is
  orthogonal and simultaneous per minute.

### Graph traversal and cloning

- **Clone Graph (`01_traversal/clone_graph.py`)** — Input: a node in a
  connected undirected graph, possibly cyclic. Output: a deep-copy start node
  or `None`. Constraint: each original identity must map to exactly one clone.

### Union-Find

- **Graph Valid Tree (`02_union_find/graph_valid_tree.py`)** — Input: `n`
  zero-based vertices and undirected edges. Output: whether the graph is one
  connected acyclic tree. Constraint: a tree has exactly `n-1` edges.
- **Connected Components (`02_union_find/number_connected_components.py`)** —
  Input: `n` vertices and undirected edges. Output: the number of maximal
  reachable groups. Constraint: isolated vertices count as components.

### Shortest paths

- **Word Ladder (`04_shortest_path/word_ladder.py`)** — Input: equal-length
  `begin_word`, `end_word`, and a dictionary. Output: shortest sequence length
  including both endpoints, or `0`. Constraint: one lowercase character
  changes per step and intermediate words must be in the dictionary.
- **Snakes and Ladders (`04_shortest_path/snakes_and_ladders.py`)** — Input:
  an `n x n` boustrophedon board with `-1` or a destination square. Output:
  minimum dice throws from square `1` to `n*n`, or `-1`. Constraint: each
  throw advances 1 through 6 and a snake/ladder is taken immediately.
- **Evaluate Division (`04_shortest_path/evaluate_division.py`)** — Input:
  equations `a / b = value` and variable queries. Output: one quotient per
  query, or `-1.0` for unknown/disconnected variables. Constraint: equation
  values are positive and variables may form cycles.
- **Cheapest Flights Within K Stops
  (`04_shortest_path/cheapest_flights_k_stops.py`)** — Input: directed
  `[from, to, price]` flights, source, destination, and `k`. Output: cheapest
  permitted price, or `-1`. Constraint: at most `k+1` edges may be used and
  prices are non-negative.

### Minimum spanning tree

- **Min Cost to Connect All Points (`05_mst/min_cost_connect_points.py`)** —
  Input: planar points. Output: minimum total Manhattan-distance cost to
  connect every point. Constraint: the answer is an MST cost; zero or one
  point costs `0`.

## BFS, DFS, Union-Find, and shortest paths

* **BFS** is the default for an unweighted shortest path or a wave spreading
  one layer at a time. Mark a node when enqueuing it, not when dequeuing it.
* **DFS** is usually clearer for reachability, flood fill, and component
  marking. It does not by itself guarantee a shortest path.
* **Union-Find** is ideal when edges arrive incrementally and the question is
  “are these vertices already connected?”. It does not produce a route.
* **Topological sort** applies only to directed dependency graphs. Kahn's
  indegree invariant and DFS's three-color invariant are equivalent cycle
  tests; Kahn's order is directly usable.
* **Dijkstra** requires non-negative weights. For a bounded number of edges,
  use round-based relaxation (Cheapest Flights). For a negative-edge graph,
  use Bellman-Ford instead.

The older flat files (`course_schedule.py`, `network_delay_time.py`, and
`shortest_path_in_maze.py`) remain import-compatible. Their behavior is
documented here so the grouped solutions can be compared without silently
replacing existing learning examples. The related `Problems/UnionFind`
implementations (`number_of_provinces.py` and `redundant_connection.py`) are
also intentionally retained: they demonstrate the same DS with an adjacency
matrix and with 1-indexed input, respectively. The new grouped files use
zero-indexed edge lists and focus on the graph-pattern decision.
