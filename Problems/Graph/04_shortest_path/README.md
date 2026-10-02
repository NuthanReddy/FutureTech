# Shortest Paths and Ratio Queries

## Pattern identification steps

1. Identify equal-cost moves, a bounded flight count, or repeated multiplicative ratio queries.
2. Choose BFS for moves, copy-based Bellman-Ford rounds for the edge budget, and weighted Union-Find for consistent ratios.
3. Preserve first-discovery BFS distance, previous-round-only costs, or x/parent[x] weights.
4. Check limits: k stops allow k+1 edges; each landing uses one board jump; ratio queries assume consistent equations and are not shortest paths.

Per-problem cues and choices: [parent identification map](../README.md#identification-map).
