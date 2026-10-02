# Undirected Connectivity

## Pattern identification steps

1. Recognize a group count or a connected, acyclic tree test without needing a route.
2. Choose a Union-Find counter for groups; add the n-1 edge check for trees.
3. Each accepted union joins distinct roots; a same-root edge closes a cycle.
4. Check limits: include isolated vertices; use zero-based labels; this pattern does not solve directed cycles or edge deletion.

Per-problem cues and choices: [parent identification map](../README.md#identification-map).
