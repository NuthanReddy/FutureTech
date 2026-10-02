# Minimum Spanning Tree

## Pattern identification steps

1. Recognize minimum total cost to connect all points, not minimum source-to-target distance.
2. Choose dense Prim for the implicit complete Manhattan-distance graph.
3. Maintain best[v] as the cheapest edge from the selected tree to unused v.
4. Check limits: this implementation is O(n²); zero or one point costs zero, and arbitrary disconnected graphs need separate handling.

Per-problem cue and choice: [parent identification map](../README.md#identification-map).
