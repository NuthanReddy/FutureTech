# Union-Find Problems

Use disjoint sets when undirected connectivity matters but explicit routes do not.

## Pattern identification steps

1. Recognize component counting or an edge joining vertices already connected.
2. Choose a component counter for a symmetric matrix, or failed-union detection for a tree plus one edge.
3. Merge only distinct roots; decrease counts only on a successful merge.
4. Check limits: ordinary Union-Find cannot handle directed reachability or deletions; the redundant-edge solution assumes one extra edge and labels 1..n.

| Problem / file | Identification cue | Chosen pattern |
| --- | --- | --- |
| Provinces — `number_of_provinces.py` | Groups in a symmetric city adjacency matrix | Upper-triangle scan + size-weighted Union-Find |
| Redundant Connection — `redundant_connection.py` | Undirected tree with one added edge | Input-order Union-Find; first failed union closes the sole cycle |
