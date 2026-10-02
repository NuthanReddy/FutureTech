# Graph and Grid Traversal

## Pattern identification steps

1. Recognize connected regions, cyclic copying, boundary reachability, or simultaneous spread.
2. Choose flood-fill BFS, identity-map BFS, reverse-border DFS, or multi-source BFS respectively.
3. Mark on discovery; preserve one clone per identity and one minute per infection layer.
4. Check limits: grids use four neighbors; flood fills may mutate input; reachability DFS does not minimize distance.

Per-problem cues and choices: [parent identification map](../README.md#identification-map).
