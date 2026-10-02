# Intervals

## Pattern identification steps

1. Decide whether the input describes consecutive runs, covered ranges, or competing schedules.
2. Choose run compression, start-sorted merging/insertion, end-sorted selection, or a room-end heap.
3. Prove the maintained result: completed union, earliest compatible finish, or one end time per allocated room.
4. Fix endpoint semantics: merge touching coverage, but allow scheduling reuse at equal endpoints.
5. Check ordering/disjointness preconditions; weighted selection needs more than earliest-finish greedy.

## Implemented problem recognition map

| Problem | Recognition cue -> chosen pattern |
| --- | --- |
| Summary Ranges | Sorted distinct consecutive integers -> maximal-run compression |
| Merge Intervals | Union of arbitrary overlapping ranges -> sort starts and coalesce |
| Insert Interval | One addition to sorted disjoint ranges -> before/overlap/after phases |
| Non-overlapping Intervals | Fewest removals for compatible intervals -> earliest-finish greedy |
| Meeting Rooms II | Reuse rooms as meetings finish -> start-sorted scan + minimum end-time heap |

These solutions cover the interval-shaped Top Interview 150 exercises:

| Problem | Problem statement (input → required output) | Pattern | Time | Extra space |
| --- | --- | ---: | ---: |
| Summary Ranges | Sorted distinct integers → maximal consecutive ranges as strings | run compression | O(n) | O(n) output |
| Merge Intervals | Arbitrary intervals → disjoint union after merging overlaps | sort then coalesce | O(n log n) | O(n) |
| Insert Interval | Sorted disjoint intervals plus one interval → sorted disjoint result | three linear phases | O(n) | O(n) |
| Non-overlapping Intervals | Intervals → minimum number to remove for no overlap | earliest finish greedy | O(n log n) | O(n) |
| Meeting Rooms II | Meeting start/end pairs → minimum simultaneous rooms | start-sorted end-time heap | O(n log n) | O(n) |

## Inputs and constraints

Each interval is `[start, end]` with `start <= end`. `summary_ranges` expects
sorted distinct integers. `insert` additionally expects sorted,
pairwise-disjoint intervals. The other interval functions accept arbitrary
interval ordering. Outputs are either interval/string lists or an integer
count. The usual interview constraint is `0 <= n` with integer endpoints.

The frequent mistake is mixing two boundary conventions.  Merging uses
`start <= end` because touching coverage is continuous; meeting scheduling uses
`start < end` because a room is reusable at the exact finishing time.
