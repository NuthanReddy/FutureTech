# Sorted Set

Dynamic ordering, neighbor lookup, and duplicate-safe window membership.

## Pattern identification steps

1. Look for predecessor/successor checks or sorted access during insertions and removals.
2. Store intervals by start; use (value, index) keys when equal values must remain distinct.
3. Maintain disjoint half-open bookings or exactly the current window's keys.
4. Check the API's rank support: this median implementation materializes the window, costing O(k) per read.
5. Prefer a hash set for membership only; use two heaps or an order-statistic tree for faster medians.

## Problem cues → patterns

| Problem | Identification cue → pattern |
| --- | --- |
| [My Calendar I](my_calendar.py) | Reject online overlaps → ordered intervals + predecessor/successor checks; touching endpoints are allowed. |
| [Sliding window median](sliding_window_median.py) | Add/remove duplicates while reading middle values → unique (value, index) keys + sorted window traversal. |
