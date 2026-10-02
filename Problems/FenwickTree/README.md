# Fenwick Tree

Mutable prefix sums and compressed-value frequency counts.

## Pattern identification steps

1. Look for interleaved point changes and sum/count queries.
2. Use index-based sums for arrays; compress values to ranks for frequency queries.
3. Keep 1-based tree indices and propagate deltas; derive ranges by subtracting prefixes.
4. Prefer plain prefix sums for static data; use a segment tree for arbitrary range minima.

## Problem cues → patterns

| Problem | Identification cue → pattern |
| --- | --- |
| [Count inversions](count_inversions.py) | Smaller values to the right → reverse scan + compressed frequency BIT; query rank − 1 before insertion. |
| [Mutable range sum](range_sum_query_mutable.py) | Assignments mixed with interval sums → delta updates + prefix subtraction. |

The current mutable-sum constructor builds by repeated updates: O(n log n).
