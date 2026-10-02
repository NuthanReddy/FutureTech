# Segment Tree

Range aggregates with point updates.

## Pattern identification steps

1. Look for repeated interval aggregates while individual values change.
2. Choose the merge and identity: min/+∞ for minima, sum/0 for frequencies.
3. Keep each node equal to its children's merged aggregate after every update.
4. Compress values for rank queries; exclude the current rank when counting strictly smaller values.
5. Prefer prefix sums for static sums or a Fenwick tree for point-update sums/counts.

## Problem cues → patterns

| Problem | Identification cue → pattern |
| --- | --- |
| [Smaller after self](count_of_smaller_after_self.py) | Per-element smaller suffix counts → reverse scan + compressed frequency segment tree. |
| [Range minimum](range_minimum_query.py) | Interval minima with replacements → min segment tree + point updates. |
