# Skip List

Dynamic ordered keys with expected logarithmic lookup and updates.

## Pattern identification steps

1. Look for ordered pending items or a sorted collection with frequent insertions/deletions.
2. Key pending values by ID; store occurrence counts when collection values repeat.
3. Keep the stream pointer at the next unflushed ID and retain only positive occurrence counts.
4. Check aggregation support: this range-sum implementation scans from the smallest key, worst-case O(d) for d distinct keys.
5. Prefer an array for bounded dense stream IDs; use an augmented tree for frequent fast range sums.

## Problem cues → patterns

| Problem | Identification cue → pattern |
| --- | --- |
| [Ordered stream](design_ordered_stream.py) | Out-of-order arrivals, consecutive output → pending ID map + advancing read pointer. |
| [Dynamic range sum](range_sum_sorted_list.py) | Insert/delete repeated values and sum by value bounds → ordered value/count map + scan. |
