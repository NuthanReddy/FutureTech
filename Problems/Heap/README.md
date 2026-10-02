# Heap pattern recognition

## Pattern identification steps

1. Look for repeated extrema, an online median, or the next item across sorted sequences.
2. Choose a frontier min-heap, balanced dual heaps, or lazy extrema heaps backed by an authoritative map.
3. Maintain the invariant: one candidate per sequence, ordered/balanced halves, or a current valid root.
4. Check limits: merging needs sorted inputs; empty median/price queries fail; corrections retain stale entries. Bounded frequencies favor buckets, not heaps.

## Per-problem recognition

| Source | Recognition cue | Chosen pattern / invariant |
|---|---|---|
| [Merge K Sorted Lists](merge_k_sorted_lists.py) | k ascending integer sequences | Min-heap frontier; one next candidate per active sequence |
| [Find Median from Stream](find_median_from_stream.py) | Median after each insertion | Lower max-heap + upper min-heap; lower values <= upper values, sizes equal or lower one larger |
| [Stock Price Fluctuation](stock_price_fluctuation.py) | Timestamp corrections plus min/max queries | Authoritative timestamp map + lazy dual heaps; queried roots match current records |
| [Top K Frequent Elements](top_k_frequent_elements.py) (legacy import) | Rank values by counts bounded by input length | Reuse `Problems.ArraysHashing.Frequency.top_k_frequent_elements`; bucket c holds frequency-c values, scanned high-to-low |

Merge: O(N log k) time, O(k) heap space plus O(N) output. Median:
O(log n) insertion, O(1) lookup, O(n) space. Stock updates cost O(log u)
for u total updates; lazy pops are charged to updates, but one query may
discard many entries and retained storage can grow with u. Frequency buckets:
O(n) time/space. The merge implementation accepts Python lists, not linked nodes.
