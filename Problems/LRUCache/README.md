# Cache eviction patterns

## Pattern identification steps

1. Read the eviction cue: least recent access, or least frequent access with recency ties.
2. Choose a key map plus one recency list for LRU, or per-frequency recency lists for LFU.
3. Keep one node per key; refresh recency on access/update and keep LFU's minimum live frequency correct.
4. Check capacity/miss contracts: the LRU adapter requires positive capacity and returns -1 on misses; LFU stores nothing at nonpositive capacity. Neither policy provides TTL or thread safety.

## Per-problem recognition

| Source | Recognition cue | Chosen pattern / invariant |
|---|---|---|
| [LRU Cache](lru_cache_problem.py) (adapter) | O(1) access and least-recent eviction | Reuse `DataStructures.LRUCache`; key map and one doubly linked recency list stay synchronized; translate `KeyError` to -1 |
| [LFU Cache](lfu_cache.py) | Least frequent eviction, LRU tie-break | Key map + frequency-indexed lists; evict the LRU node in the minimum live frequency bucket |

`process_operations` in the LRU adapter is an operation-sequence driver, not
a separate algorithm. Both caches have O(1) average get/put; LRU uses
O(capacity) space. LFU's live nodes are capacity-bounded, but this implementation
retains empty frequency lists, so metadata can grow with accesses.
