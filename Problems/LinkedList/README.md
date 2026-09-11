# LeetCode Top Interview 150: Linked List

This directory implements the **exact eleven-problem Linked List section** as
small runnable modules grouped by the pointer invariant or supporting data
structure that makes each solution work.

Run any example from the repository root with module syntax:

```powershell
python -m Problems.LinkedList.in_place_reversal.reverse_nodes_in_k_group
```

Shared `ListNode`, `RandomListNode`, construction, and serialization helpers
live in `helpers.py`. The solution modules expose readable snake-case
functions plus LeetCode-compatible `Solution` methods where LeetCode expects
that interface.

## Problem statements

| Problem | Input | Required output | Key constraints |
|---|---|---|---|
| Add Two Numbers | Two non-empty reverse-order decimal-digit lists | A new reverse-order digit list for their sum | Digits 0-9; at most 100 nodes per list |
| Merge Two Sorted Lists | Two nondecreasing singly linked lists | One nondecreasing list containing all input nodes | Each list may be empty and has at most 50 nodes |
| Copy List with Random Pointer | A list whose `random` pointers are null or point inside the list | A deep copy preserving `next` and `random` edges | 0-1,000 nodes; do not share nodes with source |
| Reverse Linked List II | List head and one-based inclusive `left`, `right` positions | The same list with only that range reversed | 1-500 nodes; `1 <= left <= right <= n` |
| Reverse Nodes in k-Group | List head and positive `k` | Reverse complete groups of `k`; retain a short final group | 1-5,000 nodes; `1 <= k <= n` |
| Linked List Cycle | A list that may point back to an earlier node | `True` if its `next` edges form a cycle, otherwise `False` | 0-10,000 nodes; input must not be modified |
| Remove Nth Node From End | List head and positive `n` | The list without the `n`th node from the end | 1-30 nodes; `1 <= n <= nodelist length` |
| Rotate List | List head and non-negative `k` | List rotated right by `k` positions | 0-500 nodes; `0 <= k <= 2 * 10^9` |
| Remove Duplicates from Sorted List II | Nondecreasing singly linked list | List retaining only values that occur exactly once | 0-300 nodes; all duplicate runs are removed |
| Partition List | List head and pivot `x` | Stable partition: values below `x` before all others | 0-200 nodes; preserve order within each side |
| LRU Cache | Positive capacity and `get`/`put` operations | Lookup or `-1`; insert/update with LRU eviction | Capacity 1-3,000; up to 200,000 operations; O(1) average operations |

## Pattern map

| Group | Problems | Core invariant | Time | Extra space |
|---|---|---|---:|---:|
| `arithmetic_and_merge` | Add Two Numbers; Merge Two Sorted Lists | A sentinel owns the result head while a tail advances | O(n) | O(n) result / O(1) merge |
| `in_place_reversal` | Reverse Linked List II; Reverse Nodes in k-Group | Save the next edge before rewiring the current edge | O(n) | O(1) |
| `fast_slow_pointers` | Linked List Cycle; Remove Nth Node From End; Rotate List | Compare pointer speeds or convert endpoint distance into a gap/cut point | O(n) | O(1) |
| `filtering_and_partitioning` | Remove Duplicates from Sorted List II; Partition List | Sentinels make head deletion and output-chain assembly uniform | O(n) | O(1) |
| `hash_map_cloning` | Copy List with Random Pointer | Original-node identity maps to clone identity | O(n) | O(n) |
| `cache_design` | LRU Cache | Hash lookup plus a doubly linked recency order | O(1) per operation | O(capacity) |

## Critique and comparison

### Arithmetic and merge

**Add Two Numbers** is structurally a merge, but its progress is driven by
carry rather than ordering. Allocating a new result is the correct default:
reusing input nodes would save allocations but makes carry handling harder to
audit and unexpectedly destroys an operand.

**Merge Two Sorted Lists** safely reuses nodes because each chosen node is
already final in the output order. The `<=` tie rule also makes the merge
stable with respect to the first list. A copying version is preferable only
when callers require immutable inputs.

### In-place reversal

**Reverse Linked List II** uses head insertion inside one bounded range. It is
compact because the predecessor stays fixed, but position validation matters:
invalid bounds are rejected before any rewiring occurs.

**Reverse Nodes in k-Group** is the more error-prone generalization. Looking
ahead to find the kth node before reversal is essential; otherwise an
incomplete final group may be accidentally reversed. A stack-based alternative
is easier to visualize but uses O(k) extra space and does not practice the
pointer-reversal invariant.

### Fast/slow distance transformations

**Linked List Cycle** is the foundational fast/slow-pointer problem. Floyd's
algorithm detects a cycle without retaining every node identity: once both
pointers enter a cycle, their speed difference guarantees a meeting. A set of
visited nodes is a good debugging alternative, but it uses O(n) memory.

**Remove Nth Node From End** keeps an `n`-node gap so a single traversal can
locate the predecessor of the target. Computing the full length first is also
valid, but the gap technique generalizes better to streaming pointer problems.

**Rotate List** first measures length, then forms a temporary cycle and cuts it.
Unlike removal, length is required because `k` must be reduced modulo `n`.
Array conversion is simpler but sacrifices O(1) auxiliary space and node
identity.

### Filtering and stable partitioning

**Remove Duplicates from Sorted List II** removes entire equal-value runs, not
just repeated nodes. Sorted order is the enabling constraint: duplicates are
contiguous, so no frequency map is needed.

**Partition List** is not an unstable quicksort partition. Two chains preserve
relative order on both sides of `x`. Detaching each visited node prevents stale
links from creating accidental cycles when the chains are joined.

### Identity-map cloning

**Copy List with Random Pointer** is a graph clone disguised as a list problem.
The dictionary solution is explicit and safe. The interleaving-node alternative
achieves O(1) auxiliary space, but temporarily mutates the source and requires
three delicate passes; it is worthwhile only when memory is the dominant
constraint.

### Linked structure design

**LRU Cache** combines two structures because neither is sufficient alone: a
dictionary has no recency order, and a linked list cannot find arbitrary keys
in O(1). Sentinel endpoints remove empty-list branches. Python's
`collections.OrderedDict` is a strong production alternative, but implementing
the list directly exposes the interview invariant and matches the intended
design exercise.

## Edge cases emphasized

- Empty and single-node lists.
- Empty lists, self-cycles, and cycles that begin after an acyclic prefix.
- Carry creating a new most-significant digit.
- Reversal ranges touching the head or leaving a short final group.
- Removing the original head.
- Rotation counts larger than the list length.
- Duplicate runs at either boundary or spanning the entire list.
- Duplicate values in a random-pointer list, proving identity rather than value
  is used for cloning.
- LRU capacity one, updates, reads that refresh recency, and invalid capacity.
