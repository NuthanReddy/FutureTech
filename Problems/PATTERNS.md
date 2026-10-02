# Pattern identification reference

All categories in this repository, their main subpatterns, and representative
LeetCode practice problems. Examples are not an exhaustive LeetCode inventory
or a claim that every example is implemented here. Patterns overlap: one problem
may use hashing + sliding window, or binary search + greedy.

## Identification steps

1. **Output:** existence, count, optimum, all solutions, or repeated queries?
2. **Structure:** contiguous range, sorted sequence, links, tree, graph, or rows?
3. **Constraints:** can you enumerate, sort, store states, or scan only once?
4. **Choice:** match a cue below; define the smallest state and its invariant.
5. **Proof:** check assumptions, correctness, and time/space before committing.

## Decision tree

Follow the relevant branch, then use the tables to choose its exact variant.
If several branches apply, combine them; a cue is not a correctness proof.

```text
START: what must the result preserve or optimize?
|
+-- Relational rows? -> SQL
|   +-- Collapse groups -> aggregation / joins
|   +-- Compare/rank ordered rows -> window functions
|   +-- Missing matches -> anti-join; consecutive runs -> gaps and islands
|
+-- Enumerate choices? -> backtracking
|   +-- Repeated equivalent states, only count/best/existence needed -> DP
|
+-- Repeated updates/queries or streaming data?
|   +-- Extremes / top-k / median -> heap(s)
|   +-- Predecessor, successor, ordered insertion -> ordered set / skip list
|   +-- Point updates + prefix sums -> Fenwick tree
|   +-- General associative range aggregate / range updates -> segment tree
|   +-- Eviction policy -> hash map + linked order / frequency buckets
|   +-- Word prefixes -> trie; approximate membership allowed -> Bloom filter
|
+-- Linked nodes? -> dummy-node rewiring / fast-slow / reversal / cloning
+-- Tree?
|   +-- Ancestor/subtree result -> DFS / postorder
|   +-- Result by depth -> BFS
|   +-- BST ordering available -> inorder / bound propagation
+-- Graph or grid movement?
|   +-- Reachability / components -> DFS/BFS or Union-Find
|   +-- Prerequisites -> topological sort
|   +-- Shortest route -> BFS (unit), Dijkstra (nonnegative), relaxation (bounded)
|   +-- Connect everything at minimum cost -> minimum spanning tree
|
+-- Array/string/ranges?
|   +-- Contiguous?
|   |   +-- Fixed length -> rolling window; extrema -> monotonic deque
|   |   +-- Shrinking restores feasibility -> variable sliding window
|   |   +-- Sum/count relation, negatives possible -> prefix sums + hashing
|   +-- Sorted/orderable?
|   |   +-- Discard half via ordered predicate / structural proof -> binary search
|   |   +-- Compare ends / merge / compact -> two pointers
|   |   +-- Overlap / active events -> interval merge / sweep line
|   +-- Lookup, frequency, equivalence -> hash map/set / canonical key
|   +-- Nested syntax -> stack; next smaller/greater -> monotonic stack
|   +-- Palindrome substring -> center expansion; prefixes -> trie
|   +-- Matrix layout only -> boundaries / transpose / marker simulation
|
+-- Optimization/counting remains?
    +-- Local choice provably safe -> greedy
    +-- Repeated subproblems -> DP (choose state shape below)
    +-- Small search space, no reusable states -> backtracking/brute force
    +-- Binary representation/algebra -> bit operations / exponentiation
    +-- Explicit chronological rules -> event simulation
```

## Arrays, hashing, strings, and pointers

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Membership / complement lookup | Need repeats or a partner -> store processed values/indices -> query before inserting when indices must differ. | [217 Contains Duplicate][lc217], [1 Two Sum][lc1] |
| Frequency counting | Multiplicity matters -> count each value -> compare counts, not just membership. | [242 Valid Anagram][lc242], [383 Ransom Note][lc383] |
| Canonical-key grouping | Equivalent items must share a group -> compute a signature -> equal signatures mean the same equivalence class. | [49 Group Anagrams][lc49] |
| Frequency buckets | Need top frequencies in a bounded batch -> count -> bucket by count and scan downward. | [347 Top K Frequent Elements][lc347] |
| Prefix/suffix accumulation | Each answer excludes its own element -> combine left/right aggregates -> keep the current element out of both. | [238 Product of Array Except Self][lc238] |
| Prefix sum + hash map/set | Subarray sums/counts, including negatives -> rewrite using prefix differences -> store prior prefixes with the required counts/indices. | [560 Subarray Sum Equals K][lc560], [974 Subarray Sums Divisible by K][lc974] |
| Sequence-start set scanning | Unsorted consecutive values -> deduplicate -> extend only values without predecessors. | [128 Longest Consecutive Sequence][lc128] |
| Region validation | Independent row/column/box uniqueness -> maintain one set per region -> reject repeated nonempty values. | [36 Valid Sudoku][lc36] |
| Opposing two pointers | Sorted pair or symmetric comparison -> examine both ends -> move only the end ruled out by the comparison. | [167 Two Sum II][lc167], [125 Valid Palindrome][lc125] |
| Subsequence matching pointers | Test ordered inclusion, not an optimum -> advance the candidate only on a match -> consumed characters form a matched prefix. | [392 Is Subsequence][lc392] |
| Fixed choice + two pointers | Sorted triple target -> fix one value, solve the remaining pair -> skip duplicates without losing unique answers. | [15 3Sum][lc15] |
| Read/write compaction | Filter or deduplicate in place -> scan with read pointer -> write only accepted items into the final prefix. | [26 Remove Duplicates from Sorted Array][lc26], [27 Remove Element][lc27] |
| Sorted merging | Two ordered streams -> consume the smaller head -> write backward when the destination overlaps unread input. | [88 Merge Sorted Array][lc88], [21 Merge Two Sorted Lists][lc21] |
| Boundary-limited two pointers | Water depends on the weaker boundary -> advance that side -> maintain boundary maxima if accumulating trapped water. | [11 Container With Most Water][lc11], [42 Trapping Rain Water][lc42] |
| Center expansion | Longest contiguous palindrome -> enumerate odd/even centers -> expand while mirrored characters match; not subsequence DP. | [5 Longest Palindromic Substring][lc5], [647 Palindromic Substrings][lc647] |
| Run-length scanning | Output depends on consecutive equal items -> consume a maximal run -> emit its length/value once. | [38 Count and Say][lc38], [443 String Compression][lc443] |

## Matrix layout and simulation

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Shrinking boundaries | Visit a rectangular perimeter in order -> maintain four bounds -> guard crossed bounds before traversing the opposite edge. | [54 Spiral Matrix][lc54] |
| Index transformation | Square matrix rotation -> transpose and reverse, or cycle four coordinates -> move each value to its mapped destination. | [48 Rotate Image][lc48] |
| In-place marker storage | Entire rows/columns depend on original flags -> save first-row/column flags -> mark first, rewrite second. | [73 Set Matrix Zeroes][lc73] |
| Simultaneous-state encoding | All cells update from the old board -> store old/new bits together -> read only old state until the final pass. | [289 Game of Life][lc289] |

## Windows, stacks, and queues

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Fixed-size window | Every range has length k -> add incoming/remove outgoing state -> evaluate only full windows. | [643 Maximum Average Subarray I][lc643], [567 Permutation in String][lc567] |
| Variable window: longest feasible | Contiguous range with repairable violations -> expand -> shrink/jump until valid, then score. | [3 Longest Substring Without Repeating Characters][lc3], [424 Longest Repeating Character Replacement][lc424] |
| Variable window: shortest covering | Need minimum coverage -> expand until covered -> score before shrinking away required data; sum shrinking needs nonnegative inputs. | [76 Minimum Window Substring][lc76], [209 Minimum Size Subarray Sum][lc209] |
| Monotonic deque | Need sliding extrema -> expire old indices -> discard dominated candidates from the back. | [239 Sliding Window Maximum][lc239], [1438 Longest Continuous Subarray With Absolute Diff Less Than or Equal to Limit][lc1438] |
| Parsing / evaluation stack | Nested delimiters or postfix expressions -> push unresolved work -> resolve on a matching close/operator. | [20 Valid Parentheses][lc20], [150 Evaluate Reverse Polish Notation][lc150], [224 Basic Calculator][lc224] |
| Stack with auxiliary aggregate | Stack queries need O(1) minimum -> maintain the minimum for each depth -> restore it on pop. | [155 Min Stack][lc155] |
| Monotonic stack | Next greater/smaller or maximal span -> keep unresolved indices ordered -> popping finalizes a boundary/answer. | [739 Daily Temperatures][lc739], [84 Largest Rectangle in Histogram][lc84] |
| Greedy ordered stack | Cars cannot pass -> sort by position -> compare arrival times against the fleet ahead. | [853 Car Fleet][lc853] |

## Search, greedy, intervals, and bits

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Binary search on position | Sorted or structurally ordered search -> identify which half can contain the target -> maintain a precise search interval. | [704 Binary Search][lc704], [33 Search in Rotated Sorted Array][lc33] |
| Lower/upper bounds | Need insertion position or duplicate range -> search first >= target / first > target -> keep the transition inside the interval. | [35 Search Insert Position][lc35], [34 Find First and Last Position of Element in Sorted Array][lc34] |
| Peak / rotated minimum search | Local slope or rotated sorted structure rules out a region -> retain a half guaranteed to contain an answer -> do not assume a globally monotone peak predicate. | [162 Find Peak Element][lc162], [153 Find Minimum in Rotated Sorted Array][lc153] |
| Binary search on answer | Feasibility changes once across candidate answers -> build a monotone predicate -> find its transition. | [875 Koko Eating Bananas][lc875], [1011 Capacity To Ship Packages Within D Days][lc1011] |
| Binary search on partition | Two sorted arrays, rank/median needed -> search the smaller split -> ensure both left maxima are <= both right minima. | [4 Median of Two Sorted Arrays][lc4] |
| Greedy feasible placement | Earliest valid action cannot reduce later capacity -> take it -> update constraints before the next action. | [605 Can Place Flowers][lc605] |
| Greedy farthest reach | Cover a line or advance with minimum jumps -> accumulate reachable endpoints -> commit the farthest extension at each boundary. | [45 Jump Game II][lc45], [1326 Minimum Number of Taps to Open to Water a Garden][lc1326] |
| Greedy exchange choice | Need optimal scheduling/assignment -> choose earliest finish or another exchange-proven priority -> preserve remaining options. | [435 Non-overlapping Intervals][lc435], [455 Assign Cookies][lc455] |
| Interval merging / insertion | Need union of coverage -> sort then coalesce, or use pre-overlap/overlap/post-overlap phases for sorted disjoint input. | [56 Merge Intervals][lc56], [57 Insert Interval][lc57] |
| Run compression of sorted values | Convert consecutive values into ranges -> extend while adjacent difference is one -> emit each maximal run. | [228 Summary Ranges][lc228] |
| Sweep line / active intervals | Need simultaneous count or resource usage -> order endpoints -> process equal-time ties according to overlap rules. | [253 Meeting Rooms II][lc253], [1094 Car Pooling][lc1094] |
| XOR cancellation | Values repeat with a known parity -> XOR cancels pairs -> verify the multiplicity assumption first. | [136 Single Number][lc136], [268 Missing Number][lc268] |
| Bit counting / bit DP | Need set-bit counts -> clear the lowest set bit, or reuse the shifted prefix count -> respect width for signed inputs. | [191 Number of 1 Bits][lc191], [338 Counting Bits][lc338] |
| Fixed-width bit transformations | Reverse/combine a binary representation -> process bit positions -> distinguish numeric value from fixed-width encoding. | [190 Reverse Bits][lc190], [201 Bitwise AND of Numbers Range][lc201] |
| Bitwise carry propagation | Addition without arithmetic operators -> XOR gives partial sum, shifted AND gives carry -> mask to the intended width. | [371 Sum of Two Integers][lc371] |
| Numeric digit extraction | Reverse decimal digits with overflow limits -> extract by remainder/division -> check bounds before appending each digit. | [7 Reverse Integer][lc7] |
| Exponentiation by squaring | Large exponent -> halve it repeatedly -> multiply on odd powers; handle negative exponents separately. | [50 Pow(x, n)][lc50] |

## Backtracking and dynamic programming

Choose **backtracking** to enumerate assignments. Choose **DP** when many
assignments reach the same sufficient state and only their aggregate is needed.
Choose **greedy** only when a local-choice proof eliminates other branches.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Subsets / combinations | Unordered selections -> advance the start index -> reuse the index only when repeated selection is allowed. | [78 Subsets][lc78], [77 Combinations][lc77], [39 Combination Sum][lc39] |
| Permutations / product choices | Order or one choice per position matters -> track used items/position -> undo the exact branch state. | [46 Permutations][lc46], [17 Letter Combinations of a Phone Number][lc17] |
| Constraint backtracking | Partial assignments can be rejected -> maintain local constraints -> prune before recursing and restore on return. | [22 Generate Parentheses][lc22], [52 N-Queens II][lc52], [79 Word Search][lc79] |
| 1D linear DP | Result depends on earlier/later positions -> define a prefix/suffix state -> combine legal predecessor choices. | [70 Climbing Stairs][lc70], [198 House Robber][lc198], [139 Word Break][lc139] |
| Grid DP | Movement follows acyclic cell dependencies -> define each cell's count/cost -> combine only allowed incoming moves. | [62 Unique Paths][lc62], [64 Minimum Path Sum][lc64] |
| 0/1 knapsack / subset DP | Each item can be used once -> state item index + target/capacity -> roll targets backward to prevent reuse. | [416 Partition Equal Subset Sum][lc416], [494 Target Sum][lc494] |
| Unbounded knapsack DP | Items/coins can repeat -> state remaining amount -> choose iteration order for minimum, unordered count, or ordered count. | [322 Coin Change][lc322], [518 Coin Change II][lc518], [377 Combination Sum IV][lc377] |
| Subsequence DP | Non-contiguous ordered selection -> one sequence uses ending-index state, two use prefix pairs -> extend compatible matches or skip; LIS also permits binary-search tails. | [300 Longest Increasing Subsequence][lc300], [1143 Longest Common Subsequence][lc1143] |
| Two-sequence / matching DP | Compare prefixes/suffixes of two sequences -> enumerate edit/match/skip transitions -> handle empty prefixes explicitly. | [72 Edit Distance][lc72], [44 Wildcard Matching][lc44] |
| Interval DP | Choosing a split affects both sides of a contiguous interval -> state (left, right) -> solve shorter dependencies first. | [312 Burst Balloons][lc312], [1039 Minimum Score Triangulation of Polygon][lc1039] |
| Palindrome partition DP | Minimize cuts between palindromic pieces -> precompute valid substrings -> optimize cuts over prefix boundaries. | [132 Palindrome Partitioning II][lc132] |
| State-machine DP | Legal next actions depend on a mode/history -> state day + holding/cooldown/etc. -> permit only valid transitions. | [309 Best Time to Buy and Sell Stock with Cooldown][lc309], [714 Best Time to Buy and Sell Stock with Transaction Fee][lc714] |

## Graphs and connectivity

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| DFS/BFS traversal / flood fill | Reachable cells/nodes or components -> visit neighbors -> mark discovery once; traverse only allowed edges. | [200 Number of Islands][lc200], [130 Surrounded Regions][lc130] |
| Graph cloning with identity map | Copy nodes with cycles/shared references -> map original identity to clone -> register clones before following neighbors. | [133 Clone Graph][lc133] |
| Multi-source / reverse traversal | Distance from any source or reachability to boundaries -> seed all sources -> reverse edges when searching destinations backward. | [994 Rotting Oranges][lc994], [417 Pacific Atlantic Water Flow][lc417] |
| Topological ordering | Directed prerequisites -> indegrees + queue or DFS colors -> leftover nodes/back edges prove a cycle. | [207 Course Schedule][lc207], [210 Course Schedule II][lc210] |
| Unweighted shortest path | Every move costs one -> BFS by discovery depth -> mark states before enqueueing. | [127 Word Ladder][lc127], [909 Snakes and Ladders][lc909] |
| Dijkstra | Minimum distance with nonnegative weights -> pop smallest tentative distance -> skip stale entries; negative edges invalidate the proof. | [743 Network Delay Time][lc743], [1631 Path With Minimum Effort][lc1631] |
| Bounded-edge relaxation | Cheapest route with at most k+1 edges -> perform copied relaxation rounds -> never read this round's updates as prior-round costs. | [787 Cheapest Flights Within K Stops][lc787] |
| Union-Find connectivity | Repeated merges/component tests -> union representatives -> same-root edges reveal cycles in an undirected graph. | [547 Number of Provinces][lc547], [684 Redundant Connection][lc684], [261 Graph Valid Tree][lc261] |
| Weighted Union-Find | Consistent relative ratios across merged groups -> store node/parent weights -> preserve ratios during compression/union. | [399 Evaluate Division][lc399] |
| Minimum spanning tree | Connect all vertices with minimum total edge weight -> Prim/Kruskal -> add the cheapest safe edge, not shortest paths from one source. | [1584 Min Cost to Connect All Points][lc1584] |

## Trees, linked lists, and tries

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Tree DFS / postorder summaries | Parent answer needs child summaries -> define the return value -> combine children without conflating global and extendable answers. | [104 Maximum Depth of Binary Tree][lc104], [124 Binary Tree Maximum Path Sum][lc124] |
| Paired / mirrored tree traversal | Compare corresponding structure -> traverse node pairs -> include absent children in equality checks. | [100 Same Tree][lc100], [101 Symmetric Tree][lc101] |
| Tree path-state DFS | Query depends on root-to-node history -> pass remaining sum/prefix -> accept full-path answers only at the correct terminal node. | [112 Path Sum][lc112], [129 Sum Root to Leaf Numbers][lc129] |
| Tree level-order BFS | Output grouped by depth -> freeze the current queue length -> process one level before the next. | [102 Binary Tree Level Order Traversal][lc102], [199 Binary Tree Right Side View][lc199] |
| Traversal-based reconstruction | Root-first/root-last plus inorder -> locate the root's split -> consume traversal in matching subtree order. | [105 Construct Binary Tree from Preorder and Inorder Traversal][lc105], [106 Construct Binary Tree from Inorder and Postorder Traversal][lc106] |
| Tree rewiring / threading | Change links in traversal/level order -> preserve unvisited children -> attach each node once. | [114 Flatten Binary Tree to Linked List][lc114], [117 Populating Next Right Pointers in Each Node II][lc117] |
| Ancestor aggregation | Find where two targets meet -> return subtree matches -> two matching child sides identify the LCA. | [236 Lowest Common Ancestor of a Binary Tree][lc236] |
| Complete-tree shortcut | Completeness is guaranteed -> compare boundary heights -> count perfect subtrees directly; not valid for arbitrary trees. | [222 Count Complete Tree Nodes][lc222] |
| BST inorder / ordered iterator | Need sorted tree values, rank, or adjacent difference -> inorder -> stop early or retain only the previous value. | [230 Kth Smallest Element in a BST][lc230], [173 Binary Search Tree Iterator][lc173], [530 Minimum Absolute Difference in BST][lc530] |
| BST bounds | Need global ordering validity -> propagate open lower/upper bounds -> enforce every ancestor constraint, not only the parent. | [98 Validate Binary Search Tree][lc98] |
| Linked-list fast/slow / gap pointers | Cycle, midpoint, or kth-from-end -> choose relative speed/gap -> maintain the separation that exposes the target. | [141 Linked List Cycle][lc141], [19 Remove Nth Node From End of List][lc19] |
| Linked-list reversal / partition | In-place segment or stable regrouping -> save next before rewiring -> use dummy heads for boundary-safe chains. | [92 Reverse Linked List II][lc92], [25 Reverse Nodes in k-Group][lc25], [86 Partition List][lc86] |
| Linked-list carry / rotation | Digit arithmetic or cyclic shift -> track carry or length/tail -> reconnect once and terminate the final chain. | [2 Add Two Numbers][lc2], [61 Rotate List][lc61] |
| Random-pointer cloning | References may point anywhere -> map nodes or interleave copies -> preserve both next and random identities. | [138 Copy List with Random Pointer][lc138] |
| Trie prefix lookup | Many shared-prefix queries -> store character edges -> distinguish a prefix node from a complete word. | [208 Implement Trie][lc208], [648 Replace Words][lc648] |
| Trie + branching search | Wildcards/many words share prefixes -> traverse trie and search state together -> prune absent edges, restore board visits. | [211 Design Add and Search Words][lc211], [212 Word Search II][lc212] |

## Online structures and simulation

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Heap frontier / top-k | Repeatedly need the best remaining candidate -> maintain only active candidates or k best -> push replacements after extraction. | [23 Merge k Sorted Lists][lc23], [215 Kth Largest Element in an Array][lc215] |
| Two-heap median | Median after each insertion -> split lower/upper halves -> maintain order and a size difference of at most one. | [295 Find Median from Data Stream][lc295] |
| Heap + lazy invalidation | Updates leave obsolete heap entries -> keep authoritative versions in a map -> discard stale entries at query time. | [2034 Stock Price Fluctuation][lc2034] |
| Ordered multiset | Need neighbors/ranks plus duplicates -> maintain ordered values and multiplicity -> distinguish logarithmic trees from linear Python list insertion. | [480 Sliding Window Median][lc480], [729 My Calendar I][lc729] |
| Skip list | Need dynamic ordered search/insert/delete -> randomized levels -> preserve sorted links at every level; contiguous output alone needs no skip list. | [1206 Design Skiplist][lc1206]; simpler contrast: [1656 Design an Ordered Stream][lc1656] |
| Fenwick tree | Point updates plus invertible prefix/range sums -> index partial aggregates -> subtract prefix queries for ranges. | [307 Range Sum Query - Mutable][lc307], [315 Count of Smaller Numbers After Self][lc315] |
| Segment tree | Repeated general range aggregates -> define an associative merge and identity -> update covering nodes; use lazy tags for bulk updates. | [307 Range Sum Query - Mutable][lc307], [715 Range Module][lc715] |
| LRU / LFU eviction | Capacity-bound lookup with recency/frequency policy -> map + linked order/buckets -> synchronize lookup, movement, and eviction. | [146 LRU Cache][lc146], [460 LFU Cache][lc460] |
| Bloom filter / Count-Min Sketch | Exact storage is too costly and approximation is acceptable -> hash into bits/counters -> accept false positives/overestimates, not exact answers. | No direct standard LeetCode equivalent here; [217][lc217] and [347][lc347] require exact answers and are contrasts, not substitutes. |
| Event simulation | Explicit rules define chronological behavior -> model event order and live state -> resolve all same-time events consistently. | [621 Task Scheduler][lc621], [1834 Single-Threaded CPU][lc1834] |

## SQL

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Joins / aggregation | Output combines entities or collapses groups -> choose output grain -> prevent join fan-out before grouping. | [175 Combine Two Tables][lc175], [570 Managers with at Least 5 Direct Reports][lc570] |
| Ranking windows | Top values per group, ties matter -> partition -> choose ROW_NUMBER, RANK, or DENSE_RANK for the tie rule. | [178 Rank Scores][lc178], [185 Department Top Three Salaries][lc185] |
| Ordered adjacency / islands | Compare previous/next rows or identify consecutive runs -> specify partition/order -> LAG/LEAD and accumulate boundary flags. | [180 Consecutive Numbers][lc180] |
| Missing-match anti-join | Entities with no related rows -> NOT EXISTS or left join + null test -> avoid NOT IN surprises with NULL. | [183 Customers Who Never Order][lc183] |
| Temporal joins | Match dated events to effective intervals -> define endpoints and grain -> check actual date gaps, not just neighboring rows. | [197 Rising Temperature][lc197] |

Schema/index design and dialect-specific JSON queries are also covered in the
[SQL guide](./SQL/README.md); they are not universal algorithm templates.

## Common extensions (not dedicated implementations here)

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Tree DP | Local choices constrain child choices -> return a result per allowed mode -> combine independent child subtrees. | [337 House Robber III][lc337] |
| DAG DP | Directed dependencies are acyclic -> order states topologically -> aggregate predecessor results. | [329 Longest Increasing Path in a Matrix][lc329] |
| Bitmask DP | Small item universe, subset history matters -> encode used items in bits -> cache mask plus necessary remaining state. | [698 Partition to K Equal Sum Subsets][lc698] |
| Digit DP | Count bounded integers satisfying digit rules -> state position/tightness/started status plus rule state -> count valid suffixes. | [902 Numbers At Most N Given Digit Set][lc902] |

## Avoid the common misclassification

**Contiguous != subsequence. Sorted != automatically binary search.
Minimum/maximum != automatically greedy. A grid != automatically grid DP.
An online query != automatically a segment tree.**

When a branch fails its assumptions, return to the output and constraints.
For repository implementations and their limitations, use the category guides
linked from [README.md](./README.md).

[lc1]: https://leetcode.com/problems/two-sum/
[lc2]: https://leetcode.com/problems/add-two-numbers/
[lc3]: https://leetcode.com/problems/longest-substring-without-repeating-characters/
[lc4]: https://leetcode.com/problems/median-of-two-sorted-arrays/
[lc5]: https://leetcode.com/problems/longest-palindromic-substring/
[lc7]: https://leetcode.com/problems/reverse-integer/
[lc11]: https://leetcode.com/problems/container-with-most-water/
[lc15]: https://leetcode.com/problems/3sum/
[lc17]: https://leetcode.com/problems/letter-combinations-of-a-phone-number/
[lc19]: https://leetcode.com/problems/remove-nth-node-from-end-of-list/
[lc20]: https://leetcode.com/problems/valid-parentheses/
[lc21]: https://leetcode.com/problems/merge-two-sorted-lists/
[lc22]: https://leetcode.com/problems/generate-parentheses/
[lc23]: https://leetcode.com/problems/merge-k-sorted-lists/
[lc25]: https://leetcode.com/problems/reverse-nodes-in-k-group/
[lc26]: https://leetcode.com/problems/remove-duplicates-from-sorted-array/
[lc27]: https://leetcode.com/problems/remove-element/
[lc33]: https://leetcode.com/problems/search-in-rotated-sorted-array/
[lc34]: https://leetcode.com/problems/find-first-and-last-position-of-element-in-sorted-array/
[lc35]: https://leetcode.com/problems/search-insert-position/
[lc36]: https://leetcode.com/problems/valid-sudoku/
[lc38]: https://leetcode.com/problems/count-and-say/
[lc39]: https://leetcode.com/problems/combination-sum/
[lc42]: https://leetcode.com/problems/trapping-rain-water/
[lc44]: https://leetcode.com/problems/wildcard-matching/
[lc45]: https://leetcode.com/problems/jump-game-ii/
[lc46]: https://leetcode.com/problems/permutations/
[lc48]: https://leetcode.com/problems/rotate-image/
[lc49]: https://leetcode.com/problems/group-anagrams/
[lc50]: https://leetcode.com/problems/powx-n/
[lc52]: https://leetcode.com/problems/n-queens-ii/
[lc54]: https://leetcode.com/problems/spiral-matrix/
[lc56]: https://leetcode.com/problems/merge-intervals/
[lc57]: https://leetcode.com/problems/insert-interval/
[lc61]: https://leetcode.com/problems/rotate-list/
[lc62]: https://leetcode.com/problems/unique-paths/
[lc64]: https://leetcode.com/problems/minimum-path-sum/
[lc70]: https://leetcode.com/problems/climbing-stairs/
[lc72]: https://leetcode.com/problems/edit-distance/
[lc73]: https://leetcode.com/problems/set-matrix-zeroes/
[lc76]: https://leetcode.com/problems/minimum-window-substring/
[lc77]: https://leetcode.com/problems/combinations/
[lc78]: https://leetcode.com/problems/subsets/
[lc79]: https://leetcode.com/problems/word-search/
[lc84]: https://leetcode.com/problems/largest-rectangle-in-histogram/
[lc86]: https://leetcode.com/problems/partition-list/
[lc88]: https://leetcode.com/problems/merge-sorted-array/
[lc92]: https://leetcode.com/problems/reverse-linked-list-ii/
[lc98]: https://leetcode.com/problems/validate-binary-search-tree/
[lc100]: https://leetcode.com/problems/same-tree/
[lc101]: https://leetcode.com/problems/symmetric-tree/
[lc102]: https://leetcode.com/problems/binary-tree-level-order-traversal/
[lc104]: https://leetcode.com/problems/maximum-depth-of-binary-tree/
[lc105]: https://leetcode.com/problems/construct-binary-tree-from-preorder-and-inorder-traversal/
[lc106]: https://leetcode.com/problems/construct-binary-tree-from-inorder-and-postorder-traversal/
[lc112]: https://leetcode.com/problems/path-sum/
[lc114]: https://leetcode.com/problems/flatten-binary-tree-to-linked-list/
[lc117]: https://leetcode.com/problems/populating-next-right-pointers-in-each-node-ii/
[lc124]: https://leetcode.com/problems/binary-tree-maximum-path-sum/
[lc125]: https://leetcode.com/problems/valid-palindrome/
[lc127]: https://leetcode.com/problems/word-ladder/
[lc128]: https://leetcode.com/problems/longest-consecutive-sequence/
[lc129]: https://leetcode.com/problems/sum-root-to-leaf-numbers/
[lc130]: https://leetcode.com/problems/surrounded-regions/
[lc132]: https://leetcode.com/problems/palindrome-partitioning-ii/
[lc133]: https://leetcode.com/problems/clone-graph/
[lc136]: https://leetcode.com/problems/single-number/
[lc138]: https://leetcode.com/problems/copy-list-with-random-pointer/
[lc139]: https://leetcode.com/problems/word-break/
[lc141]: https://leetcode.com/problems/linked-list-cycle/
[lc146]: https://leetcode.com/problems/lru-cache/
[lc150]: https://leetcode.com/problems/evaluate-reverse-polish-notation/
[lc153]: https://leetcode.com/problems/find-minimum-in-rotated-sorted-array/
[lc155]: https://leetcode.com/problems/min-stack/
[lc162]: https://leetcode.com/problems/find-peak-element/
[lc167]: https://leetcode.com/problems/two-sum-ii-input-array-is-sorted/
[lc173]: https://leetcode.com/problems/binary-search-tree-iterator/
[lc175]: https://leetcode.com/problems/combine-two-tables/
[lc178]: https://leetcode.com/problems/rank-scores/
[lc180]: https://leetcode.com/problems/consecutive-numbers/
[lc183]: https://leetcode.com/problems/customers-who-never-order/
[lc185]: https://leetcode.com/problems/department-top-three-salaries/
[lc190]: https://leetcode.com/problems/reverse-bits/
[lc191]: https://leetcode.com/problems/number-of-1-bits/
[lc197]: https://leetcode.com/problems/rising-temperature/
[lc198]: https://leetcode.com/problems/house-robber/
[lc199]: https://leetcode.com/problems/binary-tree-right-side-view/
[lc200]: https://leetcode.com/problems/number-of-islands/
[lc201]: https://leetcode.com/problems/bitwise-and-of-numbers-range/
[lc207]: https://leetcode.com/problems/course-schedule/
[lc208]: https://leetcode.com/problems/implement-trie-prefix-tree/
[lc209]: https://leetcode.com/problems/minimum-size-subarray-sum/
[lc210]: https://leetcode.com/problems/course-schedule-ii/
[lc211]: https://leetcode.com/problems/design-add-and-search-words-data-structure/
[lc212]: https://leetcode.com/problems/word-search-ii/
[lc215]: https://leetcode.com/problems/kth-largest-element-in-an-array/
[lc217]: https://leetcode.com/problems/contains-duplicate/
[lc222]: https://leetcode.com/problems/count-complete-tree-nodes/
[lc224]: https://leetcode.com/problems/basic-calculator/
[lc228]: https://leetcode.com/problems/summary-ranges/
[lc230]: https://leetcode.com/problems/kth-smallest-element-in-a-bst/
[lc236]: https://leetcode.com/problems/lowest-common-ancestor-of-a-binary-tree/
[lc238]: https://leetcode.com/problems/product-of-array-except-self/
[lc239]: https://leetcode.com/problems/sliding-window-maximum/
[lc242]: https://leetcode.com/problems/valid-anagram/
[lc253]: https://leetcode.com/problems/meeting-rooms-ii/
[lc261]: https://leetcode.com/problems/graph-valid-tree/
[lc268]: https://leetcode.com/problems/missing-number/
[lc289]: https://leetcode.com/problems/game-of-life/
[lc295]: https://leetcode.com/problems/find-median-from-data-stream/
[lc300]: https://leetcode.com/problems/longest-increasing-subsequence/
[lc307]: https://leetcode.com/problems/range-sum-query-mutable/
[lc309]: https://leetcode.com/problems/best-time-to-buy-and-sell-stock-with-cooldown/
[lc312]: https://leetcode.com/problems/burst-balloons/
[lc315]: https://leetcode.com/problems/count-of-smaller-numbers-after-self/
[lc322]: https://leetcode.com/problems/coin-change/
[lc329]: https://leetcode.com/problems/longest-increasing-path-in-a-matrix/
[lc337]: https://leetcode.com/problems/house-robber-iii/
[lc338]: https://leetcode.com/problems/counting-bits/
[lc347]: https://leetcode.com/problems/top-k-frequent-elements/
[lc371]: https://leetcode.com/problems/sum-of-two-integers/
[lc377]: https://leetcode.com/problems/combination-sum-iv/
[lc383]: https://leetcode.com/problems/ransom-note/
[lc392]: https://leetcode.com/problems/is-subsequence/
[lc399]: https://leetcode.com/problems/evaluate-division/
[lc416]: https://leetcode.com/problems/partition-equal-subset-sum/
[lc417]: https://leetcode.com/problems/pacific-atlantic-water-flow/
[lc424]: https://leetcode.com/problems/longest-repeating-character-replacement/
[lc435]: https://leetcode.com/problems/non-overlapping-intervals/
[lc443]: https://leetcode.com/problems/string-compression/
[lc455]: https://leetcode.com/problems/assign-cookies/
[lc460]: https://leetcode.com/problems/lfu-cache/
[lc480]: https://leetcode.com/problems/sliding-window-median/
[lc494]: https://leetcode.com/problems/target-sum/
[lc518]: https://leetcode.com/problems/coin-change-ii/
[lc530]: https://leetcode.com/problems/minimum-absolute-difference-in-bst/
[lc547]: https://leetcode.com/problems/number-of-provinces/
[lc560]: https://leetcode.com/problems/subarray-sum-equals-k/
[lc567]: https://leetcode.com/problems/permutation-in-string/
[lc570]: https://leetcode.com/problems/managers-with-at-least-5-direct-reports/
[lc605]: https://leetcode.com/problems/can-place-flowers/
[lc621]: https://leetcode.com/problems/task-scheduler/
[lc643]: https://leetcode.com/problems/maximum-average-subarray-i/
[lc647]: https://leetcode.com/problems/palindromic-substrings/
[lc648]: https://leetcode.com/problems/replace-words/
[lc684]: https://leetcode.com/problems/redundant-connection/
[lc698]: https://leetcode.com/problems/partition-to-k-equal-sum-subsets/
[lc704]: https://leetcode.com/problems/binary-search/
[lc714]: https://leetcode.com/problems/best-time-to-buy-and-sell-stock-with-transaction-fee/
[lc715]: https://leetcode.com/problems/range-module/
[lc729]: https://leetcode.com/problems/my-calendar-i/
[lc739]: https://leetcode.com/problems/daily-temperatures/
[lc743]: https://leetcode.com/problems/network-delay-time/
[lc787]: https://leetcode.com/problems/cheapest-flights-within-k-stops/
[lc853]: https://leetcode.com/problems/car-fleet/
[lc875]: https://leetcode.com/problems/koko-eating-bananas/
[lc902]: https://leetcode.com/problems/numbers-at-most-n-given-digit-set/
[lc909]: https://leetcode.com/problems/snakes-and-ladders/
[lc974]: https://leetcode.com/problems/subarray-sums-divisible-by-k/
[lc994]: https://leetcode.com/problems/rotting-oranges/
[lc1011]: https://leetcode.com/problems/capacity-to-ship-packages-within-d-days/
[lc1039]: https://leetcode.com/problems/minimum-score-triangulation-of-polygon/
[lc1094]: https://leetcode.com/problems/car-pooling/
[lc1143]: https://leetcode.com/problems/longest-common-subsequence/
[lc1206]: https://leetcode.com/problems/design-skiplist/
[lc1326]: https://leetcode.com/problems/minimum-number-of-taps-to-open-to-water-a-garden/
[lc1438]: https://leetcode.com/problems/longest-continuous-subarray-with-absolute-diff-less-than-or-equal-to-limit/
[lc1584]: https://leetcode.com/problems/min-cost-to-connect-all-points/
[lc1631]: https://leetcode.com/problems/path-with-minimum-effort/
[lc1656]: https://leetcode.com/problems/design-an-ordered-stream/
[lc1834]: https://leetcode.com/problems/single-threaded-cpu/
[lc2034]: https://leetcode.com/problems/stock-price-fluctuation/
