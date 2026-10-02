# Pattern identification reference

All categories in this repository, their main subpatterns, and representative
LeetCode practice problems. Examples are not an exhaustive LeetCode inventory
or a claim that every example is implemented here. Patterns overlap: one problem
may use hashing + sliding window, or binary search + greedy.

## How to use this guide

A **pattern** is a reusable way to solve a family of problems. For example,
Two Sum and duplicate detection both benefit from remembering values already
seen. The questions differ, but the useful tool is the same: a hash map or set.

Use the **recognition questions** in each section as a quick checklist.
Several "yes" answers suggest a candidate pattern; they do not prove it.
The final question is always: **why is this choice safe for this problem?**

Do not memorize a pattern from a problem title. First describe a simple solution,
notice what work it repeats, and look for a pattern that removes that repetition.
Read the decision tree for a starting point, then the matching table row and
one linked practice problem. The tables are a reference, not a reading checklist.

**Suggested learning order:** arrays/hashing, two pointers, sliding windows,
stacks, binary search, linked lists, tree traversal, backtracking, graphs, then
dynamic programming. Leave specialized query structures and advanced DP until
the simpler patterns feel familiar.

### Vocabulary used below

| Term | Plain-language meaning |
| --- | --- |
| Contiguous / subarray / substring | Adjacent elements with no gaps. In `[1, 2, 3]`, `[1, 2]` is a subarray; `[1, 3]` is not. |
| Subsequence | Keep the original order, but allow gaps. `[1, 3]` is a subsequence of `[1, 2, 3]`. |
| Prefix / suffix | The beginning / ending portion of a sequence. For `"abc"`, `"ab"` is a prefix and `"bc"` is a suffix. |
| State | Information needed to continue solving: a running sum, two indices, or the best result for a smaller problem. |
| Invariant | A fact kept true after every step. For example, "the current window contains no repeated characters." |
| Feasible / valid | A candidate meets the problem's rules. A window with a repeated character is not valid for a uniqueness requirement. |
| Monotone predicate | A yes/no test that changes at most once as candidates increase, such as "can this speed finish in time?" |
| Hash map / set | A dictionary stores key-value pairs; a set stores unique values. Both usually support fast lookup. |
| Stack / queue / deque | Remove the newest item / oldest item / either end, respectively. |
| Heap | A structure that makes the smallest or largest available item easy to retrieve; it is not a fully sorted list. |
| DFS / BFS | Depth-first search follows a branch before returning; breadth-first search visits nearby nodes before farther ones. |
| DP / memoization | Dynamic programming reuses smaller results; memoization stores results of recursive calls instead of recomputing them. |
| BST / trie | A binary search tree orders values in left/right subtrees; a trie stores shared word prefixes along character paths. |
| Online / in place | Answer as updates arrive / reuse the input's storage rather than building a separate result structure. |
| Time / extra space | How work / additional storage grows with input size `n`: `O(n)` is linear, `O(log n)` repeatedly halves the work, and `O(n^2)` often checks pairs. |

## Identification steps

The numbered comments in the solution files follow these same five steps.

1. **Output:** Say exactly what to return: a boolean, count, best value, indices, or all answers. Finding one pair is different from listing every pair.
2. **Structure:** Notice useful input properties. Are values sorted? Must selected elements be adjacent? Are there links, neighbors, or prerequisites?
3. **Constraints:** Check what is allowed and affordable. Can values be negative? May the input change? Checking every pair takes `O(n^2)` work and may be too slow for a large array.
4. **Choice:** Pick a pattern and name what you will remember. Explain how each new item changes that state, rather than just naming a data structure.
5. **Why it works:** Explain what stays true and why no valid answer is missed. Check empty/boundary cases and estimate time and extra space.

## Decision tree

Read this as a set of questions, not a rule that the first matching branch wins.
For example, a grid can be treated as a graph when you can move between cells,
but as a DP table when moves have a one-way dependency. Use the tables to check
the assumptions of the suggested pattern.

```text
START: what kind of work does the question ask you to do?
|
+-- Work with database rows? -> SQL
|   +-- Totals per group / combine tables -> grouping / joins
|   +-- Rank rows or compare neighboring rows -> window functions
|   +-- Find missing matches / consecutive runs -> anti-join / gaps and islands
|
+-- List all valid choices or arrangements? -> backtracking
|   +-- Only need a count/best result, and smaller problems repeat? -> DP
|
+-- Answer repeatedly while data changes?
|   +-- Smallest/largest values, top k, or median -> heap(s)
|   +-- Find neighboring sorted values -> ordered set / skip list
|   +-- Change one value, ask for range sums -> Fenwick tree
|   +-- More general range queries/updates -> segment tree
|   +-- Remove cache entries by usage -> LRU / LFU cache
|   +-- Search word prefixes -> trie
|   +-- "Might be present" is acceptable -> Bloom filter
|
+-- Linked list? -> move pointers / reverse links / copy nodes
+-- Tree?
|   +-- Need child results or a path result -> DFS
|   +-- Need results level by level -> BFS
|   +-- Values obey BST ordering -> inorder traversal / value bounds
+-- Graph or grid movement?
|   +-- Can I reach it? Which nodes belong together? -> DFS/BFS / Union-Find
|   +-- Tasks have prerequisites -> topological sort
|   +-- Fewest equal-cost moves -> BFS
|   +-- Cheapest route with nonnegative edge costs -> Dijkstra
|   +-- Cheapest route with an edge/stop limit -> bounded relaxation
|   +-- Cheapest way to connect all nodes -> minimum spanning tree
|
+-- Array/string/ranges?
|   +-- Must the answer use adjacent elements?
|   |   +-- Exactly k elements -> fixed sliding window
|   |   +-- Remove from the left to repair the rule -> variable sliding window
|   |   +-- Range sum/count, possibly negative values -> prefix sums + hashing
|   |   +-- Need min/max as a window moves -> monotonic deque
|   +-- Can ordering eliminate work?
|   |   +-- Prove half the candidates cannot contain an answer -> binary search
|   |   +-- Compare ends / merge sorted values -> two pointers
|   |   +-- Merge overlaps / count active events -> intervals / sweep line
|   +-- Remember values, counts, or matching groups -> hash map/set
|   +-- Match nested brackets/expressions -> stack
|   +-- Find next smaller/greater value -> monotonic stack
|   +-- Find a palindrome substring -> expand around centers
|   +-- Rotate/visit/update a matrix -> index and boundary simulation
|
+-- Need a best value or count, but no earlier branch settles the choice?
    +-- Prove a locally best choice never hurts later choices -> greedy
    +-- The same smaller questions repeat -> DP
    +-- Few possibilities, no useful repeated results -> try them directly
    +-- Rules concern binary digits or repeated powers -> bit operations / squaring
    +-- Rules describe events over time -> event simulation
```

## Worked examples: from clues to a pattern

### Two Sum: remembering earlier values

Given `[2, 7, 11, 15]` and target `9`:

1. **Output:** Return two different indices, not the values themselves.
2. **Structure:** The array is unsorted; checking every possible pair repeats work.
3. **Constraints:** Keep the original indices. A linear scan with extra storage avoids sorting and checking all pairs.
4. **Choice:** Use a dictionary from value to index. At `2`, look for `9 - 2 = 7`; it is absent, so store `2: 0`. At `7`, look for `2`, which is already stored.
5. **Why it works:** Return `[0, 1]`. Looking up before storing means the partner always comes from an earlier, different index. Each item is processed once: average `O(n)` time and `O(n)` extra space.

### Fixed-size window: reuse an overlapping calculation

Find the maximum sum of exactly two adjacent values in `[2, 1, 5, 1]`.
The window sums are `2 + 1 = 3`, `1 + 5 = 6`, and `5 + 1 = 6`.
Instead of resumming each pair, update `3 - 2 + 5 = 6`, then `6 - 1 + 1 = 6`.
The invariant is that the running sum describes exactly the current window.
This takes `O(n)` time and `O(1)` extra space.

A **variable** window needs an additional reason that moving the left edge
works. For a shortest sum-at-least-5 window, `[1, -1, 5]` is a warning:
removing `1` drops the sum from `5` to `4`, but removing the next `-1` raises it
back to `5`. The usual positive-value shrinking rule would miss `[5]`.

### Greedy or DP: a good-looking choice is not enough

In Can Place Flowers, planting at the earliest safe empty plot leaves at least
as much space to the right as delaying that first planting. That is the reason
the local choice is safe; record the planting before checking the next plot.

For coins `[1, 3, 4]` and amount `6`, choosing the largest coin first gives
`4 + 1 + 1` (three coins), but `3 + 3` needs only two. So the same greedy idea
is not safe for arbitrary coin values. Use DP: remember the fewest coins for
each smaller amount, then compute `best[a] = 1 + min(best[a - coin])` over
usable coins. Start with `best[0] = 0`; unreachable amounts stay marked impossible.

## Arrays, hashing, strings, and pointers

Think of hashing as a notebook for things already seen. A **complement** is the
missing partner, such as `target - current`. A **canonical key** is a shared
label for equivalent items: sorting `"eat"` and `"tea"` gives `"aet"` for both.
Two pointers are simply two positions you move using a rule, not two nested loops.

### Recognition questions

- Am I searching for a value, a matching partner, or repeated values? Consider a hash map/set.
- Do I care how many times each value appears? Consider frequency counting.
- Are the values sorted, or am I comparing two ends? Consider two pointers.
- Does a range result come from totals before its boundaries? Consider prefix sums.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Membership / complement lookup | Use when you need to know whether a value or its required partner was seen earlier. Store values in a set, or value-to-index pairs in a dictionary; check before inserting so one index cannot be used twice. | [217 Contains Duplicate][lc217], [1 Two Sum][lc1] |
| Frequency counting | Use when the number of copies matters. Count each value or character: `"aab"` and `"abb"` contain the same letters but have different counts. | [242 Valid Anagram][lc242], [383 Ransom Note][lc383] |
| Canonical-key grouping | Use when different-looking items belong to the same group. Compute a shared signature, such as sorted letters or letter counts, and store items with the same signature together. | [49 Group Anagrams][lc49] |
| Frequency buckets | Need top frequencies in a bounded batch -> count -> bucket by count and scan downward. | [347 Top K Frequent Elements][lc347] |
| Prefix/suffix accumulation | Use when each position needs a result from everything except itself. For products, multiply the product to its left by the product to its right; neither includes the current value. | [238 Product of Array Except Self][lc238] |
| Prefix sum + hash map/set | Use when range sums can be expressed as differences of totals from the beginning. With `prefix[0] = 0`, the sum of indices `l` through `r` is `prefix[r + 1] - prefix[l]`; store earlier totals/counts to find matching ranges, even with negative values. | [560 Subarray Sum Equals K][lc560], [974 Subarray Sums Divisible by K][lc974] |
| Sequence-start set scanning | Unsorted consecutive values -> deduplicate -> extend only values without predecessors. | [128 Longest Consecutive Sequence][lc128] |
| Region validation | Independent row/column/box uniqueness -> maintain one set per region -> reject repeated nonempty values. | [36 Valid Sudoku][lc36] |
| Opposing two pointers | Use when comparing the two ends tells you which side can be discarded. In a sorted pair-sum problem, increase the left value if the sum is too small, or decrease the right value if it is too large. | [167 Two Sum II][lc167], [125 Valid Palindrome][lc125] |
| Subsequence matching pointers | Test ordered inclusion, not an optimum -> advance the candidate only on a match -> consumed characters form a matched prefix. | [392 Is Subsequence][lc392] |
| Fixed choice + two pointers | Sorted triple target -> fix one value, solve the remaining pair -> skip duplicates without losing unique answers. | [15 3Sum][lc15] |
| Read/write compaction | Use when removing items without building a new array. A read position examines each item; a write position marks where the next accepted item belongs. The part before the write position is always the finished result. | [26 Remove Duplicates from Sorted Array][lc26], [27 Remove Element][lc27] |
| Sorted merging | Two ordered streams -> consume the smaller head -> write backward when the destination overlaps unread input. | [88 Merge Sorted Array][lc88], [21 Merge Two Sorted Lists][lc21] |
| Boundary-limited two pointers | Water depends on the weaker boundary -> advance that side -> maintain boundary maxima if accumulating trapped water. | [11 Container With Most Water][lc11], [42 Trapping Rain Water][lc42] |
| Center expansion | Longest contiguous palindrome -> enumerate odd/even centers -> expand while mirrored characters match; not subsequence DP. | [5 Longest Palindromic Substring][lc5], [647 Palindromic Substrings][lc647] |
| Run-length scanning | Output depends on consecutive equal items -> consume a maximal run -> emit its length/value once. | [38 Count and Say][lc38], [443 String Compression][lc443] |

## Matrix layout and simulation

Separate **where a value belongs** from **what its value should become**.
For a rotation you map coordinates; for Game of Life you must preserve the
original board until every new value has been calculated. A matrix problem
does not require graph search unless movement or connectivity matters.

### Recognition questions

- Am I visiting cells in a specified order rather than searching for a path?
- Can I describe the answer by moving boundaries or mapping coordinates?
- Must every new cell value use the original board, not partially updated values?

These clues suggest matrix simulation; the update rule decides which variant.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Shrinking boundaries | Visit a rectangular perimeter in order -> maintain four bounds -> guard crossed bounds before traversing the opposite edge. | [54 Spiral Matrix][lc54] |
| Index transformation | Square matrix rotation -> transpose and reverse, or cycle four coordinates -> move each value to its mapped destination. | [48 Rotate Image][lc48] |
| In-place marker storage | Entire rows/columns depend on original flags -> save first-row/column flags -> mark first, rewrite second. | [73 Set Matrix Zeroes][lc73] |
| Simultaneous-state encoding | All cells update from the old board -> store old/new bits together -> read only old state until the final pass. | [289 Game of Life][lc289] |

## Windows, stacks, and queues

A **window** is an adjacent range with left and right boundaries. Slide it
instead of starting each range calculation again. A **monotonic** stack/deque
keeps candidates in increasing or decreasing order. A stack may remove an item
when its next-greater/smaller answer is found; a window deque removes an expired
item or one that a newer item makes unnecessary.

### Recognition questions

- Must the answer use adjacent elements? Consider a window.
- Is the length fixed, or can removing leftmost items repair the rule?
- Do I repeatedly need the biggest/smallest value in that window? Consider a monotonic deque.
- Do I need to resolve the most recent unfinished item first? Consider a stack.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Fixed-size window | Use when every candidate has exactly `k` items. Add the new item and remove the item leaving the left end; update the answer only when the window is full. | [643 Maximum Average Subarray I][lc643], [567 Permutation in String][lc567] |
| Variable window: longest feasible | Use when extending may break a rule and removing leftmost items can repair it. For unique characters, shrink or jump past a duplicate before recording the length. Character Replacement has a specialized maximum-frequency optimization. | [3 Longest Substring Without Repeating Characters][lc3], [424 Longest Repeating Character Replacement][lc424] |
| Variable window: shortest covering | Use when a range must contain enough required information. Expand until it qualifies, then record and shrink while it still qualifies; positive/nonnegative sums allow this, but arbitrary negative sums do not. | [76 Minimum Window Substring][lc76], [209 Minimum Size Subarray Sum][lc209] |
| Monotonic deque | Use for the maximum/minimum of each moving window. Store candidate indices in value order; remove expired indices from the front and weaker older candidates from the back. The front is the current answer. | [239 Sliding Window Maximum][lc239], [1438 Longest Continuous Subarray With Absolute Diff Less Than or Equal to Limit][lc1438] |
| Parsing / evaluation stack | Nested delimiters or postfix expressions -> push unresolved work -> resolve on a matching close/operator. | [20 Valid Parentheses][lc20], [150 Evaluate Reverse Polish Notation][lc150], [224 Basic Calculator][lc224] |
| Stack with auxiliary aggregate | Stack queries need O(1) minimum -> maintain the minimum for each depth -> restore it on pop. | [155 Min Stack][lc155] |
| Monotonic stack | Next greater/smaller or maximal span -> keep unresolved indices ordered -> popping finalizes a boundary/answer. | [739 Daily Temperatures][lc739], [84 Largest Rectangle in Histogram][lc84] |
| Greedy ordered stack | Cars cannot pass -> sort by position -> compare arrival times against the fleet ahead. | [853 Car Fleet][lc853] |

## Search, greedy, intervals, and bits

Binary search needs a reason to discard half the possibilities, not merely a
sorted-looking input. Greedy needs a reason a choice cannot hurt the final
answer, not merely a request for a minimum. A **sweep line** processes starts
and ends in order, like moving a cursor through a schedule.

### Greedy linear scan checklist

- Is the input an array or a sequence I can scan from left to right?
- Does placement validity depend on nearby positions or a small amount of state?
- Am I trying to maximize/count placements, or reach a placement target?
- Can I commit to a valid position without undoing earlier choices?
- Can I prove taking it now does not reduce the best possible future result?

If these answers are yes, **greedy linear scan** is a strong candidate.
In Can Place Flowers, an empty plot with empty neighbors is locally valid,
and choosing the earliest such plot leaves room for later placements.
The last question is essential: local validity alone does not prove optimality.

### Other recognition questions

- Does a yes/no test change only once as candidate answers increase? Consider binary search on the answer.
- Do start/end ranges overlap or compete for resources? Consider interval merging or a sweep line.
- Do paired values cancel, or does the question explicitly involve binary digits? Consider bit operations.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Binary search on position | Use when comparing the middle lets you rule out a whole half. Keep a search interval that still contains the target if it exists; rotated arrays need an extra check to identify which half is sorted. | [704 Binary Search][lc704], [33 Search in Rotated Sorted Array][lc33] |
| Lower/upper bounds | Need insertion position or duplicate range -> search first >= target / first > target -> keep the transition inside the interval. | [35 Search Insert Position][lc35], [34 Find First and Last Position of Element in Sorted Array][lc34] |
| Peak / rotated minimum search | Local slope or rotated sorted structure rules out a region -> retain a half guaranteed to contain an answer -> do not assume a globally monotone peak predicate. | [162 Find Peak Element][lc162], [153 Find Minimum in Rotated Sorted Array][lc153] |
| Binary search on answer | Use when candidate answers have an ordered yes/no boundary. If speed `s` finishes in time, every faster speed also does; test a speed and search for the first one that succeeds. | [875 Koko Eating Bananas][lc875], [1011 Capacity To Ship Packages Within D Days][lc1011] |
| Binary search on partition | Two sorted arrays, rank/median needed -> search the smaller split -> ensure both left maxima are <= both right minima. | [4 Median of Two Sorted Arrays][lc4] |
| Greedy feasible placement | Use when taking the earliest valid action leaves at least as much room for later actions as delaying it. Plant only when the plot and its neighbors are empty, and record the planting before continuing. | [605 Can Place Flowers][lc605] |
| Greedy farthest reach | Cover a line or advance with minimum jumps -> accumulate reachable endpoints -> commit the farthest extension at each boundary. | [45 Jump Game II][lc45], [1326 Minimum Number of Taps to Open to Water a Garden][lc1326] |
| Greedy exchange choice | Use when you can replace a choice in an optimal answer with your choice without making the answer worse. Keeping the earliest-finishing compatible interval leaves the most time for later intervals. | [435 Non-overlapping Intervals][lc435], [455 Assign Cookies][lc455] |
| Interval merging / insertion | Need union of coverage -> sort then coalesce, or use pre-overlap/overlap/post-overlap phases for sorted disjoint input. | [56 Merge Intervals][lc56], [57 Insert Interval][lc57] |
| Run compression of sorted values | Convert consecutive values into ranges -> extend while adjacent difference is one -> emit each maximal run. | [228 Summary Ranges][lc228] |
| Sweep line / active intervals | Need simultaneous count or resource usage -> order endpoints -> process equal-time ties according to overlap rules. | [253 Meeting Rooms II][lc253], [1094 Car Pooling][lc1094] |
| XOR cancellation | Use when every unwanted value occurs an even number of times. XOR obeys `x ^ x = 0` and `x ^ 0 = x`, so matching copies disappear; this does not solve arbitrary frequency problems. | [136 Single Number][lc136], [268 Missing Number][lc268] |
| Bit counting / bit DP | Need set-bit counts -> clear the lowest set bit, or reuse the shifted prefix count -> respect width for signed inputs. | [191 Number of 1 Bits][lc191], [338 Counting Bits][lc338] |
| Fixed-width bit transformations | Reverse/combine a binary representation -> process bit positions -> distinguish numeric value from fixed-width encoding. | [190 Reverse Bits][lc190], [201 Bitwise AND of Numbers Range][lc201] |
| Bitwise carry propagation | Addition without arithmetic operators -> XOR gives partial sum, shifted AND gives carry -> mask to the intended width. | [371 Sum of Two Integers][lc371] |
| Numeric digit extraction | Reverse decimal digits with overflow limits -> extract by remainder/division -> check bounds before appending each digit. | [7 Reverse Integer][lc7] |
| Exponentiation by squaring | Large exponent -> halve it repeatedly -> multiply on odd powers; handle negative exponents separately. | [50 Pow(x, n)][lc50] |

## Backtracking and dynamic programming

**Backtracking** tries a choice, explores it, then undoes it before trying the
next choice. **DP** remembers the answer to a smaller question so it is solved
only once. **Greedy** commits to one choice without exploring alternatives,
which requires a safety argument.

For DP, write a sentence defining each stored result before writing a formula:
`best[a]` means "the fewest coins needed for amount a." A **transition** builds
a result from smaller results, and a **base case** is an answer known directly,
such as zero coins for amount zero.

### Recognition questions

- Must I list every valid arrangement? Consider backtracking.
- Can different choices lead to the same smaller question? Consider DP.
- Do I only need the count/best result, rather than every arrangement?
- Can I describe a smaller result by position, amount, interval, or current mode?

The last question helps choose the DP state. If a choice can be permanently
accepted with a safety proof, check whether greedy can avoid exploring states.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Subsets / combinations | Unordered selections -> advance the start index -> reuse the index only when repeated selection is allowed. | [78 Subsets][lc78], [77 Combinations][lc77], [39 Combination Sum][lc39] |
| Permutations / product choices | Order or one choice per position matters -> track used items/position -> undo the exact branch state. | [46 Permutations][lc46], [17 Letter Combinations of a Phone Number][lc17] |
| Constraint backtracking | Partial assignments can be rejected -> maintain local constraints -> prune before recursing and restore on return. | [22 Generate Parentheses][lc22], [52 N-Queens II][lc52], [79 Word Search][lc79] |
| 1D linear DP | Use when one position summarizes a smaller question. For stairs, ways to reach step `i` come from steps `i-1` and `i-2`; for houses, compare skipping a house with robbing it and skipping its neighbor. | [70 Climbing Stairs][lc70], [198 House Robber][lc198], [139 Word Break][lc139] |
| Grid DP | Use when cells depend on already solved cells without cycles. With right/down movement, count or minimize paths using the cell above and the cell to the left; initialize the starting cell and boundaries. | [62 Unique Paths][lc62], [64 Minimum Path Sum][lc64] |
| 0/1 knapsack / subset DP | Use when each item is either taken once or skipped. Store what targets/capacities can be reached; when compressing to one array, update backward so the same item is not reused in that iteration. | [416 Partition Equal Subset Sum][lc416], [494 Target Sum][lc494] |
| Unbounded knapsack DP | Use when the same item/coin may be taken repeatedly. Reuse smaller amounts, but distinguish the goal: fewest coins, unordered combinations, or ordered sequences. Loop order changes what a counting solution counts. | [322 Coin Change][lc322], [518 Coin Change II][lc518], [377 Combination Sum IV][lc377] |
| Subsequence DP | Non-contiguous ordered selection -> one sequence uses ending-index state, two use prefix pairs -> extend compatible matches or skip; LIS also permits binary-search tails. | [300 Longest Increasing Subsequence][lc300], [1143 Longest Common Subsequence][lc1143] |
| Two-sequence / matching DP | Compare prefixes/suffixes of two sequences -> enumerate edit/match/skip transitions -> handle empty prefixes explicitly. | [72 Edit Distance][lc72], [44 Wildcard Matching][lc44] |
| Interval DP | Use when solving a segment means trying a split or a final operation inside it. Store the best answer for each pair of boundaries, combine smaller segments, and try every permitted split. | [312 Burst Balloons][lc312], [1039 Minimum Score Triangulation of Polygon][lc1039] |
| Palindrome partition DP | Minimize cuts between palindromic pieces -> precompute valid substrings -> optimize cuts over prefix boundaries. | [132 Palindrome Partitioning II][lc132] |
| State-machine DP | Use when the same position allows different actions depending on history. A stock trader may be holding a share, free to buy, or cooling down; store a result for each mode and allow only legal moves between modes. | [309 Best Time to Buy and Sell Stock with Cooldown][lc309], [714 Best Time to Buy and Sell Stock with Transaction Fee][lc714] |

## Graphs and connectivity

A **node** is an item and an **edge** is a connection; a grid cell can be a node
with neighboring cells as its edges. A **component** is a group connected by
paths. An edge's **weight** is its cost. First decide whether you need any path,
the fewest moves, the cheapest path, or the cheapest way to connect everything:
these are different questions.

### Recognition questions

- Are there items connected by allowed moves, even if no graph is drawn?
- Do I need reachable groups, a shortest route, or prerequisite order?
- Does every move cost the same? If so, BFS can find the fewest moves.
- Is the goal to connect all nodes cheaply, rather than travel to one node? Consider a spanning tree.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| DFS/BFS traversal / flood fill | Reachable cells/nodes or components -> visit neighbors -> mark discovery once; traverse only allowed edges. | [200 Number of Islands][lc200], [130 Surrounded Regions][lc130] |
| Graph cloning with identity map | Copy nodes with cycles/shared references -> map original identity to clone -> register clones before following neighbors. | [133 Clone Graph][lc133] |
| Multi-source / reverse traversal | Distance from any source or reachability to boundaries -> seed all sources -> reverse edges when searching destinations backward. | [994 Rotting Oranges][lc994], [417 Pacific Atlantic Water Flow][lc417] |
| Topological ordering | Use when tasks must wait for prerequisites. Count each task's unfinished prerequisites, process tasks whose count is zero, and release their dependents. If not all tasks can be processed, the dependency graph contains a cycle. | [207 Course Schedule][lc207], [210 Course Schedule II][lc210] |
| Unweighted shortest path | Use when every move has the same cost. BFS visits distance 0, then 1, then 2, so the first discovery has the fewest moves; mark a node when adding it to the queue. | [127 Word Ladder][lc127], [909 Snakes and Ladders][lc909] |
| Dijkstra | Use for cheapest paths with nonnegative edge costs. Repeatedly take the smallest known cost from a heap and improve neighboring costs; ignore outdated heap entries. Minimum Effort uses the largest edge on a path instead of adding costs. | [743 Network Delay Time][lc743], [1631 Path With Minimum Effort][lc1631] |
| Bounded-edge relaxation | Use when price alone is not enough: the route also has a stop limit. Compute each round from a copy of the previous costs so one round adds at most one edge; `k` stops allow `k+1` edges. | [787 Cheapest Flights Within K Stops][lc787] |
| Union-Find connectivity | Use when groups are repeatedly joined and you need to test membership. Give each group a representative, join representatives, and compare them; an undirected edge within an existing group closes a cycle. | [547 Number of Provinces][lc547], [684 Redundant Connection][lc684], [261 Graph Valid Tree][lc261] |
| Weighted Union-Find | Consistent relative ratios across merged groups -> store node/parent weights -> preserve ratios during compression/union. | [399 Evaluate Division][lc399] |
| Minimum spanning tree | Use when all nodes must be connected as cheaply as possible, not when finding one cheapest route. Prim grows a connected set by its cheapest outgoing edge; Kruskal adds cheapest edges that do not create a cycle. | [1584 Min Cost to Connect All Points][lc1584] |

## Trees, linked lists, and tries

A **subtree** is a node together with its descendants. **Postorder** solves
children before their parent; **inorder** visits left subtree, node, then right
subtree. A **dummy head** is a temporary node before a linked list, making
changes at the first real node work like changes anywhere else.

### Recognition questions

- Does a parent answer need results from its children? Consider tree DFS.
- Is the answer organized by depth? Consider level-order BFS.
- Must I change links without losing unvisited nodes? Consider linked-list rewiring.
- Do many words share prefixes? Consider a trie.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Tree DFS / postorder summaries | Use when a parent's answer needs answers from its children. Height is one plus the larger child height. For path sums, keep separate the best full path and the one branch that can continue upward. | [104 Maximum Depth of Binary Tree][lc104], [124 Binary Tree Maximum Path Sum][lc124] |
| Paired / mirrored tree traversal | Compare corresponding structure -> traverse node pairs -> include absent children in equality checks. | [100 Same Tree][lc100], [101 Symmetric Tree][lc101] |
| Tree path-state DFS | Query depends on root-to-node history -> pass remaining sum/prefix -> accept full-path answers only at the correct terminal node. | [112 Path Sum][lc112], [129 Sum Root to Leaf Numbers][lc129] |
| Tree level-order BFS | Output grouped by depth -> freeze the current queue length -> process one level before the next. | [102 Binary Tree Level Order Traversal][lc102], [199 Binary Tree Right Side View][lc199] |
| Traversal-based reconstruction | Root-first/root-last plus inorder -> locate the root's split -> consume traversal in matching subtree order. | [105 Construct Binary Tree from Preorder and Inorder Traversal][lc105], [106 Construct Binary Tree from Inorder and Postorder Traversal][lc106] |
| Tree rewiring / threading | Change links in traversal/level order -> preserve unvisited children -> attach each node once. | [114 Flatten Binary Tree to Linked List][lc114], [117 Populating Next Right Pointers in Each Node II][lc117] |
| Ancestor aggregation | Find where two targets meet -> return subtree matches -> two matching child sides identify the LCA. | [236 Lowest Common Ancestor of a Binary Tree][lc236] |
| Complete-tree shortcut | Completeness is guaranteed -> compare boundary heights -> count perfect subtrees directly; not valid for arbitrary trees. | [222 Count Complete Tree Nodes][lc222] |
| BST inorder / ordered iterator | Need sorted tree values, rank, or adjacent difference -> inorder -> stop early or retain only the previous value. | [230 Kth Smallest Element in a BST][lc230], [173 Binary Search Tree Iterator][lc173], [530 Minimum Absolute Difference in BST][lc530] |
| BST bounds | Use when every descendant must obey its ancestors' ordering. Pass the allowed lower/upper values down the tree; checking only a node against its parent misses violations deeper in a subtree. | [98 Validate Binary Search Tree][lc98] |
| Linked-list fast/slow / gap pointers | Use when you cannot jump directly to an index. A two-step pointer catches a one-step pointer in a cycle; a fixed gap lets the trailing pointer find a node counted from the end. | [141 Linked List Cycle][lc141], [19 Remove Nth Node From End of List][lc19] |
| Linked-list reversal / partition | Use when the answer changes links rather than values. Save the next node before changing a link so the remaining list is not lost; temporary heads simplify segment boundaries and stable groups. | [92 Reverse Linked List II][lc92], [25 Reverse Nodes in k-Group][lc25], [86 Partition List][lc86] |
| Linked-list carry / rotation | Digit arithmetic or cyclic shift -> track carry or length/tail -> reconnect once and terminate the final chain. | [2 Add Two Numbers][lc2], [61 Rotate List][lc61] |
| Random-pointer cloning | References may point anywhere -> map nodes or interleave copies -> preserve both next and random identities. | [138 Copy List with Random Pointer][lc138] |
| Trie prefix lookup | Many shared-prefix queries -> store character edges -> distinguish a prefix node from a complete word. | [208 Implement Trie][lc208], [648 Replace Words][lc648] |
| Trie + branching search | Wildcards/many words share prefixes -> traverse trie and search state together -> prune absent edges, restore board visits. | [211 Design Add and Search Words][lc211], [212 Word Search II][lc212] |

## Online structures and simulation

Start by listing the operations the problem repeatedly needs: get a minimum,
find a neighboring value, update one index, or sum a range. Choose a structure
for those operations, not for its name. A **multiset** keeps duplicates;
**lazy invalidation** leaves old entries in place until a query encounters them.
Range-query structures are usually unnecessary for a single static query.

### Recognition questions

- Do values change between queries?
- Do I repeatedly need the best item, neighboring sorted values, or a range total?
- Which operations must stay fast as the input grows?
- Are approximate answers explicitly allowed, or must results be exact?

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Heap frontier / top-k | Repeatedly need the best remaining candidate -> maintain only active candidates or k best -> push replacements after extraction. | [23 Merge k Sorted Lists][lc23], [215 Kth Largest Element in an Array][lc215] |
| Two-heap median | Use when values arrive one at a time and you need the middle value after each insertion. Keep the lower half in a max-heap and upper half in a min-heap; balance their sizes so the middle value(s) are at the tops. | [295 Find Median from Data Stream][lc295] |
| Heap + lazy invalidation | Updates leave obsolete heap entries -> keep authoritative versions in a map -> discard stale entries at query time. | [2034 Stock Price Fluctuation][lc2034] |
| Ordered multiset | Need neighbors/ranks plus duplicates -> maintain ordered values and multiplicity -> distinguish logarithmic trees from linear Python list insertion. | [480 Sliding Window Median][lc480], [729 My Calendar I][lc729] |
| Skip list | Need dynamic ordered search/insert/delete -> randomized levels -> preserve sorted links at every level; contiguous output alone needs no skip list. | [1206 Design Skiplist][lc1206]; simpler contrast: [1656 Design an Ordered Stream][lc1656] |
| Fenwick tree | Use for changing individual values and repeatedly asking for prefix/range sums. Store selected partial sums so each update/query touches `O(log n)` entries; a range sum is the difference of two prefix sums. | [307 Range Sum Query - Mutable][lc307], [315 Count of Smaller Numbers After Self][lc315] |
| Segment tree | Use for repeated range queries such as sum/min/max with updates. Each node summarizes a segment; combine child summaries in a consistent way. For many range updates, lazy tags postpone work until a smaller segment is needed. | [307 Range Sum Query - Mutable][lc307], [715 Range Module][lc715] |
| LRU / LFU eviction | Use when a full cache must remove the least recently used (LRU) or least frequently used (LFU) item. A dictionary finds entries quickly; linked order or frequency groups track which entry to remove. | [146 LRU Cache][lc146], [460 LFU Cache][lc460] |
| Bloom filter / Count-Min Sketch | Exact storage is too costly and approximation is acceptable -> hash into bits/counters -> accept false positives/overestimates, not exact answers. | No direct standard LeetCode equivalent here; [217][lc217] and [347][lc347] require exact answers and are contrasts, not substitutes. |
| Event simulation | Explicit rules define chronological behavior -> model event order and live state -> resolve all same-time events consistently. | [621 Task Scheduler][lc621], [1834 Single-Threaded CPU][lc1834] |

## SQL

First define what one output row represents: one customer, employee, group,
or event. This is the **grain**. A join can accidentally multiply rows when
one item matches several others. SQL **window functions** calculate across
related rows without collapsing them into one row as `GROUP BY` does.

### Recognition questions

- What does one output row represent?
- Am I combining related rows, reducing a group to a total, or keeping rows while ranking them?
- Do I need a previous/next event or an entity with no match?
- Could joining tables duplicate values before I count or sum them?

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Joins / aggregation | Output combines entities or collapses groups -> choose output grain -> prevent join fan-out before grouping. | [175 Combine Two Tables][lc175], [570 Managers with at Least 5 Direct Reports][lc570] |
| Ranking windows | Use for top values within each group. `ROW_NUMBER` numbers rows individually, `RANK` shares ranks but leaves gaps after ties, and `DENSE_RANK` shares ranks without gaps; choose the rule the question asks for. | [178 Rank Scores][lc178], [185 Department Top Three Salaries][lc185] |
| Ordered adjacency / islands | Compare previous/next rows or identify consecutive runs -> specify partition/order -> LAG/LEAD and accumulate boundary flags. | [180 Consecutive Numbers][lc180] |
| Missing-match anti-join | Entities with no related rows -> NOT EXISTS or left join + null test -> avoid NOT IN surprises with NULL. | [183 Customers Who Never Order][lc183] |
| Temporal joins | Match dated events to effective intervals -> define endpoints and grain -> check actual date gaps, not just neighboring rows. | [197 Rising Temperature][lc197] |

Schema/index design and dialect-specific JSON queries are also covered in the
[SQL guide](./SQL/README.md); they are not universal algorithm templates.

## Common extensions (not dedicated implementations here)

These are later topics, not prerequisites for starting LeetCode. A **DAG** is a
directed graph with no cycles. A **bitmask** uses individual binary digits to
record which items have been selected. In digit DP, **tight** means the chosen
digits still match the upper bound, and **started** tracks whether a non-leading
digit has been chosen.

| Pattern | Steps to identify and apply | LeetCode practice |
| --- | --- | --- |
| Tree DP | Local choices constrain child choices -> return a result per allowed mode -> combine independent child subtrees. | [337 House Robber III][lc337] |
| DAG DP | Directed dependencies are acyclic -> order states topologically -> aggregate predecessor results. | [329 Longest Increasing Path in a Matrix][lc329] |
| Bitmask DP | Small item universe, subset history matters -> encode used items in bits -> cache mask plus necessary remaining state. | [698 Partition to K Equal Sum Subsets][lc698] |
| Digit DP | Count bounded integers satisfying digit rules -> state position/tightness/started status plus rule state -> count valid suffixes. | [902 Numbers At Most N Given Digit Set][lc902] |

## Avoid the common misclassification

| Tempting shortcut | What to ask instead |
| --- | --- |
| "It says subarray, so use a window." | Can removing the left item reliably repair the rule? Negative sums may require prefix sums or a different technique. |
| "It is sorted, so use binary search." | Do you need one position, or must you compare/merge several values? Two pointers may fit better. |
| "It asks for a minimum, so use greedy." | Can you prove the local choice is safe? The coin example shows why the output alone is not enough. |
| "It is a grid, so use grid DP." | Do dependencies have cycles? General movement often needs graph traversal instead. |
| "It needs queries, so use a segment tree." | Are values changing? Plain prefix sums may suffice; a Fenwick tree may be simpler for point updates and sums. |

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
