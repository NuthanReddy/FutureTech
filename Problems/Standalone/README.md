# Legacy standalone problems

Scripts remain in `Problems/`; filenames are not reliable pattern labels.
Mappings describe the implemented approach or explicitly marked legacy intent,
not a claim that every implementation is correct.

## Classification checklist

1. Identify the actual output: enumerate, count, optimize, find a path, or simulate events.
2. Use the cue: repeated states -> DP; prefix equality -> hash counting; sorted inputs -> merge/compaction; local cells -> grid scan.
3. Choose minimal state and transitions: indices, running sums, budgets, intervals, or queues.
4. State the invariant and limits: distinguish contiguous substrings from subsequences, greedy coverage from DP, and approximate counts from exact counts.

## Problem mapping: cues -> chosen pattern

| Script / problem | Cue -> chosen pattern |
|---|---|
| [BeautifulSum.py](../BeautifulSum.py) | Most disjoint zero-sum segments -> prefix-sum set + earliest-finish greedy, resetting after a match. |
| [DistanceCount.py](../DistanceCount.py) | Fixed 3x3 neighborhood sums -> bounded grid stencil including self; legacy output slicing differs from a same-shape result. |
| [duplicatewithwindow.py](../duplicatewithwindow.py) | Keep at most two sorted duplicates -> read/write two-pointer compaction (legacy indexing is incomplete). |
| [ElectricWire.py](../ElectricWire.py) | Lowest straight route over trees -> minimax row/column scan; legacy extrema code does not maintain route maxima correctly. |
| [fib.py](../fib.py) | Two predecessor recurrence -> constant-space rolling DP with F(0)=0, F(1)=1. |
| [fibonoci.py: fib](../fibonoci.py) | Fibonacci-style recurrence -> naive recursion without memoization, using bases 1, 1. |
| [fibonoci.py: fib2](../fibonoci.py) | Repeated predecessor subproblems -> bottom-up array DP, using bases 1, 1. |
| [fibonoci.py: fib3](../fibonoci.py) | Repeated increasing-index queries -> persistent incremental DP cache. |
| [IsMazeSolvable.py](../IsMazeSolvable.py) | Right/down path existence -> DFS backtracking, not general four-direction reachability; legacy goal/aliasing issues remain. |
| [LocalExtrema.py: solution](../LocalExtrema.py) | Peaks/valleys with equal-height plateaus -> run-aware linear scan; legacy boundary checks are incomplete. |
| [LocalExtrema.py: foo](../LocalExtrema.py) | Matrix anti-diagonals -> diagonal-index traversal attempt; unfinished scratch implementation, not a working traversal. |
| [LongestPalindromicSubSequence.py](../LongestPalindromicSubSequence.py) | Contiguous palindrome result -> center expansion, not longest-subsequence DP; even-center logic is inconsistent. |
| [MaxBombDamage.py](../MaxBombDamage.py) | Best bomb location -> brute-force square-window enumeration; O(rows * cols * (2r+1)^2), not general O(m^2). |
| [MergeArray.py](../MergeArray.py) | Sorted inputs and destination capacity -> backward two-pointer merge; pre-existing syntax error prevents execution. |
| [MergeSortedArrays.py](../MergeSortedArrays.py) | Sorted heads -> recursive merge; repeated slicing adds copying cost. |
| [MinCoinsForSum.py](../MinCoinsForSum.py) | Fewest reusable coins -> unbounded amount DP, not denomination greedy; zero amount is not handled. |
| [MinCuts.py](../MinCuts.py) | Minimum palindrome cuts -> interval palindrome table + prefix optimization DP. |
| [MinCuts2.py](../MinCuts2.py) | Repeated substring partitions -> memoized interval DP; concatenated index keys can collide. |
| [minFountains.py](../minFountains.py) | Fewest ranges covering a line -> farthest-reach greedy interval coverage, despite the `dp` variable name. |
| [NonOverlappingZeroSegments.py](../NonOverlappingZeroSegments.py) | Most disjoint zero sums -> prefix-index hashing + earliest-finish greedy boundary tracking. |
| [numDict.py](../numDict.py) | Words sharing keypad codes -> hash grouping via character encoding; mapping supports only listed letters. |
| [ReverseToEquate.py](../ReverseToEquate.py) | Arbitrary reversals preserve element counts -> multiset equality via frequency map. |
| [StableSegments.py](../StableSegments.py) | Equal endpoints with an interior-sum constraint -> algebraic prefix-sum hash counting; legacy formula/indexing needs review. |
| [stockTrade.py](../stockTrade.py) | Streaming orders with leftovers -> per-stock seller-priority/buyer-FIFO queue simulation; incomplete parsing and queue integration. |
| [Test.py](../Test.py) | Target-sum rectangles -> intended row-band compression + prefix-frequency subarray counting; legacy band loops do not enumerate all row bands. |
| [TopKWords.py](../TopKWords.py) | Approximate frequent words -> Count-Min Sketch + heap selection; collisions and stale stored estimates limit accuracy. |
| [WaysToSum.py](../WaysToSum.py) | Ordered scoring sequences without adjacent fours -> memoized DP on remaining runs and previous-four flag. |

## Exclusions

- `Basics.py`: list-slicing examples only; no algorithmic problem.
- `IndexMailIds.py`: sample email/text data only; no matching implementation.
- `Stripe.py`: greeting function only; no algorithmic problem.
- Commented-out alternatives, printing/check helpers, and sample invocations are
  not separate problems. `Test.py` is included because it implements algorithms.
