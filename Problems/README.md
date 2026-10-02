# Problems

Problem solutions are organized directly under `Problems/` by reusable
algorithmic pattern. The Top Interview 150 solutions are intentionally
distributed across these pattern folders rather than placed under a separate
`Top150/` directory.

## Pattern identification steps

See the [pattern reference and decision tree](./PATTERNS.md) for identification
steps, subpatterns, and linked LeetCode practice problems.

1. Identify the output: existence, count, optimum, enumeration, or online query.
2. Inspect structure: contiguous ranges, sorted values, links, trees, graphs, or relational rows.
3. Match constraints to a category below; choose its smallest sufficient state.
4. State the invariant and check the pattern's assumptions before coding.

Each category guide includes recognition steps and a problem-to-pattern map;
solution docstrings/comments give problem-specific identification cues.

## Top Interview 150 pattern groups

- [Arrays and Hashing](./ArraysHashing/README.md)
- [Backtracking](./Backtracking/README.md)
- [Binary Search](./BinarySearch/README.md)
- [Bit Manipulation](./Bit_Manipulation/README.md)
- [Dynamic Programming](./Dynamic%20Programming/README.md)
- [Graphs](./Graph/README.md)
- [Intervals](./Intervals/README.md)
- [Linked Lists](./LinkedList/README.md)
- [Matrix](./Matrix/README.md)
- [Sliding Window](./SlidingWindow/README.md)
- [Stack and Queue](./StackQueue/README.md)
- [Trees](./Trees/README.md)
- [Trie](./Trie/README.md)
- [Two Pointers](./TwoPointers/README.md)

Each group README includes problem statements, the selected pattern and
invariant, alternative approaches, complexity, edge cases, and critique.
Existing standalone exercises remain in their original folders when they
represent a different implementation or learning objective.

## Additional pattern groups

- [Bloom Filter](./BloomFilter/README.md)
- [BST](./BST/README.md)
- [Combinations](./Combinations/README.md)
- [Fenwick Tree](./FenwickTree/README.md)
- [Greedy](./Greedy/README.md)
- [Heap](./Heap/README.md)
- [LRU / LFU Cache](./LRUCache/README.md)
- [Segment Tree](./SegmentTree/README.md)
- [Skip List](./SkipList/README.md)
- [Sorted Set](./SortedSet/README.md)
- [SQL](./SQL/README.md)
- [Standalone exercises](./Standalone/README.md)
- [Strings](./Strings/README.md)
- [Union-Find](./UnionFind/README.md)

## Running focused tests

From the repository root:

```powershell
$tests = Get-ChildItem tests -Filter 'test_top150_*.py'
uv run --no-sync pytest $tests tests/test_arrays_hashing.py -q
```
