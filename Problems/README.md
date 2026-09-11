# Problems

Problem solutions are organized directly under `Problems/` by reusable
algorithmic pattern. The Top Interview 150 solutions are intentionally
distributed across these pattern folders rather than placed under a separate
`Top150/` directory.

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
- [Sliding Window](./SlidingWindow/TopInterview150.md)
- [Stack and Queue](./StackQueue/README.md)
- [Trees](./Trees/README.md)
- [Trie](./Trie/README.md)
- [Two Pointers](./TwoPointers/README.md)

Each group README includes problem statements, the selected pattern and
invariant, alternative approaches, complexity, edge cases, and critique.
Existing standalone exercises remain in their original folders when they
represent a different implementation or learning objective.

## Running focused tests

From the repository root:

```powershell
$tests = Get-ChildItem tests -Filter 'test_top150_*.py'
uv run --no-sync pytest $tests tests/test_arrays_hashing.py -q
```
