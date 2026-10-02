# Combinations

## Pattern identification steps

1. Look for competing slots sharing a character budget: model resource-allocation search, not just combinations of indices.
2. Choose remaining slots and character frequencies as state; branch on the next slot to fill.
3. Require `L // 2` pairs and, for odd L, one leftover center; prune insufficient pairs or characters.
4. Copy the budget for each branch so consumed characters cannot leak into sibling choices; maximize filled slots.
5. Check limits: slot-order search is factorial-scale without memoization, and fixed character allocation does not explore every allocation.

## Problem mapping: cues -> chosen pattern

| Problem | Cue -> chosen pattern; invariant/pruning |
|---|---|
| [Maximum filled palindrome slots](MaxPalindromes.py) | Shared character frequencies and competing slot lengths -> resource-allocation backtracking over slot order; consume pairs/center from a copied budget and reject infeasible slots. |

The implementation chooses pairs and centers in dictionary order rather than
branching over character allocations. A center can consume a useful pair, so
this search is not a general exhaustive optimizer over all allocations.
`test_MaxPalindromes.py` checks this problem; it is not a separate mapping entry.
