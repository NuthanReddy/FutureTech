# Strings

## Pattern identification steps

1. Look for transformations based on adjacent equal characters, rather than global counts.
2. Choose run-length encoding for consecutive runs; iterate it when each term describes the previous one.
3. Keep a count for the active run; output contains exactly the completed runs.
4. Flush the last run after scanning; check the non-empty seed and positive term index.
5. Do not use run-length encoding for anagrams or compression across separated equal characters.

| Problem | Identification cue |
| --- | --- |
| [Count and Say](leetcode_38_count_and_say.py) | Each term describes consecutive runs of the previous term -> iterative run-length encoding with cached terms. |

`countAndSay` builds terms from seed `"1"` for `n >= 1`; its `rle` helper
encodes one non-empty term, not a separate problem.
