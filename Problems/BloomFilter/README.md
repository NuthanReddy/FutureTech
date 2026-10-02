# Bloom Filter

Approximate membership with certain negatives and possible false positives.

## Pattern identification steps

1. Look for large membership workloads where false positives are acceptable.
2. Choose capacity and false-positive rate; use an insert-only filter for these problems.
3. Normalize keys consistently and query before inserting when detecting repeats.
4. Preserve inserted bits: absence is certain, but a positive needs exact verification when correctness matters.
5. Use an exact set for guaranteed membership or deletion; these examples retain ground truth for measurement, not memory savings.

## Problem cues → patterns

| Problem | Identification cue → pattern |
| --- | --- |
| [Duplicate URLs](duplicate_url_detector.py) | Stream needs probable-repeat flags → query-before-add Bloom membership. |
| [Spell checker](spell_checker.py) | Quickly reject unknown dictionary words → lowercase dictionary Bloom membership; positives remain tentative. |
