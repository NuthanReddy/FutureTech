from __future__ import annotations

import hashlib
import math
import random
from typing import Any


class CountMinSketch:
    """Probabilistic frequency table for estimating event counts in a stream.

    A Count-Min Sketch uses a 2-D array of counters (depth × width) with
    pairwise-independent hash functions to approximate the frequency of any
    item.  Estimates are **never lower** than the true count (no undercounting)
    but may overcount by at most *ε·N* with probability at most *1 − δ*, where
    *N* is the total count of all insertions.

    Accuracy guarantees (for width *w* and depth *d*):
        - ε  = e / w      (additive error factor)
        - δ  = (1/2)^d    (failure probability)

    Time complexity:
        add       — O(d)  where d = depth (number of hash functions)
        estimate  — O(d)
        merge     — O(w·d)
    Space complexity: O(w·d)
    """

    # A large Mersenne prime used in the universal hash family.
    _PRIME = (1 << 61) - 1

    def __init__(
        self,
        width: int = 0,
        depth: int = 0,
        *,
        epsilon: float = 0.0,
        delta: float = 0.0,
    ) -> None:
        """Create a Count-Min Sketch.

        Supply **either** explicit *width*/*depth* **or** *epsilon*/*delta*
        (not both).

        Args:
            width:   Number of counters per row.
            depth:   Number of rows (hash functions).
            epsilon: Desired additive error factor (0 < ε < 1).
            delta:   Desired failure probability  (0 < δ < 1).
        """
        has_dims = width > 0 and depth > 0
        has_params = epsilon > 0 and delta > 0

        if has_dims and has_params:
            raise ValueError("Provide width/depth OR epsilon/delta, not both")
        if not has_dims and not has_params:
            raise ValueError("Provide width/depth or epsilon/delta")

        if has_params:
            # w = ⌈e / ε⌉,  d = ⌈ln(1/δ)⌉
            width = int(math.ceil(math.e / epsilon))
            depth = int(math.ceil(math.log(1.0 / delta)))

        self._width = width
        self._depth = depth
        self._total: int = 0

        # depth × width counter table (list of lists for clarity)
        self._table: list[list[int]] = [
            [0] * width for _ in range(depth)
        ]

        # Pairwise-independent hash family: h_i(x) = ((a_i * x + b_i) % p) % w
        rng = random.Random(42)
        self._hash_params: list[tuple[int, int]] = [
            (rng.randint(1, self._PRIME - 1), rng.randint(0, self._PRIME - 1))
            for _ in range(depth)
        ]

    # ---- public API ----

    def add(self, item: Any, count: int = 1) -> None:
        """Increment the count for *item* by *count*.  O(d)."""
        h = self._item_hash(item)
        for row, (a, b) in enumerate(self._hash_params):
            col = ((a * h + b) % self._PRIME) % self._width
            self._table[row][col] += count
        self._total += count

    def estimate(self, item: Any) -> int:
        """Return the estimated frequency of *item* (never undercounts).  O(d)."""
        h = self._item_hash(item)
        return min(
            self._table[row][((a * h + b) % self._PRIME) % self._width]
            for row, (a, b) in enumerate(self._hash_params)
        )

    def merge(self, other: CountMinSketch) -> CountMinSketch:
        """Return a **new** sketch that is the element-wise sum of *self* and *other*.

        Both sketches must share the same dimensions and hash parameters.
        O(w·d).
        """
        if self._width != other._width or self._depth != other._depth:
            raise ValueError("Sketches must have the same width and depth")
        if self._hash_params != other._hash_params:
            raise ValueError("Sketches must share hash parameters to merge")

        merged = CountMinSketch(width=self._width, depth=self._depth)
        merged._hash_params = list(self._hash_params)
        merged._total = self._total + other._total
        for row in range(self._depth):
            for col in range(self._width):
                merged._table[row][col] = (
                    self._table[row][col] + other._table[row][col]
                )
        return merged

    # ---- dunder helpers ----

    def __repr__(self) -> str:
        return (
            f"CountMinSketch(width={self._width}, depth={self._depth}, "
            f"total={self._total})"
        )

    # ---- properties ----

    @property
    def width(self) -> int:
        """Number of counters per row."""
        return self._width

    @property
    def depth(self) -> int:
        """Number of rows (hash functions)."""
        return self._depth

    @property
    def total(self) -> int:
        """Total count of all insertions."""
        return self._total

    # ---- internal helpers ----

    @staticmethod
    def _item_hash(item: Any) -> int:
        """Deterministic 64-bit hash of *item* via SHA-256."""
        raw = str(item).encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        return int.from_bytes(digest[:8], "big")


if __name__ == "__main__":
    # --- 1. Insert items with known frequencies ---
    actual: dict[str, int] = {
        "apple": 100,
        "banana": 50,
        "cherry": 10,
        "date": 1,
        "elderberry": 500,
    }

    cms = CountMinSketch(width=1000, depth=5)
    for item, freq in actual.items():
        cms.add(item, freq)
    print(cms)

    # --- 2. Show estimates vs actual counts ---
    print("\n  Item         Actual  Estimate  Overcount?")
    print("  " + "-" * 44)
    for item, freq in actual.items():
        est = cms.estimate(item)
        over = est - freq
        print(f"  {item:<12} {freq:>6}  {est:>8}  +{over}")

    # --- 3. Demonstrate estimates >= actual (never undercount) ---
    all_ok = all(cms.estimate(it) >= ct for it, ct in actual.items())
    print(f"\nAll estimates >= actual? {all_ok}")

    # --- 4. Merge two sketches ---
    cms_a = CountMinSketch(width=1000, depth=5)
    cms_b = CountMinSketch(width=1000, depth=5)
    for item, freq in actual.items():
        half = freq // 2
        cms_a.add(item, half)
        cms_b.add(item, freq - half)

    cms_merged = cms_a.merge(cms_b)
    print(f"\nMerged sketch: {cms_merged}")
    print("  Merged estimates match single-sketch?", end=" ")
    match = all(
        cms_merged.estimate(it) == cms.estimate(it) for it in actual
    )
    print(match)
