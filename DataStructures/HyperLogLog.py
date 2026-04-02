from __future__ import annotations

import hashlib
import math
from typing import Any


class HyperLogLog:
    """Probabilistic cardinality estimator for counting distinct elements.

    HyperLogLog estimates the number of unique items seen in a stream using
    only a small, fixed amount of memory regardless of the actual cardinality.

    Algorithm overview:
        1. Hash each item to a 64-bit value.
        2. Use the first *p* bits as a register index (m = 2^p registers).
        3. Count leading zeros in the remaining bits (+1) as the *rank*.
        4. Store the maximum rank observed per register.
        5. Use the harmonic mean of 2^(-rank) across registers, with bias
           correction (α_m), to produce the raw estimate.
        6. Apply small-range correction (LinearCounting) and large-range
           correction (hash-collision compensation for 64-bit hashes).

    Accuracy: standard error ≈ 1.04 / √m  (e.g. ~0.81 % for p=14).

    Time complexity:
        add       — O(1)
        estimate  — O(m)
        merge     — O(m)
    Space complexity: O(m) = O(2^p) registers (1 byte each)
    """

    def __init__(self, p: int = 14) -> None:
        """Create a HyperLogLog counter.

        Args:
            p: Precision — number of bits for register indexing.
               Must be in [4, 16].  m = 2^p registers are allocated.
        """
        if not (4 <= p <= 16):
            raise ValueError("p must be between 4 and 16")

        self._p = p
        self._m = 1 << p                     # number of registers
        self._registers = bytearray(self._m)  # max rank per bucket
        self._alpha = self._compute_alpha(self._m)

    # ---- public API ----

    def add(self, item: Any) -> None:
        """Observe *item*.  O(1)."""
        h = self._hash64(item)

        # First p bits → register index
        idx = h >> (64 - self._p)
        # Remaining (64-p) bits → rank (position of first 1-bit from left + 1)
        bits = 64 - self._p
        remaining = h & ((1 << bits) - 1)
        rank = self._leading_zeros(remaining, bits) + 1

        if rank > self._registers[idx]:
            self._registers[idx] = rank

    def estimate(self) -> float:
        """Return the estimated number of distinct items seen.  O(m)."""
        m = self._m

        # Raw harmonic-mean estimator
        indicator = sum(2.0 ** (-r) for r in self._registers)
        raw = self._alpha * m * m / indicator

        # Small-range correction (LinearCounting)
        if raw <= 2.5 * m:
            zeros = self._registers.count(0)
            if zeros > 0:
                return m * math.log(m / zeros)
            return raw

        # Large-range correction (64-bit hash collision)
        two_64 = 2.0 ** 64
        if raw > two_64 / 30.0:
            return -two_64 * math.log1p(-raw / two_64)

        return raw

    def merge(self, other: HyperLogLog) -> HyperLogLog:
        """Return a **new** HLL that is the register-wise max of *self* and *other*.

        Both instances must have the same precision *p*.  O(m).
        """
        if self._p != other._p:
            raise ValueError("Cannot merge HyperLogLogs with different p")

        merged = HyperLogLog(p=self._p)
        for i in range(self._m):
            merged._registers[i] = max(self._registers[i], other._registers[i])
        return merged

    # ---- dunder helpers ----

    def __repr__(self) -> str:
        return (
            f"HyperLogLog(p={self._p}, m={self._m}, "
            f"estimate={self.estimate():.0f})"
        )

    # ---- properties ----

    @property
    def precision(self) -> int:
        """Number of bits used for register indexing."""
        return self._p

    @property
    def num_registers(self) -> int:
        """Total number of registers (2^p)."""
        return self._m

    # ---- internal helpers ----

    @staticmethod
    def _hash64(item: Any) -> int:
        """Deterministic 64-bit hash of *item* via SHA-256."""
        raw = str(item).encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        return int.from_bytes(digest[:8], "big")

    @staticmethod
    def _leading_zeros(value: int, bits: int) -> int:
        """Count leading zero bits in *value* treated as a *bits*-wide integer."""
        if value == 0:
            return bits
        # bit_length gives position of highest set bit
        return bits - value.bit_length()

    @staticmethod
    def _compute_alpha(m: int) -> float:
        """Bias-correction constant α_m."""
        if m == 16:
            return 0.673
        if m == 32:
            return 0.697
        if m == 64:
            return 0.709
        # General formula for m >= 128
        return 0.7213 / (1.0 + 1.079 / m)


if __name__ == "__main__":
    # --- 1. Add many distinct items ---
    n = 100_000
    hll = HyperLogLog(p=14)
    for i in range(n):
        hll.add(f"user-{i}")

    est = hll.estimate()
    error_pct = abs(est - n) / n * 100
    print(hll)
    print(f"Actual distinct: {n:>10}")
    print(f"Estimated:       {est:>10.0f}")
    print(f"Relative error:  {error_pct:>9.2f}%")

    # --- 2. Theoretical error bound ---
    m = hll.num_registers
    theory = 1.04 / math.sqrt(m) * 100
    print(f"Expected σ:      {theory:>9.2f}%")

    # --- 3. Merge two HLLs ---
    hll_a = HyperLogLog(p=14)
    hll_b = HyperLogLog(p=14)

    # First half of items in A, second half in B (with some overlap)
    overlap = 10_000
    split = n // 2
    for i in range(split + overlap):
        hll_a.add(f"user-{i}")
    for i in range(split, n):
        hll_b.add(f"user-{i}")

    hll_merged = hll_a.merge(hll_b)
    merged_est = hll_merged.estimate()
    merged_err = abs(merged_est - n) / n * 100
    print(f"\n--- Merge demo ---")
    print(f"HLL-A estimate:  {hll_a.estimate():>10.0f}  (saw {split + overlap} items)")
    print(f"HLL-B estimate:  {hll_b.estimate():>10.0f}  (saw {n - split} items)")
    print(f"Merged estimate: {merged_est:>10.0f}  (actual {n} distinct)")
    print(f"Merged error:    {merged_err:>9.2f}%")
