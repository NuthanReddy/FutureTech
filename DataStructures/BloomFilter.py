from __future__ import annotations

import hashlib
import math
from typing import Any


class BloomFilter:
    """Probabilistic set that supports fast membership queries.

    A Bloom filter can tell you with certainty that an element is *not* in
    the set.  A positive answer may be a false positive.

    Time complexity:
        add            — O(k)  where k = number of hash functions
        might_contain  — O(k)
    Space complexity: O(m)  where m = bit-array size
    """

    def __init__(
        self,
        expected_items: int = 1000,
        false_positive_rate: float = 0.01,
    ) -> None:
        """Create a BloomFilter.

        Args:
            expected_items: Estimated number of elements to insert.
            false_positive_rate: Desired false-positive probability (0 < p < 1).
        """
        if expected_items <= 0:
            raise ValueError("expected_items must be positive")
        if not (0 < false_positive_rate < 1):
            raise ValueError("false_positive_rate must be in (0, 1)")

        self._expected_items = expected_items
        self._fp_rate = false_positive_rate

        # Optimal bit-array size: m = -(n * ln(p)) / (ln2)^2
        self._size = self._optimal_size(expected_items, false_positive_rate)
        # Optimal hash count: k = (m / n) * ln2
        self._num_hashes = self._optimal_hashes(self._size, expected_items)

        self._bits = bytearray(self._size)
        self._count = 0

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        """Calculate optimal bit-array size."""
        m = -(n * math.log(p)) / (math.log(2) ** 2)
        return max(int(math.ceil(m)), 1)

    @staticmethod
    def _optimal_hashes(m: int, n: int) -> int:
        """Calculate optimal number of hash functions."""
        k = (m / n) * math.log(2)
        return max(int(round(k)), 1)

    def _get_hash_indices(self, item: Any) -> list[int]:
        """Generate *k* independent bit positions using double hashing.

        Uses SHA-256 split into two 128-bit halves (h1, h2), then computes
        ``(h1 + i * h2) % m`` for i in range(k).
        """
        raw = str(item).encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        h1 = int.from_bytes(digest[:16], "big")
        h2 = int.from_bytes(digest[16:], "big")
        return [(h1 + i * h2) % self._size for i in range(self._num_hashes)]

    def add(self, item: Any) -> None:
        """Add *item* to the filter.

        Time complexity: O(k)
        """
        for idx in self._get_hash_indices(item):
            self._bits[idx] = 1
        self._count += 1

    def might_contain(self, item: Any) -> bool:
        """Return True if *item* is *possibly* in the set, False if definitely not.

        Time complexity: O(k)
        """
        return all(self._bits[idx] for idx in self._get_hash_indices(item))

    def __contains__(self, item: Any) -> bool:
        """Support ``item in bloom_filter`` syntax."""
        return self.might_contain(item)

    def __repr__(self) -> str:
        return (
            f"BloomFilter(size={self._size}, hashes={self._num_hashes}, "
            f"items_added={self._count})"
        )

    @property
    def size(self) -> int:
        """Number of bits in the filter."""
        return self._size

    @property
    def num_hashes(self) -> int:
        """Number of hash functions."""
        return self._num_hashes

    @property
    def items_added(self) -> int:
        """Number of items that have been added."""
        return self._count


if __name__ == "__main__":
    n = 10_000
    fp_rate = 0.01
    bf = BloomFilter(expected_items=n, false_positive_rate=fp_rate)
    print(bf)

    # Insert n items
    for i in range(n):
        bf.add(f"item-{i}")

    # Verify all inserted items are found
    false_negatives = sum(
        1 for i in range(n) if f"item-{i}" not in bf
    )
    print(f"False negatives: {false_negatives}  (should always be 0)")

    # Measure actual false-positive rate on items never inserted
    test_count = 100_000
    false_positives = sum(
        1 for i in range(n, n + test_count) if f"item-{i}" in bf
    )
    actual_fp = false_positives / test_count
    print(f"False positives: {false_positives}/{test_count} = {actual_fp:.4%}")
    print(f"Target FP rate: {fp_rate:.4%}")
