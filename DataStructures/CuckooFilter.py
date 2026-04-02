from __future__ import annotations

import hashlib
import random
from typing import Any


class CuckooFilter:
    """Space-efficient probabilistic set that supports insert, lookup, and **delete**.

    A Cuckoo Filter stores compact fingerprints of items in a hash table
    that uses cuckoo hashing to resolve collisions.  Like a Bloom filter it
    may produce false positives, but it additionally supports deletion —
    something a standard Bloom filter cannot do safely.

    Trade-offs vs. Bloom filter:
        • Supports deletion with no false-negative risk.
        • Slightly higher memory per item for equivalent false-positive rate,
          but better cache locality (fewer memory accesses per lookup).
        • Fixed capacity — the filter can become full.

    Time complexity  (amortised):
        insert   — O(1)   (worst-case O(max_kicks) on eviction chains)
        contains — O(1)   (check two buckets of ≤ bucket_size entries each)
        delete   — O(1)
    Space complexity: O(capacity × bucket_size × fingerprint_size) bits

    False-positive rate ≈ 2 × bucket_size / 2^fingerprint_size
    """

    def __init__(
        self,
        capacity: int = 1024,
        bucket_size: int = 4,
        fingerprint_size: int = 8,
        max_kicks: int = 500,
    ) -> None:
        """Create a CuckooFilter.

        Args:
            capacity: Number of buckets in the filter.
            bucket_size: Maximum fingerprints stored per bucket.
            fingerprint_size: Fingerprint length in bits (1–64).
            max_kicks: Maximum eviction relocations before declaring the
                       filter full.

        Raises:
            ValueError: On invalid parameter values.
        """
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        if bucket_size <= 0:
            raise ValueError("bucket_size must be positive")
        if not (1 <= fingerprint_size <= 64):
            raise ValueError("fingerprint_size must be in [1, 64]")
        if max_kicks <= 0:
            raise ValueError("max_kicks must be positive")

        self._capacity = capacity
        self._bucket_size = bucket_size
        self._fingerprint_size = fingerprint_size
        self._max_kicks = max_kicks
        self._fp_mask = (1 << fingerprint_size) - 1

        # Each bucket is a list holding up to *bucket_size* fingerprints.
        self._buckets: list[list[int]] = [[] for _ in range(capacity)]
        self._count = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _fingerprint(self, item: Any) -> int:
        """Derive a non-zero fingerprint from *item*.

        Uses SHA-256 and masks to *fingerprint_size* bits.  A zero
        fingerprint is remapped to 1 so that zero can represent "empty".

        Time complexity: O(1)
        """
        raw = str(item).encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        fp = int.from_bytes(digest[:8], "big") & self._fp_mask
        return fp if fp != 0 else 1

    def _hash1(self, item: Any) -> int:
        """Compute the primary bucket index for *item*.

        Time complexity: O(1)
        """
        raw = str(item).encode("utf-8")
        digest = hashlib.sha256(raw).digest()
        # Use bytes 8–16 so that hash1 is independent of the fingerprint.
        h = int.from_bytes(digest[8:16], "big")
        return h % self._capacity

    def _hash2(self, index: int, fingerprint: int) -> int:
        """Compute the alternate bucket index via ``index XOR hash(fingerprint)``.

        Time complexity: O(1)
        """
        fp_bytes = fingerprint.to_bytes(8, "big")
        h = int.from_bytes(hashlib.sha256(fp_bytes).digest()[:8], "big")
        return (index ^ h) % self._capacity

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def insert(self, item: Any) -> bool:
        """Insert *item* into the filter.

        Returns True on success, False if the filter is full (after
        *max_kicks* evictions).

        Time complexity: O(1) amortised, O(max_kicks) worst-case.
        """
        fp = self._fingerprint(item)
        i1 = self._hash1(item)
        i2 = self._hash2(i1, fp)

        # Try both candidate buckets first.
        for idx in (i1, i2):
            if len(self._buckets[idx]) < self._bucket_size:
                self._buckets[idx].append(fp)
                self._count += 1
                return True

        # Both full — begin eviction chain.
        idx = random.choice((i1, i2))
        for _ in range(self._max_kicks):
            evict_pos = random.randrange(len(self._buckets[idx]))
            fp, self._buckets[idx][evict_pos] = (
                self._buckets[idx][evict_pos],
                fp,
            )
            idx = self._hash2(idx, fp)
            if len(self._buckets[idx]) < self._bucket_size:
                self._buckets[idx].append(fp)
                self._count += 1
                return True

        # Filter is considered full.
        return False

    def contains(self, item: Any) -> bool:
        """Check whether *item* is (probably) in the filter.

        May return a false positive, but never a false negative for items
        that have been inserted and not deleted.

        Time complexity: O(1)
        """
        fp = self._fingerprint(item)
        i1 = self._hash1(item)
        i2 = self._hash2(i1, fp)
        return fp in self._buckets[i1] or fp in self._buckets[i2]

    def delete(self, item: Any) -> bool:
        """Remove *item* from the filter.

        Returns True if the item was found (and removed), False otherwise.
        Deleting an item that was never inserted may remove a different item
        that shares the same fingerprint — use with care.

        Time complexity: O(1)
        """
        fp = self._fingerprint(item)
        i1 = self._hash1(item)
        i2 = self._hash2(i1, fp)

        for idx in (i1, i2):
            if fp in self._buckets[idx]:
                self._buckets[idx].remove(fp)
                self._count -= 1
                return True
        return False

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __contains__(self, item: Any) -> bool:
        """Support ``item in cuckoo_filter`` syntax."""
        return self.contains(item)

    def __len__(self) -> int:
        """Return the number of items currently stored."""
        return self._count

    def __repr__(self) -> str:
        return (
            f"CuckooFilter(capacity={self._capacity}, "
            f"bucket_size={self._bucket_size}, "
            f"fingerprint_bits={self._fingerprint_size}, "
            f"items={self._count})"
        )


# ------------------------------------------------------------------
# Demo
# ------------------------------------------------------------------

if __name__ == "__main__":
    random.seed(42)

    # --- 1. Basic insert and membership ---
    print("=== Insert & Membership ===")
    cf = CuckooFilter(capacity=1024, bucket_size=4, fingerprint_size=12)
    items = [f"user-{i}" for i in range(200)]
    for item in items:
        assert cf.insert(item), f"Failed to insert {item}"
    print(cf)

    false_negatives = sum(1 for item in items if item not in cf)
    print(f"False negatives: {false_negatives}  (should always be 0)")

    # --- 2. Delete and verify ---
    print("\n=== Delete ===")
    to_delete = items[:50]
    for item in to_delete:
        assert cf.delete(item), f"Failed to delete {item}"
    print(f"Items after deletion: {len(cf)}")

    still_found = sum(1 for item in to_delete if item in cf)
    print(f"Deleted items still found: {still_found}  (should be 0)")

    remaining_ok = sum(1 for item in items[50:] if item in cf)
    print(f"Remaining items found: {remaining_ok}/{len(items[50:])}  (should match)")

    # --- 3. False positive rate ---
    print("\n=== False Positive Rate ===")
    test_count = 100_000
    false_positives = sum(
        1 for i in range(test_count) if f"random-key-{i}" in cf
    )
    actual_fp = false_positives / test_count
    # Theoretical FP ≈ 2 * bucket_size / 2^fingerprint_size
    theoretical_fp = 2 * 4 / (2 ** 12)
    print(f"False positives: {false_positives}/{test_count} = {actual_fp:.4%}")
    print(f"Theoretical upper bound: ~{theoretical_fp:.4%}")

    # --- 4. Filter full behaviour ---
    print("\n=== Filter Full ===")
    small_cf = CuckooFilter(capacity=16, bucket_size=4, fingerprint_size=8, max_kicks=100)
    inserted = 0
    for i in range(200):
        if small_cf.insert(f"item-{i}"):
            inserted += 1
        else:
            print(f"Filter full after inserting {inserted} items "
                  f"(capacity={16 * 4} slots)")
            break
    print(small_cf)
