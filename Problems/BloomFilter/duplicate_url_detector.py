# ---------------------------------------------------------------------------
# Problem: Duplicate URL Detector Using a Bloom Filter
# ---------------------------------------------------------------------------
# Given a stream of URLs (simulated), detect probable duplicates using a
# Bloom Filter.
#
# 1. Process URLs one by one. If the Bloom Filter says "might contain",
#    flag the URL as a probable duplicate.
# 2. Track actual duplicates with a regular set to measure accuracy:
#    - True positives  : correctly detected duplicates
#    - False positives : unique URLs misidentified as duplicates
#    - False negatives : should be 0 (Bloom Filter guarantee)
# 3. Show space savings compared to storing every URL in a Python set.
#
# Complexity (per URL):
#   Time:  O(k)  where k = number of hash functions
#   Space: O(m)  for the Bloom Filter bit-array (much less than a set)
# ---------------------------------------------------------------------------

from __future__ import annotations

import os
import sys

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(_project_root, "DataStructures"))

from BloomFilter import BloomFilter


def generate_url_stream(
    unique_count: int = 500,
    duplicate_count: int = 100,
    duplicate_offset: int = 50,
) -> list[str]:
    """Build a simulated URL stream with known duplicates.

    Args:
        unique_count: Number of distinct URLs to generate.
        duplicate_count: Number of duplicates to append at the end.
        duplicate_offset: Pick duplicates from the first *duplicate_offset*
            unique URLs so we know exactly which ones are repeated.

    Returns:
        Ordered list of URLs (some duplicated).
    """
    unique_urls = [f"https://example.com/page/{i}" for i in range(unique_count)]

    # Duplicate a subset deterministically.
    duplicates = [
        unique_urls[i % duplicate_offset] for i in range(duplicate_count)
    ]
    return unique_urls + duplicates


def detect_duplicates(
    url_stream: list[str],
    expected_items: int,
    false_positive_rate: float = 0.01,
) -> dict[str, object]:
    """Process a URL stream and detect duplicates with a Bloom Filter.

    Returns a dict of statistics and lists for analysis.
    """
    bf = BloomFilter(
        expected_items=expected_items,
        false_positive_rate=false_positive_rate,
    )

    # Ground-truth tracking.
    seen_set: set[str] = set()

    flagged_duplicates: list[str] = []
    true_positives: list[str] = []
    false_positives: list[str] = []
    missed_duplicates: list[str] = []  # False negatives (should stay empty).

    for url in url_stream:
        is_actually_seen = url in seen_set
        bloom_says_seen = bf.might_contain(url)

        if bloom_says_seen:
            flagged_duplicates.append(url)
            if is_actually_seen:
                true_positives.append(url)
            else:
                false_positives.append(url)
        else:
            if is_actually_seen:
                missed_duplicates.append(url)

        bf.add(url)
        seen_set.add(url)

    return {
        "bloom_filter": bf,
        "total_urls": len(url_stream),
        "unique_urls": len(seen_set),
        "flagged_duplicates": flagged_duplicates,
        "true_positives": true_positives,
        "false_positives": false_positives,
        "missed_duplicates": missed_duplicates,
    }


def estimate_set_memory(url_count: int, avg_url_len: int = 35) -> int:
    """Rough estimate of CPython set memory for *url_count* string entries.

    Each str object ≈ 50 + len(s) bytes; set entry overhead ≈ 72 bytes.
    """
    per_entry = 72 + 50 + avg_url_len
    return url_count * per_entry


if __name__ == "__main__":
    print("=" * 60)
    print("  Duplicate URL Detector Using a Bloom Filter")
    print("=" * 60)

    # -- Generate stream --------------------------------------------------------
    unique_count = 1000
    duplicate_count = 200
    stream = generate_url_stream(
        unique_count=unique_count,
        duplicate_count=duplicate_count,
        duplicate_offset=100,
    )

    print(f"\nStream length      : {len(stream)} URLs")
    print(f"Unique URLs        : {unique_count}")
    print(f"Duplicate entries  : {duplicate_count}")

    # -- Detect duplicates ------------------------------------------------------
    stats = detect_duplicates(
        stream,
        expected_items=unique_count,
        false_positive_rate=0.01,
    )

    bf: BloomFilter = stats["bloom_filter"]
    tp = stats["true_positives"]
    fp = stats["false_positives"]
    missed = stats["missed_duplicates"]
    flagged = stats["flagged_duplicates"]

    print(f"\n--- Detection Results ---")
    print(f"Flagged as duplicate : {len(flagged)}")
    print(f"True positives       : {len(tp)}")
    print(f"False positives      : {len(fp)}")
    print(f"Missed (FN)          : {len(missed)}  (should be 0)")

    if duplicate_count > 0:
        precision = len(tp) / len(flagged) if flagged else 0.0
        recall = len(tp) / duplicate_count
        print(f"\nPrecision : {precision:.2%}")
        print(f"Recall    : {recall:.2%}")

    # -- Space comparison -------------------------------------------------------
    bloom_bytes = bf.size  # 1 byte per bit in this implementation
    set_bytes = estimate_set_memory(unique_count)

    print(f"\n--- Space Comparison ---")
    print(f"Bloom filter size : {bloom_bytes:,} bytes")
    print(f"Python set estimate: {set_bytes:,} bytes")
    savings = (1 - bloom_bytes / set_bytes) * 100 if set_bytes > 0 else 0
    print(f"Space savings      : {savings:.1f}%")

    # -- Edge cases -------------------------------------------------------------
    print("\n--- Edge Cases ---")

    # Empty stream
    empty_stats = detect_duplicates([], expected_items=10)
    assert len(empty_stats["flagged_duplicates"]) == 0
    print("  Empty stream: no duplicates flagged          ✓")

    # All identical URLs
    all_same = ["https://x.com"] * 10
    same_stats = detect_duplicates(all_same, expected_items=1)
    assert len(same_stats["true_positives"]) == 9
    assert len(same_stats["false_positives"]) == 0
    assert len(same_stats["missed_duplicates"]) == 0
    print("  All-identical stream: 9/9 duplicates caught  ✓")

    # All unique URLs
    all_unique = [f"https://u.com/{i}" for i in range(100)]
    uniq_stats = detect_duplicates(all_unique, expected_items=100)
    assert len(uniq_stats["true_positives"]) == 0
    assert len(uniq_stats["missed_duplicates"]) == 0
    print(f"  All-unique stream: 0 true dup, "
          f"{len(uniq_stats['false_positives'])} false pos  ✓")

    # Single URL
    single_stats = detect_duplicates(["https://one.com"], expected_items=1)
    assert len(single_stats["flagged_duplicates"]) == 0
    print("  Single URL stream: nothing flagged           ✓")

    print("\nAll checks passed ✓")
