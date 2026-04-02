import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.BloomFilter import BloomFilter


class TestBloomFilter:
    def test_no_false_negatives(self) -> None:
        bf = BloomFilter(expected_items=1000, false_positive_rate=0.01)
        items = [f"item_{i}" for i in range(500)]
        for item in items:
            bf.add(item)
        for item in items:
            assert item in bf, f"False negative for {item}"

    def test_false_positive_rate_reasonable(self) -> None:
        bf = BloomFilter(expected_items=1000, false_positive_rate=0.05)
        for i in range(1000):
            bf.add(f"member_{i}")
        fp_count = sum(
            1 for i in range(10000) if f"nonmember_{i}" in bf
        )
        fp_rate = fp_count / 10000
        # Allow up to 3x the configured rate to account for statistical variance
        assert fp_rate < 0.15, f"FP rate {fp_rate:.3f} too high"

    def test_items_added_count(self) -> None:
        bf = BloomFilter(expected_items=100)
        assert bf.items_added == 0
        bf.add("a")
        bf.add("b")
        assert bf.items_added == 2

    def test_repr(self) -> None:
        bf = BloomFilter(expected_items=100, false_positive_rate=0.01)
        r = repr(bf)
        assert "BloomFilter" in r

    def test_size_and_num_hashes(self) -> None:
        bf = BloomFilter(expected_items=1000, false_positive_rate=0.01)
        assert bf.size > 0
        assert bf.num_hashes > 0

    def test_contains_uses_might_contain(self) -> None:
        bf = BloomFilter(expected_items=100)
        bf.add("hello")
        assert bf.might_contain("hello") is True
        assert ("hello" in bf) == bf.might_contain("hello")

    def test_invalid_params_raise(self) -> None:
        with pytest.raises(ValueError):
            BloomFilter(expected_items=0)
        with pytest.raises(ValueError):
            BloomFilter(expected_items=100, false_positive_rate=0.0)
        with pytest.raises(ValueError):
            BloomFilter(expected_items=100, false_positive_rate=1.0)
