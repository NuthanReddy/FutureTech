import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.CountMinSketch import CountMinSketch
from DataStructures.HyperLogLog import HyperLogLog
from DataStructures.CuckooFilter import CuckooFilter


# --- CountMinSketch ---

class TestCountMinSketch:
    def test_estimates_gte_actual(self) -> None:
        cms = CountMinSketch(width=1000, depth=5)
        cms.add("a", 3)
        cms.add("b", 7)
        cms.add("a", 2)
        assert cms.estimate("a") >= 5
        assert cms.estimate("b") >= 7

    def test_total(self) -> None:
        cms = CountMinSketch(width=100, depth=4)
        cms.add("x", 10)
        cms.add("y", 5)
        assert cms.total == 15

    def test_merge(self) -> None:
        cms1 = CountMinSketch(width=100, depth=4)
        cms2 = CountMinSketch(width=100, depth=4)
        cms1.add("a", 3)
        cms2.add("a", 7)
        merged = cms1.merge(cms2)
        assert merged.estimate("a") >= 10
        assert merged.total == cms1.total + cms2.total

    def test_merge_incompatible_raises(self) -> None:
        cms1 = CountMinSketch(width=100, depth=4)
        cms2 = CountMinSketch(width=200, depth=4)
        with pytest.raises(ValueError):
            cms1.merge(cms2)

    def test_epsilon_delta_constructor(self) -> None:
        cms = CountMinSketch(epsilon=0.01, delta=0.001)
        assert cms.width > 0
        assert cms.depth > 0

    def test_repr(self) -> None:
        cms = CountMinSketch(width=100, depth=4)
        assert "CountMinSketch" in repr(cms)


# --- HyperLogLog ---

class TestHyperLogLog:
    def test_estimate_within_10_percent(self) -> None:
        hll = HyperLogLog(p=14)
        n = 10000
        for i in range(n):
            hll.add(f"item_{i}")
        estimate = hll.estimate()
        error = abs(estimate - n) / n
        assert error < 0.10, f"Estimate {estimate} is {error:.2%} off from {n}"

    def test_empty_estimate(self) -> None:
        hll = HyperLogLog()
        assert hll.estimate() == 0.0

    def test_merge(self) -> None:
        hll1 = HyperLogLog(p=10)
        hll2 = HyperLogLog(p=10)
        for i in range(5000):
            hll1.add(f"a_{i}")
        for i in range(5000):
            hll2.add(f"b_{i}")
        merged = hll1.merge(hll2)
        estimate = merged.estimate()
        error = abs(estimate - 10000) / 10000
        assert error < 0.15, f"Merged estimate {estimate} too far from 10000"

    def test_merge_different_precision_raises(self) -> None:
        hll1 = HyperLogLog(p=10)
        hll2 = HyperLogLog(p=12)
        with pytest.raises(ValueError):
            hll1.merge(hll2)

    def test_precision_property(self) -> None:
        hll = HyperLogLog(p=12)
        assert hll.precision == 12
        assert hll.num_registers == 2 ** 12

    def test_invalid_precision_raises(self) -> None:
        with pytest.raises(ValueError):
            HyperLogLog(p=3)
        with pytest.raises(ValueError):
            HyperLogLog(p=17)


# --- CuckooFilter ---

class TestCuckooFilter:
    def test_insert_and_contains(self) -> None:
        cf = CuckooFilter(capacity=1024)
        assert cf.insert("hello") is True
        assert cf.contains("hello") is True
        assert "hello" in cf

    def test_no_false_negatives(self) -> None:
        cf = CuckooFilter(capacity=1024)
        items = [f"item_{i}" for i in range(200)]
        for item in items:
            cf.insert(item)
        for item in items:
            assert cf.contains(item), f"False negative for {item}"

    def test_delete(self) -> None:
        cf = CuckooFilter(capacity=1024)
        cf.insert("abc")
        assert cf.delete("abc") is True
        assert cf.contains("abc") is False

    def test_delete_nonexistent(self) -> None:
        cf = CuckooFilter(capacity=1024)
        assert cf.delete("nope") is False

    def test_len(self) -> None:
        cf = CuckooFilter(capacity=1024)
        assert len(cf) == 0
        cf.insert("a")
        cf.insert("b")
        assert len(cf) == 2
        cf.delete("a")
        assert len(cf) == 1

    def test_repr(self) -> None:
        cf = CuckooFilter(capacity=64)
        assert "CuckooFilter" in repr(cf)

    def test_invalid_params_raise(self) -> None:
        with pytest.raises(ValueError):
            CuckooFilter(capacity=0)
