import sys
import os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DataStructures.LRUCache import LRUCache


class TestLRUCache:
    def test_get_put_basic(self) -> None:
        cache = LRUCache(3)
        cache.put("a", 1)
        cache.put("b", 2)
        assert cache.get("a") == 1
        assert cache.get("b") == 2

    def test_get_missing_key_raises(self) -> None:
        cache = LRUCache(2)
        with pytest.raises(KeyError):
            cache.get("missing")

    def test_eviction_when_capacity_exceeded(self) -> None:
        cache = LRUCache(2)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("c", 3)  # evicts "a"
        assert "a" not in cache
        assert cache.get("b") == 2
        assert cache.get("c") == 3

    def test_access_makes_item_most_recently_used(self) -> None:
        cache = LRUCache(2)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.get("a")  # "a" is now MRU, "b" is LRU
        cache.put("c", 3)  # evicts "b"
        assert "b" not in cache
        assert cache.get("a") == 1
        assert cache.get("c") == 3

    def test_put_existing_key_updates_value_and_moves_to_front(self) -> None:
        cache = LRUCache(2)
        cache.put("a", 1)
        cache.put("b", 2)
        cache.put("a", 10)  # update "a", now MRU
        cache.put("c", 3)  # evicts "b"
        assert "b" not in cache
        assert cache.get("a") == 10

    def test_delete(self) -> None:
        cache = LRUCache(3)
        cache.put("x", 100)
        deleted_val = cache.delete("x")
        assert deleted_val == 100
        assert "x" not in cache
        assert len(cache) == 0

    def test_delete_missing_raises(self) -> None:
        cache = LRUCache(2)
        with pytest.raises(KeyError):
            cache.delete("nope")

    def test_contains(self) -> None:
        cache = LRUCache(2)
        cache.put("k", "v")
        assert "k" in cache
        assert "z" not in cache

    def test_len(self) -> None:
        cache = LRUCache(5)
        assert len(cache) == 0
        cache.put("a", 1)
        cache.put("b", 2)
        assert len(cache) == 2

    def test_capacity_one(self) -> None:
        cache = LRUCache(1)
        cache.put("a", 1)
        assert cache.get("a") == 1
        cache.put("b", 2)  # evicts "a"
        assert "a" not in cache
        assert cache.get("b") == 2

    def test_invalid_capacity_raises(self) -> None:
        with pytest.raises(ValueError):
            LRUCache(0)
        with pytest.raises(ValueError):
            LRUCache(-1)

    def test_repr(self) -> None:
        cache = LRUCache(2)
        cache.put("a", 1)
        assert "LRUCache" in repr(cache)
