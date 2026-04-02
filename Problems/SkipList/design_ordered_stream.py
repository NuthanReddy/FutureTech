# ---------------------------------------------------------------------------
# Problem: Design an Ordered Stream (using a Skip List)
# ---------------------------------------------------------------------------
# You receive (id, value) pairs potentially out of order, where ids are
# positive integers starting from 1.  After each insertion, return all
# consecutive values starting from the current read pointer.
#
# Interface:
#   OrderedStream(n)           — initialise for ids 1..n
#   insert(id, value) -> list  — insert a pair and flush consecutive values
#
# Example:
#   os = OrderedStream(5)
#   os.insert(3, "ccc")   -> []          # waiting for id 1
#   os.insert(1, "aaa")   -> ["aaa"]     # flush id 1; id 2 not yet here
#   os.insert(2, "bbb")   -> ["bbb", "ccc"]  # flush ids 2 and 3
#   os.insert(5, "eee")   -> []          # waiting for id 4
#   os.insert(4, "ddd")   -> ["ddd", "eee"]  # flush ids 4 and 5
#
# We use a SkipList to hold pending (not-yet-flushed) items in sorted order
# by id, giving O(log n) insert and O(log n) search per flush step.
#
# Complexity:
#   insert : O(log n) amortised  (each element is inserted and deleted once)
#   Space  : O(n) worst case for the skip list
# ---------------------------------------------------------------------------

from __future__ import annotations

import os
import sys

_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(_project_root, "DataStructures"))

from SkipList import SkipList


class OrderedStream:
    """Ordered stream backed by a SkipList for pending items.

    Args:
        n: The total number of ids expected (1-based).
    """

    def __init__(self, n: int) -> None:
        if n < 0:
            raise ValueError("n must be non-negative")
        self._n = n
        self._pointer = 1  # Next id we want to flush.
        self._pending = SkipList()

    def insert(self, id_num: int, value: str) -> list[str]:
        """Insert *(id_num, value)* and return consecutive values from pointer.

        Args:
            id_num: The id of this value (1-based).
            value: The associated string value.

        Returns:
            A list of values flushed in id order.

        Raises:
            ValueError: If *id_num* is outside [1, n].
        """
        if id_num < 1 or id_num > self._n:
            raise ValueError(f"id_num {id_num} out of range [1, {self._n}]")

        self._pending.insert(id_num, value)

        result: list[str] = []
        while True:
            val = self._pending.search(self._pointer)
            if val is None:
                break
            result.append(val)
            self._pending.delete(self._pointer)
            self._pointer += 1

        return result

    @property
    def pointer(self) -> int:
        """Current read pointer (next expected id)."""
        return self._pointer


if __name__ == "__main__":
    print("=" * 60)
    print("  Design an Ordered Stream (Skip List)")
    print("=" * 60)

    # -- Example walkthrough ----------------------------------------------------
    print("\n--- Example ---")
    os_stream = OrderedStream(5)
    operations = [
        (3, "ccc"),
        (1, "aaa"),
        (2, "bbb"),
        (5, "eee"),
        (4, "ddd"),
    ]

    for id_num, value in operations:
        result = os_stream.insert(id_num, value)
        print(f"  insert({id_num}, {value!r})  ->  {result}")

    # -- Assertions for correctness -------------------------------------------
    os2 = OrderedStream(5)
    assert os2.insert(3, "ccc") == []
    assert os2.insert(1, "aaa") == ["aaa"]
    assert os2.insert(2, "bbb") == ["bbb", "ccc"]
    assert os2.insert(5, "eee") == []
    assert os2.insert(4, "ddd") == ["ddd", "eee"]
    print("  Assertions passed ✓")

    # -- In-order insertion (everything flushes immediately) -------------------
    print("\n--- In-order insertion ---")
    os3 = OrderedStream(4)
    assert os3.insert(1, "a") == ["a"]
    assert os3.insert(2, "b") == ["b"]
    assert os3.insert(3, "c") == ["c"]
    assert os3.insert(4, "d") == ["d"]
    print("  Each insert flushes immediately ✓")

    # -- Reverse-order insertion (everything flushes at the end) ---------------
    print("\n--- Reverse-order insertion ---")
    os4 = OrderedStream(4)
    assert os4.insert(4, "d") == []
    assert os4.insert(3, "c") == []
    assert os4.insert(2, "b") == []
    assert os4.insert(1, "a") == ["a", "b", "c", "d"]
    print("  Single bulk flush at the end ✓")

    # -- Single element --------------------------------------------------------
    print("\n--- Single element ---")
    os5 = OrderedStream(1)
    assert os5.insert(1, "only") == ["only"]
    print("  n=1 works ✓")

    # -- Edge: n=0 (no elements expected) --------------------------------------
    print("\n--- n=0 (empty stream) ---")
    os6 = OrderedStream(0)
    print("  Created with n=0 ✓")

    # -- Edge: invalid id ------------------------------------------------------
    print("\n--- Invalid id ---")
    os7 = OrderedStream(3)
    try:
        os7.insert(0, "bad")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"  insert(0, ...) raised ValueError: {e}  ✓")

    try:
        os7.insert(4, "bad")
        assert False, "Should have raised ValueError"
    except ValueError as e:
        print(f"  insert(4, ...) raised ValueError: {e}  ✓")

    # -- Larger test -----------------------------------------------------------
    print("\n--- Larger test (n=100, random-ish order) ---")
    import random
    random.seed(42)
    n = 100
    ids = list(range(1, n + 1))
    random.shuffle(ids)
    os8 = OrderedStream(n)
    collected: list[str] = []
    for i in ids:
        collected.extend(os8.insert(i, f"v{i}"))
    assert collected == [f"v{i}" for i in range(1, n + 1)]
    print(f"  All {n} values flushed in correct order ✓")

    print("\nAll checks passed ✓")
