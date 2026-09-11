"""Min Stack.

Problem: Design a stack of integers supporting ``push``, ``pop``, ``top``,
and retrieving the minimum element, with every operation in O(1) time.

Input: a sequence of stack operations and integer values supplied to
``MinStack.push``.
Output: ``pop``/``top`` return the top integer and ``get_min`` returns the
smallest integer currently stored; empty-stack operations raise ``IndexError``.
Constraints: duplicate and negative values are allowed; the stack may be
empty between operations.
"""

from __future__ import annotations


class MinStack:
    """Implement push, pop, top, and get_min in O(1) time.

    The second stack stores the minimum value of every prefix.  Thus the
    minimum before a pop is still available after the corresponding prefix is
    restored.  This is clearer and safer than encoding two values into one
    arithmetic sentinel.
    """

    def __init__(self) -> None:
        self._values: list[int] = []
        self._minimums: list[int] = []

    def push(self, value: int) -> None:
        """Push *value* and update the prefix minimum."""
        self._values.append(value)
        self._minimums.append(value if not self._minimums else min(value, self._minimums[-1]))

    def pop(self) -> int:
        """Remove and return the top value."""
        if not self._values:
            raise IndexError("pop from empty MinStack")
        self._minimums.pop()
        return self._values.pop()

    def top(self) -> int:
        """Return the top value without removing it."""
        if not self._values:
            raise IndexError("top from empty MinStack")
        return self._values[-1]

    def get_min(self) -> int:
        """Return the current minimum."""
        if not self._minimums:
            raise IndexError("minimum from empty MinStack")
        return self._minimums[-1]


if __name__ == "__main__":
    stack = MinStack()
    stack.push(-2)
    stack.push(0)
    stack.push(-3)
    assert stack.get_min() == -3
    assert stack.pop() == -3
    assert stack.top() == 0
    assert stack.get_min() == -2
    print("Min Stack: all checks passed")
