"""Daily Temperatures.

Problem: Given daily temperatures, for each day find how many days must pass
until a strictly warmer temperature occurs. Use zero when no warmer day exists.

Input: ``temperatures``, a list of integer temperatures ordered by day.
Output: a list of equal length containing the waiting-day count for each day.
Constraints: an empty list is valid; equal temperatures are not warmer, and
the usual problem bounds allow a linear-time solution.
"""

from __future__ import annotations


def daily_temperatures(temperatures: list[int]) -> list[int]:
    """Return waiting days for each temperature using a decreasing index stack.

    An index remains on the stack while its answer is unknown.  When a warmer
    temperature arrives, it resolves every colder index at the top.  Each
    index is pushed once and popped once, so the total work is linear.

    Complexity: O(n) time and O(n) space.
    """
    answer = [0] * len(temperatures)
    unresolved: list[int] = []

    for current_index, current_temperature in enumerate(temperatures):
        while unresolved and temperatures[unresolved[-1]] < current_temperature:
            previous_index = unresolved.pop()
            answer[previous_index] = current_index - previous_index
        unresolved.append(current_index)
    return answer


if __name__ == "__main__":
    assert daily_temperatures([73, 74, 75, 71, 69, 72, 76, 73]) == [1, 1, 4, 2, 1, 1, 0, 0]
    assert daily_temperatures([30, 40, 50, 60]) == [1, 1, 1, 0]
    print("Daily Temperatures: all checks passed")
