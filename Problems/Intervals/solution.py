"""Top Interview 150 interval problems.

The common pattern is to sort by the left endpoint and make one greedy pass.
Intervals are treated as half-open at the boundary for scheduling decisions:
``[1, 2]`` and ``[2, 3]`` do not overlap.  This matches the usual meeting-room
interpretation and keeps the boundary rule explicit.

Problem statements:
* ``summary_ranges``: Given a sorted, distinct integer array, return the
  maximal consecutive runs as ``"a->b"`` or a single-number string.
* ``merge``: Given arbitrary intervals, merge every overlapping interval and
  return the disjoint union.
* ``insert``: Given sorted, pairwise-disjoint intervals and one new interval,
  insert it and merge overlaps, returning sorted disjoint intervals.
* ``erase_overlap_intervals``: Given intervals, remove the fewest intervals so
  the remainder are non-overlapping; return that minimum count.
* ``min_meeting_rooms``: Given meeting start/end times, return the minimum
  rooms required so every meeting can occur without conflict.

Inputs use ``List[int]`` or ``List[List[int]]``.  Interval endpoints satisfy
``start <= end``; the standard constraints are ``n >= 0`` and integer
endpoints.  Outputs are respectively a list of strings, interval lists, or an
integer count.  The algorithms are intended for up to typical interview-scale
``n`` (sorting dominates at ``O(n log n)``).
"""

from typing import List


def summary_ranges(nums: List[int]) -> List[str]:
    """Return maximal consecutive ranges from sorted distinct ``nums``."""
    if not nums:
        return []
    result: List[str] = []
    start = previous = nums[0]
    for value in nums[1:] + [None]:  # sentinel flushes the final run
        if value is not None and value == previous + 1:
            previous = value
            continue
        result.append(str(start) if start == previous else f"{start}->{previous}")
        if value is not None:
            start = previous = value
    return result


def merge(intervals: List[List[int]]) -> List[List[int]]:
    """Return the disjoint union of arbitrary overlapping ``intervals``."""
    if not intervals:
        return []
    merged: List[List[int]] = []
    for start, end in sorted(intervals):
        # ``start <= last_end`` deliberately merges touching intervals too;
        # they represent one continuous covered range.
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    return merged


def insert(intervals: List[List[int]], new_interval: List[int]) -> List[List[int]]:
    """Insert ``new_interval`` into sorted disjoint ``intervals`` and merge."""
    result: List[List[int]] = []
    index = 0
    while index < len(intervals) and intervals[index][1] < new_interval[0]:
        result.append(intervals[index])
        index += 1
    while index < len(intervals) and intervals[index][0] <= new_interval[1]:
        new_interval[0] = min(new_interval[0], intervals[index][0])
        new_interval[1] = max(new_interval[1], intervals[index][1])
        index += 1
    result.append(new_interval)
    result.extend(intervals[index:])
    return result


def erase_overlap_intervals(intervals: List[List[int]]) -> int:
    """Return minimum removals needed to make ``intervals`` non-overlapping.

    Keeping the interval with the earliest finishing time leaves the most room
    for all later intervals—the same exchange argument used by activity
    selection.
    """
    if not intervals:
        return 0
    removals = 0
    end = float("-inf")
    for start, finish in sorted(intervals, key=lambda item: item[1]):
        if start < end:
            removals += 1
        else:
            end = finish
    return removals


def min_meeting_rooms(intervals: List[List[int]]) -> int:
    """Return minimum rooms needed for all meetings in ``intervals``."""
    if not intervals:
        return 0
    starts = sorted(start for start, _ in intervals)
    ends = sorted(end for _, end in intervals)
    start_index = end_index = active = peak = 0
    while start_index < len(starts):
        if end_index == len(ends) or starts[start_index] < ends[end_index]:
            active += 1
            peak = max(peak, active)
            start_index += 1
        else:
            # A meeting ending exactly as another starts frees its room.
            end_index += 1
            active -= 1
    return peak
