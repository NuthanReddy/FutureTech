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

import heapq
from typing import List


def summary_ranges(nums: List[int]) -> List[str]:
    """Return maximal consecutive ranges from sorted distinct ``nums``.

    Pattern identification: sorted distinct consecutive values -> run compression ->
    start/previous delimit the current maximal run; a gap flushes it.
    """
    # 1. Output: Return maximal consecutive runs as single-number or "start->end" strings.
    # 2. Structure: Sorted distinct values belong to the same run exactly when they differ by one.
    # 3. Constraints: Assume sorted distinct integers; empty input returns [].
    #    O(n) time and O(n) extra space, including the slice/sentinel list and output.
    # 4. Choice: Track start and previous; extend on previous+1, otherwise
    #    emit the run and restart. A final None sentinel flushes the last run.
    # 5. Why it works: Every gap proves the current run cannot extend;
    #    emitting at gaps and the sentinel covers each value exactly once.
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
    """Return the disjoint union of arbitrary overlapping ``intervals``.

    Pattern identification: union of overlapping ranges -> sort starts and coalesce ->
    the output covers the processed prefix; only its last range can still extend.
    """
    # 1. Output: Return sorted ranges covering the union, with overlaps/touching ranges merged.
    # 2. Structure: We need covered ranges, not a maximum compatible selection;
    #    sorting starts makes the last output range the only one a new range can extend.
    # 3. Constraints: Assume start <= end; touching endpoints merge here.
    #    Empty input returns []; O(n log n) time and O(n) space; input is not mutated.
    # 4. Choice: Sort intervals; if start <= last end extend that end with max,
    #    otherwise append a new output range.
    # 5. Why it works: The output always covers exactly the processed intervals;
    #    sorted starts prove a separated new range cannot overlap earlier output.
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
    """Insert ``new_interval`` into sorted disjoint ``intervals`` and merge.

    Pattern identification: one addition to sorted disjoint ranges -> three phases ->
    copy ranges before, absorb touching overlaps, then append ranges after.
    """
    # 1. Output: Return sorted disjoint intervals after inserting and merging the new range.
    # 2. Structure: Existing ranges are already sorted and separate; one new range
    #    can only absorb a consecutive group, so no re-sort or choice search is needed.
    # 3. Constraints: Assume valid sorted disjoint ranges; touching endpoints merge.
    #    O(n) time/space; new_interval is mutated and output can share input lists.
    # 4. Choice: Copy ranges strictly before; widen new_interval across all
    #    touching overlaps; append it and then all remaining ranges.
    # 5. Why it works: The first/last groups cannot overlap the widened range;
    #    absorbing the middle group preserves the union without leaving overlaps.
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

    Pattern identification: fewest removals for compatible intervals -> earliest-finish greedy ->
    exchanging for an earlier finish cannot reduce future compatible choices.

    Keeping the interval with the earliest finishing time leaves the most room
    for all later intervals—the same exchange argument used by activity
    selection.
    """
    # 1. Output: Return the fewest intervals to remove so the rest do not overlap.
    # 2. Structure: We want the largest compatible selection, not merged coverage;
    #    keeping the earliest available finish cannot block a later compatible choice.
    # 3. Constraints: Assume valid intervals; touching endpoints are compatible.
    #    Empty input gives 0; O(n log n) time and O(n) sorting space.
    # 4. Choice: Sort by finish and commit when start >= last kept end, since an earlier finish is safe;
    #    otherwise count its removal. Start with end at negative infinity.
    # 5. Why it works: Replacing a kept choice by an earlier finish cannot
    #    block future choices, so this keeps the most intervals and removes the fewest.
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
    """Return minimum rooms needed for all meetings in ``intervals``.

    Pattern identification: reuse rooms as meetings finish -> start-sorted end-time heap ->
    one end per allocated room; allocate only when the earliest end exceeds start.

    The heap stores the end time of each room's currently scheduled meeting.
    Reusing the earliest-ending room is sufficient because all other rooms
    finish no earlier.
    """
    # 1. Output: Return the minimum number of rooms needed to schedule all meetings.
    # 2. Structure: Every meeting must be placed, not selected or removed;
    #    in start order, the earliest room end tells whether any room can be reused.
    # 3. Constraints: Assume positive-duration meetings; an end equal to a start is compatible.
    #    Empty input gives 0; O(n log n) time and O(n) extra space.
    # 4. Choice: Store one end time per allocated room in a min-heap;
    #    replace its earliest end if <= start, otherwise allocate another room.
    # 5. Why it works: If the earliest room is still busy, every room is busy
    #    and a new one is necessary; otherwise reusing a room avoids needless allocation.
    if not intervals:
        return 0

    room_end_times: List[int] = []
    for start, end in sorted(intervals, key=lambda interval: interval[0]):
        if room_end_times and room_end_times[0] <= start:
            heapq.heapreplace(room_end_times, end)
        else:
            heapq.heappush(room_end_times, end)
    return len(room_end_times)
