"""Top Interview 150 problems that use binary search.

Problem set covered in this module:
* Search Insert Position: given a sorted integer sequence and a target, return
  the target index if present; otherwise return the index where it can be
  inserted while preserving sorted order.  The required runtime is O(log n).
* Search a 2D Matrix: given an m x n matrix where each row is sorted and each
  row's first value is greater than the previous row's last value, return
  whether target appears in the matrix.  The required runtime is O(log(mn)).
* Find Peak Element: given a non-empty integer sequence where adjacent values
  differ, return the index of any value greater than its neighbors.  Values
  outside the array are treated as negative infinity, and O(log n) runtime is
  required.
* Search in Rotated Sorted Array: given a distinct, ascending array rotated at
  an unknown pivot and a target, return the target index or -1.  The required
  runtime is O(log n).
* Find First and Last Position: given a sorted integer sequence that may contain
  duplicates and a target, return [first_index, last_index], or [-1, -1] if the
  target is absent.  The required runtime is O(log n).
* Find Minimum in Rotated Sorted Array: given a non-empty, distinct, ascending
  array rotated at an unknown pivot, return its minimum value in O(log n) time.
* Median of Two Sorted Arrays: given two individually sorted integer sequences,
  return the median of their combined values in O(log(min(m, n))) time.  At
  least one input must be non-empty.

Binary search is more general than "find an exact value in a sorted list."
Each solution below states the property that remains true after every loop
iteration.  That loop invariant is the key to choosing safe boundary updates
and avoiding the usual off-by-one errors.

All functions are written for Python 3.10/3.11 and deliberately avoid using
``bisect`` so the underlying patterns remain visible.
"""

from collections.abc import Sequence


def _lower_bound(nums: Sequence[int], target: int) -> int:
    """Return the first index whose value is greater than or equal to target.

    The search interval is half-open: ``[left, right)``.

    Loop invariant:
    * every index before ``left`` contains a value smaller than ``target``;
    * every index at or after ``right`` contains a value at least ``target``.

    When the interval becomes empty, ``left == right`` is the only possible
    boundary between those two groups.  Returning ``len(nums)`` is valid when
    every value is smaller than the target.

    Time: O(log n)
    Extra space: O(1)
    """
    left, right = 0, len(nums)

    while left < right:
        middle = left + (right - left) // 2
        if nums[middle] < target:
            # middle cannot be the answer, nor can anything to its left.
            left = middle + 1
        else:
            # middle may be the answer, so keep it in the search interval.
            right = middle

    return left


def _upper_bound(nums: Sequence[int], target: int) -> int:
    """Return the first index whose value is strictly greater than target.

    Loop invariant for the half-open interval ``[left, right)``:
    * values before ``left`` are less than or equal to ``target``;
    * values at or after ``right`` are greater than ``target``.

    Time: O(log n)
    Extra space: O(1)
    """
    left, right = 0, len(nums)

    while left < right:
        middle = left + (right - left) // 2
        if nums[middle] <= target:
            left = middle + 1
        else:
            right = middle

    return left


def search_insert(nums: Sequence[int], target: int) -> int:
    """Return target's index or the index where it should be inserted.

    Pattern identification: sorted insertion point -> lower-bound search ->
    values before the boundary are smaller, values after it are at least target.

    Problem statement:
    Input is a sorted integer sequence ``nums`` and an integer ``target``.
    Return the index of ``target`` when it exists; otherwise return the position
    where ``target`` can be inserted without breaking sorted order.
    Key constraints: ``nums`` is sorted in ascending order, and the target
    runtime is O(log n).

    Search Insert Position is exactly the lower-bound operation: find the first
    value that is not smaller than ``target``.  This also handles an empty
    input, a target below the minimum, and a target above the maximum without
    separate branches.

    Time: O(log n)
    Extra space: O(1)
    """
    # 1. Output: Return the first target index, or its sorted insertion position.
    # 2. Structure: Values are sorted; a midpoint below target rules out everything
    #    before it, while a midpoint at least target can still be the first such value.
    # 3. Constraints: Assume ascending order; empty input returns 0.
    #    O(log n) time and O(1) extra space; duplicates use the first occurrence.
    # 4. Choice: Use lower-bound search; keep values before left below target
    #    and values at/after right at least target, until left equals right.
    # 5. Why it works: Sorted order safely eliminates half the unknown indices;
    #    the final boundary is exactly where target first belongs.
    return _lower_bound(nums, target)


def search_matrix(matrix: Sequence[Sequence[int]], target: int) -> bool:
    """Search a row-major sorted matrix as though it were one sorted array.

    Pattern identification: global row-major order -> flattened exact search ->
    any target index stays inside the retained inclusive bounds.

    Problem statement:
    Input is an m x n integer matrix and an integer ``target``.  Each row is
    sorted from left to right, and the first integer of each row is greater than
    the last integer of the previous row.  Return ``True`` if ``target`` is in
    the matrix; otherwise return ``False``.
    Key constraints: the matrix has global row-major sorted order, and the
    target runtime is O(log(rows * columns)).

    LeetCode 74 guarantees that each row is sorted and that the first value of
    a row is greater than the last value of the preceding row.  Therefore,
    flattening coordinates mathematically preserves global sorted order:

    ``flat_index -> matrix[flat_index // columns][flat_index % columns]``.

    Loop invariant:
    if ``target`` exists, its flattened index remains in the inclusive
    interval ``[left, right]``.

    Empty matrices and matrices with an empty first row contain no target.

    Time: O(log(rows * columns))
    Extra space: O(1)
    """
    # 1. Output: Return True if target occurs in the matrix, otherwise False.
    # 2. Structure: Every row follows the previous row's last value, so reading rows
    #    in order gives one sorted list and a midpoint comparison rules out half.
    # 3. Constraints: Assume rectangular, globally row-major sorted input;
    #    empty input is False. O(log(rows*columns)) time and O(1) extra space.
    # 4. Choice: Search inclusive flat bounds; divmod maps midpoint to a cell.
    #    Keep the side allowed by its comparison with target, excluding midpoint.
    # 5. Why it works: If target exists its index stays within the bounds;
    #    sorted order proves every discarded index cannot match.
    if not matrix or not matrix[0]:
        return False

    rows, columns = len(matrix), len(matrix[0])
    left, right = 0, rows * columns - 1

    while left <= right:
        middle = left + (right - left) // 2
        row, column = divmod(middle, columns)
        value = matrix[row][column]

        if value == target:
            return True
        if value < target:
            # Sorted order proves target cannot be at middle or to its left.
            left = middle + 1
        else:
            # Sorted order proves target cannot be at middle or to its right.
            right = middle - 1

    return False


def find_peak_element(nums: Sequence[int]) -> int:
    """Return the index of any peak element.

    Pattern identification: unequal neighbors reveal a slope -> peak search ->
    the chosen half always contains a peak, even without global sorted order.

    Problem statement:
    Input is a non-empty integer sequence ``nums``.  Return the index of any
    peak element, where a peak is strictly greater than its immediate
    neighbors.  Conceptual values beyond both ends are negative infinity, so an
    endpoint may be a valid peak.
    Key constraints: adjacent values are not equal, and the target runtime is
    O(log n).

    A peak is greater than its neighbors.  The problem guarantees adjacent
    values differ and conceptually places negative infinity beyond both ends.
    Comparing ``nums[middle]`` with ``nums[middle + 1]`` reveals a direction:

    * an ascending slope guarantees a peak exists strictly to the right;
    * a descending slope guarantees a peak exists at middle or to the left.

    Loop invariant:
    the inclusive interval ``[left, right]`` contains at least one peak.
    The loop uses ``left < right``, so ``middle + 1`` is always in bounds.

    A peak is undefined for an empty sequence, so this implementation raises a
    clear ``ValueError`` rather than returning a misleading index.

    Time: O(log n)
    Extra space: O(1)
    """
    # 1. Output: Return an index whose value is greater than its immediate neighbors.
    # 2. Structure: A local upward/downward slope points toward a half containing a peak.
    # 3. Constraints: Adjacent values must differ; outside neighbors are negative infinity.
    #    Empty input raises ValueError. O(log n) time and O(1) extra space.
    # 4. Choice: Keep a peak inside [left,right]; if middle rises to middle+1
    #    move left past middle, otherwise retain middle as the right boundary.
    # 5. Why it works: Following the slope reaches a peak or a valid endpoint;
    #    each retained half contains one, so the final single index is a peak.
    if not nums:
        raise ValueError("find_peak_element requires at least one value")

    left, right = 0, len(nums) - 1

    while left < right:
        middle = left + (right - left) // 2
        if nums[middle] < nums[middle + 1]:
            # We are climbing.  A peak must occur before the sequence can fall
            # or at the right boundary, so middle itself can be discarded.
            left = middle + 1
        else:
            # We are descending.  middle may itself be a peak; retain it.
            right = middle

    return left


def search_rotated(nums: Sequence[int], target: int) -> int:
    """Return target's index in a rotated sorted array, or ``-1``.

    Pattern identification: distinct rotated sorted values -> sorted-half search ->
    discard a half only when its value range excludes the target.

    Problem statement:
    Input is an integer sequence ``nums`` that was sorted in ascending order,
    then rotated at an unknown pivot, plus an integer ``target``.  Return the
    index of ``target`` if present; otherwise return ``-1``.
    Key constraints: all values are distinct, and the target runtime is
    O(log n).

    Values are distinct.  At least one half around ``middle`` must therefore
    be normally sorted.  We identify that half, then decide whether the target
    lies inside its value range.

    Loop invariant:
    if the target exists, its index remains in the inclusive interval
    ``[left, right]``.

    Time: O(log n)
    Extra space: O(1)
    """
    # 1. Output: Return target's index in the rotated array, or -1 when absent.
    # 2. Structure: Rotation breaks global sorting but leaves one midpoint half sorted;
    #    distinct endpoints tell us which half, whose value range can exclude target.
    # 3. Constraints: Assume distinct values and a valid rotation; empty input gives -1.
    #    O(log n) time and O(1) extra space.
    # 4. Choice: Keep any target in [left,right]; identify the sorted half
    #    and keep it only when its endpoint range contains target.
    # 5. Why it works: The sorted half's range proves whether target belongs
    #    there; discarding the other half never removes an existing target.
    left, right = 0, len(nums) - 1

    while left <= right:
        middle = left + (right - left) // 2
        if nums[middle] == target:
            return middle

        if nums[left] <= nums[middle]:
            # The left half is sorted.  Use a half-open value check because
            # middle was already shown not to equal target.
            if nums[left] <= target < nums[middle]:
                right = middle - 1
            else:
                left = middle + 1
        else:
            # Otherwise the right half is sorted.
            if nums[middle] < target <= nums[right]:
                left = middle + 1
            else:
                right = middle - 1

    return -1


def search_range(nums: Sequence[int], target: int) -> list[int]:
    """Return the first and last target positions, or ``[-1, -1]``.

    Pattern identification: sorted duplicate block -> lower/upper-bound searches ->
    all equal values occupy the half-open interval between those boundaries.

    Problem statement:
    Input is a sorted integer sequence ``nums`` that may contain duplicates and
    an integer ``target``.  Return a two-item list containing the first and last
    positions of ``target``.  Return ``[-1, -1]`` when ``target`` is absent.
    Key constraints: ``nums`` is sorted in non-decreasing order, and the target
    runtime is O(log n).

    Two boundary searches are safer and asymptotically faster than finding one
    occurrence and scanning across a potentially long block of duplicates.
    ``_lower_bound`` locates the start, while ``_upper_bound - 1`` locates the
    end.

    Time: O(log n)
    Extra space: O(1)
    """
    # 1. Output: Return [first_index,last_index] for target, or [-1,-1] if absent.
    # 2. Structure: Equal values form one consecutive block in a sorted sequence.
    # 3. Constraints: Assume non-decreasing order; duplicates and empty input are allowed.
    #    Two binary searches take O(log n) time and O(1) extra space.
    # 4. Choice: Find the first value >= target and check it matches;
    #    find the first value > target and subtract one for the last occurrence.
    # 5. Why it works: Each search preserves its sorted boundary; all target
    #    values lie between the two boundaries, including a block at either end.
    first = _lower_bound(nums, target)

    # A lower bound can point one past the list or at the next larger value.
    if first == len(nums) or nums[first] != target:
        return [-1, -1]

    last = _upper_bound(nums, target) - 1
    return [first, last]


def find_min_rotated(nums: Sequence[int]) -> int:
    """Return the minimum value in a rotated sorted array.

    Pattern identification: distinct rotation pivot -> midpoint/right comparison ->
    retain the half containing the minimum, including midpoint when it may win.

    Problem statement:
    Input is a non-empty integer sequence ``nums`` that was sorted in ascending
    order, then rotated at an unknown pivot.  Return the minimum value in the
    sequence.
    Key constraints: all values are distinct, and the target runtime is
    O(log n).

    Values are distinct.  Comparing ``nums[middle]`` with the current rightmost
    value tells us which side contains the rotation pivot:

    * ``nums[middle] > nums[right]`` means the minimum is strictly right;
    * otherwise, middle could be the minimum, so it must be retained.

    Loop invariant:
    the minimum value is inside the inclusive interval ``[left, right]``.

    An empty input has no minimum and raises ``ValueError``.

    Time: O(log n)
    Extra space: O(1)
    """
    # 1. Output: Return the minimum value, not its index, in the rotated array.
    # 2. Structure: Rotation creates one drop; a midpoint above the right end
    #    puts that drop to its right, letting us discard the high-value half.
    # 3. Constraints: Assume a valid distinct-value rotation; empty input raises ValueError.
    #    O(log n) time and O(1) extra space.
    # 4. Choice: Keep the minimum in [left,right]; middle > right's value
    #    moves left past middle, otherwise move right to middle, retaining it.
    # 5. Why it works: A larger midpoint lies before the low segment; otherwise
    #    the minimum is at or left of middle. The final cell must be the minimum.
    if not nums:
        raise ValueError("find_min_rotated requires at least one value")

    left, right = 0, len(nums) - 1

    while left < right:
        middle = left + (right - left) // 2
        if nums[middle] > nums[right]:
            # middle is in the high, pre-pivot segment.
            left = middle + 1
        else:
            # middle belongs to the low segment and may be the minimum.
            right = middle

    return nums[left]


def find_median_sorted_arrays(
    first: Sequence[int],
    second: Sequence[int],
) -> float:
    """Return the median of two sorted arrays in logarithmic time.

    Pattern identification: two sorted arrays, middle rank -> shorter-array partition search ->
    left size stays fixed; cross-boundary order identifies the valid cut.

    Problem statement:
    Input is two individually sorted integer sequences, ``first`` and
    ``second``.  Return the median value of the combined sorted multiset.  When
    the total number of values is even, return the average of the two middle
    values as a float.
    Key constraints: at least one input is non-empty, both inputs are sorted in
    non-decreasing order, and the target runtime is O(log(min(m, n))).

    We binary-search a partition in the shorter array.  Its partner partition
    in the longer array is derived so the combined left side always contains
    half of all values (or one extra value when the total length is odd).

    A partition is valid when every value on the combined left side is no
    greater than every value on the combined right side:

    ``first_left <= second_right and second_left <= first_right``.

    Loop invariant:
    if a valid partition exists, its cut position in the shorter array remains
    in the inclusive interval ``[left, right]``.

    Sentinel infinities make cuts at either end use the same comparisons as
    interior cuts.  At least one input must contain a value.

    Time: O(log(min(m, n)))
    Extra space: O(1)
    """
    # 1. Output: Return the combined median as a float, averaging two middles if needed.
    # 2. Structure: Each array is already sorted; only values beside their cuts
    #    need checking to tell whether all lower-half values precede all upper-half values.
    # 3. Constraints: Assume both sorted; both empty raises ValueError.
    #    O(log(min(m,n)+1)) time and O(1) extra space; neither input is merged.
    # 4. Choice: Search cuts in the shorter input, fixing the combined left size.
    #    Move left if its left value is too large, right if its right value is too small.
    # 5. Why it works: A valid cut stays in the search bounds; when cross-boundary
    #    order holds, max(left) and min(right) are exactly the middle-ranked values.
    if not first and not second:
        raise ValueError("at least one sorted array must be non-empty")

    # Searching the shorter input is what provides the required complexity and
    # ensures the derived cut in the longer input is always meaningful.
    if len(first) > len(second):
        first, second = second, first

    first_length, second_length = len(first), len(second)
    left_size = (first_length + second_length + 1) // 2
    left, right = 0, first_length

    while left <= right:
        first_cut = left + (right - left) // 2
        second_cut = left_size - first_cut

        first_left = first[first_cut - 1] if first_cut > 0 else float("-inf")
        first_right = (
            first[first_cut] if first_cut < first_length else float("inf")
        )
        second_left = (
            second[second_cut - 1] if second_cut > 0 else float("-inf")
        )
        second_right = (
            second[second_cut] if second_cut < second_length else float("inf")
        )

        if first_left <= second_right and second_left <= first_right:
            if (first_length + second_length) % 2 == 1:
                return float(max(first_left, second_left))

            left_maximum = max(first_left, second_left)
            right_minimum = min(first_right, second_right)
            return (left_maximum + right_minimum) / 2.0

        if first_left > second_right:
            # The shorter array contributes too many values to the left side.
            right = first_cut - 1
        else:
            # It contributes too few values to the left side.
            left = first_cut + 1

    # Sorted inputs always admit a valid partition.  Reaching this line means
    # the precondition was violated, so fail explicitly instead of returning
    # an arbitrary result.
    raise ValueError("inputs must be sorted in non-decreasing order")
