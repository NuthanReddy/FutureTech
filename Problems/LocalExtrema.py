# you can write to stdout for debugging purposes, e.g.
# print("this is a debug message")

# Pattern identification: count peaks/valleys across equal-height runs -> plateau-aware linear scan;
# intended invariant: compare each run with its outside neighbors; legacy endpoint handling is incomplete.
def solution(A):
    # 1. Output: Attempt to count local peak and valley runs, treating equal heights as a plateau.
    # 2. Structure: Only a run's outside neighbors decide peak or valley status, suggesting a left-to-right scan.
    # 3. Constraints: Single and two-item inputs have special cases; empty input is unchecked.
    # 4. Choice: Track previous height and run size while comparing nearby values.
    # 5. Why it works: Each run should be counted once against valid outside neighbors;
    # this scan omits the first endpoint and may read A[i+1] past the last plateau.
    # write your code in Python 3.6
    local_extrema_count = 0
    length = len(A)
    if length == 1:
        return 1
    if length == 2:
        return 1 + (A[0] != A[1])
    size = 1
    prev_height = A[0]
    for i in range(1, length):
        if A[i] == prev_height:
            size += 1
            local_extrema_count += (A[i] > A[i - size] and A[i] > A[i + 1])
            local_extrema_count += (A[i] < A[i - size] and A[i] < A[i + 1])
        else:
            size = 1
            if i == length - 1:
                local_extrema_count += 1
                continue
            local_extrema_count += (A[i] > A[i - 1] and A[i] > A[i + 1])
            local_extrema_count += (A[i] < A[i - 1] and A[i] < A[i + 1])
            prev_height = A[i]
    return local_extrema_count


#
# print(solution([2, 2, 3, 4, 3, 3, 2, 2, 1, 1, 2, 5]))
#
# print(solution([-3, -3]))

# Pattern identification: visit matrix anti-diagonals -> diagonal-index traversal;
# intended invariant: row + column stays constant within a diagonal; this scratch traversal is unfinished.
def foo(arr):
    # 1. Output: Attempt to print every matrix cell in anti-diagonal order.
    # 2. Structure: Constant row+column identifies each anti-diagonal, so index movement can describe the order.
    # 3. Constraints: Assert a nonempty matrix; assume rectangular rows with at least one column.
    # 4. Choice: Track row, column, and diagonal number, moving to the next proposed start.
    # 5. Why it works: Traversal should visit each diagonal cell once, but row+1>=row
    # is always true, so the within-diagonal move is unreachable and starts can repeat forever.
    a = dict()
    # [[1,2,3,4], [5,6,7,8], [9,10,11,12]]
    assert (len(arr) > 0)
    rows = len(arr)
    cols = len(arr[0])
    print(cols, rows)
    curr_col = 0
    curr_row = 0
    diag_no = 0
    while curr_col < cols or curr_row < rows:
        print(arr[curr_row][curr_col])
        if curr_row + 1 >= curr_row or curr_col - 1 <= 0:
            curr_col = min(cols - 1, diag_no + 1)
            curr_row = min(0, diag_no - curr_col + 1)
            diag_no += 1
            print(curr_col, curr_row, diag_no)
        else:
            curr_row = curr_row + 1
            curr_col = curr_col - 1


foo([[1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]])
