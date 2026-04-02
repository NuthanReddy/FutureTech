from collections import deque

def longest_k_stable_subarray(arr, k):
    if not arr:
        return 0, (0, -1)
    maxd = deque()  # indices of elements in decreasing order
    mind = deque()  # indices of elements in increasing order
    left = 0
    best_len = 0
    best_range = (0, 0)
    for right, val in enumerate(arr):
        # maintain max deque (decreasing)
        while maxd and arr[maxd[-1]] <= val:
            maxd.pop()
        maxd.append(right)
        # maintain min deque (increasing)
        while mind and arr[mind[-1]] >= val:
            mind.pop()
        mind.append(right)

        # shrink window until condition satisfied
        while arr[maxd[0]] - arr[mind[0]] > k:
            if maxd[0] == left:
                maxd.popleft()
            if mind[0] == left:
                mind.popleft()
            left += 1

        # update best
        cur_len = right - left + 1
        if cur_len > best_len:
            best_len = cur_len
            best_range = (left, right)

    return best_len, best_range


print(longest_k_stable_subarray([1, 3, 6, 4, 1, 2], 3))

# Summary — fastest practical solution: Use a sliding window with two monotonic deques (one for current maximum, one for current minimum) to find the longest contiguous subarray whose max - min <= k in O(n) time and O(n) worst‑case extra space. This is the simplest, fastest approach for streaming or large arrays.
#
# Data structure options (comparison)
# Structure	                    Time per element	Space	    When to pick
# Two monotonic deques	        O(1) amortized	    O(n)	    Best for single pass sliding window; minimal code.
# Balanced BST / multiset	    O(log n)	        O(n)	    When you need order-statistics or deletions by value.
# Segment tree / Fenwick tree	O(log n)	        O(n)	    When you need range queries and updates offline/online.
# Sparse table (RMQ)	        O(1)	            O(n log n)	When queries are many and array is static. O(nlogn) build

# Why I pick monotonic deques: they maintain current window max/min in amortized constant time by storing indices in decreasing/increasing order; sliding the left pointer simply pops indices out of range. This yields a single-pass O(n) algorithm ideal for longest contiguous window problems.

# Walkthrough with full variable trace (example)
# Input: arr = [1, 3, 6, 4, 1, 2], k = 3
# We will show right, left, maxd (indices), mind (indices), arr[maxd[0]], arr[mind[0]], best_len, best_range.
#
# right=0, val=1
#
# maxd: [0] ; mind: [0] ; left=0
#
# max=1, min=1 → diff=0 ≤ 3
#
# best_len=1, best_range=(0,0)
#
# right=1, val=3
#
# maxd: pop 0 (1 ≤ 3) → [1] ; mind: [0,1] (since 3 ≥ previous)
#
# max=3, min=1 → diff=2 ≤ 3
#
# best_len=2, best_range=(0,1)
#
# right=2, val=6
#
# maxd: pop 1 → pop none further → [2] ; mind: [0,1,2]
#
# max=6, min=1 → diff=5 > 3 → shrink:
#
# left=0 → maxd[0]=2 !=0 ; mind[0]=0 == left → mind.popleft() → left=1
#
# now max=6, min=3 → diff=3 ≤ 3
#
# window [1..2], cur_len=2 → best_len stays 2
#
# right=3, val=4
#
# maxd: arr[2]=6 >4 so append → [2,3] ; mind: pop 2 (6 ≥4) → [1,3]
#
# max=6, min=3 → diff=3 ≤3
#
# cur_len = 3 (left=1,right=3) → best_len=3, best_range=(1,3)
#
# right=4, val=1
#
# maxd: append → [2,3,4] ; mind: pop 3 (4 ≥1), pop 1 (3 ≥1) → [4]
#
# max=6, min=1 → diff=5 >3 → shrink:
#
# left=1: maxd[0]=2 !=1 ; mind[0]=4 !=1 → left=2
#
# now max=6, min=1 → diff=5 >3 → left=3 (if mind[0]==2? no) but we check:
#
# if left moved to 2, still maxd[0]=2 == left? yes then pop maxd → maxd becomes [3,4]
#
# recompute max=arr[3]=4, min=1 → diff=3 ≤3
#
# final left=3, window [3..4], cur_len=2
#
# right=5, val=2
#
# maxd: arr[4]=1 ≤2 so pop 4; arr[3]=4 >2 → maxd=[3,5] ; mind: arr[4]=1 ≤2 so append → [4,5]
#
# max=4, min=1 → diff=3 ≤3
#
# cur_len = 3 (left=3,right=5) → best_len remains 3 (first found)
#
# Result returned: best_len = 3, best_range = (1,3) (subarray [3,6,4]).
#
# Notes & references: Sliding-window with monotonic deques is a standard O(n) pattern for maintaining window min/max;
# balanced trees or segment trees are alternatives when deletions by value or many offline queries are required.