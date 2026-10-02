

# Pattern identification: cover a line with fewest fountain ranges -> greedy interval coverage;
# track farthest reachable end and commit a new range at the current coverage boundary.
def min_cnt_foun(a, N):
    # 1. Output: Return the fewest fountains needed to cover positions 0 through N-1.
    # 2. Structure: Positions lie on a line, and each fountain covers one unbroken interval.
    # 3. Constraints: Assume N=len(a)>0 and nonnegative integer ranges; empty input fails at dp[0].
    # 4. Choice: Greedily scan starts and remember the farthest end; commit when current coverage runs out.
    # 5. Why it works: At the next uncovered position, replace any covering choice with
    # the farthest-reaching available one: earlier positions stay covered and later options cannot shrink.
    # dp[i] is the farthest exclusive right endpoint of any range starting at i.
    # Despite the variable name, these endpoints support a greedy scan, not DP.
    dp = [-1] * N

    # Traverse the array
    for i in range(N):
        idx_left = max(i - a[i], 0)
        idx_right = min(i + (a[i] + 1), N)
        dp[idx_left] = max(dp[idx_left], idx_right)

    cnt_fount = 1
    idx_right = dp[0]

    # Stores index of next fountain
    # that needed to be activated
    idx_next = 0

    # Traverse dp[] array
    for i in range(N):
        idx_next = max(idx_next, dp[i])

        # If left most fountain
        # cover all its range
        if i == idx_right:
            cnt_fount += 1
            idx_right = idx_next

    return cnt_fount


if __name__ == '__main__':
    a = [1, 2, 1]
    N = len(a)

    print(min_cnt_foun(a, N))
