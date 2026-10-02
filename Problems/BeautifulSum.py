# Pattern identification: maximize disjoint zero-sum segments -> prefix-sum set + earliest-finish greedy;
# repeated prefixes expose a zero sum; reset the prefix state after each accepted segment.
def max_beautiful_segments(n, a):
    # 1. Output: Return the maximum number of non-overlapping zero-sum segments.
    # 2. Structure: Scan an array left to right; equal running sums reveal a zero-sum segment.
    # 3. Constraints: Negative values make sum-based window shrinking unsafe; assume 0<=n<=len(a).
    # 4. Choice: Remember sums since the last commitment; take the first repeat and reset for disjointness.
    # 5. Why it works: Replace an optimum's first segment with this earliest-ending one:
    # all its later segments still fit, so committing now cannot reduce the final count.
    prefix_sum = 0
    seen = set()
    seen.add(0)
    count = 0

    for i in range(n):
        prefix_sum += a[i]
        print("before:", prefix_sum, i, seen, count)
        if prefix_sum in seen:
            # when the sum is zero, treat the rest as new problem and proceed.
            count += 1
            # Reset for non-overlapping segments
            seen = set()
            seen.add(0)
            prefix_sum = 0
        else:
            seen.add(prefix_sum)
        print("after:", prefix_sum, i, seen, count)

    return count


# Testing the example cases:
print(max_beautiful_segments(5, [2, 1, -3, 2, 1]))  # Output: 1
print(max_beautiful_segments(7, [12, -4, 4, 43, -3, -5, 8]))  # Output: 2
print(max_beautiful_segments(6, [-4, 0, 3, 0, 1, 0]))  # Output: 3
