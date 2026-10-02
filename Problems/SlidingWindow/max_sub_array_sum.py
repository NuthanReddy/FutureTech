# Pattern identification: maximum sum of exactly k adjacent items -> fixed rolling sum;
# for 1 <= k <= len(arr), subtract outgoing/add incoming to retain exactly k items.
def max_k_sub_array_sum(arr, k):
    # 1. Output: Return the largest sum of exactly k adjacent values, or -1 if k exceeds the length.
    # 2. Structure: Every answer uses exactly k neighbors; sliding one place changes just the outgoing and incoming item.
    # 3. Constraints: Assume k >= 1; negative values are allowed because the window size never changes.
    # 4. Choice: Sum the first window, then subtract arr[i-k] and add arr[i] while retaining the best sum.
    # 5. Why it works: The running sum always represents exactly the current k items, and every window is visited.
    #    O(n) time; the initial arr[:k] slice uses O(k) temporary extra space.
    n = len(arr)
    if k > n:
        return -1
    elif k == n:
        return sum(arr)
    else:
        window_sum = sum(arr[:k])
        max_sum = window_sum
        for i in range(k, n):
            window_sum = window_sum - arr[i - k] + arr[i]
            #print(arr[i - k], arr[i], window_sum, max_sum)
            max_sum = max(max_sum, window_sum)
        return max_sum


print(max_k_sub_array_sum([2, 1, 5, 1, 3, 2], 3))
print(max_k_sub_array_sum([2, 3, 4, 1, 5], 2))
