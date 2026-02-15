def max_k_sub_array_sum(arr, k):
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
