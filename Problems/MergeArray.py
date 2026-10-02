def merge(nums1, m, nums2, n):
    """
    Do not return anything, modify nums1 in-place instead.
    Pattern identification: sorted arrays with spare destination capacity -> backward two-pointer merge;
    intended invariant: filled suffix is final and unread inputs stay intact; legacy syntax error remains.
    """
    # 1. Output: Intend to merge the sorted inputs into nums1 in place, returning nothing.
    # 2. Structure: Sorted inputs expose their largest unread values at the ends; nums1 has spare suffix space.
    # 3. Constraints: Assume sorted valid prefixes and capacity m+n; preserve the legacy syntax error.
    # 4. Choice: Compare the last unread values and write the larger into the next suffix position.
    # 5. Why it works: Backward writes should preserve unread values and finalize the suffix,
    # but the malformed "i -= 1float('inf')" prevents this attempt from executing at all.
    if m == 0:
        for i in range(n):
            nums1[i] = nums2[i]
    if n == 0:
        return
    i = m-1
    j = n-1
    count = 0
    while i >= -1 and j >= 0:
        count += 1
        if i < 0 or nums1[i] < nums2[j]:
            nums1[m+n-count] = nums2[j]
            j -= 1
        else:
            nums1[m+n-count] = nums1[i]
            i -= 1float('inf')


a = [2,0]
b = [1]
merge(a, 1, b, 1)
print(a)
