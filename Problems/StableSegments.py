# find segments >=3 length where egde nodes value - sum(values of nodes in between)
# Pattern identification: endpoint/interior-sum relation -> prefix-sum hash counting;
# accumulate earlier (prefix, endpoint) keys and query (s - 3 * value, value); legacy indexing needs review.
from collections import defaultdict


def count_stable_segments(input):
    # 1. Output: Seek segments of length at least three whose equal endpoints each equal the interior sum.
    # 2. Structure: Equal endpoints v require total sum 3*v, so a running sum can look up matching earlier starts.
    # 3. Constraints: Values may include zero or negatives; fewer than three items returns zero.
    # 4. Choice: Count earlier (prefix-before-start, start-value) pairs and query using s-3*v.
    # 5. Why it works: The key equation tests equal endpoints and the sum relation, but
    # the scan omits the final endpoint and inserts starts too soon to enforce length three.
    n = len(input)
    if n < 3:
        return 0
    d = defaultdict(int)
    d[(0, input[0])] = 1
    r, s = 0, 0
    for i in range(n - 1):
        s += input[i]
        r += d[(s - 3 * input[i], input[i])]
        d[(s, input[i + 1])] += 1
    return r


print(count_stable_segments([9, 3, 3, 3, 9]))
print(count_stable_segments([9, 3, 1, 2, 3, 9, 10]))
print(count_stable_segments([10, 9, 3, 1, 2, 3, 9]))