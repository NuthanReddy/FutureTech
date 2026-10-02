"""Matrix Chain Multiplication example for interval DP."""

from functools import lru_cache
from math import inf
from typing import List


def matrix_chain_memo(dimensions: List[int]) -> int:
    """Return minimum multiplication cost using memoized interval DP.

    Pattern identification: parenthesize a fixed matrix chain -> interval split DP ->
    every final split combines optimal subchains plus their multiplication cost.
    """
    # 1. Output: Return the fewest scalar multiplications needed for the matrix chain.
    # 2. Structure: Matrix order cannot change, only grouping can; every final
    #    multiplication joins two consecutive groups, with repeated group costs.
    # 3. Constraints: Assume positive compatible dimensions; zero/one matrix costs 0.
    #    For n matrices: O(n^3) time, O(n^2) space; recursion depth must fit Python's limit.
    # 4. Choice: Remember best cost for matrices left..right; one matrix costs 0. Try each split:
    #    left cost + right cost + dimensions[left]*dimensions[split+1]*dimensions[right+1].
    # 5. Why it works: Every parenthesization has a final split tried here;
    #    optimal subchains at that split minimize its total multiplication cost.
    matrix_count = len(dimensions) - 1
    if matrix_count <= 1:
        return 0

    @lru_cache(maxsize=None)
    def dfs(left: int, right: int) -> int:
        if left == right:
            return 0

        best_cost = inf
        for split in range(left, right):
            left_cost = dfs(left, split)
            right_cost = dfs(split + 1, right)
            merge_cost = dimensions[left] * dimensions[split + 1] * dimensions[right + 1]
            best_cost = min(best_cost, left_cost + right_cost + merge_cost)

        return best_cost

    return dfs(0, matrix_count - 1)


def matrix_chain_tab(dimensions: List[int]) -> int:
    """Return minimum multiplication cost using bottom-up interval DP.

    Pattern identification: parenthesize a fixed matrix chain -> increasing-length DP ->
    all shorter subchains are optimal before evaluating every final split.
    """
    # 1. Output: Return the minimum scalar multiplication cost for the whole chain.
    # 2. Structure: Only consecutive matrices can form a group; a group's last
    #    multiplication splits it into shorter groups that can be solved first.
    # 3. Constraints: Assume positive compatible dimensions; zero/one matrix costs 0.
    #    For n matrices: O(n^3) time and O(n^2) space.
    # 4. Choice: dp[left][right] stores that group's best cost; single matrices cost 0. Fill by length,
    #    minimizing both subchain costs plus the dimension-product merge cost.
    # 5. Why it works: Shorter intervals are solved before they are read;
    #    testing every final split covers all ways to parenthesize this interval.
    matrix_count = len(dimensions) - 1
    if matrix_count <= 1:
        return 0

    dp = [[0] * matrix_count for _ in range(matrix_count)]

    for chain_length in range(2, matrix_count + 1):
        for left in range(0, matrix_count - chain_length + 1):
            right = left + chain_length - 1
            dp[left][right] = inf

            for split in range(left, right):
                merge_cost = dimensions[left] * dimensions[split + 1] * dimensions[right + 1]
                total_cost = dp[left][split] + dp[split + 1][right] + merge_cost
                dp[left][right] = min(dp[left][right], total_cost)

    return dp[0][matrix_count - 1]
