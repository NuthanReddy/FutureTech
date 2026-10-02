"""Product of Array Except Self - prefix/suffix accumulation.

Problem statement:
    Given an integer list ``nums``, return a list ``answer`` where
    ``answer[i]`` equals the product of every ``nums`` element except
    ``nums[i]``.  Products fit in the prescribed integer range.  Division is
    forbidden, and the expected solution runs in ``O(n)`` time; the empty
    list returns an empty list.
"""

from __future__ import annotations


def product_except_self(nums: list[int]) -> list[int]:
    """Return products excluding each corresponding input position.

    Pattern identification: all-except-self products without division -> prefix/suffix;
    accumulators exclude the current index before being multiplied into its output.

    On the left-to-right pass, ``result[index]`` receives the product of all
    values strictly to its left.  On the right-to-left pass, ``suffix`` is
    the product strictly to the right and is multiplied into that partial
    result.  Thus every output cell ends with ``left_product * right_product``
    and zeros require no special case.

    A division-based approach is rejected because zero values need separate
    handling and the problem explicitly forbids division.  Keeping the output
    list as the prefix workspace achieves the usual ``O(1)`` auxiliary-space
    requirement (the returned list itself is not counted).

    Complexity:
        Time ``O(n)``, auxiliary space ``O(1)`` beyond the output.
    """
    # 1. Output: Return one product per index using every value except the value at that index.
    # 2. Structure: Excluding one position leaves values to its left and right; neighboring outputs reuse those products.
    # 3. Constraints: Division is forbidden; zeros and negative values work without special branches.
    # 4. Choice: Reuse running left and right products in two passes instead of multiplying all other values per index.
    # 5. Why it works: Each accumulator excludes the current position until after its output is updated.
    #    Two passes take O(n) time and O(1) extra space beyond the returned list.
    result = [1] * len(nums)

    prefix = 1
    for index, number in enumerate(nums):
        result[index] = prefix
        prefix *= number

    suffix = 1
    for index in range(len(nums) - 1, -1, -1):
        result[index] *= suffix
        suffix *= nums[index]

    return result


if __name__ == "__main__":
    assert product_except_self([1, 2, 3, 4]) == [24, 12, 8, 6]
    assert product_except_self([-1, 1, 0, -3, 3]) == [0, 0, 9, 0, 0]
    print("product_except_self examples passed")
