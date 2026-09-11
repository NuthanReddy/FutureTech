"""Top Interview 150 bit-manipulation problems.

All bit operations here assume Python's integers, while ``reverse_bits`` and
``get_sum`` explicitly mask to 32 bits to model the interview problem's
unsigned/two's-complement machine word.

Problem statements:
* ``single_number``: Given integers where exactly one occurs once and every
  other value occurs twice, return the unique value.
* ``hamming_weight``: Given a non-negative 32-bit integer, return its number of
  set bits.
* ``count_bits``: Given ``n >= 0``, return a list containing the set-bit count
  for every integer from 0 through ``n``.
* ``reverse_bits``: Given a 32-bit unsigned integer, return the integer formed
  by reversing its 32 bits.
* ``missing_number``: Given distinct values from ``[0, n]`` with one missing,
  return the missing value.
* ``get_sum``: Given two signed 32-bit integers, compute their sum without
  using ``+`` or ``-``.
* ``reverse_integer``: Given a signed 32-bit integer, reverse its decimal
  digits and return 0 if the result overflows the signed 32-bit range.

Inputs are integer lists or integers under the constraints stated above.
Outputs are an integer, a list of integers, or (for ``count_bits``) the full
range of popcounts.  All algorithms use constant auxiliary space except the
required output list and the temporary digit string in ``reverse_integer``.
"""


def single_number(nums: list[int]) -> int:
    """Return the value occurring once in ``nums``."""
    answer = 0
    for value in nums:
        answer ^= value
    return answer


def hamming_weight(n: int) -> int:
    """Return the number of set bits in non-negative integer ``n``."""
    count = 0
    while n:
        n &= n - 1
        count += 1
    return count


def count_bits(n: int) -> list[int]:
    """Return popcounts for every integer in the inclusive range ``[0, n]``."""
    result = [0] * (n + 1)
    for value in range(1, n + 1):
        result[value] = result[value >> 1] + (value & 1)
    return result


def reverse_bits(n: int) -> int:
    """Return the 32-bit value obtained by reversing ``n``'s bits."""
    result = 0
    for _ in range(32):
        result = (result << 1) | (n & 1)
        n >>= 1
    return result & 0xFFFFFFFF


def missing_number(nums: list[int]) -> int:
    """Return the missing value from the distinct range ``[0, len(nums)]``."""
    answer = len(nums)
    for index, value in enumerate(nums):
        answer ^= index ^ value
    return answer


def get_sum(a: int, b: int) -> int:
    """Return ``a + b`` for signed 32-bit inputs without ``+`` or ``-``."""
    mask = 0xFFFFFFFF
    sign_bit = 0x80000000
    while b & mask:
        carry = (a & b) << 1
        a = (a ^ b) & mask
        b = carry & mask
    return a if a < sign_bit else a - (mask + 1)


def reverse_integer(x: int) -> int:
    """Reverse decimal digits, returning zero on signed 32-bit overflow."""
    sign = -1 if x < 0 else 1
    reversed_value = int(str(abs(x))[::-1])
    reversed_value *= sign
    return reversed_value if -(2**31) <= reversed_value <= 2**31 - 1 else 0
