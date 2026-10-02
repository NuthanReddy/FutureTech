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
    """Return the value occurring once in ``nums``.

    Pattern identification: one singleton among pairs -> XOR cancellation ->
    the accumulator is the processed prefix XOR, with equal pairs cancelled.
    """
    # 1. Output: Return the one value that appears once.
    # 2. Structure: Every other value appears exactly twice, allowing pair cancellation.
    # 3. Constraints: Assume exactly one singleton and all other counts equal two.
    #    O(n) time and O(1) space for bounded-size integers; empty input returns 0.
    # 4. Choice: Start answer=0 and XOR each value into the running answer.
    # 5. Why it works: Equal values XOR to zero and order does not matter;
    #    after all pairs cancel, only the singleton remains.
    answer = 0
    for value in nums:
        answer ^= value
    return answer


def hamming_weight(n: int) -> int:
    """Return the number of set bits in non-negative integer ``n``.

    Pattern identification: count set bits -> clear the lowest set bit ->
    each iteration removes exactly one bit and increments its count.
    """
    # 1. Output: Return how many binary digits of n are one.
    # 2. Structure: n & (n-1) clears exactly the lowest remaining one-bit.
    # 3. Constraints: n must be non-negative; zero returns 0.
    #    O(k) time for k set bits and O(1) space under the 32-bit assumption.
    # 4. Choice: Repeatedly clear one set bit and increment count until n is zero.
    # 5. Why it works: Each iteration removes and counts exactly one original
    #    set bit; stopping at zero means every such bit has been counted.
    count = 0
    while n:
        n &= n - 1
        count += 1
    return count


def count_bits(n: int) -> list[int]:
    """Return popcounts for every integer in the inclusive range ``[0, n]``.

    Pattern identification: counts for all integers through n -> shift-parent DP ->
    popcount(value) equals the known parent count plus its low bit.
    """
    # 1. Output: Return a list of set-bit counts for every value from 0 through n.
    # 2. Structure: Shifting right removes one bit and reaches a smaller known value.
    # 3. Constraints: Assume n >= 0; n=0 returns [0]. O(n+1) time and
    #    O(n+1) output space, using constant-cost bounded integer operations.
    # 4. Choice: Seed count(0)=0; in ascending order set each count to
    #    result[value >> 1] plus its last bit, value & 1.
    # 5. Why it works: All bits except the last are counted by the earlier
    #    parent entry, and adding the last bit counts every bit exactly once.
    result = [0] * (n + 1)
    for value in range(1, n + 1):
        result[value] = result[value >> 1] + (value & 1)
    return result


def reverse_bits(n: int) -> int:
    """Return the 32-bit value obtained by reversing ``n``'s bits.

    Pattern identification: reverse a fixed-width word -> shift/append 32 bits ->
    the result holds the reversal of the consumed low bits.
    """
    # 1. Output: Return the unsigned integer obtained by reversing exactly 32 bits.
    # 2. Structure: Reading low bits first yields their reversed order when appended.
    # 3. Constraints: Assume a 32-bit unsigned input; leading zeros are significant.
    #    Exactly 32 iterations take O(1) time and O(1) space.
    # 4. Choice: Start result=0; shift it left, append n's low bit, and shift n
    #    right, repeating 32 times before masking the result to 32 bits.
    # 5. Why it works: After each step result reverses the consumed low bits;
    #    processing the full width also places original leading zeros correctly.
    result = 0
    for _ in range(32):
        result = (result << 1) | (n & 1)
        n >>= 1
    return result & 0xFFFFFFFF


def missing_number(nums: list[int]) -> int:
    """Return the missing value from the distinct range ``[0, len(nums)]``.

    Pattern identification: one gap in distinct 0..n -> expected/actual XOR ->
    all present values cancel, leaving only the absent value.
    """
    # 1. Output: Return the missing number in the inclusive range 0..len(nums).
    # 2. Structure: Distinct input values match every expected value except one.
    # 3. Constraints: Assume n distinct values from 0..n; empty input returns 0.
    #    O(n) time and O(1) space for bounded-size integers.
    # 4. Choice: Start with n; XOR each index 0..n-1 and its input value
    #    into the same accumulator.
    # 5. Why it works: Every present number occurs once in each group and
    #    cancels; the missing number occurs only in the expected group.
    answer = len(nums)
    for index, value in enumerate(nums):
        answer ^= index ^ value
    return answer


def get_sum(a: int, b: int) -> int:
    """Return ``a + b`` for signed 32-bit inputs without ``+`` or ``-``.

    Pattern identification: addition without arithmetic operators -> XOR and carry ->
    a + b modulo 2**32 is preserved until the masked carry is zero.
    """
    # 1. Output: Return the signed 32-bit sum, wrapping if the mathematical sum overflows.
    # 2. Structure: XOR adds without carries; shared one-bits create next-position carries.
    # 3. Constraints: Assume signed 32-bit inputs; the loop uses no '+' or '-',
    #    but final signed conversion uses subtraction. O(1) time/space for this fixed width.
    # 4. Choice: Replace a by masked XOR and b by masked shifted shared bits
    #    until no carry remains; convert the final word to signed form.
    # 5. Why it works: Sum modulo 2**32 is unchanged by every update;
    #    carries move left and eventually vanish, leaving the encoded sum.
    mask = 0xFFFFFFFF
    sign_bit = 0x80000000
    while b & mask:
        carry = (a & b) << 1
        a = (a ^ b) & mask
        b = carry & mask
    return a if a < sign_bit else a - (mask + 1)


def reverse_integer(x: int) -> int:
    """Reverse decimal digits, returning zero on signed 32-bit overflow.

    Pattern identification: decimal digit reversal -> reverse magnitude string ->
    restore the sign and return only values within signed 32-bit bounds.
    """
    # 1. Output: Return x with decimal digits reversed, or 0 for signed 32-bit overflow.
    # 2. Structure: The sign is separate from the magnitude's decimal digit order.
    # 3. Constraints: Assume signed 32-bit input; trailing zeros disappear on conversion.
    #    O(d) time and space for d decimal digits, bounded by the fixed input width.
    # 4. Choice: Reverse the magnitude string, convert back to an integer,
    #    restore the sign, then check both signed 32-bit boundaries.
    # 5. Why it works: String reversal puts each digit in its required place;
    #    restoring the sign and rejecting out-of-range values enforces the result rule.
    sign = -1 if x < 0 else 1
    reversed_value = int(str(abs(x))[::-1])
    reversed_value *= sign
    return reversed_value if -(2**31) <= reversed_value <= 2**31 - 1 else 0
