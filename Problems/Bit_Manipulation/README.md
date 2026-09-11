# Bit Manipulation

| Problem | Problem statement (input → required output) | Invariant | Time | Extra space |
| --- | --- | ---: | ---: |
| Single Number | List with one singleton and all others twice → singleton | `x ^ x == 0`, `x ^ 0 == x` | O(n) | O(1) |
| Number of 1 Bits | Non-negative 32-bit integer → number of set bits | clear one set bit per loop | O(number of set bits) | O(1) |
| Counting Bits | `n >= 0` → popcount list for every integer `0..n` | `popcount(i) = popcount(i >> 1) + low_bit` | O(n) | O(n) |
| Reverse Bits | 32-bit unsigned integer → its 32-bit bit reversal | shift 32 source bits into result | O(32) | O(1) |
| Missing Number | Distinct values from `0..n` with one absent → absent value | XOR indices and values | O(n) | O(1) |
| Sum of Two Integers | Two signed 32-bit integers → sum without `+` or `-` | XOR sum plus shifted carry | O(32) | O(1) |
| Reverse Integer | Signed 32-bit integer → reversed decimal digits, or 0 on overflow | digit extraction with bounds check | O(digits) | O(digits) |

## Inputs and constraints

The bit problems use fixed-width 32-bit semantics where stated, even though
Python integers are arbitrary precision. `single_number` assumes exactly one
value occurs once and every other value occurs twice. `missing_number` assumes
distinct values from the complete range `0..n` with one missing. Outputs are
integers except `count_bits`, which returns a list of `n + 1` integers.

Python has arbitrary-precision signed integers, unlike the fixed-width machine
words assumed by these exercises.  Consequently `reverse_bits` and `get_sum`
mask every intermediate value; omitting that detail can make a solution pass
positive examples while looping forever or returning the wrong negative value.
