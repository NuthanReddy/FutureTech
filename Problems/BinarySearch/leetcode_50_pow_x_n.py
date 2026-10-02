#
# @lc app=leetcode id=50 lang=python3
#
# [50] Pow(x, n)
#

# @lc code=start
class Solution:
    def myPow(self, x: float, n: int) -> float:
        """Pattern identification: large integer exponent -> binary exponentiation ->
        result * x**n stays equal to the normalized power as n halves.
        """
        # 1. Output: Return x raised to the integer exponent n.
        # 2. Structure: Binary exponent digits let repeated squaring replace n multiplications.
        # 3. Constraints: Negative n requires nonzero x; n=0 returns 1.
        #    O(log(abs(n)+1)) time and O(1) space; floating-point rounding still applies.
        # 4. Choice: Normalize negatives by taking the reciprocal; start result=1.
        #    Multiply on odd n, then square x and halve n.
        # 5. Why it works: result*x**n remains the normalized original power;
        #    when n becomes zero, result alone equals that power up to rounding.
        if n < 0:
            x = 1 / x
            n = -n
        result = 1
        # fast exponentiation
        # 13 can be represented as 1101 in binary, which means x^13 = x^(1*2^0) * x^(0*2^1) * x^(1*2^2) * x^(1*2^3)
        while n > 0:
            if n % 2 == 1:
                result *= x
            x *= x
            n //= 2
        return result
# @lc code=end
