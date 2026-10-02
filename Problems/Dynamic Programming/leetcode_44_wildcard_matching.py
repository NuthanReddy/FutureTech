#
# @lc app=leetcode id=44 lang=python3
#
# [44] Wildcard Matching
#

# @lc code=start
class Solution:
    def isMatch(self, s: str, p: str) -> bool:
        """Pattern identification: full-string match with ?/* -> two-prefix DP ->
        each state matches complete prefixes; * skips itself or consumes a character.
        """
        # 1. Output: Return whether pattern p matches all of string s.
        # 2. Structure: '?' uses one character but '*' has several possible lengths;
        #    choices revisit the same string/pattern beginnings, so remember their matches.
        # 3. Constraints: Empty strings/patterns are allowed; matching is not partial.
        #    For lengths m and n: O((m+1)*(n+1)) time and space.
        # 4. Choice: dp[i][j] says s[:i] matches p[:j]; seed empty/empty and leading-star matches.
        #    '*' skips itself (left) or consumes a character (top); a matching letter/'?' uses diagonal.
        # 5. Why it works: A star either consumes nothing or one more character;
        #    these cases and single-character matches cover all full-prefix matches.
        m, n = len(s), len(p)
        dp = [[False] * (n + 1) for _ in range(m + 1)]
        dp[0][0] = True
        for j in range(1, n + 1):
            if p[j - 1] == "*":
                dp[0][j] = dp[0][j - 1]
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if p[j - 1] == "*":
                    dp[i][j] = dp[i][j - 1] or dp[i - 1][j]
                elif p[j - 1] == "?" or s[i - 1] == p[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1]
                
        return dp[m][n]
# @lc code=end
