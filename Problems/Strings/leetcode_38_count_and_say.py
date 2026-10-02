#
# @lc app=leetcode id=38 lang=python3
#
# [38] Count and Say
#
# Pattern identification: each term describes adjacent runs -> iterative run-length encoding;
# cached terms form the sequence prefix; each encoding emits completed runs plus the final run.

# @lc code=start


class Solution:
    seq = ["1"]

    def countAndSay(self, n: int) -> str:
        if n <= len(self.seq):
            return self.seq[n - 1]
        for i in range(len(self.seq), n):
            self.seq.append(self.rle(self.seq[-1]))
        return self.seq[n - 1]
    
    def rle(self, s: str) -> str:
        count = 1
        result = []
        for i in range(1, len(s)):
            if s[i] == s[i - 1]:
                count += 1
            else:
                result.append(str(count))
                result.append(s[i - 1])
                count = 1
        result.append(str(count))
        result.append(s[-1])
        return "".join(result)
        
# @lc code=end
