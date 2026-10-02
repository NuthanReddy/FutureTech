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
        # 1. Output: Return the nth count-and-say term, starting with "1" as term one.
        # 2. Structure: Only consecutive equal digits form a run; each term depends on the previous one, not a frequency total.
        # 3. Constraints: Assume n >= 1; the class-level sequence cache is shared across calls and instances.
        # 4. Choice: Reuse cached terms; otherwise scan each previous term's runs and append count/digit pairs until term n exists.
        # 5. Why it works: The cache stays a correct sequence prefix, and rle emits each completed run and the final run.
        #    New work is linear in encoded term lengths; the cache retains the total length of all stored terms.
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
