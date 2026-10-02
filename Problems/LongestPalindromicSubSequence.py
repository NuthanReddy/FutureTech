# Pattern identification: return a contiguous palindrome -> expand around odd/even centers, not subsequence DP;
# intended invariant: endpoints enclose a palindrome; the legacy even-center indices are inconsistent.
def longestPalindrome(s: str) -> str:
    # 1. Output: Seek a longest contiguous palindrome, not a subsequence; first try odd lengths.
    # 2. Structure: Contiguous palindromes mirror around a center; trying each center avoids testing every slice.
    # 3. Constraints: Expansion must stay in bounds; empty input leaves the result "".
    # 4. Choice: Start both pointers at each center, expand matching endpoints, and keep the longest slice.
    # 5. Why it works: A matched pair extends an already-palindromic interior; every odd
    # palindrome has one of these centers, so this phase finds the best odd-length candidate.
    n = len(s)
    if n == 1:
        return s

    max_l = 0
    max_p = ""
    # odd
    for i in range(n):
        right_i = i
        left_i = i
        while (right_i < n and left_i >= 0):
            if s[right_i] == s[left_i]:
                if max_l < (right_i - left_i + 1):
                    max_l = (right_i - left_i + 1)
                    max_p = s[left_i:right_i + 1]
            else:
                break
            right_i += 1
            left_i -= 1
        print(i, max_p)
    # even
    # 1. Output: Try to improve the same result with an even-length palindrome.
    # 2. Structure: Even palindromes mirror around an adjacent-character gap, which single-center scans miss.
    # 3. Constraints: Stay within the string; the initial reversed pointers give length zero.
    # 4. Choice: This attempt starts right at i and left at i+1, then moves them outward.
    # 5. Why it works: The first comparison checks the center pair without saving it;
    # the next repeats that pair in normal order, after which matching outer pairs extend a palindrome.
    for i in range(n - 1):
        right_i = i
        left_i = i + 1
        while (right_i < n and left_i >= 0):
            if s[right_i] == s[left_i]:
                if max_l < (right_i - left_i + 1):
                    max_l = (right_i - left_i + 1)
                    max_p = s[left_i:right_i + 1]
            else:
                break
            right_i += 1
            left_i -= 1
        print(i, max_p)
    return max_p

longestPalindrome("aacabdkacaa")