from time import time


# Pattern identification: two predecessor recurrence -> naive recursive decomposition;
# each result sums the two preceding terms; repeated states are not cached.
def fib(n):
    # 1. Output: Return term n of the shifted sequence 1, 1, 2, 3, ... .
    # 2. Structure: The recurrence directly splits a term into two smaller terms, making recursion natural.
    # 3. Constraints: Assume integer n>=0; large n repeats work exponentially and risks deep recursion.
    # 4. Choice: Return one at indices zero and one; recursively sum fib(n-2) and fib(n-1).
    # 5. Why it works: Both calls reduce n toward the base cases, and adding their
    # correct terms follows the recurrence; unlike fib.py, the zeroth term is one.
    if n==0:
        return 1
    if n==1:
        return 1
    return fib(n-2) +fib(n-1)


# Pattern identification: repeated Fibonacci subproblems -> bottom-up DP;
# fill earlier terms before dp[i]; this variant assumes n >= 1.
def fib2(n):
    # 1. Output: Return term n of the sequence starting with two ones.
    # 2. Structure: Recursive branches ask for the same indices repeatedly; a table avoids that repeated work.
    # 3. Constraints: This variant assumes n>=1; n=0 fails when assigning dp[1].
    # 4. Choice: Store the base terms, then fill dp[i] from dp[i-1] and dp[i-2].
    # 5. Why it works: On valid inputs, both predecessors are already correct before
    # each update; filling the table takes O(n) additions and O(n) stored numbers.
    dp = [0]*(n+1)
    dp[0] = 1
    dp[1] = 1

    for i in range(2, n+1):
        dp[i] = dp[i-1] + dp[i-2]

    return dp[n]


fib_arr = [1, 1]


# Pattern identification: repeated queries for later terms -> persistent incremental DP;
# fib_arr retains the computed prefix and extends it only for missing indices.
def fib3(n):
    # 1. Output: Return term n of the sequence starting with two ones across repeated calls.
    # 2. Structure: Later queries need the same earlier terms, so a persistent list avoids recomputing them.
    # 3. Constraints: Assume n>=0 and fib_arr remains an unmodified correct prefix.
    # 4. Choice: Extend the global list from its current length through n, adding two predecessors.
    # 5. Why it works: Every appended term extends the correct prefix; a previously
    # computed index needs no new additions, while new indices reuse earlier work.
    global fib_arr
    l = len(fib_arr)

    for i in range(l, n+1):
        fib_arr.append(fib_arr[i-2]+ fib_arr[i-1])
    return fib_arr[n]


print("Bottom Up - Global")
t1 = time()
print(fib3(1000))
t2 = time()
print(f'Function fib3 executed in {(t2 - t1):.4f}s')

t1 = time()
print(fib3(1001))
t2 = time()
print(f'Function fib3 - run2 executed in {(t2 - t1):.4f}s')
print("")

print("Bottom Up")
t1 = time()
print(fib2(1000))
t2 = time()
print(f'Function fib2 executed in {(t2 - t1):.4f}s')

t1 = time()
print(fib2(1001))
t2 = time()
print(f'Function fib2 - run2 executed in {(t2 - t1):.4f}s')

print("")

print("Top Down")
t1 = time()
print(fib(30))
t2 = time()
print(f'Function fib executed in {(t2 - t1):.4f}s')
