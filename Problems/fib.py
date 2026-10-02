# Pattern identification: recurrence needs only two predecessors -> rolling-state DP;
# prev_fib and curr_fib hold consecutive Fibonacci values before each update.
def fibonacci(n):
    # 1. Output: Return Fibonacci term n with F(0)=0 and F(1)=1.
    # 2. Structure: Each term needs only the last two values, so older terms need not be stored.
    # 3. Constraints: Assume an integer n>=0; only two previous values need storage.
    # 4. Choice: Start with 0 and 1, then replace the pair with current and their sum.
    # 5. Why it works: The pair always holds consecutive terms; after n-1 updates
    # current is F(n), using O(n) additions and O(1) stored numbers.
    if n == 0:
        return 0
    elif n == 1:
        return 1
    prev_fib = 0
    curr_fib = 1
    for _ in range(n - 1):
        new_fib = curr_fib + prev_fib
        prev_fib = curr_fib
        curr_fib = new_fib
    return curr_fib


for i in range(10):
    print(fibonacci(i))
