# Stack and Queue Patterns — Top Interview 150

This package covers the stack/queue problems from the Top Interview 150 set that
are most useful as reusable patterns. Each problem is kept in a small,
stand-alone module so it can be imported or run directly.

## Coverage by pattern

| Pattern | Problems | Why this pattern fits |
| --- | --- | --- |
| Delimiter stack | [Valid Parentheses](./01_Stack_Parsing/valid_parentheses.py) | The most recent unmatched opener must be closed first (LIFO). |
| Stack-backed design | [Min Stack](./02_Stack_Design/min_stack.py) | A second stack stores the minimum for every prefix. |
| Expression stacks | [Evaluate RPN](./01_Stack_Parsing/evaluate_reverse_polish_notation.py), [Basic Calculator](./01_Stack_Parsing/basic_calculator.py) | Operators consume the most recent operands or pending signs. |
| Monotonic stack | [Daily Temperatures](./03_Monotonic_Stack/daily_temperatures.py), [Largest Rectangle](./03_Monotonic_Stack/largest_rectangle.py) | Discard entries that can no longer be the next greater boundary. |
| Greedy stack | [Car Fleet](./04_Greedy_Stack/car_fleet.py) | Cars are processed from the target backwards; slower arrival times absorb faster cars. |
| Monotonic queue/deque | [Sliding Window Maximum](./05_Monotonic_Queue/sliding_window_maximum.py) | Keep only candidates that can still become a window maximum. |

## Problem statements

Each linked module repeats its statement in its module docstring. The concise
contracts below make the inputs and required outputs visible before opening
the implementation.

### Valid Parentheses

Given a string containing parentheses, brackets, braces, and possibly other
characters, return whether every opening bracket is closed by the matching
bracket in correct nesting order. Input is a string; output is a Boolean.
Non-bracket characters are ignored here. Empty input is valid.

### Evaluate Reverse Polish Notation

Given a valid postfix expression as a list of integer literals and `+`, `-`,
`*`, `/` operators, return its integer result. Division truncates toward zero.
Each operator has two prior operands; division by zero is invalid.

### Basic Calculator

Evaluate a string containing non-negative integers, `+`, `-`, parentheses, and
whitespace, including nested parentheses. Input is the expression string;
output is its integer value. The standard constraints guarantee valid syntax.

### Min Stack

Design a stack of integers supporting `push`, `pop`, `top`, and minimum lookup
in constant time. Operations receive or return integers; `get_min` returns the
smallest current value. Duplicate and negative values are allowed, and empty
stack queries are invalid.

### Daily Temperatures

For each day in a list of temperatures, return the number of days until a
strictly warmer temperature; return zero if none exists. Input is an integer
temperature list; output is an equally long integer list. Equal temperatures
do not count as warmer.

### Largest Rectangle in Histogram

Given non-negative heights of unit-width contiguous histogram bars, return the
largest rectangle area formed by one or more adjacent bars. Input is the
height list; output is an integer area. An empty histogram has area zero.

### Car Fleet

Given a target, starting positions, and positive speeds for cars traveling in
one lane, count fleets reaching the target. A faster car cannot pass a car
ahead, and meeting cars become one fleet. Input arrays are paired and equal in
length; output is the fleet count.

### Sliding Window Maximum

Given an integer list and fixed window size `k`, return the maximum from every
contiguous window in left-to-right order. Output length is `n-k+1` when
`1 <= k <= n`; this implementation returns an empty list for invalid `k`.

## Method comparisons and rationale

### Stack parsing and expression evaluation

Balanced delimiters require remembering all unmatched opening delimiters, so a
stack is the direct representation of the grammar. Reverse Polish notation
avoids precedence and parentheses: operands are pushed, and an operator pops
its right operand then its left operand. The calculator uses one stack of
`(previous_total, sign)` contexts because an opening parenthesis saves exactly
the state needed to resume after the matching close.

Alternatives such as repeated string replacement or converting RPN to infix
are less explicit and can become quadratic or introduce precedence bugs.
The implementations are single-pass, `O(n)` time, and `O(n)` space. They
validate malformed input where the problem statement does not guarantee valid
input, but do not attempt to recover from errors.

### Min Stack

Keeping only one stack and scanning it for `min()` makes `get_min()` `O(n)`.
The paired value/minimum stacks make every operation `O(1)` at the cost of
`O(n)` extra space. Encoding the previous minimum into one arithmetic stack is
possible, but is harder to read and can overflow in fixed-width languages; it
is intentionally not used here.

### Monotonic stack

For each item, a naive search to the right is `O(n²)`. A monotonic stack makes
each index enter and leave once, giving `O(n)` time and `O(n)` space. The
stored indices (rather than values) preserve distances and rectangle widths.
Equal values use a deliberate tie rule in each module; changing it casually
can duplicate work or produce incorrect widths.

### Greedy car fleet

Sorting cars by position descending is required because the car nearest the
target determines the arrival-time barrier for cars behind it. A car joins the
current fleet when its arrival time is no greater than that barrier. Sorting
by speed or simulating time is unnecessary and can be slower. Complexity is
`O(n log n)` for sorting and `O(n)` extra space.

### Monotonic queue

Recomputing `max(window)` costs `O(k)` per window. A max-heap improves this to
`O(n log k)` but needs lazy deletion of stale entries. A deque stores indices
in decreasing-value order, removes expired indices from the front, and removes
dominated indices from the back. Each index is inserted and removed at most
once: `O(n)` time and `O(k)` space. The function returns an empty list for
invalid window sizes rather than inventing a sentinel maximum.

## Running examples

From the repository root:

```powershell
python "Problems/StackQueue/03_Monotonic_Stack/daily_temperatures.py"
python "Problems/StackQueue/05_Monotonic_Queue/sliding_window_maximum.py"
```

Every module includes focused assertions in its `__main__` block.
