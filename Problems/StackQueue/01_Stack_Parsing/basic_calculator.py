# Pattern identification: nested +/- expression -> stack of outer total/sign contexts;
# total evaluates the current scope, and each saved frame resumes its parent scope.
"""Basic Calculator.

Problem: Evaluate an arithmetic expression containing non-negative integers,
``+``, ``-``, parentheses, and optional whitespace. Parentheses may nest.

Input: ``expression``, a string such as ``"(1+(4+5+2)-3)+(6+8)"``.
Output: the integer value of the expression.
Constraints: the standard problem guarantees valid syntax and does not use
multiplication or division; this implementation raises ``ValueError`` for
unexpected characters or unmatched parentheses.
"""

from __future__ import annotations


def calculate(expression: str) -> int:
    """Return the value of a Basic Calculator expression.

    ``stack`` stores the total and sign that were active before each opening
    parenthesis.  On ``)``, the inner total is multiplied by the saved sign
    and added to the outer total.  Treating a unary sign as a sign before the
    next number or parenthesized group also handles inputs such as ``1-(-2)``.

    Complexity: O(n) time and O(n) space for nested parentheses.
    """
    # 1. Output: Return the integer value of the expression.
    # 2. Structure: Numbers and +/- signs form scopes nested by parentheses; spaces separate nothing important.
    # 3. Constraints: Assume valid calculator syntax without */; O(n) time and O(n) worst-case saved scopes.
    # 4. Choice: The newest opened parentheses close first, so save outer (total, sign) on a stack.
    # Track the inner total and next sign until a closing parenthesis restores that saved outer work.
    # 5. Why it works: total evaluates the consumed terms in this scope; closing a scope restores its parent.
    # Applying the saved sign includes the whole inner result exactly once in the outer total.
    stack: list[tuple[int, int]] = []
    total = 0
    sign = 1
    index = 0

    while index < len(expression):
        character = expression[index]
        if character.isspace():
            index += 1
        elif character.isdigit():
            number = 0
            while index < len(expression) and expression[index].isdigit():
                number = number * 10 + int(expression[index])
                index += 1
            total += sign * number
            sign = 1
        elif character in "+-":
            sign = 1 if character == "+" else -1
            index += 1
        elif character == "(":
            stack.append((total, sign))
            total, sign = 0, 1
            index += 1
        elif character == ")":
            if not stack:
                raise ValueError("unmatched closing parenthesis")
            outer_total, outer_sign = stack.pop()
            total = outer_total + outer_sign * total
            index += 1
        else:
            raise ValueError(f"unexpected character: {character!r}")

    if stack:
        raise ValueError("unmatched opening parenthesis")
    return total


if __name__ == "__main__":
    assert calculate("1 + 1") == 2
    assert calculate(" 2-1 + 2 ") == 3
    assert calculate("(1+(4+5+2)-3)+(6+8)") == 23
    assert calculate("1-(-2)") == 3
    print("Basic Calculator: all checks passed")
