"""Evaluate Reverse Polish Notation.

Problem: Given a valid arithmetic expression in Reverse Polish (postfix)
notation, evaluate it using integer addition, subtraction, multiplication,
and division.

Input: ``tokens``, a list of integer literals and operators ``+``, ``-``,
``*``, and ``/``; each operator has two previous operands.
Output: the single integer result, with division truncated toward zero.
Constraints: tokens are normally valid and contain no division by zero; this
module additionally raises clear errors for malformed input.
"""

from __future__ import annotations


def eval_reverse_polish(tokens: list[str]) -> int:
    """Evaluate *tokens*, truncating division toward zero.

    In RPN, an operator always applies to the two most recently produced
    values.  It therefore needs no precedence table or look-ahead.  Python's
    ``//`` rounds toward negative infinity, unlike the interview problem's
    truncation rule, so division is implemented through absolute values.

    Complexity: O(n) time and O(n) space.
    """
    stack: list[int] = []
    operators = {"+", "-", "*", "/"}

    for token in tokens:
        if token not in operators:
            try:
                stack.append(int(token))
            except ValueError as error:
                raise ValueError(f"invalid RPN token: {token!r}") from error
            continue

        if len(stack) < 2:
            raise ValueError("operator does not have two operands")
        right = stack.pop()
        left = stack.pop()
        if token == "+":
            result = left + right
        elif token == "-":
            result = left - right
        elif token == "*":
            result = left * right
        else:
            if right == 0:
                raise ZeroDivisionError("RPN division by zero")
            result = (abs(left) // abs(right)) * (-1 if (left < 0) ^ (right < 0) else 1)
        stack.append(result)

    if len(stack) != 1:
        raise ValueError("RPN expression must leave exactly one value")
    return stack[0]


if __name__ == "__main__":
    assert eval_reverse_polish(["2", "1", "+", "3", "*"]) == 9
    assert eval_reverse_polish(["4", "13", "5", "/", "+"]) == 6
    assert eval_reverse_polish(["7", "-3", "/"]) == -2
    print("Evaluate RPN: all checks passed")
