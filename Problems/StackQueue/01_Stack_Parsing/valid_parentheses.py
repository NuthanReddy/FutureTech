"""Valid Parentheses.

Problem: Given a string containing ``()``, ``[]``, ``{}``, and optionally
other characters, determine whether every opening bracket is closed by the
matching bracket in the correct nesting order.

Input: ``text``, the string to inspect.
Output: ``True`` when brackets are balanced and correctly nested; otherwise
``False``.  This implementation ignores non-bracket characters.
Constraints: the input may be empty and can contain arbitrarily nested
brackets; malformed closing or unmatched opening brackets are invalid.
"""

from __future__ import annotations


def is_valid_parentheses(text: str) -> bool:
    """Return whether *text* contains correctly nested bracket pairs.

    Non-bracket characters are ignored, which makes the helper useful for
    expressions such as ``"(a + b) * [c]"``.  The top of the stack must match
    each closing bracket; matching only counts is not sufficient because
    nesting order matters.

    Complexity: O(n) time and O(n) worst-case space.
    """
    closing_to_open = {")": "(", "]": "[", "}": "{"}
    stack: list[str] = []

    for character in text:
        if character in "([{":
            stack.append(character)
        elif character in closing_to_open:
            if not stack or stack.pop() != closing_to_open[character]:
                return False

    return not stack


if __name__ == "__main__":
    assert is_valid_parentheses("()[]{}")
    assert is_valid_parentheses("a + ([b] * {c})")
    assert not is_valid_parentheses("(]")
    assert not is_valid_parentheses("([)]")
    assert not is_valid_parentheses("(")
    print("Valid Parentheses: all checks passed")
