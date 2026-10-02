# Pattern identification: nested matching delimiters -> LIFO opener stack;
# the top is the next required match, and success leaves no unmatched openers.
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
    # 1. Output: Return whether every bracket has a matching partner in the correct nesting order.
    # 2. Structure: A closer must match the most recent unclosed opener; other characters are ignored.
    # 3. Constraints: Empty text is valid; reject early closers and leftover openers. O(n) time/space.
    # 4. Choice: Matching counts cannot check nesting, so stack openers and match each closer against the newest one.
    # A dictionary gives the required opener type; reject a mismatch instead of searching deeper in the stack.
    # 5. Why it works: The stack contains exactly the unclosed openers in encounter order.
    # Matching the newest preserves nesting; an empty final stack means every opener was closed.
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
