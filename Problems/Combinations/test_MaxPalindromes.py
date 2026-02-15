# Test cases for max_palindromes function
from MaxPalindromes import max_palindromes

def test_max_palindromes():
    # Test 1: Simple case
    # slots [3, 2, 1] need 6 chars total (3+2+1), so freq must have at least 6 chars
    slots = [3, 2, 1]
    freq = {'a': 4, 'b': 2}  # 6 chars total: 2 pairs from 'a', 1 pair from 'b'
    assert max_palindromes(slots, freq) == 3

    # Test 2: Not enough pairs
    slots = [4, 3]
    freq = {'a': 2, 'b': 1}
    assert max_palindromes(slots, freq) == 1

    # Test 3: No center available
    slots = [3]
    freq = {'a': 2}
    assert max_palindromes(slots, freq) == 0

    # Test 4: Large input
    # 3 slots of length 5 need: 6 pairs + 3 centers = 15 chars
    # freq has 7 pairs + 1 single = 15 chars, so all 3 can be filled
    slots = [5, 5, 5]
    freq = {'a': 6, 'b': 6, 'c': 3}
    assert max_palindromes(slots, freq) == 3

    # Test 5: All slots can be filled
    slots = [2, 2, 2]
    freq = {'a': 6}
    assert max_palindromes(slots, freq) == 3

    print("All tests passed.")

if __name__ == "__main__":
    test_max_palindromes()
