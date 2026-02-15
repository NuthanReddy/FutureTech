# given a list of slots with length as an array of integers and a list of characters and their frequencies
# as a dictionary, find max no of palindromes formed by filling the slots with the given characters

def max_palindromes(slot_lengths, char_freq):
    """
    For a palindrome of length L:
    - We need L // 2 pairs of characters
    - If L is odd, we need 1 additional character for the center

    Key insight: total characters needed = L (pairs contribute 2 each, center contributes 1)
    We can use any character for the center, even if it could form a pair.
    """

    def backtrack(remaining_slots, freq):
        if not remaining_slots:
            return 0

        max_count = 0
        # Try to fill each remaining slot
        for i, slot_len in enumerate(remaining_slots):
            needed_pairs = slot_len // 2
            needed_center = slot_len % 2

            # Check if we have enough pairs
            available_pairs = sum(v // 2 for v in freq.values())
            total_chars = sum(freq.values())

            # After using needed_pairs, we need at least 1 char left for center (if odd)
            if available_pairs < needed_pairs:
                continue
            if needed_center and total_chars < slot_len:
                continue

            # Try to allocate - use pairs first
            freq_copy = dict(freq)
            pairs_to_use = needed_pairs
            for c in list(freq_copy.keys()):
                use = min(freq_copy[c] // 2, pairs_to_use)
                freq_copy[c] -= use * 2
                pairs_to_use -= use
                if pairs_to_use == 0:
                    break

            if pairs_to_use > 0:
                continue

            # Allocate center if needed
            if needed_center:
                center_found = False
                for c in freq_copy:
                    if freq_copy[c] > 0:
                        freq_copy[c] -= 1
                        center_found = True
                        break
                if not center_found:
                    continue

            # Remove this slot and recurse
            new_slots = remaining_slots[:i] + remaining_slots[i+1:]
            count = 1 + backtrack(new_slots, freq_copy)
            max_count = max(max_count, count)

        return max_count

    return backtrack(list(slot_lengths), dict(char_freq))
