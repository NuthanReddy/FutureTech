words = ["abcd", "good", "food", "hoof", "hood"]


def get_num_repr(word):
    char_dict = {"a": "2", "b": "2", "c": "2", "d": "3", "e": "3", "f": "3", "o": "6", "g": "4", "h": "4"}
    num_char = ""
    for char in word:
        num_char += char_dict[char]
    return int(num_char)


# Pattern identification: retrieve words by keypad code -> hash-based grouping;
# each bucket contains words with the same numeric encoding; the character map is partial.
def build_word_dict(words):
    # 1. Output: Return a dictionary mapping numeric keypad codes to lists of matching words.
    # 2. Structure: Many words share one digit code, so dictionary buckets support direct code-to-word lookup.
    # 3. Constraints: Words must be nonempty and use only letters in the helper's partial mapping.
    # 4. Choice: Encode each word, then append it to its code's bucket or create that bucket.
    # 5. Why it works: After each word, every processed word is stored under its own code;
    # collisions deliberately share a bucket, so a lookup retrieves all matching processed words.
    word_dict = {}
    for word in words:
        num_repr = get_num_repr(word)
        try:
            word_dict[num_repr].append(word)
        except KeyError:
            word_dict[num_repr] = [word]
    return word_dict


word_dict = build_word_dict(words)
print(word_dict[4663])
