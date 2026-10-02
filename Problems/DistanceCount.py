matrix = [
    [1, 0, 1, 0, 1],
    [0, 1, 1, 0, 0],
    [1, 1, 0, 1, 0],
    [1, 0, 1, 0, 1],
    [0, 1, 0, 1, 0]
]



'''
result = [
    [2, 4, 3, 3, 1],
    [4, 6, 5, 4, 2],
    [4, 6, 5, 4, 2],
    [4, 5, 5, 4, 3],
    [2, 3, 3, 3, 2]
]
'''


# Pattern identification: sum a fixed 3x3 neighborhood -> bounded grid stencil;
# count only in-bounds offsets (including self); legacy output slicing is unchanged.
def count_neighbours(input):
    # 1. Output: Intend to return the sum of each cell's 3x3 neighborhood, including itself.
    # 2. Structure: Only immediate grid neighbors matter, so nine fixed offsets replace any path search.
    # 3. Constraints: Out-of-bounds neighbors contribute zero; empty input is returned unchanged.
    # 4. Choice: Sum safe_get values for every offset and write each sum into output[r][c].
    # 5. Why it works: Each written sum covers the right neighbors, but output[1:-1]
    # drops the first computed row and retains padding, so the returned grid is misaligned.
    rows = len(input)
    if rows == 0:
        return input
    cols = len(input[0])
    output = [[0 for x in range(cols + 2)] for x in range(rows + 2)]
    indices = [(-1,-1), (-1,0), (-1,1), (0,-1), (0,0), (0,1), (1,-1), (1,0), (1,1)]
    for r in range(rows):
        for c in range(cols):
            output[r][c] = sum([safe_get(input, r+x, c+y) for (x, y) in indices])
            print(output)
    return [x for x in output[1:-1]]


def safe_get(input, r, c):
    rows = len(input)
    cols = len(input[0])
    if 0 <= r < rows and 0 <= c < cols:
        return input[r][c]
    else:
        return 0


print(count_neighbours(matrix))