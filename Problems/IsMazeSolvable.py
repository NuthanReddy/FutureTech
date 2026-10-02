def is_safe(x, y, N, M):
    return 0 <= x < N and 0 <= y < M and maze[x][y] == 1


# Pattern identification: find a right/down maze path -> DFS with backtracking;
# mark the tentative path and undo failed branches; legacy goal validation/aliasing is unchanged.
def solve_maze(x, y, solution):
    # 1. Output: Attempt to return whether a right/down path reaches the bottom-right cell.
    # 2. Structure: Open cells are 1s; choosing down or right can hit a dead end, so alternatives matter.
    # 3. Constraints: Assume nonempty matching grids; only right and down moves are tried.
    # 4. Choice: Mark a safe cell, try down then right, and clear the mark if both fail.
    # 5. Why it works: A valid path should contain only open cells, but the goal is
    # accepted before checking safety; the demo also aliases solution and maze, changing the input.
    N = len(solution)
    M = len(solution[0])
    if x == N - 1 and y == M - 1:
        solution[x][y] = 1
        return True

    if is_safe(x, y, N, M):
        solution[x][y] = 1
        print("")
        print("before", x, y)
        print_maze(solution)
        if solve_maze(x + 1, y, solution):
            return True
        if solve_maze(x, y + 1, solution):
            return True
        # Neither continuation worked; remove this tentative path cell.
        solution[x][y] = 0  # Backtrack
        print("after", x, y)
        print_maze(solution)

    return False


def print_maze(maze):
    for row in maze:
        print(" ".join(map(str, row)))


if __name__ == "__main__":
    maze = [
        [1, 0, 0, 0],
        [1, 1, 0, 1],
        [0, 1, 0, 0],
        [1, 1, 1, 1]
    ]
    if solve_maze(0, 0, maze):
        print("")
        print_maze(maze)
    else:
        print("No solution found")
