"""
Social Graph System (Facebook-like)

Implements a social graph using an adjacency list with support for:
- Add / remove friendships (bidirectional)
- Mutual friends between two users
- Friend-of-friend recommendations ("People You May Know")
- Degrees of separation via BFS
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Optional


class SocialGraph:
    """In-memory social graph backed by an adjacency list (dict of sets)."""

    def __init__(self) -> None:
        self._adj: dict[str, set[str]] = defaultdict(set)

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def add_user(self, user: str) -> None:
        """Register a user node (no-op if already present)."""
        if user not in self._adj:
            self._adj[user] = set()

    def add_friend(self, user_a: str, user_b: str) -> None:
        """Create a bidirectional friendship edge between *user_a* and *user_b*."""
        if user_a == user_b:
            raise ValueError("A user cannot befriend themselves")
        self._adj[user_a].add(user_b)
        self._adj[user_b].add(user_a)

    def remove_friend(self, user_a: str, user_b: str) -> None:
        """Remove the bidirectional friendship edge, if it exists."""
        self._adj[user_a].discard(user_b)
        self._adj[user_b].discard(user_a)

    def friends(self, user: str) -> set[str]:
        """Return the friend set of *user*."""
        return set(self._adj.get(user, set()))

    def are_friends(self, user_a: str, user_b: str) -> bool:
        """Check whether two users are directly connected."""
        return user_b in self._adj.get(user_a, set())

    # ------------------------------------------------------------------
    # Graph queries
    # ------------------------------------------------------------------

    def mutual_friends(self, user_a: str, user_b: str) -> set[str]:
        """Return friends common to both *user_a* and *user_b*.

        Time complexity: O(min(deg(a), deg(b))) with hash-set intersection.
        """
        return self._adj.get(user_a, set()) & self._adj.get(user_b, set())

    def friend_recommendations(self, user: str, limit: int = 10) -> list[tuple[str, int]]:
        """Recommend people via friend-of-friend scoring.

        Each candidate is ranked by the number of mutual friends they share
        with *user*.  Already-friends and the user themselves are excluded.

        Returns:
            Sorted list of (candidate_user, mutual_count) descending by count.

        Time complexity: O(d^2) where d = average degree.
        """
        scores: dict[str, int] = defaultdict(int)
        user_friends = self._adj.get(user, set())

        for friend in user_friends:
            for fof in self._adj.get(friend, set()):
                if fof != user and fof not in user_friends:
                    scores[fof] += 1

        ranked = sorted(scores.items(), key=lambda x: (-x[1], x[0]))
        return ranked[:limit]

    def degrees_of_separation(
        self, source: str, target: str, max_depth: int = 6
    ) -> Optional[int]:
        """Compute the shortest-path length between *source* and *target* using BFS.

        Args:
            source: Starting user.
            target: Destination user.
            max_depth: Maximum hops to explore (default 6, the classic
                       "six degrees of separation").

        Returns:
            Number of hops, or ``None`` if no path exists within *max_depth*.

        Time complexity: O(b^d) worst case, where b = branching factor,
        d = depth of shortest path.
        """
        if source == target:
            return 0
        if source not in self._adj or target not in self._adj:
            return None

        visited: set[str] = {source}
        queue: deque[tuple[str, int]] = deque([(source, 0)])

        while queue:
            current, depth = queue.popleft()
            if depth >= max_depth:
                continue
            for neighbor in self._adj.get(current, set()):
                if neighbor == target:
                    return depth + 1
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, depth + 1))

        return None

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def user_count(self) -> int:
        return len(self._adj)

    def edge_count(self) -> int:
        return sum(len(nbrs) for nbrs in self._adj.values()) // 2

    def __repr__(self) -> str:
        return f"SocialGraph(users={self.user_count()}, edges={self.edge_count()})"


# ======================================================================
# Demo
# ======================================================================

def _demo() -> None:
    g = SocialGraph()

    # Build a small social network
    #
    #   Alice -- Bob -- Eve -- Grace
    #     |       |             |
    #   Charlie--Diana -- Frank-+
    #     |
    #   Heidi
    #
    people = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Heidi"]
    for p in people:
        g.add_user(p)

    edges = [
        ("Alice", "Bob"),
        ("Alice", "Charlie"),
        ("Bob", "Diana"),
        ("Bob", "Eve"),
        ("Charlie", "Diana"),
        ("Charlie", "Heidi"),
        ("Diana", "Frank"),
        ("Eve", "Grace"),
        ("Frank", "Grace"),
    ]
    for a, b in edges:
        g.add_friend(a, b)

    print(f"Graph: {g}")
    print()

    # Friend list
    print("Alice's friends:", sorted(g.friends("Alice")))
    print("Bob's friends:  ", sorted(g.friends("Bob")))
    print()

    # Mutual friends
    mutual = g.mutual_friends("Alice", "Diana")
    print(f"Mutual friends (Alice, Diana): {sorted(mutual)}")
    mutual2 = g.mutual_friends("Alice", "Eve")
    print(f"Mutual friends (Alice, Eve):   {sorted(mutual2)}")
    print()

    # Friend recommendations
    recs = g.friend_recommendations("Alice", limit=5)
    print("Recommendations for Alice (People You May Know):")
    for candidate, mutual_count in recs:
        print(f"  {candidate:10s} -- {mutual_count} mutual friend(s)")
    print()

    # Degrees of separation
    pairs = [
        ("Alice", "Grace"),
        ("Alice", "Frank"),
        ("Alice", "Heidi"),
        ("Alice", "Alice"),
        ("Grace", "Heidi"),
    ]
    print("Degrees of separation:")
    for src, dst in pairs:
        deg = g.degrees_of_separation(src, dst)
        label = str(deg) if deg is not None else "not connected"
        print(f"  {src:8s} -> {dst:8s} : {label}")
    print()

    # Remove a friend and verify
    print("Removing friendship: Alice -- Bob")
    g.remove_friend("Alice", "Bob")
    print("Alice's friends after removal:", sorted(g.friends("Alice")))
    print("Are Alice and Bob still friends?", g.are_friends("Alice", "Bob"))
    print()

    # Updated recommendations after removal
    recs2 = g.friend_recommendations("Alice", limit=5)
    print("Updated recommendations for Alice:")
    for candidate, mutual_count in recs2:
        print(f"  {candidate:10s} -- {mutual_count} mutual friend(s)")


if __name__ == "__main__":
    _demo()
