"""
Search Autocomplete / Typeahead System
=======================================

A trie-based autocomplete system that returns top-k most frequent completions
for a given prefix. Supports:

- Prefix-based lookup in O(L) where L = prefix length
- Precomputed top-k suggestions at every trie node for O(1) retrieval
- Query frequency tracking and logging
- Offline trie rebuild from aggregated query logs
- Trending query boost via time-decayed scoring

Architecture
------------
- TrieNode: stores children, end-of-word flag, frequency, and top-k cache
- Trie: insert, search, delete, and autocomplete operations
- QueryLogger: append-only log of search queries with timestamps
- AutocompleteService: orchestrates trie + logger + periodic rebuild

Complexity
----------
- Insert: O(L) where L = query length
- Autocomplete: O(L) prefix traversal + O(1) top-k lookup (precomputed)
- Trie rebuild: O(N * k * log k) where N = total nodes, k = top-k size
"""

from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Trie Node
# ---------------------------------------------------------------------------

class TrieNode:
    """A single node in the prefix trie."""

    __slots__ = ("children", "is_end", "frequency", "query", "top_k")

    def __init__(self) -> None:
        self.children: dict[str, TrieNode] = {}
        self.is_end: bool = False
        self.frequency: int = 0
        self.query: str = ""
        self.top_k: list[tuple[str, int]] = []

    def __repr__(self) -> str:
        return (
            f"TrieNode(is_end={self.is_end}, freq={self.frequency}, "
            f"children={list(self.children.keys())})"
        )


# ---------------------------------------------------------------------------
# Trie
# ---------------------------------------------------------------------------

class Trie:
    """
    Prefix trie with frequency counts and precomputed top-k suggestions.

    Each node caches the top-k most frequent completions reachable from
    that prefix, so autocomplete queries return in O(prefix_length) time.
    """

    def __init__(self, k: int = 10) -> None:
        self.root = TrieNode()
        self.k = k
        self._size = 0

    @property
    def size(self) -> int:
        """Number of distinct queries stored in the trie."""
        return self._size

    # -- Mutators -----------------------------------------------------------

    def insert(self, query: str, frequency: int = 1) -> None:
        """Insert a query with its frequency count."""
        if not query:
            return
        node = self.root
        for char in query.lower():
            if char not in node.children:
                node.children[char] = TrieNode()
            node = node.children[char]
        if not node.is_end:
            self._size += 1
        node.is_end = True
        node.frequency = frequency
        node.query = query.lower()

    def delete(self, query: str) -> bool:
        """Remove a query from the trie. Returns True if it existed."""
        if not query:
            return False
        return self._delete_helper(self.root, query.lower(), 0)

    def _delete_helper(self, node: TrieNode, query: str, depth: int) -> bool:
        if depth == len(query):
            if not node.is_end:
                return False
            node.is_end = False
            node.frequency = 0
            node.query = ""
            self._size -= 1
            return True
        char = query[depth]
        if char not in node.children:
            return False
        found = self._delete_helper(node.children[char], query, depth + 1)
        child = node.children[char]
        if not child.is_end and not child.children:
            del node.children[char]
        return found

    # -- Queries ------------------------------------------------------------

    def search(self, query: str) -> Optional[int]:
        """Return frequency if the exact query exists, else None."""
        node = self._find_node(query.lower())
        if node and node.is_end:
            return node.frequency
        return None

    def autocomplete(self, prefix: str) -> list[tuple[str, int]]:
        """
        Return precomputed top-k suggestions for the given prefix.

        Returns a list of (query, frequency) tuples sorted by frequency
        descending. Call rebuild_top_k() after bulk inserts to refresh.
        """
        node = self._find_node(prefix.lower())
        if node is None:
            return []
        return list(node.top_k)

    def starts_with(self, prefix: str) -> bool:
        """Return True if any query in the trie starts with this prefix."""
        return self._find_node(prefix.lower()) is not None

    # -- Top-K Precomputation -----------------------------------------------

    def rebuild_top_k(self) -> None:
        """
        Rebuild the top-k suggestion cache at every node via post-order
        traversal. Call after bulk inserts or trie rebuild.

        Complexity: O(N * k * log k) where N = number of nodes.
        """
        self._rebuild_node(self.root)

    def _rebuild_node(self, node: TrieNode) -> list[tuple[str, int]]:
        """Post-order traversal to propagate top-k up from leaves."""
        candidates: list[tuple[str, int]] = []
        if node.is_end:
            candidates.append((node.query, node.frequency))
        for child in node.children.values():
            candidates.extend(self._rebuild_node(child))
        candidates.sort(key=lambda x: x[1], reverse=True)
        node.top_k = candidates[: self.k]
        return node.top_k

    # -- Helpers ------------------------------------------------------------

    def _find_node(self, prefix: str) -> Optional[TrieNode]:
        """Traverse the trie to find the node for the given prefix."""
        node = self.root
        for char in prefix:
            if char not in node.children:
                return None
            node = node.children[char]
        return node

    def get_all_queries(self) -> list[tuple[str, int]]:
        """Return all (query, frequency) pairs stored in the trie."""
        results: list[tuple[str, int]] = []
        self._collect(self.root, results)
        return results

    def _collect(
        self, node: TrieNode, results: list[tuple[str, int]]
    ) -> None:
        if node.is_end:
            results.append((node.query, node.frequency))
        for child in node.children.values():
            self._collect(child, results)


# ---------------------------------------------------------------------------
# Query Logger
# ---------------------------------------------------------------------------

@dataclass
class QueryLogEntry:
    """A single logged search query."""
    query: str
    timestamp: float
    user_id: str = "anonymous"


class QueryLogger:
    """
    Append-only query log for tracking search queries.

    Supports aggregation into frequency counts for trie rebuilds and
    time-windowed queries for trending detection.
    """

    def __init__(self) -> None:
        self._logs: list[QueryLogEntry] = []

    def log(self, query: str, user_id: str = "anonymous") -> None:
        """Log a completed search query."""
        self._logs.append(
            QueryLogEntry(
                query=query.lower().strip(),
                timestamp=time.time(),
                user_id=user_id,
            )
        )

    @property
    def count(self) -> int:
        return len(self._logs)

    def aggregate_frequencies(self) -> dict[str, int]:
        """Aggregate all logs into query -> total count."""
        freq: dict[str, int] = defaultdict(int)
        for entry in self._logs:
            freq[entry.query] += 1
        return dict(freq)

    def aggregate_recent(self, window_seconds: float = 300.0) -> dict[str, int]:
        """Aggregate queries from the last `window_seconds`."""
        cutoff = time.time() - window_seconds
        freq: dict[str, int] = defaultdict(int)
        for entry in self._logs:
            if entry.timestamp >= cutoff:
                freq[entry.query] += 1
        return dict(freq)

    def get_user_history(self, user_id: str) -> dict[str, int]:
        """Get query frequencies for a specific user."""
        freq: dict[str, int] = defaultdict(int)
        for entry in self._logs:
            if entry.user_id == user_id:
                freq[entry.query] += 1
        return dict(freq)

    def clear(self) -> None:
        """Clear all logs (e.g., after a trie rebuild)."""
        self._logs.clear()


# ---------------------------------------------------------------------------
# Autocomplete Service
# ---------------------------------------------------------------------------

class AutocompleteService:
    """
    Orchestrates the trie, query logger, and periodic rebuilds.

    Provides the main interface for:
    - Getting autocomplete suggestions for a prefix
    - Logging completed search queries
    - Rebuilding the trie from aggregated log data
    - Applying trending boosts
    """

    def __init__(self, k: int = 10) -> None:
        self.k = k
        self.trie = Trie(k=k)
        self.logger = QueryLogger()
        self._blocked_terms: set[str] = set()

    # -- Public API ---------------------------------------------------------

    def suggest(
        self,
        prefix: str,
        limit: Optional[int] = None,
        user_id: Optional[str] = None,
    ) -> list[tuple[str, int]]:
        """
        Get top-k autocomplete suggestions for a prefix.

        Args:
            prefix: The search prefix typed so far.
            limit: Override the default k.
            user_id: Optional user ID for personalized re-ranking.

        Returns:
            List of (query, score) tuples, highest score first.
        """
        if not prefix:
            return []

        results = self.trie.autocomplete(prefix.lower())

        # Filter blocked terms
        results = [
            (q, f) for q, f in results if q not in self._blocked_terms
        ]

        # Personalization boost
        if user_id:
            results = self._apply_personalization(results, user_id)

        k = limit or self.k
        return results[:k]

    def record_query(self, query: str, user_id: str = "anonymous") -> None:
        """Record a completed search query in the log."""
        self.logger.log(query, user_id=user_id)

    def block_term(self, term: str) -> None:
        """Add a term to the blocklist."""
        self._blocked_terms.add(term.lower().strip())

    def unblock_term(self, term: str) -> None:
        """Remove a term from the blocklist."""
        self._blocked_terms.discard(term.lower().strip())

    # -- Trie Rebuild -------------------------------------------------------

    def rebuild_from_logs(self) -> int:
        """
        Rebuild the trie entirely from aggregated query logs.

        Returns the number of distinct queries inserted.
        """
        frequencies = self.logger.aggregate_frequencies()
        return self._build_trie(frequencies)

    def rebuild_with_trending(
        self, trending_window: float = 300.0, trending_boost: int = 100
    ) -> int:
        """
        Rebuild trie with a boost for recently trending queries.

        Queries that appeared in the recent window get an additive boost
        to their frequency score.
        """
        base_freq = self.logger.aggregate_frequencies()
        recent_freq = self.logger.aggregate_recent(trending_window)

        # Merge: base frequency + trending boost
        merged: dict[str, int] = dict(base_freq)
        for query, count in recent_freq.items():
            boost = count * trending_boost
            merged[query] = merged.get(query, 0) + boost

        return self._build_trie(merged)

    def seed_queries(self, queries: dict[str, int]) -> None:
        """
        Seed the trie with a dictionary of {query: frequency}.
        Useful for initial population from a database export.
        """
        self._build_trie(queries)

    def _build_trie(self, frequencies: dict[str, int]) -> int:
        """Build a new trie from a frequency map and precompute top-k."""
        new_trie = Trie(k=self.k)
        for query, freq in frequencies.items():
            new_trie.insert(query, frequency=freq)
        new_trie.rebuild_top_k()
        self.trie = new_trie
        return new_trie.size

    # -- Personalization ----------------------------------------------------

    def _apply_personalization(
        self,
        results: list[tuple[str, int]],
        user_id: str,
        boost_factor: float = 1.5,
    ) -> list[tuple[str, int]]:
        """Re-rank results by boosting queries the user has searched before."""
        user_history = self.logger.get_user_history(user_id)
        boosted = []
        for query, score in results:
            if query in user_history:
                boosted.append((query, int(score * boost_factor)))
            else:
                boosted.append((query, score))
        boosted.sort(key=lambda x: x[1], reverse=True)
        return boosted

    # -- Stats --------------------------------------------------------------

    def stats(self) -> dict[str, int]:
        """Return service statistics."""
        return {
            "trie_size": self.trie.size,
            "total_logged_queries": self.logger.count,
            "blocked_terms": len(self._blocked_terms),
        }


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _print_header(text: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}")


def _print_suggestions(prefix: str, suggestions: list[tuple[str, int]]) -> None:
    print(f'\n  Prefix: "{prefix}"')
    if not suggestions:
        print("  (no suggestions)")
        return
    for i, (query, score) in enumerate(suggestions, 1):
        print(f"  {i:>3}. {query:<35} score={score}")


def demo_basic_trie() -> None:
    """Demonstrate basic trie operations."""
    _print_header("1. Basic Trie Operations")

    trie = Trie(k=5)

    # Insert sample queries with frequencies
    queries = {
        "facebook": 98500,
        "facebook login": 72300,
        "facebook marketplace": 54100,
        "facetime": 61200,
        "face swap app": 45100,
        "factory reset iphone": 38900,
        "google": 120000,
        "google maps": 95000,
        "google translate": 88000,
        "gmail": 76000,
        "github": 67000,
        "github copilot": 42000,
        "amazon": 110000,
        "amazon prime": 89000,
        "apple": 85000,
        "apple store": 62000,
    }

    for query, freq in queries.items():
        trie.insert(query, frequency=freq)
    trie.rebuild_top_k()

    print(f"\n  Trie size: {trie.size} queries")

    # Exact search
    freq = trie.search("facebook")
    print(f'  Exact search "facebook": frequency={freq}')

    freq = trie.search("not_exist")
    print(f'  Exact search "not_exist": frequency={freq}')

    # Prefix check
    print(f'  Starts with "goo": {trie.starts_with("goo")}')
    print(f'  Starts with "xyz": {trie.starts_with("xyz")}')

    # Autocomplete
    for prefix in ["fac", "goo", "g", "a", "app"]:
        _print_suggestions(prefix, trie.autocomplete(prefix))


def demo_query_logging() -> None:
    """Demonstrate query logging and frequency aggregation."""
    _print_header("2. Query Logging and Aggregation")

    logger = QueryLogger()

    # Simulate user searches
    search_events = [
        ("python tutorial", "user-1"),
        ("python tutorial", "user-2"),
        ("python tutorial", "user-1"),
        ("python list comprehension", "user-1"),
        ("python list comprehension", "user-3"),
        ("python django", "user-2"),
        ("python flask", "user-3"),
        ("python flask", "user-2"),
        ("python flask", "user-1"),
        ("javascript tutorial", "user-2"),
        ("javascript react", "user-2"),
        ("javascript react", "user-3"),
    ]

    for query, uid in search_events:
        logger.log(query, user_id=uid)

    print(f"\n  Total logged queries: {logger.count}")

    # Aggregate
    freq = logger.aggregate_frequencies()
    print("\n  Aggregated frequencies:")
    for query, count in sorted(freq.items(), key=lambda x: -x[1]):
        print(f"    {query:<35} count={count}")

    # User history
    print("\n  User-1 search history:")
    history = logger.get_user_history("user-1")
    for query, count in sorted(history.items(), key=lambda x: -x[1]):
        print(f"    {query:<35} count={count}")


def demo_autocomplete_service() -> None:
    """Demonstrate the full autocomplete service with rebuild and personalization."""
    _print_header("3. Autocomplete Service - Full Workflow")

    service = AutocompleteService(k=5)

    # Step 1: Seed with initial data (simulates loading from DB)
    print("\n  Step 1: Seeding trie with initial query frequencies...")
    initial_data = {
        "how to cook rice": 50000,
        "how to tie a tie": 42000,
        "how to screenshot": 38000,
        "how to lose weight": 35000,
        "how to make money": 32000,
        "how to draw": 28000,
        "how are you": 25000,
        "hotel near me": 22000,
        "home depot": 45000,
        "honda civic": 18000,
        "weather today": 90000,
        "weather tomorrow": 72000,
        "weather radar": 55000,
        "walmart hours": 48000,
        "what time is it": 40000,
    }
    service.seed_queries(initial_data)
    stats = service.stats()
    print(f"  Trie seeded: {stats['trie_size']} queries")

    _print_suggestions("how", service.suggest("how"))
    _print_suggestions("wea", service.suggest("wea"))

    # Step 2: Simulate user searches (query logging)
    print("\n  Step 2: Simulating user search queries...")
    user_searches = [
        ("how to cook pasta", "user-A"),
        ("how to cook pasta", "user-B"),
        ("how to cook pasta", "user-A"),
        ("how to cook pasta", "user-C"),
        ("how to cook pasta", "user-D"),
        ("how to cook rice", "user-A"),
        ("how to cook rice", "user-B"),
        ("weather today", "user-A"),
        ("weather today", "user-B"),
        ("weather today", "user-C"),
        ("weather today", "user-D"),
        ("weather today", "user-E"),
        ("weather this weekend", "user-A"),
        ("weather this weekend", "user-B"),
        ("weather this weekend", "user-C"),
        ("home depot near me", "user-B"),
        ("home depot near me", "user-C"),
    ]
    for query, uid in user_searches:
        service.record_query(query, user_id=uid)
    print(f"  Logged {service.logger.count} queries")

    # Step 3: Rebuild trie from logs
    print("\n  Step 3: Rebuilding trie from query logs...")
    count = service.rebuild_from_logs()
    print(f"  Rebuilt trie with {count} distinct queries")

    _print_suggestions("how", service.suggest("how"))
    _print_suggestions("wea", service.suggest("wea"))

    # Step 4: Rebuild with trending boost
    print("\n  Step 4: Rebuilding with trending boost...")
    count = service.rebuild_with_trending(
        trending_window=600.0, trending_boost=5000
    )
    print(f"  Rebuilt trie with trending: {count} queries")
    _print_suggestions("how", service.suggest("how"))

    # Step 5: Personalization
    print("\n  Step 5: Personalized suggestions for user-A...")
    _print_suggestions(
        "how (user-A)",
        service.suggest("how", user_id="user-A"),
    )

    # Step 6: Content filtering
    print("\n  Step 6: Content filtering...")
    service.block_term("how to cook pasta")
    print('  Blocked "how to cook pasta"')
    _print_suggestions("how", service.suggest("how"))
    service.unblock_term("how to cook pasta")


def demo_incremental_typing() -> None:
    """Simulate a user typing character-by-character and getting suggestions."""
    _print_header("4. Simulating Character-by-Character Typing")

    service = AutocompleteService(k=5)
    service.seed_queries(
        {
            "facebook": 98500,
            "facebook login": 72300,
            "facebook marketplace": 54100,
            "facetime": 61200,
            "face swap app": 45100,
            "factory reset": 38900,
            "fast food near me": 55000,
            "fashion nova": 42000,
            "family dollar": 36000,
            "fantasy football": 33000,
        }
    )

    typed = "facebook"
    print(f'\n  User is typing: "{typed}"')
    print(f"  Showing suggestions at each keystroke:\n")

    for i in range(1, len(typed) + 1):
        prefix = typed[:i]
        suggestions = service.suggest(prefix, limit=3)
        top = ", ".join(f"{q}({s})" for q, s in suggestions) if suggestions else "(none)"
        print(f'    After "{prefix:<10}" -> {top}')


def demo_delete_and_rebuild() -> None:
    """Show deletion and re-ranking after removing a query."""
    _print_header("5. Delete and Re-rank")

    trie = Trie(k=5)
    queries = {
        "python tutorial": 80000,
        "python download": 60000,
        "python list": 55000,
        "python dictionary": 50000,
        "python for loop": 45000,
        "python class": 40000,
    }
    for q, f in queries.items():
        trie.insert(q, f)
    trie.rebuild_top_k()

    print("\n  Before deletion:")
    _print_suggestions("python", trie.autocomplete("python"))

    trie.delete("python tutorial")
    trie.rebuild_top_k()

    print("\n  After deleting 'python tutorial':")
    _print_suggestions("python", trie.autocomplete("python"))
    print(f"\n  Trie size: {trie.size}")


def main() -> None:
    """Run all demos."""
    print("Search Autocomplete / Typeahead System")
    print("=" * 60)

    demo_basic_trie()
    demo_query_logging()
    demo_autocomplete_service()
    demo_incremental_typing()
    demo_delete_and_rebuild()

    _print_header("All demos completed successfully.")


if __name__ == "__main__":
    main()
