"""
Web Crawler Simulation
======================
Demonstrates core web crawler concepts:
- URL Frontier with priority queue
- BFS crawl strategy with depth limiting
- Bloom filter for URL deduplication
- Per-domain politeness delay
- Content fingerprinting for content dedup

This is a simulation using an in-memory web graph (no real HTTP requests).
"""

import hashlib
import heapq
import time
from collections import defaultdict
from typing import Optional


# ---------------------------------------------------------------------------
# Bloom Filter -- space-efficient probabilistic set for URL dedup
# ---------------------------------------------------------------------------

class BloomFilter:
    """
    Bloom filter for URL deduplication.

    Uses multiple hash functions to set bits in a bit array.
    - No false negatives: if we say 'not seen', it is definitely not seen.
    - Small false positive rate: occasionally says 'seen' for unseen URLs.
    """

    def __init__(self, capacity: int = 10000, fp_rate: float = 0.01):
        import math
        self.capacity = capacity
        self.fp_rate = fp_rate
        # Optimal bit array size: m = -n*ln(p) / (ln2)^2
        self.size = int(-capacity * math.log(fp_rate) / (math.log(2) ** 2))
        # Optimal number of hash functions: k = (m/n) * ln2
        self.num_hashes = max(1, int((self.size / capacity) * math.log(2)))
        self.bit_array = [False] * self.size
        self.count = 0

    def _hashes(self, item: str) -> list[int]:
        """Generate k hash positions using double hashing."""
        h1 = int(hashlib.md5(item.encode()).hexdigest(), 16)
        h2 = int(hashlib.sha1(item.encode()).hexdigest(), 16)
        return [(h1 + i * h2) % self.size for i in range(self.num_hashes)]

    def add(self, item: str) -> None:
        """Add an item to the Bloom filter."""
        for pos in self._hashes(item):
            self.bit_array[pos] = True
        self.count += 1

    def __contains__(self, item: str) -> bool:
        """Check if an item might be in the set (probabilistic)."""
        return all(self.bit_array[pos] for pos in self._hashes(item))

    def __len__(self) -> int:
        return self.count

    def __repr__(self) -> str:
        bits_set = sum(self.bit_array)
        return (
            f"BloomFilter(capacity={self.capacity}, "
            f"bits={self.size}, hashes={self.num_hashes}, "
            f"items={self.count}, fill={bits_set}/{self.size})"
        )


# ---------------------------------------------------------------------------
# URL Frontier -- priority queue with per-domain politeness
# ---------------------------------------------------------------------------

class URLFrontier:
    """
    Two-level URL frontier:
    Level 1 - Priority queue (min-heap) orders URLs by priority.
    Level 2 - Per-domain politeness tracking ensures we do not
              hit the same domain too frequently.
    """

    def __init__(self, politeness_delay: float = 0.5):
        self._heap: list[tuple[int, int, int, str]] = []
        self._counter = 0  # tie-breaker for heap ordering
        self._domain_last_fetch: dict[str, float] = {}
        self.politeness_delay = politeness_delay

    @staticmethod
    def extract_domain(url: str) -> str:
        """Extract domain from a URL string."""
        # Simple extraction for simulation URLs like "http://site-a.com/page1"
        url = url.replace("http://", "").replace("https://", "")
        return url.split("/")[0]

    def add(self, url: str, priority: int = 5, depth: int = 0) -> None:
        """Add a URL to the frontier with given priority (lower = higher priority)."""
        self._counter += 1
        heapq.heappush(self._heap, (priority, depth, self._counter, url))

    def get_next(self) -> Optional[tuple[str, int, int]]:
        """
        Get the next URL respecting politeness.
        Returns (url, priority, depth) or None if frontier is empty.

        Skips domains that were fetched too recently and re-queues them.
        """
        skipped: list[tuple[int, int, int, str]] = []
        result = None

        while self._heap:
            priority, depth, counter, url = heapq.heappop(self._heap)
            domain = self.extract_domain(url)
            last_fetch = self._domain_last_fetch.get(domain, 0)
            elapsed = time.time() - last_fetch

            if elapsed >= self.politeness_delay:
                self._domain_last_fetch[domain] = time.time()
                result = (url, priority, depth)
                break
            else:
                skipped.append((priority, depth, counter, url))

        # Put skipped URLs back
        for item in skipped:
            heapq.heappush(self._heap, item)

        return result

    def is_empty(self) -> bool:
        return len(self._heap) == 0

    def size(self) -> int:
        return len(self._heap)

    def __repr__(self) -> str:
        return f"URLFrontier(size={self.size()}, delay={self.politeness_delay}s)"


# ---------------------------------------------------------------------------
# Simulated Web Graph
# ---------------------------------------------------------------------------

def build_web_graph() -> dict[str, dict]:
    """
    Build a simulated web graph for crawling.
    Each page has content and outgoing links.
    """
    return {
        "http://site-a.com/": {
            "content": "Welcome to Site A - Home Page",
            "links": [
                "http://site-a.com/about",
                "http://site-a.com/products",
                "http://site-b.com/",
            ],
        },
        "http://site-a.com/about": {
            "content": "About Site A - Founded in 2020",
            "links": [
                "http://site-a.com/",
                "http://site-a.com/team",
            ],
        },
        "http://site-a.com/products": {
            "content": "Site A Products - Widgets and Gadgets",
            "links": [
                "http://site-a.com/",
                "http://site-a.com/products/widget",
                "http://site-a.com/products/gadget",
            ],
        },
        "http://site-a.com/team": {
            "content": "Site A Team - Alice, Bob, Charlie",
            "links": ["http://site-a.com/about"],
        },
        "http://site-a.com/products/widget": {
            "content": "Widget Pro - The best widget on the market",
            "links": ["http://site-a.com/products"],
        },
        "http://site-a.com/products/gadget": {
            "content": "Gadget X - Next generation gadget",
            "links": [
                "http://site-a.com/products",
                "http://site-c.com/reviews",
            ],
        },
        "http://site-b.com/": {
            "content": "Site B - News Portal",
            "links": [
                "http://site-b.com/tech",
                "http://site-b.com/science",
                "http://site-a.com/",
            ],
        },
        "http://site-b.com/tech": {
            "content": "Tech News - Latest in technology",
            "links": [
                "http://site-b.com/",
                "http://site-b.com/tech/ai",
            ],
        },
        "http://site-b.com/science": {
            "content": "Science News - Discoveries and breakthroughs",
            "links": ["http://site-b.com/"],
        },
        "http://site-b.com/tech/ai": {
            "content": "AI News - Machine learning advances",
            "links": [
                "http://site-b.com/tech",
                "http://site-c.com/",
            ],
        },
        "http://site-c.com/": {
            "content": "Site C - Review Platform",
            "links": [
                "http://site-c.com/reviews",
                "http://site-c.com/ratings",
            ],
        },
        "http://site-c.com/reviews": {
            "content": "Product Reviews - Honest user reviews",
            "links": [
                "http://site-c.com/",
                "http://site-a.com/products",
            ],
        },
        "http://site-c.com/ratings": {
            "content": "Ratings Dashboard - Top rated products",
            "links": ["http://site-c.com/"],
        },
    }


# ---------------------------------------------------------------------------
# Robots.txt Checker (simulated)
# ---------------------------------------------------------------------------

class RobotsChecker:
    """Simulated robots.txt enforcement."""

    def __init__(self) -> None:
        # Simulated disallow rules per domain
        self.rules: dict[str, list[str]] = {
            "site-a.com": ["/private/", "/admin/"],
            "site-b.com": ["/internal/"],
            "site-c.com": [],
        }
        self.cache_hits = 0
        self.cache_misses = 0

    def is_allowed(self, url: str) -> bool:
        """Check if the URL is allowed by robots.txt rules."""
        domain = URLFrontier.extract_domain(url)
        path = "/" + "/".join(url.replace("http://", "").split("/")[1:])
        disallowed = self.rules.get(domain, [])

        for rule in disallowed:
            if path.startswith(rule):
                return False
        return True


# ---------------------------------------------------------------------------
# Web Crawler -- BFS with all components integrated
# ---------------------------------------------------------------------------

class WebCrawler:
    """
    BFS web crawler with:
    - URL frontier (priority queue + politeness)
    - Bloom filter for URL dedup
    - Content fingerprinting for content dedup
    - robots.txt checking
    - Configurable max depth
    """

    def __init__(
        self,
        max_depth: int = 3,
        max_pages: int = 100,
        politeness_delay: float = 0.1,
        allowed_domains: Optional[list[str]] = None,
    ):
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.allowed_domains = allowed_domains

        self.frontier = URLFrontier(politeness_delay=politeness_delay)
        self.url_dedup = BloomFilter(capacity=1000, fp_rate=0.01)
        self.robots = RobotsChecker()

        # Storage
        self.crawled_pages: dict[str, dict] = {}
        self.content_hashes: set[str] = set()  # content-level dedup

        # Stats
        self.pages_fetched = 0
        self.pages_skipped_dedup = 0
        self.pages_skipped_robots = 0
        self.pages_skipped_depth = 0
        self.pages_skipped_domain = 0
        self.pages_skipped_content_dedup = 0

    @staticmethod
    def content_fingerprint(content: str) -> str:
        """Compute a content fingerprint for deduplication."""
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def _is_domain_allowed(self, url: str) -> bool:
        """Check if URL's domain is in the allowed domains list."""
        if self.allowed_domains is None:
            return True
        domain = URLFrontier.extract_domain(url)
        return domain in self.allowed_domains

    def add_seeds(self, seed_urls: list[str]) -> None:
        """Add seed URLs to the frontier at depth 0 with highest priority."""
        for url in seed_urls:
            if url not in self.url_dedup:
                self.url_dedup.add(url)
                self.frontier.add(url, priority=0, depth=0)
                print(f"  [SEED] Added: {url}")

    def fetch_page(self, url: str, web_graph: dict[str, dict]) -> Optional[dict]:
        """Simulate fetching a page from the web graph."""
        page = web_graph.get(url)
        if page is None:
            return None
        # Simulate network latency
        time.sleep(0.01)
        return {
            "url": url,
            "content": page["content"],
            "links": page["links"],
            "status_code": 200,
            "fetched_at": time.time(),
        }

    def crawl(self, web_graph: dict[str, dict]) -> dict[str, dict]:
        """
        Execute BFS crawl over the web graph.
        Returns dict of all successfully crawled pages.
        """
        print("\n" + "=" * 60)
        print("CRAWL STARTED")
        print("=" * 60)
        print(f"  Max depth:  {self.max_depth}")
        print(f"  Max pages:  {self.max_pages}")
        print(f"  Frontier:   {self.frontier}")
        print(f"  Bloom:      {self.url_dedup}")
        if self.allowed_domains:
            print(f"  Domains:    {self.allowed_domains}")
        print("-" * 60)

        start_time = time.time()

        while not self.frontier.is_empty() and self.pages_fetched < self.max_pages:
            result = self.frontier.get_next()
            if result is None:
                # All domains are in politeness cooldown; brief wait
                time.sleep(0.05)
                continue

            url, priority, depth = result

            # Check depth limit
            if depth > self.max_depth:
                self.pages_skipped_depth += 1
                continue

            # Check robots.txt
            if not self.robots.is_allowed(url):
                self.pages_skipped_robots += 1
                print(f"  [BLOCKED]  robots.txt: {url}")
                continue

            # Check domain restriction
            if not self._is_domain_allowed(url):
                self.pages_skipped_domain += 1
                continue

            # Fetch the page
            page = self.fetch_page(url, web_graph)
            if page is None:
                print(f"  [404]      Not found: {url}")
                continue

            # Content-level dedup
            fingerprint = self.content_fingerprint(page["content"])
            if fingerprint in self.content_hashes:
                self.pages_skipped_content_dedup += 1
                print(f"  [DUP]      Content duplicate: {url}")
                continue

            # Store the page
            self.content_hashes.add(fingerprint)
            self.crawled_pages[url] = {
                "content": page["content"],
                "fingerprint": fingerprint,
                "depth": depth,
                "priority": priority,
                "fetched_at": page["fetched_at"],
                "outgoing_links": len(page["links"]),
            }
            self.pages_fetched += 1

            domain = URLFrontier.extract_domain(url)
            print(
                f"  [CRAWLED]  depth={depth} "
                f"domain={domain:20s} "
                f"url={url}"
            )

            # Extract and enqueue new links (BFS: depth + 1)
            new_links = 0
            for link in page["links"]:
                if link not in self.url_dedup:
                    self.url_dedup.add(link)
                    child_priority = min(priority + 1, 9)
                    self.frontier.add(link, priority=child_priority, depth=depth + 1)
                    new_links += 1
                else:
                    self.pages_skipped_dedup += 1

            if new_links > 0:
                print(
                    f"             -> Discovered {new_links} new link(s), "
                    f"frontier size: {self.frontier.size()}"
                )

        elapsed = time.time() - start_time
        self._print_summary(elapsed)
        return self.crawled_pages

    def _print_summary(self, elapsed: float) -> None:
        """Print crawl summary statistics."""
        print("\n" + "=" * 60)
        print("CRAWL COMPLETE")
        print("=" * 60)
        print(f"  Duration:              {elapsed:.2f}s")
        print(f"  Pages fetched:         {self.pages_fetched}")
        print(f"  Pages/second:          {self.pages_fetched / max(elapsed, 0.001):.1f}")
        print(f"  URLs in Bloom filter:  {len(self.url_dedup)}")
        print(f"  Frontier remaining:    {self.frontier.size()}")
        print(f"  Skipped (URL dedup):   {self.pages_skipped_dedup}")
        print(f"  Skipped (content dup): {self.pages_skipped_content_dedup}")
        print(f"  Skipped (robots.txt):  {self.pages_skipped_robots}")
        print(f"  Skipped (max depth):   {self.pages_skipped_depth}")
        print(f"  Skipped (domain):      {self.pages_skipped_domain}")

        # Depth distribution
        depth_counts: dict[int, int] = defaultdict(int)
        for info in self.crawled_pages.values():
            depth_counts[info["depth"]] += 1

        print("\n  Depth distribution:")
        for d in sorted(depth_counts):
            bar = "#" * depth_counts[d]
            print(f"    depth {d}: {depth_counts[d]:3d} pages  {bar}")

        # Domain distribution
        domain_counts: dict[str, int] = defaultdict(int)
        for url in self.crawled_pages:
            domain = URLFrontier.extract_domain(url)
            domain_counts[domain] += 1

        print("\n  Domain distribution:")
        for domain in sorted(domain_counts):
            bar = "#" * domain_counts[domain]
            print(f"    {domain:25s} {domain_counts[domain]:3d} pages  {bar}")

        print("\n  Bloom filter stats:")
        print(f"    {self.url_dedup}")
        print("=" * 60)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def demo_bloom_filter() -> None:
    """Demonstrate Bloom filter behavior."""
    print("=" * 60)
    print("BLOOM FILTER DEMO")
    print("=" * 60)

    bf = BloomFilter(capacity=100, fp_rate=0.05)
    print(f"  Created: {bf}")

    test_urls = [
        "http://example.com/page1",
        "http://example.com/page2",
        "http://example.com/page3",
    ]

    for url in test_urls:
        print(f"  Adding: {url}")
        bf.add(url)

    print(f"\n  After adding {len(test_urls)} URLs: {bf}")

    # Check membership
    print("\n  Membership checks:")
    for url in test_urls:
        status = "FOUND (correct)" if url in bf else "NOT FOUND (error!)"
        print(f"    {url} -> {status}")

    unseen = "http://example.com/never-added"
    status = "FOUND (false positive)" if unseen in bf else "NOT FOUND (correct)"
    print(f"    {unseen} -> {status}")
    print()


def demo_frontier() -> None:
    """Demonstrate URL frontier with priority and politeness."""
    print("=" * 60)
    print("URL FRONTIER DEMO")
    print("=" * 60)

    frontier = URLFrontier(politeness_delay=0.2)

    # Add URLs with different priorities
    urls_with_priority = [
        ("http://site-a.com/page1", 3),
        ("http://site-a.com/page2", 1),
        ("http://site-b.com/page1", 2),
        ("http://site-b.com/page2", 5),
        ("http://site-c.com/page1", 0),
    ]

    for url, prio in urls_with_priority:
        frontier.add(url, priority=prio, depth=0)
        print(f"  Added: priority={prio} url={url}")

    print(f"\n  Frontier size: {frontier.size()}")
    print("\n  Dequeueing (respecting priority + politeness):")

    fetched = 0
    attempts = 0
    while not frontier.is_empty() and attempts < 20:
        result = frontier.get_next()
        if result:
            url, prio, depth = result
            domain = URLFrontier.extract_domain(url)
            print(f"    [{fetched + 1}] priority={prio} domain={domain:15s} url={url}")
            fetched += 1
        else:
            time.sleep(0.1)
        attempts += 1

    print(f"\n  Fetched {fetched} URLs in {attempts} attempts")
    print()


def demo_full_crawl() -> None:
    """Demonstrate full BFS crawl of the simulated web graph."""
    web_graph = build_web_graph()
    print(f"Web graph: {len(web_graph)} pages across 3 domains\n")

    # Scenario 1: Unrestricted crawl
    print(">>> SCENARIO 1: Full crawl (all domains, depth=3)")
    crawler = WebCrawler(max_depth=3, max_pages=50, politeness_delay=0.05)
    crawler.add_seeds(["http://site-a.com/"])
    crawler.crawl(web_graph)

    # Scenario 2: Domain-restricted crawl
    print("\n\n>>> SCENARIO 2: Domain-restricted crawl (site-a.com only)")
    crawler2 = WebCrawler(
        max_depth=5,
        max_pages=50,
        politeness_delay=0.05,
        allowed_domains=["site-a.com"],
    )
    crawler2.add_seeds(["http://site-a.com/"])
    crawler2.crawl(web_graph)

    # Scenario 3: Shallow crawl with multiple seeds
    print("\n\n>>> SCENARIO 3: Shallow crawl (depth=1, multiple seeds)")
    crawler3 = WebCrawler(max_depth=1, max_pages=50, politeness_delay=0.05)
    crawler3.add_seeds([
        "http://site-a.com/",
        "http://site-b.com/",
        "http://site-c.com/",
    ])
    crawler3.crawl(web_graph)


if __name__ == "__main__":
    demo_bloom_filter()
    demo_frontier()
    demo_full_crawl()
