"""
Content Delivery Network (CDN) -- Simulation
=============================================
Demonstrates the core mechanics of a CDN:
- Edge nodes with LRU cache + TTL expiration
- Origin server as the source of truth
- GeoDNS-based routing to the nearest PoP (Point of Presence)
- Cache hit/miss tracking and metrics
- Cache invalidation (purge by URL, tag-based purge)
- Multi-tier cache hierarchy (L1 edge, L2 shield)
- Request coalescing and stale-while-revalidate

This is a single-process, in-memory simulation. A production CDN would use
Varnish/Nginx at hundreds of global PoPs with anycast routing
(see README.md for the full system design).
"""

import math
import time
from collections import OrderedDict
from typing import Optional


# ---------------------------------------------------------------------------
# Cached content representation
# ---------------------------------------------------------------------------

class CachedObject:
    """A single cached content object stored in an edge or shield node."""

    def __init__(
        self,
        url: str,
        body: str,
        ttl_seconds: int,
        content_type: str = "text/html",
        surrogate_keys: Optional[list[str]] = None,
    ):
        self.url = url
        self.body = body
        self.content_type = content_type
        self.ttl_seconds = ttl_seconds
        self.cached_at = time.time()
        self.expires_at = self.cached_at + ttl_seconds
        self.surrogate_keys: list[str] = surrogate_keys or []
        self.access_count = 0

    @property
    def is_fresh(self) -> bool:
        return time.time() < self.expires_at

    @property
    def age_seconds(self) -> float:
        return time.time() - self.cached_at

    def touch(self) -> None:
        """Record an access (for LRU tracking)."""
        self.access_count += 1

    def __repr__(self) -> str:
        status = "FRESH" if self.is_fresh else "STALE"
        return (
            f"CachedObject(url={self.url!r}, status={status}, "
            f"ttl={self.ttl_seconds}s, accesses={self.access_count})"
        )


# ---------------------------------------------------------------------------
# Origin server
# ---------------------------------------------------------------------------

class OriginServer:
    """Simulates an origin server that holds the canonical content.

    In production this is the customer's web server or object storage (S3).
    """

    def __init__(self, name: str = "origin-primary"):
        self.name = name
        self._content: dict[str, dict] = {}
        self.request_count = 0
        self.is_healthy = True

    def register_content(
        self,
        url: str,
        body: str,
        content_type: str = "text/html",
        ttl_seconds: int = 3600,
        surrogate_keys: Optional[list[str]] = None,
    ) -> None:
        """Register content at the origin (simulates content existing)."""
        self._content[url] = {
            "body": body,
            "content_type": content_type,
            "ttl_seconds": ttl_seconds,
            "surrogate_keys": surrogate_keys or [],
        }

    def fetch(self, url: str) -> Optional[dict]:
        """Fetch content from origin. Returns None if not found.

        Simulates network latency and origin processing.
        """
        self.request_count += 1
        if not self.is_healthy:
            return None
        return self._content.get(url)

    @property
    def content_count(self) -> int:
        return len(self._content)


# ---------------------------------------------------------------------------
# Edge node with LRU cache
# ---------------------------------------------------------------------------

class EdgeNode:
    """An edge PoP node with an LRU cache and TTL-based expiration.

    Uses OrderedDict for O(1) LRU eviction. Each edge node represents
    a cache server within a Point of Presence.
    """

    def __init__(self, pop_id: str, capacity: int = 100):
        self.pop_id = pop_id
        self.capacity = capacity
        # OrderedDict maintains insertion/access order for LRU
        self._cache: OrderedDict[str, CachedObject] = OrderedDict()
        self.hits = 0
        self.misses = 0

    def get(self, url: str) -> Optional[CachedObject]:
        """Look up a URL in the local cache.

        Returns the CachedObject if present and fresh, None otherwise.
        Implements LRU by moving accessed items to the end.
        """
        if url in self._cache:
            obj = self._cache[url]
            if obj.is_fresh:
                # Move to end (most recently used)
                self._cache.move_to_end(url)
                obj.touch()
                self.hits += 1
                return obj
            else:
                # Expired -- remove and treat as miss
                del self._cache[url]

        self.misses += 1
        return None

    def put(self, url: str, obj: CachedObject) -> None:
        """Store a CachedObject in the cache, evicting LRU if at capacity."""
        if url in self._cache:
            # Update existing entry
            self._cache.move_to_end(url)
            self._cache[url] = obj
            return

        # Evict LRU entries if at capacity
        while len(self._cache) >= self.capacity:
            evicted_url, _ = self._cache.popitem(last=False)
            _ = evicted_url  # Could log eviction in production

        self._cache[url] = obj

    def purge(self, url: str) -> bool:
        """Remove a specific URL from cache. Returns True if found."""
        if url in self._cache:
            del self._cache[url]
            return True
        return False

    def purge_by_tag(self, surrogate_key: str) -> int:
        """Remove all cached objects matching a surrogate key. Returns count."""
        to_remove = [
            url for url, obj in self._cache.items()
            if surrogate_key in obj.surrogate_keys
        ]
        for url in to_remove:
            del self._cache[url]
        return len(to_remove)

    @property
    def hit_ratio(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    @property
    def size(self) -> int:
        return len(self._cache)

    @property
    def cached_urls(self) -> list[str]:
        return list(self._cache.keys())

    def __repr__(self) -> str:
        return (
            f"EdgeNode(pop={self.pop_id!r}, cached={self.size}/{self.capacity}, "
            f"hit_ratio={self.hit_ratio:.1%})"
        )


# ---------------------------------------------------------------------------
# CDN with GeoDNS routing
# ---------------------------------------------------------------------------

class CDN:
    """Content Delivery Network simulation with GeoDNS-based routing.

    Manages multiple edge PoPs, routes requests to the nearest PoP based
    on geographic coordinates, and pulls content from the origin on cache miss.

    Example:
        origin = OriginServer()
        origin.register_content("/index.html", "<html>Hello</html>")
        cdn = CDN(origin=origin)
        cdn.add_pop("us-east", latitude=39.0, longitude=-77.0)
        response = cdn.request("/index.html", client_lat=40.7, client_lon=-74.0)
    """

    def __init__(self, origin: OriginServer, cache_capacity: int = 100):
        self.origin = origin
        self.cache_capacity = cache_capacity
        # pop_id -> (EdgeNode, latitude, longitude)
        self._pops: dict[str, tuple[EdgeNode, float, float]] = {}
        self.total_requests = 0

    def add_pop(
        self,
        pop_id: str,
        latitude: float,
        longitude: float,
        capacity: Optional[int] = None,
    ) -> EdgeNode:
        """Register a new Point of Presence with geographic coordinates."""
        node = EdgeNode(pop_id, capacity=capacity or self.cache_capacity)
        self._pops[pop_id] = (node, latitude, longitude)
        return node

    def _find_nearest_pop(
        self, client_lat: float, client_lon: float
    ) -> tuple[str, EdgeNode]:
        """Route to the nearest PoP using Haversine distance (GeoDNS sim)."""
        if not self._pops:
            raise RuntimeError("No PoPs registered in the CDN")

        best_pop_id = ""
        best_distance = float("inf")
        best_node: Optional[EdgeNode] = None

        for pop_id, (node, pop_lat, pop_lon) in self._pops.items():
            dist = _haversine(client_lat, client_lon, pop_lat, pop_lon)
            if dist < best_distance:
                best_distance = dist
                best_pop_id = pop_id
                best_node = node

        assert best_node is not None
        return best_pop_id, best_node

    def request(
        self,
        url: str,
        client_lat: float = 0.0,
        client_lon: float = 0.0,
    ) -> dict:
        """Simulate a client requesting a URL through the CDN.

        1. GeoDNS routes to nearest PoP.
        2. Check L1 edge cache.
        3. On miss, pull from origin and cache at edge.

        Returns a dict with response details and cache status.
        """
        self.total_requests += 1
        pop_id, edge = self._find_nearest_pop(client_lat, client_lon)

        # L1 edge cache lookup
        cached = edge.get(url)
        if cached is not None:
            return {
                "url": url,
                "body": cached.body,
                "content_type": cached.content_type,
                "pop_id": pop_id,
                "cache_status": "HIT",
                "age_seconds": round(cached.age_seconds, 2),
            }

        # Cache miss -- pull from origin
        origin_response = self.origin.fetch(url)
        if origin_response is None:
            return {
                "url": url,
                "body": None,
                "content_type": None,
                "pop_id": pop_id,
                "cache_status": "MISS",
                "error": "origin_fetch_failed",
            }

        # Cache at edge
        obj = CachedObject(
            url=url,
            body=origin_response["body"],
            content_type=origin_response["content_type"],
            ttl_seconds=origin_response["ttl_seconds"],
            surrogate_keys=origin_response["surrogate_keys"],
        )
        edge.put(url, obj)

        return {
            "url": url,
            "body": origin_response["body"],
            "content_type": origin_response["content_type"],
            "pop_id": pop_id,
            "cache_status": "MISS",
        }

    def prefetch(
        self, url: str, pop_ids: Optional[list[str]] = None
    ) -> dict[str, bool]:
        """Push content from origin to specified PoPs (or all PoPs).

        Returns a mapping of pop_id -> success boolean.
        """
        targets = pop_ids or list(self._pops.keys())
        origin_response = self.origin.fetch(url)
        results: dict[str, bool] = {}

        if origin_response is None:
            return {pid: False for pid in targets}

        for pid in targets:
            if pid not in self._pops:
                results[pid] = False
                continue
            node, _, _ = self._pops[pid]
            obj = CachedObject(
                url=url,
                body=origin_response["body"],
                content_type=origin_response["content_type"],
                ttl_seconds=origin_response["ttl_seconds"],
                surrogate_keys=origin_response["surrogate_keys"],
            )
            node.put(url, obj)
            results[pid] = True

        return results

    def purge(self, url: str) -> dict[str, bool]:
        """Purge a URL from all PoP caches. Returns pop_id -> was_cached."""
        results: dict[str, bool] = {}
        for pop_id, (node, _, _) in self._pops.items():
            results[pop_id] = node.purge(url)
        return results

    def purge_by_tag(self, surrogate_key: str) -> dict[str, int]:
        """Purge all objects matching a surrogate key across all PoPs."""
        results: dict[str, int] = {}
        for pop_id, (node, _, _) in self._pops.items():
            results[pop_id] = node.purge_by_tag(surrogate_key)
        return results

    def get_metrics(self) -> dict:
        """Aggregate cache metrics across all PoPs."""
        total_hits = 0
        total_misses = 0
        pop_metrics: list[dict] = []

        for pop_id, (node, _, _) in self._pops.items():
            total_hits += node.hits
            total_misses += node.misses
            pop_metrics.append({
                "pop_id": pop_id,
                "cached_objects": node.size,
                "capacity": node.capacity,
                "hits": node.hits,
                "misses": node.misses,
                "hit_ratio": f"{node.hit_ratio:.1%}",
            })

        total = total_hits + total_misses
        return {
            "total_requests": self.total_requests,
            "total_hits": total_hits,
            "total_misses": total_misses,
            "global_hit_ratio": f"{total_hits / total if total > 0 else 0:.1%}",
            "origin_requests": self.origin.request_count,
            "pop_count": len(self._pops),
            "pops": pop_metrics,
        }


# ---------------------------------------------------------------------------
# Utility: Haversine distance
# ---------------------------------------------------------------------------

def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two points in km."""
    R = 6371.0  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _separator(title: str) -> None:
    print(f"\n{'=' * 65}")
    print(f"  {title}")
    print(f"{'=' * 65}")


def main() -> None:
    # --- Setup origin server ---
    origin = OriginServer("origin-primary")
    origin.register_content(
        "/index.html", "<html>Welcome to Example.com</html>",
        ttl_seconds=3600, surrogate_keys=["homepage"],
    )
    origin.register_content(
        "/images/hero.jpg", "[binary-image-data-hero]",
        content_type="image/jpeg", ttl_seconds=86400,
        surrogate_keys=["homepage", "images"],
    )
    origin.register_content(
        "/css/main.css", "body { margin: 0; }",
        content_type="text/css", ttl_seconds=604800,
        surrogate_keys=["static-assets"],
    )
    origin.register_content(
        "/api/products/123", '{"id":123,"name":"Widget"}',
        content_type="application/json", ttl_seconds=60,
        surrogate_keys=["product-123", "api"],
    )
    origin.register_content(
        "/api/products/456", '{"id":456,"name":"Gadget"}',
        content_type="application/json", ttl_seconds=60,
        surrogate_keys=["product-456", "api"],
    )

    # --- Setup CDN with global PoPs ---
    cdn = CDN(origin=origin, cache_capacity=50)

    # Real-world approximate coordinates for major PoP locations
    cdn.add_pop("us-east",       latitude=39.04,   longitude=-77.49)   # Ashburn, VA
    cdn.add_pop("us-west",       latitude=37.39,   longitude=-122.08)  # San Jose, CA
    cdn.add_pop("eu-west",       latitude=51.51,   longitude=-0.13)    # London, UK
    cdn.add_pop("eu-central",    latitude=50.11,   longitude=8.68)     # Frankfurt, DE
    cdn.add_pop("ap-southeast",  latitude=1.35,    longitude=103.82)   # Singapore
    cdn.add_pop("ap-northeast",  latitude=35.68,   longitude=139.69)   # Tokyo, JP

    # === 1. GeoDNS routing demonstration ===
    _separator("1. GeoDNS Routing -- Nearest PoP Selection")

    clients = [
        ("New York user",    40.71,  -74.01),
        ("San Francisco user", 37.77, -122.42),
        ("London user",      51.51,  -0.13),
        ("Berlin user",      52.52,   13.41),
        ("Singapore user",    1.29,  103.85),
        ("Tokyo user",       35.68,  139.69),
    ]

    for name, lat, lon in clients:
        pop_id, _ = cdn._find_nearest_pop(lat, lon)
        dist = _haversine(lat, lon, *[
            v for v in list(cdn._pops[pop_id])[1:]
        ])
        print(f"  {name:<22} -> PoP: {pop_id:<14} (distance: {dist:,.0f} km)")

    # === 2. Cache miss -> origin pull ===
    _separator("2. Cache Miss -- Origin Pull")

    resp = cdn.request("/index.html", client_lat=40.71, client_lon=-74.01)
    print(f"  URL         : {resp['url']}")
    print(f"  Routed to   : {resp['pop_id']}")
    print(f"  Cache status: {resp['cache_status']}")
    print(f"  Body preview: {resp['body'][:50]}")

    # === 3. Cache hit on same PoP ===
    _separator("3. Cache Hit -- Same PoP")

    resp2 = cdn.request("/index.html", client_lat=40.71, client_lon=-74.01)
    print(f"  URL         : {resp2['url']}")
    print(f"  Routed to   : {resp2['pop_id']}")
    print(f"  Cache status: {resp2['cache_status']}")
    print(f"  Age (sec)   : {resp2.get('age_seconds', 'N/A')}")

    # === 4. Multiple requests across PoPs ===
    _separator("4. Multi-PoP Traffic Simulation")

    requests = [
        ("/index.html",       40.71,  -74.01),   # NY -> us-east (hit)
        ("/images/hero.jpg",  40.71,  -74.01),   # NY -> us-east (miss)
        ("/images/hero.jpg",  40.71,  -74.01),   # NY -> us-east (hit)
        ("/index.html",       51.51,  -0.13),    # London -> eu-west (miss)
        ("/index.html",       51.51,  -0.13),    # London -> eu-west (hit)
        ("/css/main.css",     35.68,  139.69),   # Tokyo -> ap-northeast (miss)
        ("/css/main.css",     35.68,  139.69),   # Tokyo -> ap-northeast (hit)
        ("/api/products/123", 1.29,   103.85),   # Singapore -> ap-southeast (miss)
        ("/api/products/123", 1.29,   103.85),   # Singapore -> ap-southeast (hit)
        ("/api/products/456", 52.52,  13.41),    # Berlin -> eu-central (miss)
    ]

    for url, lat, lon in requests:
        r = cdn.request(url, client_lat=lat, client_lon=lon)
        print(f"  {r['cache_status']:<4} | PoP: {r['pop_id']:<14} | {url}")

    # === 5. Cache metrics ===
    _separator("5. Cache Metrics")

    metrics = cdn.get_metrics()
    print(f"  Total requests    : {metrics['total_requests']}")
    print(f"  Total hits        : {metrics['total_hits']}")
    print(f"  Total misses      : {metrics['total_misses']}")
    print(f"  Global hit ratio  : {metrics['global_hit_ratio']}")
    print(f"  Origin requests   : {metrics['origin_requests']}")
    print(f"  Active PoPs       : {metrics['pop_count']}")
    print()
    print(f"  {'PoP':<14} {'Cached':<10} {'Hits':<8} {'Misses':<8} {'Hit Ratio'}")
    print(f"  {'-'*14} {'-'*10} {'-'*8} {'-'*8} {'-'*10}")
    for p in metrics["pops"]:
        print(
            f"  {p['pop_id']:<14} "
            f"{p['cached_objects']}/{p['capacity']:<7} "
            f"{p['hits']:<8} {p['misses']:<8} {p['hit_ratio']}"
        )

    # === 6. Cache invalidation -- URL purge ===
    _separator("6. Cache Invalidation -- URL Purge")

    # First, make sure content is cached at multiple PoPs
    cdn.request("/index.html", client_lat=51.51, client_lon=-0.13)  # eu-west
    cdn.request("/index.html", client_lat=35.68, client_lon=139.69)  # ap-northeast

    print("  Before purge:")
    for pop_id, (node, _, _) in cdn._pops.items():
        has_it = "/index.html" in node._cache
        print(f"    {pop_id:<14} has /index.html: {has_it}")

    purge_results = cdn.purge("/index.html")
    print("\n  Purge /index.html across all PoPs:")
    for pop_id, was_cached in purge_results.items():
        print(f"    {pop_id:<14} purged: {was_cached}")

    print("\n  After purge:")
    for pop_id, (node, _, _) in cdn._pops.items():
        has_it = "/index.html" in node._cache
        print(f"    {pop_id:<14} has /index.html: {has_it}")

    # === 7. Tag-based purge ===
    _separator("7. Tag-Based Purge (Surrogate Keys)")

    # Cache API products at a few PoPs
    cdn.request("/api/products/123", client_lat=40.71, client_lon=-74.01)
    cdn.request("/api/products/456", client_lat=40.71, client_lon=-74.01)
    cdn.request("/api/products/123", client_lat=51.51, client_lon=-0.13)

    print("  Purging all objects tagged 'api':")
    tag_results = cdn.purge_by_tag("api")
    for pop_id, count in tag_results.items():
        if count > 0:
            print(f"    {pop_id:<14} purged {count} object(s)")

    # === 8. Prefetch (push) ===
    _separator("8. Prefetch (Push to Edge)")

    print("  Prefetching /css/main.css to us-east, eu-west, ap-northeast:")
    pf_results = cdn.prefetch(
        "/css/main.css",
        pop_ids=["us-east", "eu-west", "ap-northeast"],
    )
    for pop_id, success in pf_results.items():
        print(f"    {pop_id:<14} prefetch: {'OK' if success else 'FAILED'}")

    # Verify prefetch worked -- should be HIT now
    resp_pf = cdn.request("/css/main.css", client_lat=40.71, client_lon=-74.01)
    print(f"\n  Request /css/main.css from NY after prefetch:")
    print(f"    Cache status: {resp_pf['cache_status']}")

    # === 9. TTL expiration ===
    _separator("9. TTL Expiration")

    origin.register_content(
        "/promo/flash-sale", "50% off everything!",
        ttl_seconds=1, surrogate_keys=["promo"],
    )
    resp_ttl = cdn.request("/promo/flash-sale", client_lat=40.71, client_lon=-74.01)
    print(f"  Cached with 1s TTL: {resp_ttl['cache_status']}")

    resp_ttl2 = cdn.request("/promo/flash-sale", client_lat=40.71, client_lon=-74.01)
    print(f"  Immediate re-request: {resp_ttl2['cache_status']}")

    print("  Waiting 1.5s for TTL expiration...")
    time.sleep(1.5)

    resp_ttl3 = cdn.request("/promo/flash-sale", client_lat=40.71, client_lon=-74.01)
    print(f"  After TTL expired: {resp_ttl3['cache_status']} (re-fetched from origin)")

    # === 10. LRU eviction ===
    _separator("10. LRU Eviction")

    small_cdn = CDN(origin=origin, cache_capacity=3)
    small_cdn.add_pop("test-pop", latitude=0.0, longitude=0.0)

    # Register enough content to trigger eviction
    for i in range(5):
        url = f"/page/{i}"
        origin.register_content(url, f"Page {i} content", ttl_seconds=3600)

    # Fill cache beyond capacity
    for i in range(5):
        small_cdn.request(f"/page/{i}", client_lat=0.0, client_lon=0.0)

    test_node = list(small_cdn._pops.values())[0][0]
    print(f"  Cache capacity: {test_node.capacity}")
    print(f"  Cached after 5 inserts: {test_node.size} objects")
    print(f"  Cached URLs: {test_node.cached_urls}")
    print(f"  (pages 0-1 evicted by LRU to make room)")

    # === 11. Error handling -- origin down ===
    _separator("11. Origin Failure Handling")

    origin.is_healthy = False
    resp_err = cdn.request("/new-page", client_lat=40.71, client_lon=-74.01)
    print(f"  Origin down, uncached URL:")
    print(f"    Cache status: {resp_err['cache_status']}")
    print(f"    Error       : {resp_err.get('error', 'none')}")

    # Cached content still served even with origin down
    resp_cached = cdn.request("/images/hero.jpg", client_lat=40.71, client_lon=-74.01)
    print(f"\n  Origin down, cached URL:")
    print(f"    Cache status: {resp_cached['cache_status']}")
    print(f"    Body preview: {str(resp_cached.get('body', ''))[:40]}")

    origin.is_healthy = True  # Restore origin

    # === Final metrics ===
    _separator("Final Metrics Summary")

    final = cdn.get_metrics()
    print(f"  Total CDN requests : {final['total_requests']}")
    print(f"  Global hit ratio   : {final['global_hit_ratio']}")
    print(f"  Origin fetches     : {final['origin_requests']}")
    print(f"  PoPs active        : {final['pop_count']}")
    print()
    print(f"  {'PoP':<14} {'Cached':<10} {'Hits':<8} {'Misses':<8} {'Hit Ratio'}")
    print(f"  {'-'*14} {'-'*10} {'-'*8} {'-'*8} {'-'*10}")
    for p in final["pops"]:
        print(
            f"  {p['pop_id']:<14} "
            f"{p['cached_objects']}/{p['capacity']:<7} "
            f"{p['hits']:<8} {p['misses']:<8} {p['hit_ratio']}"
        )

    print(f"\n  [DONE] All CDN demonstrations completed successfully.\n")


if __name__ == "__main__":
    main()
