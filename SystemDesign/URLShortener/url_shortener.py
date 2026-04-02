"""
URL Shortener (TinyURL) -- Simulation
======================================
Demonstrates the core mechanics of a URL shortening service:
- Base62 encoding of an auto-incrementing counter
- Custom alias support
- TTL-based expiration
- Click tracking and analytics
- Collision detection

This is a single-process, in-memory simulation. A production system would use
Cassandra for storage, Redis for caching, and Kafka for analytics events
(see README.md for the full system design).
"""

import time
import hashlib
from typing import Optional


BASE62_CHARS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
BASE = len(BASE62_CHARS)  # 62
DEFAULT_DOMAIN = "https://tiny.url"


def base62_encode(num: int) -> str:
    """Encode a positive integer into a base62 string.

    >>> base62_encode(0)
    '0'
    >>> base62_encode(61)
    'Z'
    >>> base62_encode(238328)
    '1000'
    """
    if num == 0:
        return BASE62_CHARS[0]
    chars = []
    while num > 0:
        chars.append(BASE62_CHARS[num % BASE])
        num //= BASE
    return "".join(reversed(chars))


def base62_decode(encoded: str) -> int:
    """Decode a base62 string back to an integer.

    >>> base62_decode('1000')
    238328
    """
    num = 0
    for ch in encoded:
        num = num * BASE + BASE62_CHARS.index(ch)
    return num


class URLRecord:
    """Represents a stored URL mapping with metadata."""

    def __init__(
        self,
        short_code: str,
        long_url: str,
        created_at: float,
        expires_at: Optional[float] = None,
    ):
        self.short_code = short_code
        self.long_url = long_url
        self.created_at = created_at
        self.expires_at = expires_at
        self.click_count: int = 0
        self.click_timestamps: list[float] = []

    @property
    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    def record_click(self) -> None:
        self.click_count += 1
        self.click_timestamps.append(time.time())

    def __repr__(self) -> str:
        status = "EXPIRED" if self.is_expired else "ACTIVE"
        return (
            f"URLRecord(code={self.short_code!r}, "
            f"url={self.long_url!r}, "
            f"clicks={self.click_count}, "
            f"status={status})"
        )


class URLShortener:
    """In-memory URL shortening service simulation.

    Uses a monotonically increasing counter encoded in base62 to generate
    short codes.  Supports custom aliases, TTL expiration, and click analytics.

    Example:
        svc = URLShortener()
        short = svc.shorten("https://example.com/long")
        original = svc.redirect(short)
    """

    def __init__(self, domain: str = DEFAULT_DOMAIN, counter_start: int = 100_000):
        self.domain = domain
        self._counter = counter_start
        # short_code -> URLRecord
        self._store: dict[str, URLRecord] = {}
        # long_url hash -> short_code (for deduplication)
        self._url_index: dict[str, str] = {}

    # -- helpers --------------------------------------------------------

    def _next_code(self) -> str:
        code = base62_encode(self._counter)
        self._counter += 1
        return code

    @staticmethod
    def _hash_url(url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def _code_exists(self, code: str) -> bool:
        return code in self._store

    # -- public API -----------------------------------------------------

    def shorten(
        self,
        long_url: str,
        custom_alias: Optional[str] = None,
        ttl: Optional[int] = None,
    ) -> str:
        """Create a shortened URL.

        Args:
            long_url: The original URL to shorten.
            custom_alias: Optional user-chosen alias (e.g. 'my-brand').
            ttl: Time-to-live in seconds. None means no expiration.

        Returns:
            The full short URL (e.g. 'https://tiny.url/q1w2').

        Raises:
            ValueError: If the custom alias is already taken or invalid.
        """
        # Validate
        if not long_url or not long_url.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")

        now = time.time()
        expires_at = (now + ttl) if ttl else None

        # Custom alias path
        if custom_alias:
            if not custom_alias.replace("-", "").replace("_", "").isalnum():
                raise ValueError(
                    "Alias may only contain letters, digits, hyphens, and underscores"
                )
            if self._code_exists(custom_alias):
                raise ValueError(f"Alias '{custom_alias}' is already taken")
            code = custom_alias
        else:
            # Deduplication: if same URL was shortened before, return existing
            url_hash = self._hash_url(long_url)
            if url_hash in self._url_index:
                existing_code = self._url_index[url_hash]
                existing = self._store.get(existing_code)
                if existing and not existing.is_expired:
                    return f"{self.domain}/{existing_code}"

            # Generate new code from counter
            code = self._next_code()
            # Collision guard (should not happen with counter, but defensive)
            retries = 0
            while self._code_exists(code):
                code = self._next_code()
                retries += 1
                if retries > 10:
                    raise RuntimeError("Failed to generate unique code")

            self._url_index[url_hash] = code

        record = URLRecord(
            short_code=code,
            long_url=long_url,
            created_at=now,
            expires_at=expires_at,
        )
        self._store[code] = record
        return f"{self.domain}/{code}"

    def redirect(self, short_url: str) -> str:
        """Resolve a short URL to the original long URL.

        Simulates the 301 redirect and records a click event.

        Args:
            short_url: Full short URL or just the short code.

        Returns:
            The original long URL.

        Raises:
            KeyError: If the short code does not exist.
            ValueError: If the URL has expired.
        """
        code = short_url.split("/")[-1] if "/" in short_url else short_url

        record = self._store.get(code)
        if record is None:
            raise KeyError(f"Short code '{code}' not found (404)")

        if record.is_expired:
            raise ValueError(f"Short code '{code}' has expired (410 Gone)")

        record.record_click()
        return record.long_url

    def get_analytics(self, short_url: str) -> dict:
        """Get click analytics for a short URL.

        Args:
            short_url: Full short URL or just the short code.

        Returns:
            Dict with click_count, created_at, and recent click timestamps.

        Raises:
            KeyError: If the short code does not exist.
        """
        code = short_url.split("/")[-1] if "/" in short_url else short_url

        record = self._store.get(code)
        if record is None:
            raise KeyError(f"Short code '{code}' not found")

        return {
            "short_code": record.short_code,
            "long_url": record.long_url,
            "click_count": record.click_count,
            "created_at": record.created_at,
            "is_expired": record.is_expired,
            "recent_clicks": record.click_timestamps[-10:],
        }

    def delete(self, short_url: str) -> bool:
        """Delete a short URL mapping.

        Returns True if deleted, False if not found.
        """
        code = short_url.split("/")[-1] if "/" in short_url else short_url
        record = self._store.pop(code, None)
        if record is None:
            return False
        url_hash = self._hash_url(record.long_url)
        self._url_index.pop(url_hash, None)
        return True

    @property
    def total_urls(self) -> int:
        return len(self._store)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def main() -> None:
    svc = URLShortener()

    # --- 1. Basic shortening ---
    _separator("1. Basic URL Shortening")
    urls = [
        "https://www.example.com/very/long/path/to/resource?query=1&page=2",
        "https://docs.python.org/3/library/hashlib.html",
        "https://github.com/user/repo/pull/42/files#diff-abc123",
    ]
    for url in urls:
        short = svc.shorten(url)
        print(f"  {url[:50]}...")
        print(f"    -> {short}")

    # --- 2. Custom alias ---
    _separator("2. Custom Alias")
    short_custom = svc.shorten("https://mycompany.com/careers", custom_alias="jobs")
    print(f"  Custom alias 'jobs' -> {short_custom}")

    # Try duplicate alias
    try:
        svc.shorten("https://other.com/page", custom_alias="jobs")
    except ValueError as e:
        print(f"  Duplicate alias error: {e}")

    # --- 3. Redirect + click tracking ---
    _separator("3. Redirect and Click Tracking")
    for i in range(5):
        resolved = svc.redirect(short_custom)
        if i == 0:
            print(f"  Redirect {short_custom} -> {resolved}")
    print(f"  Simulated 5 clicks on '{short_custom}'")

    # --- 4. Analytics ---
    _separator("4. Analytics")
    stats = svc.get_analytics(short_custom)
    print(f"  Short code : {stats['short_code']}")
    print(f"  Long URL   : {stats['long_url']}")
    print(f"  Clicks     : {stats['click_count']}")
    print(f"  Expired    : {stats['is_expired']}")

    # --- 5. Deduplication ---
    _separator("5. Deduplication")
    url_dup = "https://www.example.com/very/long/path/to/resource?query=1&page=2"
    short_a = svc.shorten(url_dup)
    short_b = svc.shorten(url_dup)
    print(f"  First  shorten -> {short_a}")
    print(f"  Second shorten -> {short_b}")
    print(f"  Same short URL : {short_a == short_b}")

    # --- 6. TTL expiration ---
    _separator("6. TTL Expiration")
    short_ttl = svc.shorten("https://promo.example.com/sale", ttl=1)
    print(f"  Created with 1s TTL -> {short_ttl}")
    resolved = svc.redirect(short_ttl)
    print(f"  Immediate redirect  -> {resolved} [OK]")
    print("  Waiting 1.5s for expiration...")
    time.sleep(1.5)
    try:
        svc.redirect(short_ttl)
    except ValueError as e:
        print(f"  After TTL expired   -> {e}")

    # --- 7. Error handling ---
    _separator("7. Error Handling")
    try:
        svc.shorten("not-a-valid-url")
    except ValueError as e:
        print(f"  Invalid URL : {e}")

    try:
        svc.redirect("https://tiny.url/nonexistent")
    except KeyError as e:
        print(f"  Unknown code: {e}")

    try:
        svc.shorten("https://x.com", custom_alias="bad alias!!")
    except ValueError as e:
        print(f"  Bad alias   : {e}")

    # --- 8. Deletion ---
    _separator("8. Deletion")
    deleted = svc.delete(short_custom)
    print(f"  Deleted 'jobs': {deleted}")
    try:
        svc.redirect(short_custom)
    except KeyError as e:
        print(f"  After delete : {e}")

    # --- 9. Base62 encoding demo ---
    _separator("9. Base62 Encoding Examples")
    for num in [0, 61, 62, 1000, 100_000, 3_500_000_000]:
        encoded = base62_encode(num)
        decoded = base62_decode(encoded)
        print(f"  {num:>15,d} -> '{encoded}' -> {decoded:>15,d}  [OK]")

    # --- Summary ---
    _separator("Summary")
    print(f"  Total URLs in store: {svc.total_urls}")
    print(f"  Next counter value : {svc._counter}")
    print(f"  Base62 capacity (7 chars): {62**7:,} unique codes")
    print("\n  [DONE] All demonstrations completed successfully.\n")


if __name__ == "__main__":
    main()
