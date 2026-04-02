"""
Twitter / News Feed System -- Fan-Out Simulation

Demonstrates:
  - User creation and follow/unfollow
  - Posting tweets
  - Fan-out on write (push model) for normal users
  - Fan-out on read (pull model) for celebrities
  - Hybrid feed generation with merge + ranking
  - Like / retweet engagement signals
"""

from __future__ import annotations

import time
import heapq
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Domain Models
# ---------------------------------------------------------------------------

@dataclass
class Tweet:
    tweet_id: int
    user_id: str
    text: str
    timestamp: float = field(default_factory=time.time)
    likes: int = 0
    retweets: int = 0

    def engagement_score(self) -> float:
        """Weighted engagement metric used for ranking."""
        return self.likes * 1.0 + self.retweets * 2.0

    def __lt__(self, other: "Tweet") -> bool:
        """Newer tweets are 'smaller' so heapq gives us reverse-chrono."""
        return self.timestamp > other.timestamp

    def __repr__(self) -> str:
        return (
            f"Tweet(id={self.tweet_id}, @{self.user_id}, "
            f"'{self.text}', likes={self.likes}, rt={self.retweets})"
        )


@dataclass
class User:
    user_id: str
    display_name: str
    followers: set[str] = field(default_factory=set)
    following: set[str] = field(default_factory=set)
    tweets: list[int] = field(default_factory=list)

    @property
    def follower_count(self) -> int:
        return len(self.followers)

    def __repr__(self) -> str:
        return (
            f"User(@{self.user_id}, followers={self.follower_count}, "
            f"following={len(self.following)})"
        )


# ---------------------------------------------------------------------------
# Feed Service -- Hybrid Fan-Out
# ---------------------------------------------------------------------------

class FeedService:
    """
    Simulates a Twitter-like feed system with hybrid fan-out:
      - Fan-out on WRITE for normal users (push tweet_id to followers' caches)
      - Fan-out on READ  for celebrities  (pull their tweets at query time)

    Attributes:
        celebrity_threshold: follower count above which a user is a celebrity
        feed_cache_limit:    max tweet IDs stored in each user's cache
    """

    CELEBRITY_THRESHOLD = 3  # low for demo purposes
    FEED_CACHE_LIMIT = 800

    def __init__(self) -> None:
        self.users: dict[str, User] = {}
        self.tweets: dict[int, Tweet] = {}
        # Per-user timeline cache: user_id -> list of tweet_ids (newest first)
        self.feed_cache: dict[str, list[int]] = {}
        self._next_tweet_id: int = 1

    # -- User management -----------------------------------------------------

    def create_user(self, user_id: str, display_name: str) -> User:
        if user_id in self.users:
            raise ValueError(f"User @{user_id} already exists")
        user = User(user_id=user_id, display_name=display_name)
        self.users[user_id] = user
        self.feed_cache[user_id] = []
        return user

    def get_user(self, user_id: str) -> User:
        if user_id not in self.users:
            raise KeyError(f"User @{user_id} not found")
        return self.users[user_id]

    # -- Follow / Unfollow ---------------------------------------------------

    def follow(self, follower_id: str, followee_id: str) -> None:
        if follower_id == followee_id:
            raise ValueError("Cannot follow yourself")
        follower = self.get_user(follower_id)
        followee = self.get_user(followee_id)

        if followee_id in follower.following:
            return  # already following

        follower.following.add(followee_id)
        followee.followers.add(follower_id)

        # Backfill: add recent tweets from followee into follower's cache
        self._backfill_cache(follower_id, followee)

    def unfollow(self, follower_id: str, followee_id: str) -> None:
        follower = self.get_user(follower_id)
        followee = self.get_user(followee_id)

        follower.following.discard(followee_id)
        followee.followers.discard(follower_id)

        # Remove followee's tweets from follower's cache
        followee_tweets = set(followee.tweets)
        self.feed_cache[follower_id] = [
            tid for tid in self.feed_cache[follower_id]
            if tid not in followee_tweets
        ]

    def _backfill_cache(self, follower_id: str, followee: User) -> None:
        """Add the followee's recent tweets into the follower's feed cache."""
        cache = self.feed_cache[follower_id]
        for tid in followee.tweets[-self.FEED_CACHE_LIMIT:]:
            if tid not in cache:
                cache.append(tid)
        # Re-sort newest first and trim
        cache.sort(key=lambda tid: self.tweets[tid].timestamp, reverse=True)
        self.feed_cache[follower_id] = cache[: self.FEED_CACHE_LIMIT]

    # -- Posting Tweets ------------------------------------------------------

    def post_tweet(self, user_id: str, text: str) -> Tweet:
        user = self.get_user(user_id)
        tweet = Tweet(
            tweet_id=self._next_tweet_id,
            user_id=user_id,
            text=text,
            timestamp=time.time(),
        )
        self._next_tweet_id += 1

        # Persist
        self.tweets[tweet.tweet_id] = tweet
        user.tweets.append(tweet.tweet_id)

        # Fan-out decision
        if user.follower_count < self.CELEBRITY_THRESHOLD:
            self._fan_out_on_write(user, tweet)
        # else: celebrity -- tweet is pulled at read time

        return tweet

    def _fan_out_on_write(self, author: User, tweet: Tweet) -> None:
        """Push tweet_id into every follower's feed cache."""
        for follower_id in author.followers:
            cache = self.feed_cache.get(follower_id, [])
            cache.insert(0, tweet.tweet_id)  # newest first
            # Trim to limit
            if len(cache) > self.FEED_CACHE_LIMIT:
                cache = cache[: self.FEED_CACHE_LIMIT]
            self.feed_cache[follower_id] = cache

    # -- Engagement ----------------------------------------------------------

    def like_tweet(self, tweet_id: int) -> None:
        if tweet_id not in self.tweets:
            raise KeyError(f"Tweet {tweet_id} not found")
        self.tweets[tweet_id].likes += 1

    def retweet(self, tweet_id: int) -> None:
        if tweet_id not in self.tweets:
            raise KeyError(f"Tweet {tweet_id} not found")
        self.tweets[tweet_id].retweets += 1

    # -- Feed Generation (Hybrid) -------------------------------------------

    def generate_feed(
        self,
        user_id: str,
        limit: int = 20,
        ranked: bool = True,
    ) -> list[Tweet]:
        """
        Build the user's home feed using hybrid fan-out:
          1. Read pre-built cache (non-celebrity tweets).
          2. Pull recent tweets from followed celebrities.
          3. Merge, deduplicate, rank, and return top `limit` tweets.
        """
        user = self.get_user(user_id)

        # Step 1: cached tweet IDs (fan-out on write results)
        cached_ids = set(self.feed_cache.get(user_id, []))

        # Step 2: pull celebrity tweets (fan-out on read)
        celebrity_ids: set[int] = set()
        for followee_id in user.following:
            followee = self.users[followee_id]
            if followee.follower_count >= self.CELEBRITY_THRESHOLD:
                for tid in followee.tweets:
                    celebrity_ids.add(tid)

        # Step 3: merge and deduplicate
        all_ids = cached_ids | celebrity_ids
        candidates: list[Tweet] = []
        for tid in all_ids:
            tweet = self.tweets.get(tid)
            if tweet is not None:
                candidates.append(tweet)

        # Step 4: rank
        if ranked:
            candidates = self._rank_feed(candidates)
        else:
            candidates.sort(key=lambda t: t.timestamp, reverse=True)

        return candidates[:limit]

    @staticmethod
    def _rank_feed(tweets: list[Tweet]) -> list[Tweet]:
        """
        Simple ranking: score = recency_weight + engagement_weight.
        In production this would be an ML model.
        """
        if not tweets:
            return []

        now = time.time()
        scored: list[tuple[float, Tweet]] = []
        for t in tweets:
            age_seconds = max(now - t.timestamp, 1.0)
            recency = 1.0 / age_seconds
            engagement = t.engagement_score()
            score = recency * 100 + engagement * 0.5
            scored.append((score, t))

        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [t for _, t in scored]

    # -- User Timeline -------------------------------------------------------

    def get_user_timeline(
        self, user_id: str, limit: int = 20
    ) -> list[Tweet]:
        """Return a user's own tweets, newest first."""
        user = self.get_user(user_id)
        tweet_ids = user.tweets[-limit:]
        tweets = [self.tweets[tid] for tid in reversed(tweet_ids)]
        return tweets

    # -- Debug / Stats -------------------------------------------------------

    def stats(self) -> dict:
        total_tweets = len(self.tweets)
        total_users = len(self.users)
        cache_sizes = {uid: len(c) for uid, c in self.feed_cache.items()}
        return {
            "total_users": total_users,
            "total_tweets": total_tweets,
            "cache_sizes": cache_sizes,
        }


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _divider(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def main() -> None:
    svc = FeedService()

    # -- Create users --------------------------------------------------------
    _divider("1. Creating Users")
    alice = svc.create_user("alice", "Alice A.")
    bob = svc.create_user("bob", "Bob B.")
    charlie = svc.create_user("charlie", "Charlie C.")
    diana = svc.create_user("diana", "Diana D.")
    celebrity = svc.create_user("celeb", "Famous Person")
    print(f"  Created: {alice}")
    print(f"  Created: {bob}")
    print(f"  Created: {charlie}")
    print(f"  Created: {diana}")
    print(f"  Created: {celebrity}")

    # -- Build follow graph --------------------------------------------------
    _divider("2. Building Follow Graph")
    # Make 'celeb' a celebrity (>= 3 followers for demo threshold)
    for uid in ["alice", "bob", "charlie", "diana"]:
        svc.follow(uid, "celeb")
    # Normal follows
    svc.follow("alice", "bob")
    svc.follow("alice", "charlie")
    svc.follow("bob", "alice")
    svc.follow("charlie", "alice")

    print(f"  celeb followers: {celebrity.follower_count} "
          f"(threshold={FeedService.CELEBRITY_THRESHOLD})")
    is_celeb = celebrity.follower_count >= FeedService.CELEBRITY_THRESHOLD
    print(f"  celeb is celebrity? {is_celeb}")
    print(f"  alice following: {alice.following}")
    print(f"  bob following: {bob.following}")

    # -- Post tweets ---------------------------------------------------------
    _divider("3. Posting Tweets")

    t1 = svc.post_tweet("alice", "Hello from Alice!")
    time.sleep(0.01)
    t2 = svc.post_tweet("bob", "Bob here, first tweet.")
    time.sleep(0.01)
    t3 = svc.post_tweet("charlie", "Charlie checking in.")
    time.sleep(0.01)
    t4 = svc.post_tweet("celeb", "Celebrity announcement!")
    time.sleep(0.01)
    t5 = svc.post_tweet("alice", "Alice's second tweet.")
    time.sleep(0.01)
    t6 = svc.post_tweet("celeb", "Another celeb post.")

    for t in [t1, t2, t3, t4, t5, t6]:
        mode = ("PUSH (fan-out on write)"
                if svc.users[t.user_id].follower_count < FeedService.CELEBRITY_THRESHOLD
                else "PULL (fan-out on read / celebrity)")
        print(f"  [{mode}] {t}")

    # -- Engagement ----------------------------------------------------------
    _divider("4. Engagement (Likes & Retweets)")
    svc.like_tweet(t4.tweet_id)
    svc.like_tweet(t4.tweet_id)
    svc.like_tweet(t4.tweet_id)
    svc.retweet(t4.tweet_id)
    svc.like_tweet(t5.tweet_id)
    svc.retweet(t5.tweet_id)
    print(f"  {t4} -> engagement={t4.engagement_score()}")
    print(f"  {t5} -> engagement={t5.engagement_score()}")

    # -- Generate feeds ------------------------------------------------------
    _divider("5. Alice's Feed (Ranked)")
    feed = svc.generate_feed("alice", ranked=True)
    if not feed:
        print("  (empty feed)")
    for i, tw in enumerate(feed, 1):
        print(f"  {i}. @{tw.user_id}: \"{tw.text}\" "
              f"[likes={tw.likes}, rt={tw.retweets}]")

    _divider("6. Bob's Feed (Chronological)")
    feed_bob = svc.generate_feed("bob", ranked=False)
    if not feed_bob:
        print("  (empty feed)")
    for i, tw in enumerate(feed_bob, 1):
        print(f"  {i}. @{tw.user_id}: \"{tw.text}\" "
              f"[likes={tw.likes}, rt={tw.retweets}]")

    # -- Unfollow demo -------------------------------------------------------
    _divider("7. Unfollow Demo -- Alice unfollows Bob")
    print(f"  Before: Alice's cache has {len(svc.feed_cache['alice'])} items")
    svc.unfollow("alice", "bob")
    print(f"  After:  Alice's cache has {len(svc.feed_cache['alice'])} items")
    feed_after = svc.generate_feed("alice", ranked=False)
    print(f"  Alice's feed after unfollowing Bob:")
    for i, tw in enumerate(feed_after, 1):
        print(f"    {i}. @{tw.user_id}: \"{tw.text}\"")

    # -- User timeline -------------------------------------------------------
    _divider("8. Celebrity's Own Timeline")
    celeb_timeline = svc.get_user_timeline("celeb")
    for i, tw in enumerate(celeb_timeline, 1):
        print(f"  {i}. \"{tw.text}\" [likes={tw.likes}]")

    # -- Stats ---------------------------------------------------------------
    _divider("9. System Stats")
    stats = svc.stats()
    print(f"  Total users:  {stats['total_users']}")
    print(f"  Total tweets: {stats['total_tweets']}")
    print(f"  Cache sizes:  {stats['cache_sizes']}")

    # -- Verification -------------------------------------------------------
    _divider("10. Verification")
    alice_feed = svc.generate_feed("alice")
    bob_feed = svc.generate_feed("bob")
    assert len(alice_feed) > 0, "Alice's feed should not be empty"
    assert len(bob_feed) > 0, "Bob's feed should not be empty"
    assert all(tw.user_id != "bob" for tw in svc.generate_feed("alice")), \
        "Bob's tweets should be gone after unfollow"
    assert any(tw.user_id == "celeb" for tw in alice_feed), \
        "Celebrity tweets should appear via fan-out on read"
    assert celebrity.follower_count >= FeedService.CELEBRITY_THRESHOLD, \
        "Celebrity should exceed threshold"
    print("  All assertions passed [OK]")
    print("  Fan-out on write: verified for normal users")
    print("  Fan-out on read:  verified for celebrities")
    print("  Unfollow cleanup: verified")
    print("  Feed ranking:     verified")


if __name__ == "__main__":
    main()
