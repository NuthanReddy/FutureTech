"""
Video Streaming Platform Simulation
====================================
Simulates core components of a YouTube/Netflix-style video streaming platform:
- Video upload and metadata management
- Transcoding pipeline (DAG of parallel tasks)
- Adaptive bitrate quality selection
- View tracking with watch history
- Basic recommendation engine (collaborative + content-based filtering)
"""

from __future__ import annotations

import hashlib
import random
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class VideoStatus(Enum):
    UPLOADING = "uploading"
    PROCESSING = "processing"
    READY = "ready"
    FAILED = "failed"


class TranscodeStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class Resolution(Enum):
    P240 = ("240p", 426, 240, 400_000)
    P360 = ("360p", 640, 360, 800_000)
    P480 = ("480p", 854, 480, 1_400_000)
    P720 = ("720p", 1280, 720, 2_800_000)
    P1080 = ("1080p", 1920, 1080, 5_000_000)
    P4K = ("4k", 3840, 2160, 14_000_000)

    def __init__(self, label: str, width: int, height: int, bitrate: int) -> None:
        self.label = label
        self.width = width
        self.height = height
        self.bitrate = bitrate  # bits per second


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Video:
    video_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    creator_id: str = ""
    title: str = ""
    description: str = ""
    tags: list[str] = field(default_factory=list)
    category: str = ""
    duration_sec: int = 0
    raw_size_mb: float = 0.0
    status: VideoStatus = VideoStatus.UPLOADING
    available_resolutions: list[Resolution] = field(default_factory=list)
    view_count: int = 0
    like_count: int = 0
    created_at: float = field(default_factory=time.time)


@dataclass
class TranscodingJob:
    job_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    video_id: str = ""
    resolution: Resolution = Resolution.P720
    status: TranscodeStatus = TranscodeStatus.QUEUED
    output_path: str = ""
    retry_count: int = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error_message: str = ""


@dataclass
class ViewEvent:
    user_id: str
    video_id: str
    watch_duration_sec: int
    quality: str
    timestamp: float = field(default_factory=time.time)


@dataclass
class User:
    user_id: str
    username: str
    watch_history: list[ViewEvent] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Transcoding Pipeline
# ---------------------------------------------------------------------------

class TranscodingPipeline:
    """Simulates a DAG-based transcoding pipeline.

    Pipeline stages:
        1. Validate raw upload
        2. Transcode to each resolution in parallel (simulated)
        3. Generate HLS manifests
        4. Mark video as ready
    """

    RESOLUTIONS = [
        Resolution.P240,
        Resolution.P360,
        Resolution.P480,
        Resolution.P720,
        Resolution.P1080,
        Resolution.P4K,
    ]
    MAX_RETRIES = 3
    SIMULATED_FAILURE_RATE = 0.1  # 10% chance per task for demo

    def __init__(self, failure_rate: float = 0.1) -> None:
        self.failure_rate = failure_rate
        self.jobs: list[TranscodingJob] = []

    def _validate_upload(self, video: Video) -> bool:
        """Stage 1: Validate that raw upload exists and is non-empty."""
        if video.raw_size_mb <= 0:
            print(f"  [VALIDATE] FAIL - video {video.video_id} has no data")
            return False
        checksum = hashlib.md5(video.video_id.encode()).hexdigest()[:8]
        print(f"  [VALIDATE] OK - video {video.video_id} "
              f"({video.raw_size_mb:.1f} MB, checksum={checksum})")
        return True

    def _transcode_task(self, video: Video, resolution: Resolution) -> TranscodingJob:
        """Stage 2: Simulate transcoding to a single resolution."""
        job = TranscodingJob(
            video_id=video.video_id,
            resolution=resolution,
            status=TranscodeStatus.RUNNING,
            started_at=time.time(),
        )

        for attempt in range(1, self.MAX_RETRIES + 1):
            if random.random() < self.failure_rate and attempt < self.MAX_RETRIES:
                job.retry_count += 1
                print(f"  [TRANSCODE] {resolution.label} attempt {attempt} FAILED "
                      f"(retrying...)")
                continue

            # Simulate transcoded output size
            ratio = resolution.bitrate / Resolution.P1080.bitrate
            output_mb = video.raw_size_mb * ratio * 1.2
            job.output_path = (
                f"s3://video-bucket/{video.video_id}/{resolution.label}/segments/"
            )
            job.status = TranscodeStatus.COMPLETED
            job.completed_at = time.time()
            print(f"  [TRANSCODE] {resolution.label} OK - "
                  f"{output_mb:.1f} MB -> {job.output_path}")
            break
        else:
            job.status = TranscodeStatus.FAILED
            job.error_message = "Max retries exceeded"
            print(f"  [TRANSCODE] {resolution.label} FAILED permanently")

        self.jobs.append(job)
        return job

    def _generate_manifest(self, video: Video) -> str:
        """Stage 3: Generate HLS master playlist."""
        lines = ["#EXTM3U"]
        for res in video.available_resolutions:
            lines.append(
                f"#EXT-X-STREAM-INF:BANDWIDTH={res.bitrate},"
                f"RESOLUTION={res.width}x{res.height}"
            )
            lines.append(f"{res.label}/playlist.m3u8")
        manifest = "\n".join(lines)
        print(f"  [MANIFEST] Generated HLS playlist "
              f"with {len(video.available_resolutions)} quality levels")
        return manifest

    def process(self, video: Video) -> bool:
        """Run the full transcoding pipeline for a video."""
        print(f"\n--- Transcoding Pipeline: {video.title} ({video.video_id}) ---")
        video.status = VideoStatus.PROCESSING

        # Stage 1
        if not self._validate_upload(video):
            video.status = VideoStatus.FAILED
            return False

        # Stage 2 - parallel transcoding (simulated sequentially)
        print("  [PIPELINE] Starting parallel transcode tasks...")
        completed_resolutions: list[Resolution] = []
        for resolution in self.RESOLUTIONS:
            job = self._transcode_task(video, resolution)
            if job.status == TranscodeStatus.COMPLETED:
                completed_resolutions.append(resolution)

        if not completed_resolutions:
            video.status = VideoStatus.FAILED
            print("  [PIPELINE] FAILED - no resolutions completed")
            return False

        video.available_resolutions = completed_resolutions

        # Stage 3
        self._generate_manifest(video)

        # Stage 4 - mark ready
        video.status = VideoStatus.READY
        print(f"  [PIPELINE] COMPLETE - video ready with "
              f"{len(completed_resolutions)}/{len(self.RESOLUTIONS)} resolutions")
        return True


# ---------------------------------------------------------------------------
# Adaptive Bitrate Quality Selector
# ---------------------------------------------------------------------------

class AdaptiveBitrateSelector:
    """Simulates client-side ABR quality selection based on available bandwidth."""

    # Buffer thresholds in seconds
    LOW_BUFFER = 5.0
    HIGH_BUFFER = 15.0

    def __init__(self) -> None:
        self.current_quality: Optional[Resolution] = None
        self.buffer_level_sec: float = 10.0
        self.switches: list[tuple[str, str]] = []

    def select_quality(
        self,
        available: list[Resolution],
        bandwidth_bps: int,
    ) -> Resolution:
        """Select the best resolution that fits within available bandwidth.

        Uses a buffer-based algorithm:
        - Low buffer  -> drop to lowest quality to refill.
        - High buffer -> step up to next available quality.
        - Otherwise   -> pick highest quality that fits bandwidth.
        """
        sorted_res = sorted(available, key=lambda r: r.bitrate)
        if not sorted_res:
            raise ValueError("No resolutions available")

        if self.buffer_level_sec < self.LOW_BUFFER:
            chosen = sorted_res[0]
        elif self.buffer_level_sec > self.HIGH_BUFFER:
            # Try stepping up one level from current
            feasible = [r for r in sorted_res if r.bitrate <= bandwidth_bps]
            if feasible:
                current_idx = -1
                if self.current_quality in feasible:
                    current_idx = feasible.index(self.current_quality)
                chosen = feasible[min(current_idx + 1, len(feasible) - 1)]
            else:
                chosen = sorted_res[0]
        else:
            feasible = [r for r in sorted_res if r.bitrate <= bandwidth_bps]
            chosen = feasible[-1] if feasible else sorted_res[0]

        if self.current_quality and self.current_quality != chosen:
            self.switches.append((self.current_quality.label, chosen.label))

        self.current_quality = chosen
        return chosen

    def simulate_playback(
        self,
        video: Video,
        duration_sec: int = 30,
        segment_sec: int = 5,
    ) -> list[dict[str, object]]:
        """Simulate adaptive playback over the given duration."""
        segments: list[dict[str, object]] = []
        self.buffer_level_sec = 10.0

        for seg_idx in range(0, duration_sec, segment_sec):
            # Simulate fluctuating bandwidth (1-15 Mbps)
            bandwidth = random.randint(1_000_000, 15_000_000)

            quality = self.select_quality(video.available_resolutions, bandwidth)

            # Simulate buffer changes
            download_time = (quality.bitrate * segment_sec) / bandwidth
            self.buffer_level_sec += segment_sec - download_time
            self.buffer_level_sec = max(0, min(30, self.buffer_level_sec))

            segments.append({
                "segment": seg_idx // segment_sec,
                "time_range": f"{seg_idx}s-{seg_idx + segment_sec}s",
                "bandwidth_mbps": round(bandwidth / 1_000_000, 1),
                "quality": quality.label,
                "buffer_sec": round(self.buffer_level_sec, 1),
            })

        return segments


# ---------------------------------------------------------------------------
# View Tracker
# ---------------------------------------------------------------------------

class ViewTracker:
    """Tracks video views and user watch history."""

    def __init__(self) -> None:
        self.view_counts: dict[str, int] = defaultdict(int)
        self.watch_time: dict[str, int] = defaultdict(int)  # total seconds
        self.events: list[ViewEvent] = []
        self.user_history: dict[str, list[ViewEvent]] = defaultdict(list)

    def record_view(self, event: ViewEvent) -> None:
        """Record a view event."""
        self.view_counts[event.video_id] += 1
        self.watch_time[event.video_id] += event.watch_duration_sec
        self.events.append(event)
        self.user_history[event.user_id].append(event)

    def get_video_stats(self, video_id: str) -> dict[str, int]:
        return {
            "views": self.view_counts.get(video_id, 0),
            "total_watch_time_sec": self.watch_time.get(video_id, 0),
        }

    def get_trending(self, top_n: int = 5) -> list[tuple[str, int]]:
        """Return top-N videos by view count."""
        sorted_videos = sorted(
            self.view_counts.items(), key=lambda x: x[1], reverse=True
        )
        return sorted_videos[:top_n]


# ---------------------------------------------------------------------------
# Recommendation Engine
# ---------------------------------------------------------------------------

class RecommendationEngine:
    """Basic recommendation engine using collaborative + content-based filtering.

    Collaborative: Users who watched the same videos tend to like similar content.
    Content-based: Videos with overlapping tags/categories are similar.
    """

    def __init__(self, videos: dict[str, Video], tracker: ViewTracker) -> None:
        self.videos = videos
        self.tracker = tracker

    def _collaborative_score(
        self, user_id: str, candidate_id: str
    ) -> float:
        """Score based on how many similar users watched this video.

        'Similar users' = users who share at least one watched video.
        """
        user_watched = {e.video_id for e in self.tracker.user_history.get(user_id, [])}
        if not user_watched:
            return 0.0

        # Find similar users
        similar_users: set[str] = set()
        for other_uid, events in self.tracker.user_history.items():
            if other_uid == user_id:
                continue
            other_watched = {e.video_id for e in events}
            overlap = user_watched & other_watched
            if overlap:
                similar_users.add(other_uid)

        if not similar_users:
            return 0.0

        # How many similar users watched the candidate?
        watchers = sum(
            1 for uid in similar_users
            if any(e.video_id == candidate_id
                   for e in self.tracker.user_history.get(uid, []))
        )
        return watchers / len(similar_users)

    def _content_score(self, user_id: str, candidate_id: str) -> float:
        """Score based on tag/category overlap with user's watched videos."""
        user_events = self.tracker.user_history.get(user_id, [])
        if not user_events:
            return 0.0

        candidate = self.videos.get(candidate_id)
        if not candidate:
            return 0.0

        # Collect user preference tags and categories
        user_tags: set[str] = set()
        user_categories: set[str] = set()
        for event in user_events:
            v = self.videos.get(event.video_id)
            if v:
                user_tags.update(v.tags)
                if v.category:
                    user_categories.add(v.category)

        if not user_tags and not user_categories:
            return 0.0

        # Tag overlap
        candidate_tags = set(candidate.tags)
        tag_overlap = len(user_tags & candidate_tags) / max(
            len(user_tags | candidate_tags), 1
        )

        # Category match
        cat_match = 1.0 if candidate.category in user_categories else 0.0

        return 0.6 * tag_overlap + 0.4 * cat_match

    def _popularity_score(self, candidate_id: str) -> float:
        """Normalized popularity score."""
        max_views = max(self.tracker.view_counts.values(), default=1)
        views = self.tracker.view_counts.get(candidate_id, 0)
        return views / max_views if max_views > 0 else 0.0

    def recommend(
        self, user_id: str, top_n: int = 5
    ) -> list[dict[str, object]]:
        """Generate recommendations for a user.

        Final score = 0.4 * collaborative + 0.35 * content + 0.25 * popularity
        """
        user_watched = {
            e.video_id for e in self.tracker.user_history.get(user_id, [])
        }

        scored: list[tuple[str, float, str]] = []
        for vid, video in self.videos.items():
            if vid in user_watched or video.status != VideoStatus.READY:
                continue

            collab = self._collaborative_score(user_id, vid)
            content = self._content_score(user_id, vid)
            popularity = self._popularity_score(vid)
            final = 0.4 * collab + 0.35 * content + 0.25 * popularity

            # Determine primary reason
            scores = {"collaborative": collab, "content": content, "popular": popularity}
            reason = max(scores, key=scores.get)  # type: ignore[arg-type]
            scored.append((vid, final, reason))

        scored.sort(key=lambda x: x[1], reverse=True)

        results: list[dict[str, object]] = []
        for vid, score, reason in scored[:top_n]:
            video = self.videos[vid]
            results.append({
                "video_id": vid,
                "title": video.title,
                "score": round(score, 3),
                "reason": reason,
            })
        return results


# ---------------------------------------------------------------------------
# Platform (facade)
# ---------------------------------------------------------------------------

class VideoStreamingPlatform:
    """Facade that ties all components together."""

    def __init__(self, transcode_failure_rate: float = 0.05) -> None:
        self.videos: dict[str, Video] = {}
        self.users: dict[str, User] = {}
        self.pipeline = TranscodingPipeline(failure_rate=transcode_failure_rate)
        self.abr = AdaptiveBitrateSelector()
        self.tracker = ViewTracker()
        self._rec_engine: Optional[RecommendationEngine] = None

    @property
    def rec_engine(self) -> RecommendationEngine:
        if self._rec_engine is None:
            self._rec_engine = RecommendationEngine(self.videos, self.tracker)
        return self._rec_engine

    # -- User management --
    def register_user(self, username: str) -> User:
        user = User(user_id=str(uuid.uuid4())[:8], username=username)
        self.users[user.user_id] = user
        print(f"[USER] Registered '{username}' (id={user.user_id})")
        return user

    # -- Upload --
    def upload_video(
        self,
        creator_id: str,
        title: str,
        description: str = "",
        tags: Optional[list[str]] = None,
        category: str = "",
        duration_sec: int = 300,
        raw_size_mb: float = 500.0,
    ) -> Video:
        video = Video(
            creator_id=creator_id,
            title=title,
            description=description,
            tags=tags or [],
            category=category,
            duration_sec=duration_sec,
            raw_size_mb=raw_size_mb,
        )
        self.videos[video.video_id] = video
        print(f"\n[UPLOAD] '{title}' by user {creator_id} "
              f"({raw_size_mb:.0f} MB, {duration_sec}s)")

        # Immediately kick off transcoding
        success = self.pipeline.process(video)
        if not success:
            print(f"[UPLOAD] Video '{title}' transcoding FAILED")
        return video

    # -- Playback --
    def play_video(
        self,
        user_id: str,
        video_id: str,
        watch_sec: int = 30,
    ) -> None:
        video = self.videos.get(video_id)
        if not video or video.status != VideoStatus.READY:
            print(f"[PLAY] Video {video_id} not available")
            return

        print(f"\n=== Playing: '{video.title}' ===")
        segments = self.abr.simulate_playback(video, duration_sec=watch_sec)

        print(f"  {'Seg':<5} {'Time':<12} {'BW(Mbps)':<10} "
              f"{'Quality':<8} {'Buffer(s)':<10}")
        print("  " + "-" * 50)
        for seg in segments:
            print(f"  {seg['segment']:<5} {seg['time_range']:<12} "
                  f"{seg['bandwidth_mbps']:<10} {seg['quality']:<8} "
                  f"{seg['buffer_sec']:<10}")

        if self.abr.switches:
            print(f"  Quality switches: {len(self.abr.switches)}")
        self.abr.switches.clear()

        # Record view
        quality_label = segments[-1]["quality"] if segments else "unknown"
        event = ViewEvent(
            user_id=user_id,
            video_id=video_id,
            watch_duration_sec=watch_sec,
            quality=str(quality_label),
        )
        self.tracker.record_view(event)
        user = self.users.get(user_id)
        if user:
            user.watch_history.append(event)

        stats = self.tracker.get_video_stats(video_id)
        print(f"  Views: {stats['views']} | "
              f"Total watch time: {stats['total_watch_time_sec']}s")

    # -- Recommendations --
    def get_recommendations(self, user_id: str, top_n: int = 5) -> None:
        user = self.users.get(user_id)
        if not user:
            print(f"[REC] Unknown user {user_id}")
            return

        recs = self.rec_engine.recommend(user_id, top_n=top_n)
        print(f"\n--- Recommendations for '{user.username}' ---")
        if not recs:
            print("  No recommendations available (watch more videos!)")
            return

        for i, rec in enumerate(recs, 1):
            print(f"  {i}. [{rec['score']:.3f}] {rec['title']} "
                  f"(reason: {rec['reason']})")

    # -- Trending --
    def show_trending(self, top_n: int = 5) -> None:
        trending = self.tracker.get_trending(top_n)
        print(f"\n--- Trending Videos (Top {top_n}) ---")
        for rank, (vid, views) in enumerate(trending, 1):
            title = self.videos[vid].title if vid in self.videos else "Unknown"
            print(f"  {rank}. {title} - {views} views")


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def demo() -> None:
    """Run a complete demo of the video streaming platform."""
    random.seed(42)

    platform = VideoStreamingPlatform(transcode_failure_rate=0.08)

    print("=" * 60)
    print("  VIDEO STREAMING PLATFORM SIMULATION")
    print("=" * 60)

    # --- Register users ---
    print("\n[PHASE 1] Registering users...")
    alice = platform.register_user("alice")
    bob = platform.register_user("bob")
    charlie = platform.register_user("charlie")

    # --- Upload videos ---
    print("\n[PHASE 2] Uploading videos...")

    v1 = platform.upload_video(
        creator_id=alice.user_id,
        title="Python Data Structures Tutorial",
        tags=["python", "tutorial", "programming", "data-structures"],
        category="Education",
        duration_sec=600,
        raw_size_mb=800,
    )

    v2 = platform.upload_video(
        creator_id=alice.user_id,
        title="Advanced Algorithms Explained",
        tags=["algorithms", "tutorial", "programming", "competitive"],
        category="Education",
        duration_sec=900,
        raw_size_mb=1200,
    )

    v3 = platform.upload_video(
        creator_id=bob.user_id,
        title="Guitar Lesson for Beginners",
        tags=["guitar", "music", "tutorial", "beginner"],
        category="Music",
        duration_sec=480,
        raw_size_mb=600,
    )

    v4 = platform.upload_video(
        creator_id=bob.user_id,
        title="Jazz Improvisation Masterclass",
        tags=["jazz", "music", "advanced", "improvisation"],
        category="Music",
        duration_sec=720,
        raw_size_mb=950,
    )

    v5 = platform.upload_video(
        creator_id=charlie.user_id,
        title="System Design Interview Prep",
        tags=["system-design", "interview", "programming", "architecture"],
        category="Education",
        duration_sec=1200,
        raw_size_mb=1500,
    )

    v6 = platform.upload_video(
        creator_id=charlie.user_id,
        title="Cooking Italian Pasta",
        tags=["cooking", "italian", "food", "recipe"],
        category="Food",
        duration_sec=360,
        raw_size_mb=450,
    )

    # --- Simulate viewing ---
    print("\n" + "=" * 60)
    print("[PHASE 3] Simulating video playback...")

    ready_videos = [v for v in [v1, v2, v3, v4, v5, v6]
                    if v.status == VideoStatus.READY]

    if len(ready_videos) >= 4:
        # Alice watches programming and system design content
        platform.play_video(alice.user_id, ready_videos[0].video_id, watch_sec=25)
        if len(ready_videos) > 4:
            platform.play_video(alice.user_id, ready_videos[4].video_id, watch_sec=20)

        # Bob watches music and some programming
        platform.play_video(bob.user_id, ready_videos[2].video_id, watch_sec=30)
        platform.play_video(bob.user_id, ready_videos[0].video_id, watch_sec=15)

        # Charlie watches a mix
        platform.play_video(charlie.user_id, ready_videos[0].video_id, watch_sec=20)
        platform.play_video(charlie.user_id, ready_videos[2].video_id, watch_sec=25)
        if len(ready_videos) > 5:
            platform.play_video(charlie.user_id, ready_videos[5].video_id, watch_sec=15)

        # Extra views on popular video
        platform.play_video(alice.user_id, ready_videos[0].video_id, watch_sec=10)
        platform.play_video(bob.user_id, ready_videos[0].video_id, watch_sec=10)

    # --- Trending ---
    print("\n" + "=" * 60)
    print("[PHASE 4] Analytics...")
    platform.show_trending(top_n=5)

    # --- Recommendations ---
    print("\n" + "=" * 60)
    print("[PHASE 5] Generating recommendations...")
    platform.get_recommendations(alice.user_id)
    platform.get_recommendations(bob.user_id)
    platform.get_recommendations(charlie.user_id)

    # --- Summary ---
    print("\n" + "=" * 60)
    print("  PLATFORM SUMMARY")
    print("=" * 60)
    total_videos = len(platform.videos)
    ready_count = sum(
        1 for v in platform.videos.values() if v.status == VideoStatus.READY
    )
    total_views = sum(platform.tracker.view_counts.values())
    total_watch = sum(platform.tracker.watch_time.values())
    total_jobs = len(platform.pipeline.jobs)
    completed_jobs = sum(
        1 for j in platform.pipeline.jobs
        if j.status == TranscodeStatus.COMPLETED
    )
    print(f"  Videos uploaded:     {total_videos}")
    print(f"  Videos ready:        {ready_count}")
    print(f"  Transcode jobs:      {total_jobs} "
          f"({completed_jobs} completed)")
    print(f"  Total views:         {total_views}")
    print(f"  Total watch time:    {total_watch}s")
    print(f"  Registered users:    {len(platform.users)}")
    print("=" * 60)


if __name__ == "__main__":
    demo()
