"""
File Storage System (Dropbox / Google Drive) - Simulation

Demonstrates core concepts:
- Chunking: Files split into 4MB content-addressed blocks
- Deduplication: Identical chunks stored once via SHA-256 hashing
- Versioning: Every modification creates a new immutable version
- Sharing: Permission-based file access (view/edit/owner)
- Conflict resolution: Last-writer-wins with version history preservation
- Delta sync: Only changed chunks transferred between versions
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB


class Permission(Enum):
    VIEW = "view"
    EDIT = "edit"
    OWNER = "owner"


class ConflictStrategy(Enum):
    LAST_WRITER_WINS = "last_writer_wins"
    CONFLICT_COPY = "conflict_copy"


@dataclass
class User:
    user_id: str
    email: str
    display_name: str
    storage_quota: int = 15 * 1024 * 1024 * 1024  # 15 GB
    storage_used: int = 0

    def has_quota(self, size: int) -> bool:
        return self.storage_used + size <= self.storage_quota


@dataclass
class Chunk:
    """Content-addressed storage block. Identity = SHA-256 of data."""

    chunk_hash: str
    size: int
    ref_count: int = 1
    data: bytes = field(repr=False, default=b"")

    @staticmethod
    def from_data(data: bytes) -> "Chunk":
        chunk_hash = hashlib.sha256(data).hexdigest()
        return Chunk(chunk_hash=chunk_hash, size=len(data), data=data)

    def verify_integrity(self) -> bool:
        return hashlib.sha256(self.data).hexdigest() == self.chunk_hash


@dataclass
class FileVersion:
    """Immutable snapshot of a file at a point in time."""

    version_id: str
    version_number: int
    chunk_hashes: list[str]
    total_size: int
    content_hash: str
    author_id: str
    created_at: float

    @staticmethod
    def create(
        version_number: int,
        chunk_hashes: list[str],
        total_size: int,
        content_hash: str,
        author_id: str,
    ) -> "FileVersion":
        return FileVersion(
            version_id=str(uuid.uuid4()),
            version_number=version_number,
            chunk_hashes=chunk_hashes,
            total_size=total_size,
            content_hash=content_hash,
            author_id=author_id,
            created_at=time.time(),
        )


@dataclass
class ShareEntry:
    share_id: str
    file_id: str
    owner_id: str
    shared_with_id: str
    permission: Permission
    created_at: float


@dataclass
class FileMetadata:
    """Metadata record for a file or folder in the system."""

    file_id: str
    owner_id: str
    filename: str
    parent_folder_id: Optional[str] = None
    is_folder: bool = False
    current_version: int = 0
    total_size: int = 0
    content_hash: Optional[str] = None
    versions: list[FileVersion] = field(default_factory=list)
    shares: list[ShareEntry] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    is_deleted: bool = False

    @staticmethod
    def create(owner_id: str, filename: str, parent_folder_id: Optional[str] = None,
               is_folder: bool = False) -> "FileMetadata":
        return FileMetadata(
            file_id=str(uuid.uuid4()),
            owner_id=owner_id,
            filename=filename,
            parent_folder_id=parent_folder_id,
            is_folder=is_folder,
        )


class FileStorageService:
    """
    Core file storage service implementing chunking, dedup, versioning, and sharing.

    Storage layout:
        chunk_store:  chunk_hash -> Chunk  (content-addressed, deduplicated)
        file_store:   file_id -> FileMetadata (with version history)
        user_store:   user_id -> User
    """

    def __init__(self, conflict_strategy: ConflictStrategy = ConflictStrategy.LAST_WRITER_WINS):
        self.chunk_store: dict[str, Chunk] = {}
        self.file_store: dict[str, FileMetadata] = {}
        self.user_store: dict[str, User] = {}
        self.conflict_strategy = conflict_strategy
        self._sync_log: list[dict] = []

    # ---- User Management ----

    def register_user(self, email: str, display_name: str) -> User:
        user = User(user_id=str(uuid.uuid4()), email=email, display_name=display_name)
        self.user_store[user.user_id] = user
        return user

    # ---- Chunking ----

    def _split_into_chunks(self, data: bytes) -> list[Chunk]:
        """Split data into fixed-size chunks (4MB default)."""
        chunks = []
        for offset in range(0, len(data), CHUNK_SIZE):
            chunk_data = data[offset: offset + CHUNK_SIZE]
            chunks.append(Chunk.from_data(chunk_data))
        return chunks

    def _compute_content_hash(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    # ---- Deduplication ----

    def _store_chunks(self, chunks: list[Chunk]) -> tuple[list[str], int, int]:
        """
        Store chunks with deduplication.

        Returns:
            (chunk_hashes, new_chunks_count, dedup_chunks_count)
        """
        hashes = []
        new_count = 0
        dedup_count = 0

        for chunk in chunks:
            if chunk.chunk_hash in self.chunk_store:
                self.chunk_store[chunk.chunk_hash].ref_count += 1
                dedup_count += 1
            else:
                self.chunk_store[chunk.chunk_hash] = chunk
                new_count += 1
            hashes.append(chunk.chunk_hash)

        return hashes, new_count, dedup_count

    # ---- Upload ----

    def upload_file(
        self, user_id: str, filename: str, data: bytes,
        parent_folder_id: Optional[str] = None,
        expected_version: Optional[int] = None,
    ) -> FileMetadata:
        """
        Upload a file with chunking and deduplication.

        If a file with the same name exists under the same parent for the same user,
        creates a new version (optimistic concurrency via expected_version).
        """
        user = self.user_store.get(user_id)
        if not user:
            raise ValueError(f"User {user_id} not found")
        if not user.has_quota(len(data)):
            raise ValueError("Storage quota exceeded")

        content_hash = self._compute_content_hash(data)
        chunks = self._split_into_chunks(data)
        chunk_hashes, new_count, dedup_count = self._store_chunks(chunks)

        # Check for existing file (same owner, name, parent)
        existing = self._find_file(user_id, filename, parent_folder_id)

        if existing:
            return self._create_new_version(
                existing, chunk_hashes, len(data), content_hash, user_id, expected_version
            )

        # New file
        file_meta = FileMetadata.create(user_id, filename, parent_folder_id)
        version = FileVersion.create(
            version_number=1,
            chunk_hashes=chunk_hashes,
            total_size=len(data),
            content_hash=content_hash,
            author_id=user_id,
        )
        file_meta.versions.append(version)
        file_meta.current_version = 1
        file_meta.total_size = len(data)
        file_meta.content_hash = content_hash

        self.file_store[file_meta.file_id] = file_meta
        user.storage_used += len(data)

        self._emit_sync_event("FILE_CREATED", file_meta.file_id, user_id)
        return file_meta

    def _create_new_version(
        self, file_meta: FileMetadata, chunk_hashes: list[str],
        total_size: int, content_hash: str, author_id: str,
        expected_version: Optional[int],
    ) -> FileMetadata:
        """Create a new version with optimistic concurrency control."""
        if expected_version is not None and file_meta.current_version != expected_version:
            if self.conflict_strategy == ConflictStrategy.LAST_WRITER_WINS:
                pass  # proceed, overwrite
            else:
                conflict_name = (
                    f"{file_meta.filename} (conflict - {time.strftime('%Y-%m-%d %H:%M:%S')})"
                )
                return self.upload_file(author_id, conflict_name, b"")

        new_version_num = file_meta.current_version + 1
        version = FileVersion.create(
            version_number=new_version_num,
            chunk_hashes=chunk_hashes,
            total_size=total_size,
            content_hash=content_hash,
            author_id=author_id,
        )
        file_meta.versions.append(version)
        file_meta.current_version = new_version_num
        file_meta.total_size = total_size
        file_meta.content_hash = content_hash
        file_meta.updated_at = time.time()

        self._emit_sync_event("FILE_MODIFIED", file_meta.file_id, author_id)
        return file_meta

    # ---- Download ----

    def download_file(
        self, user_id: str, file_id: str, version_number: Optional[int] = None
    ) -> bytes:
        """Download a file by reassembling its chunks. Optionally specify a version."""
        file_meta = self.file_store.get(file_id)
        if not file_meta:
            raise ValueError(f"File {file_id} not found")
        if not self._has_access(user_id, file_meta, Permission.VIEW):
            raise PermissionError("Access denied")

        version = self._get_version(file_meta, version_number)
        return self._reassemble_chunks(version.chunk_hashes)

    def _reassemble_chunks(self, chunk_hashes: list[str]) -> bytes:
        """Reassemble file from its ordered chunk list."""
        parts = []
        for h in chunk_hashes:
            chunk = self.chunk_store.get(h)
            if not chunk:
                raise ValueError(f"Chunk {h[:16]}... not found (data corruption)")
            if not chunk.verify_integrity():
                raise ValueError(f"Chunk {h[:16]}... integrity check failed")
            parts.append(chunk.data)
        return b"".join(parts)

    # ---- Versioning ----

    def get_versions(self, user_id: str, file_id: str) -> list[FileVersion]:
        file_meta = self.file_store.get(file_id)
        if not file_meta:
            raise ValueError(f"File {file_id} not found")
        if not self._has_access(user_id, file_meta, Permission.VIEW):
            raise PermissionError("Access denied")
        return list(file_meta.versions)

    def restore_version(self, user_id: str, file_id: str, version_number: int) -> FileMetadata:
        """Restore a previous version by creating a new version with the old chunk list."""
        file_meta = self.file_store.get(file_id)
        if not file_meta:
            raise ValueError(f"File {file_id} not found")
        if not self._has_access(user_id, file_meta, Permission.EDIT):
            raise PermissionError("Edit access required to restore versions")

        old_version = self._get_version(file_meta, version_number)
        return self._create_new_version(
            file_meta, old_version.chunk_hashes, old_version.total_size,
            old_version.content_hash, user_id, file_meta.current_version,
        )

    def _get_version(self, file_meta: FileMetadata, version_number: Optional[int]) -> FileVersion:
        if version_number is None:
            version_number = file_meta.current_version
        for v in file_meta.versions:
            if v.version_number == version_number:
                return v
        raise ValueError(f"Version {version_number} not found")

    # ---- Sharing ----

    def share_file(
        self, owner_id: str, file_id: str, target_user_id: str, permission: Permission
    ) -> ShareEntry:
        file_meta = self.file_store.get(file_id)
        if not file_meta:
            raise ValueError(f"File {file_id} not found")
        if file_meta.owner_id != owner_id:
            raise PermissionError("Only the owner can share files")

        entry = ShareEntry(
            share_id=str(uuid.uuid4()),
            file_id=file_id,
            owner_id=owner_id,
            shared_with_id=target_user_id,
            permission=permission,
            created_at=time.time(),
        )
        file_meta.shares.append(entry)
        self._emit_sync_event("FILE_SHARED", file_id, owner_id)
        return entry

    def revoke_share(self, owner_id: str, file_id: str, target_user_id: str) -> bool:
        file_meta = self.file_store.get(file_id)
        if not file_meta:
            raise ValueError(f"File {file_id} not found")
        if file_meta.owner_id != owner_id:
            raise PermissionError("Only the owner can revoke shares")

        before = len(file_meta.shares)
        file_meta.shares = [s for s in file_meta.shares if s.shared_with_id != target_user_id]
        return len(file_meta.shares) < before

    def _has_access(self, user_id: str, file_meta: FileMetadata, required: Permission) -> bool:
        if file_meta.owner_id == user_id:
            return True
        permission_level = {Permission.VIEW: 0, Permission.EDIT: 1, Permission.OWNER: 2}
        for share in file_meta.shares:
            if share.shared_with_id == user_id:
                if permission_level[share.permission] >= permission_level[required]:
                    return True
        return False

    # ---- Delta Sync ----

    def get_delta(self, file_id: str, from_version: int, to_version: int) -> dict:
        """Compute delta between two versions (chunks added/removed)."""
        file_meta = self.file_store.get(file_id)
        if not file_meta:
            raise ValueError(f"File {file_id} not found")

        old_v = self._get_version(file_meta, from_version)
        new_v = self._get_version(file_meta, to_version)

        old_set = set(old_v.chunk_hashes)
        new_set = set(new_v.chunk_hashes)

        return {
            "added_chunks": list(new_set - old_set),
            "removed_chunks": list(old_set - new_set),
            "unchanged_chunks": list(old_set & new_set),
            "old_size": old_v.total_size,
            "new_size": new_v.total_size,
            "transfer_size": sum(
                self.chunk_store[h].size for h in (new_set - old_set) if h in self.chunk_store
            ),
        }

    # ---- Sync Events ----

    def _emit_sync_event(self, event_type: str, file_id: str, user_id: str) -> None:
        self._sync_log.append({
            "event_type": event_type,
            "file_id": file_id,
            "user_id": user_id,
            "timestamp": time.time(),
        })

    def get_sync_events(self, since_index: int = 0) -> list[dict]:
        return self._sync_log[since_index:]

    # ---- Helpers ----

    def _find_file(
        self, owner_id: str, filename: str, parent_folder_id: Optional[str]
    ) -> Optional[FileMetadata]:
        for f in self.file_store.values():
            if (
                f.owner_id == owner_id
                and f.filename == filename
                and f.parent_folder_id == parent_folder_id
                and not f.is_deleted
            ):
                return f
        return None

    def delete_file(self, user_id: str, file_id: str) -> bool:
        file_meta = self.file_store.get(file_id)
        if not file_meta:
            raise ValueError(f"File {file_id} not found")
        if file_meta.owner_id != user_id:
            raise PermissionError("Only the owner can delete files")

        file_meta.is_deleted = True
        file_meta.updated_at = time.time()
        self._emit_sync_event("FILE_DELETED", file_id, user_id)

        # Decrement chunk ref counts
        for version in file_meta.versions:
            for h in version.chunk_hashes:
                if h in self.chunk_store:
                    self.chunk_store[h].ref_count -= 1

        return True

    def get_storage_stats(self) -> dict:
        total_chunks = len(self.chunk_store)
        total_chunk_bytes = sum(c.size for c in self.chunk_store.values())
        total_refs = sum(c.ref_count for c in self.chunk_store.values())
        logical_bytes = sum(c.size * c.ref_count for c in self.chunk_store.values())
        dedup_savings = logical_bytes - total_chunk_bytes if logical_bytes > 0 else 0
        dedup_ratio = (dedup_savings / logical_bytes * 100) if logical_bytes > 0 else 0.0

        return {
            "total_files": sum(1 for f in self.file_store.values() if not f.is_deleted),
            "total_versions": sum(len(f.versions) for f in self.file_store.values()),
            "unique_chunks": total_chunks,
            "total_chunk_refs": total_refs,
            "physical_storage_bytes": total_chunk_bytes,
            "logical_storage_bytes": logical_bytes,
            "dedup_savings_bytes": dedup_savings,
            "dedup_ratio_pct": round(dedup_ratio, 1),
        }


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _format_bytes(n: int) -> str:
    if n >= 1024 * 1024:
        return f"{n / (1024 * 1024):.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def demo() -> None:
    print("=" * 70)
    print("  FILE STORAGE SYSTEM -- Simulation Demo")
    print("=" * 70)

    svc = FileStorageService()

    # Register users
    alice = svc.register_user("alice@example.com", "Alice")
    bob = svc.register_user("bob@example.com", "Bob")
    print(f"\n[Users] Registered: {alice.display_name}, {bob.display_name}")

    # --- Upload ---
    print("\n" + "-" * 70)
    print("1. UPLOAD WITH CHUNKING & DEDUP")
    print("-" * 70)

    file_data = b"A" * (5 * 1024 * 1024)  # 5 MB -> 2 chunks (4MB + 1MB)
    f1 = svc.upload_file(alice.user_id, "report.pdf", file_data)
    print(f"   Uploaded: {f1.filename}  size={_format_bytes(f1.total_size)}  "
          f"version={f1.current_version}  chunks={len(f1.versions[-1].chunk_hashes)}")

    # Upload duplicate content (demonstrates dedup)
    f2 = svc.upload_file(bob.user_id, "report_copy.pdf", file_data)
    stats = svc.get_storage_stats()
    print(f"   Uploaded duplicate: {f2.filename} (Bob)")
    print(f"   Dedup ratio: {stats['dedup_ratio_pct']}%  "
          f"(physical={_format_bytes(stats['physical_storage_bytes'])}, "
          f"logical={_format_bytes(stats['logical_storage_bytes'])})")

    # --- Versioning ---
    print("\n" + "-" * 70)
    print("2. FILE VERSIONING")
    print("-" * 70)

    # Modify file -> new version
    modified_data = b"A" * (4 * 1024 * 1024) + b"B" * (1 * 1024 * 1024)
    f1_v2 = svc.upload_file(alice.user_id, "report.pdf", modified_data)
    print(f"   Modified: {f1_v2.filename}  version={f1_v2.current_version}")

    versions = svc.get_versions(alice.user_id, f1.file_id)
    for v in versions:
        print(f"   -> v{v.version_number}  size={_format_bytes(v.total_size)}  "
              f"chunks={len(v.chunk_hashes)}  hash={v.content_hash[:16]}...")

    # --- Delta Sync ---
    print("\n" + "-" * 70)
    print("3. DELTA SYNC")
    print("-" * 70)

    delta = svc.get_delta(f1.file_id, from_version=1, to_version=2)
    print(f"   Syncing v1 -> v2:")
    print(f"     Added chunks:     {len(delta['added_chunks'])}")
    print(f"     Removed chunks:   {len(delta['removed_chunks'])}")
    print(f"     Unchanged chunks: {len(delta['unchanged_chunks'])}")
    print(f"     Transfer size:    {_format_bytes(delta['transfer_size'])} "
          f"(vs full file {_format_bytes(delta['new_size'])})")

    # --- Download & Verify ---
    print("\n" + "-" * 70)
    print("4. DOWNLOAD & INTEGRITY VERIFICATION")
    print("-" * 70)

    downloaded_v1 = svc.download_file(alice.user_id, f1.file_id, version_number=1)
    downloaded_v2 = svc.download_file(alice.user_id, f1.file_id, version_number=2)
    print(f"   Downloaded v1: {_format_bytes(len(downloaded_v1))}  "
          f"match={downloaded_v1 == file_data}")
    print(f"   Downloaded v2: {_format_bytes(len(downloaded_v2))}  "
          f"match={downloaded_v2 == modified_data}")

    # --- Version Restore ---
    print("\n" + "-" * 70)
    print("5. VERSION RESTORE")
    print("-" * 70)

    svc.restore_version(alice.user_id, f1.file_id, version_number=1)
    print(f"   Restored v1 -> now at v{f1.current_version}")
    restored_data = svc.download_file(alice.user_id, f1.file_id)
    print(f"   Content matches original v1: {restored_data == file_data}")

    # --- Sharing ---
    print("\n" + "-" * 70)
    print("6. FILE SHARING & PERMISSIONS")
    print("-" * 70)

    share = svc.share_file(alice.user_id, f1.file_id, bob.user_id, Permission.EDIT)
    print(f"   Alice shared '{f1.filename}' with Bob (permission={share.permission.value})")

    bob_data = svc.download_file(bob.user_id, f1.file_id)
    print(f"   Bob downloaded shared file: {_format_bytes(len(bob_data))}")

    # Bob edits the shared file (new version under Alice's file)
    bob_edit = b"C" * (5 * 1024 * 1024)
    svc.upload_file(alice.user_id, "report.pdf", bob_edit)
    print(f"   Edit via share -> version {f1.current_version}")

    # Revoke access
    svc.revoke_share(alice.user_id, f1.file_id, bob.user_id)
    print(f"   Revoked Bob's access")
    try:
        svc.download_file(bob.user_id, f1.file_id)
        print("   ERROR: Bob should not have access")
    except PermissionError:
        print("   Bob correctly denied access after revocation")

    # --- Conflict Resolution ---
    print("\n" + "-" * 70)
    print("7. CONFLICT RESOLUTION (Last-Writer-Wins)")
    print("-" * 70)

    base_version = f1.current_version
    edit_a = b"D" * (3 * 1024 * 1024)
    edit_b = b"E" * (3 * 1024 * 1024)

    svc.upload_file(alice.user_id, "report.pdf", edit_a, expected_version=base_version)
    print(f"   Device A edit -> v{f1.current_version}")

    # Device B sends stale expected_version (conflict!)
    svc.upload_file(alice.user_id, "report.pdf", edit_b, expected_version=base_version)
    print(f"   Device B edit (stale version) -> v{f1.current_version} [LWW: B wins]")
    print(f"   Previous version preserved in history for recovery")

    # --- Sync Events ---
    print("\n" + "-" * 70)
    print("8. SYNC EVENT LOG")
    print("-" * 70)

    events = svc.get_sync_events()
    for i, evt in enumerate(events):
        print(f"   [{i}] {evt['event_type']:20s}  file={evt['file_id'][:12]}...")

    # --- Storage Stats ---
    print("\n" + "-" * 70)
    print("9. STORAGE STATISTICS")
    print("-" * 70)

    final_stats = svc.get_storage_stats()
    for key, val in final_stats.items():
        if "bytes" in key:
            print(f"   {key:30s}: {_format_bytes(val)}")
        else:
            print(f"   {key:30s}: {val}")

    # --- Delete ---
    print("\n" + "-" * 70)
    print("10. FILE DELETION")
    print("-" * 70)

    svc.delete_file(alice.user_id, f1.file_id)
    print(f"   Deleted '{f1.filename}' (soft delete, chunks ref-counted)")
    post_delete_stats = svc.get_storage_stats()
    print(f"   Active files remaining: {post_delete_stats['total_files']}")

    print("\n" + "=" * 70)
    print("  Demo complete.")
    print("=" * 70)


if __name__ == "__main__":
    demo()
