"""
Distributed Key-Value Store (Dynamo-like)

A simulation of a Dynamo-style distributed key-value store featuring:
- Consistent hashing with virtual nodes for data partitioning
- Quorum-based reads and writes (tunable N, R, W)
- Vector clocks for conflict detection and causal ordering
- Gossip protocol for failure detection
- Hinted handoff for availability during node failures
- Read repair for consistency convergence
"""

from __future__ import annotations

import hashlib
import random
import threading
import time
from bisect import bisect_right, insort
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Vector Clock
# ---------------------------------------------------------------------------

class VectorClock:
    """Logical clock tracking causal ordering across distributed nodes."""

    def __init__(self, clock: Optional[dict[str, int]] = None) -> None:
        self.clock: dict[str, int] = dict(clock) if clock else {}

    def increment(self, node_id: str) -> None:
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def merge(self, other: VectorClock) -> VectorClock:
        """Return a new clock that is the element-wise max of both clocks."""
        merged = dict(self.clock)
        for node_id, counter in other.clock.items():
            merged[node_id] = max(merged.get(node_id, 0), counter)
        return VectorClock(merged)

    def compare(self, other: VectorClock) -> str:
        """Compare two vector clocks.

        Returns:
            'BEFORE'     -- self happened-before other (self < other)
            'AFTER'      -- self happened-after other (self > other)
            'EQUAL'      -- identical clocks
            'CONCURRENT' -- neither dominates (conflict)
        """
        all_keys = set(self.clock.keys()) | set(other.clock.keys())
        self_le = True
        other_le = True

        for key in all_keys:
            s = self.clock.get(key, 0)
            o = other.clock.get(key, 0)
            if s > o:
                self_le = False   # self is NOT <= other
            if o > s:
                other_le = False  # other is NOT <= self

        if self_le and other_le:
            return "EQUAL"
        if self_le:
            return "BEFORE"
        if other_le:
            return "AFTER"
        return "CONCURRENT"

    def copy(self) -> VectorClock:
        return VectorClock(dict(self.clock))

    def __repr__(self) -> str:
        items = ", ".join(f"{k}:{v}" for k, v in sorted(self.clock.items()))
        return f"VC({items})"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

class NodeStatus(Enum):
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"


@dataclass
class KVEntry:
    """A single versioned key-value entry."""
    key: str
    value: Any
    vector_clock: VectorClock
    timestamp: float = field(default_factory=time.time)
    tombstone: bool = False


@dataclass
class HintedEntry:
    """Data destined for a node that was temporarily unavailable."""
    target_node_id: str
    entry: KVEntry


@dataclass
class GossipState:
    """Membership information exchanged via gossip."""
    heartbeat: int = 0
    timestamp: float = field(default_factory=time.time)
    status: NodeStatus = NodeStatus.ALIVE


# ---------------------------------------------------------------------------
# Storage Node
# ---------------------------------------------------------------------------

class StorageNode:
    """Represents a single storage node in the cluster."""

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.data: dict[str, list[KVEntry]] = defaultdict(list)
        self.hints: list[HintedEntry] = []
        self.alive = True
        self.lock = threading.Lock()

    def local_put(self, entry: KVEntry) -> bool:
        """Store an entry locally, resolving against existing versions."""
        if not self.alive:
            return False
        with self.lock:
            existing = self.data.get(entry.key, [])
            new_versions: list[KVEntry] = []
            for ex in existing:
                rel = entry.vector_clock.compare(ex.vector_clock)
                if rel == "AFTER" or rel == "EQUAL":
                    # new entry supersedes existing
                    continue
                elif rel == "BEFORE":
                    # existing entry supersedes new -- keep existing, discard new
                    return True
                else:
                    # concurrent -- keep both (sibling)
                    new_versions.append(ex)
            new_versions.append(entry)
            self.data[entry.key] = new_versions
            return True

    def local_get(self, key: str) -> list[KVEntry]:
        """Retrieve all versions of a key from local storage."""
        if not self.alive:
            return []
        with self.lock:
            entries = self.data.get(key, [])
            return [e for e in entries if not e.tombstone]

    def local_delete(self, key: str, vector_clock: VectorClock) -> bool:
        """Mark a key as deleted via tombstone."""
        if not self.alive:
            return False
        tombstone = KVEntry(
            key=key,
            value=None,
            vector_clock=vector_clock,
            tombstone=True,
        )
        return self.local_put(tombstone)

    def store_hint(self, hint: HintedEntry) -> None:
        with self.lock:
            self.hints.append(hint)

    def drain_hints(self, target_node_id: str) -> list[HintedEntry]:
        """Remove and return all hints for a given target node."""
        with self.lock:
            to_send = [h for h in self.hints if h.target_node_id == target_node_id]
            self.hints = [h for h in self.hints if h.target_node_id != target_node_id]
            return to_send

    def __repr__(self) -> str:
        status = "UP" if self.alive else "DOWN"
        return f"StorageNode({self.node_id}, {status}, keys={len(self.data)})"


# ---------------------------------------------------------------------------
# Consistent Hash Ring
# ---------------------------------------------------------------------------

class ConsistentHashRing:
    """Consistent hash ring with virtual nodes."""

    def __init__(self, virtual_nodes: int = 64) -> None:
        self.virtual_nodes = virtual_nodes
        self.ring: list[int] = []  # sorted list of hashes
        self.ring_map: dict[int, str] = {}  # hash -> node_id
        self.nodes: set[str] = set()

    @staticmethod
    def _hash(key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)

    def add_node(self, node_id: str) -> None:
        self.nodes.add(node_id)
        for i in range(self.virtual_nodes):
            vnode_key = f"{node_id}#VN{i}"
            h = self._hash(vnode_key)
            if h not in self.ring_map:
                insort(self.ring, h)
                self.ring_map[h] = node_id

    def remove_node(self, node_id: str) -> None:
        self.nodes.discard(node_id)
        to_remove = [h for h, nid in self.ring_map.items() if nid == node_id]
        for h in to_remove:
            self.ring.remove(h)
            del self.ring_map[h]

    def get_node(self, key: str) -> Optional[str]:
        """Find the primary node responsible for a key."""
        if not self.ring:
            return None
        h = self._hash(key)
        idx = bisect_right(self.ring, h) % len(self.ring)
        return self.ring_map[self.ring[idx]]

    def get_preference_list(self, key: str, n: int) -> list[str]:
        """Return an ordered list of N distinct physical nodes for a key.

        Walks clockwise from the key's position, skipping duplicate physical
        nodes (from virtual node overlap).
        """
        if not self.ring:
            return []
        h = self._hash(key)
        idx = bisect_right(self.ring, h) % len(self.ring)
        result: list[str] = []
        seen: set[str] = set()
        for offset in range(len(self.ring)):
            pos = (idx + offset) % len(self.ring)
            node_id = self.ring_map[self.ring[pos]]
            if node_id not in seen:
                seen.add(node_id)
                result.append(node_id)
                if len(result) == n:
                    break
        return result

    def __repr__(self) -> str:
        return f"ConsistentHashRing(nodes={sorted(self.nodes)}, vnodes={len(self.ring)})"


# ---------------------------------------------------------------------------
# Gossip Protocol
# ---------------------------------------------------------------------------

class GossipProtocol:
    """Simple gossip-based failure detector."""

    SUSPECT_TIMEOUT = 5.0  # seconds without heartbeat -> suspected
    DEAD_TIMEOUT = 15.0    # seconds suspected -> dead

    def __init__(self) -> None:
        self.states: dict[str, GossipState] = {}
        self.lock = threading.Lock()

    def register_node(self, node_id: str) -> None:
        with self.lock:
            self.states[node_id] = GossipState()

    def heartbeat(self, node_id: str) -> None:
        with self.lock:
            if node_id in self.states:
                self.states[node_id].heartbeat += 1
                self.states[node_id].timestamp = time.time()
                self.states[node_id].status = NodeStatus.ALIVE

    def check_health(self) -> dict[str, NodeStatus]:
        """Evaluate node health based on heartbeat freshness."""
        now = time.time()
        with self.lock:
            for node_id, state in self.states.items():
                elapsed = now - state.timestamp
                if state.status == NodeStatus.DEAD:
                    continue
                if elapsed > self.DEAD_TIMEOUT:
                    state.status = NodeStatus.DEAD
                elif elapsed > self.SUSPECT_TIMEOUT:
                    state.status = NodeStatus.SUSPECTED
            return {nid: s.status for nid, s in self.states.items()}

    def gossip_exchange(self, sender_id: str, receiver_id: str) -> None:
        """Simulate a gossip round between two nodes."""
        with self.lock:
            if sender_id in self.states and receiver_id in self.states:
                # Receiver learns sender is alive
                sender_state = self.states[sender_id]
                receiver_state = self.states[receiver_id]
                # Merge: each learns the other is alive
                sender_state.timestamp = max(sender_state.timestamp, time.time())
                receiver_state.timestamp = max(receiver_state.timestamp, time.time())

    def mark_alive(self, node_id: str) -> None:
        with self.lock:
            if node_id in self.states:
                self.states[node_id].status = NodeStatus.ALIVE
                self.states[node_id].timestamp = time.time()

    def get_status(self, node_id: str) -> NodeStatus:
        with self.lock:
            if node_id in self.states:
                return self.states[node_id].status
            return NodeStatus.DEAD


# ---------------------------------------------------------------------------
# Distributed KV Store (Coordinator)
# ---------------------------------------------------------------------------

class DistributedKVStore:
    """Dynamo-like distributed key-value store coordinator.

    Args:
        num_nodes: Number of storage nodes in the cluster.
        replication_factor: N -- number of replicas per key.
        read_quorum: R -- number of nodes that must respond for a read.
        write_quorum: W -- number of nodes that must acknowledge a write.
        virtual_nodes: Number of virtual nodes per physical node on the ring.
    """

    def __init__(
        self,
        num_nodes: int = 5,
        replication_factor: int = 3,
        read_quorum: int = 2,
        write_quorum: int = 2,
        virtual_nodes: int = 64,
    ) -> None:
        self.n = replication_factor
        self.r = read_quorum
        self.w = write_quorum

        # Build storage nodes
        self.nodes: dict[str, StorageNode] = {}
        self.ring = ConsistentHashRing(virtual_nodes=virtual_nodes)
        self.gossip = GossipProtocol()

        for i in range(num_nodes):
            node_id = f"node_{i}"
            self.nodes[node_id] = StorageNode(node_id)
            self.ring.add_node(node_id)
            self.gossip.register_node(node_id)
            self.gossip.heartbeat(node_id)

    # -- Public API ---------------------------------------------------------

    def put(self, key: str, value: Any, context: Optional[VectorClock] = None) -> VectorClock:
        """Write a key-value pair with quorum replication.

        Args:
            key: The key to store.
            value: The value to associate with the key.
            context: Optional vector clock from a previous read (for updates).

        Returns:
            The new vector clock after the write.

        Raises:
            RuntimeError: If the write quorum cannot be met.
        """
        pref_list = self._get_healthy_preference_list(key)
        if len(pref_list) < self.w:
            raise RuntimeError(
                f"Cannot meet write quorum: need {self.w}, "
                f"only {len(pref_list)} healthy nodes available"
            )

        # Determine coordinator (first healthy node in preference list)
        coordinator_id = pref_list[0]

        # Build vector clock
        vc = context.copy() if context else VectorClock()
        vc.increment(coordinator_id)

        entry = KVEntry(key=key, value=value, vector_clock=vc)

        # Replicate to N nodes, need W acks
        acks = 0
        target_nodes = pref_list[: self.n]
        full_pref = self._get_healthy_preference_list(key)

        for node_id in target_nodes:
            node = self.nodes[node_id]
            if node.alive and node.local_put(entry):
                acks += 1
            else:
                # Hinted handoff: find a substitute node
                self._hinted_handoff(node_id, entry, full_pref, target_nodes)
                acks += 1  # count the hint as an ack for availability

        if acks < self.w:
            raise RuntimeError(
                f"Write quorum not met: got {acks} acks, need {self.w}"
            )

        return vc

    def get(self, key: str) -> list[tuple[Any, VectorClock]]:
        """Read a key with quorum, returning all versions.

        Returns:
            List of (value, vector_clock) tuples. Multiple entries indicate
            conflicting concurrent writes (siblings).

        Raises:
            RuntimeError: If the read quorum cannot be met.
        """
        pref_list = self._get_healthy_preference_list(key)
        if len(pref_list) < self.r:
            raise RuntimeError(
                f"Cannot meet read quorum: need {self.r}, "
                f"only {len(pref_list)} healthy nodes available"
            )

        target_nodes = pref_list[: self.n]
        all_entries: list[KVEntry] = []
        responses = 0

        for node_id in target_nodes:
            node = self.nodes[node_id]
            if node.alive:
                entries = node.local_get(key)
                all_entries.extend(entries)
                responses += 1
                if responses >= self.r:
                    break

        if responses < self.r:
            raise RuntimeError(
                f"Read quorum not met: got {responses} responses, need {self.r}"
            )

        # Reconcile: remove entries that are dominated by newer versions
        reconciled = self._reconcile(all_entries)

        # Trigger read repair in background
        self._read_repair(key, reconciled, target_nodes)

        return [(e.value, e.vector_clock) for e in reconciled]

    def delete(self, key: str, context: Optional[VectorClock] = None) -> VectorClock:
        """Delete a key by writing a tombstone.

        Args:
            key: The key to delete.
            context: Vector clock from a previous read.

        Returns:
            The vector clock of the tombstone.
        """
        pref_list = self._get_healthy_preference_list(key)
        if len(pref_list) < self.w:
            raise RuntimeError(
                f"Cannot meet write quorum: need {self.w}, "
                f"only {len(pref_list)} healthy nodes available"
            )

        coordinator_id = pref_list[0]
        vc = context.copy() if context else VectorClock()
        vc.increment(coordinator_id)

        target_nodes = pref_list[: self.n]
        acks = 0
        for node_id in target_nodes:
            node = self.nodes[node_id]
            if node.alive and node.local_delete(key, vc):
                acks += 1

        if acks < self.w:
            raise RuntimeError(
                f"Delete quorum not met: got {acks} acks, need {self.w}"
            )
        return vc

    # -- Node management ----------------------------------------------------

    def node_down(self, node_id: str) -> None:
        """Simulate a node going offline."""
        if node_id in self.nodes:
            self.nodes[node_id].alive = False

    def node_up(self, node_id: str) -> None:
        """Simulate a node coming back online and replay hinted handoffs."""
        if node_id in self.nodes:
            self.nodes[node_id].alive = True
            self.gossip.mark_alive(node_id)
            self._replay_hints(node_id)

    def add_node(self, node_id: str) -> None:
        """Add a new node to the cluster."""
        self.nodes[node_id] = StorageNode(node_id)
        self.ring.add_node(node_id)
        self.gossip.register_node(node_id)
        self.gossip.heartbeat(node_id)

    def remove_node(self, node_id: str) -> None:
        """Remove a node from the cluster."""
        self.ring.remove_node(node_id)
        if node_id in self.nodes:
            del self.nodes[node_id]

    def cluster_status(self) -> dict[str, str]:
        """Return the health status of all nodes."""
        health = self.gossip.check_health()
        result = {}
        for node_id, node in self.nodes.items():
            if not node.alive:
                result[node_id] = "DOWN"
            else:
                result[node_id] = health.get(node_id, NodeStatus.DEAD).value
        return result

    # -- Internal helpers ---------------------------------------------------

    def _get_healthy_preference_list(self, key: str) -> list[str]:
        """Get preference list, filtering to only alive nodes."""
        full_list = self.ring.get_preference_list(key, len(self.nodes))
        return [nid for nid in full_list if self.nodes[nid].alive]

    def _hinted_handoff(
        self,
        target_id: str,
        entry: KVEntry,
        full_pref: list[str],
        already_used: list[str],
    ) -> None:
        """Store a hint on a substitute node for later delivery."""
        for candidate_id in full_pref:
            if candidate_id not in already_used and self.nodes[candidate_id].alive:
                hint = HintedEntry(target_node_id=target_id, entry=entry)
                self.nodes[candidate_id].store_hint(hint)
                return

    def _replay_hints(self, recovered_node_id: str) -> None:
        """Replay hinted handoff data to a recovered node."""
        target_node = self.nodes[recovered_node_id]
        for node in self.nodes.values():
            if node.node_id == recovered_node_id:
                continue
            hints = node.drain_hints(recovered_node_id)
            for hint in hints:
                target_node.local_put(hint.entry)

    def _reconcile(self, entries: list[KVEntry]) -> list[KVEntry]:
        """Remove entries dominated by newer versions, keep concurrent siblings."""
        if not entries:
            return []
        result: list[KVEntry] = []
        for candidate in entries:
            dominated = False
            new_result: list[KVEntry] = []
            for existing in result:
                rel = candidate.vector_clock.compare(existing.vector_clock)
                if rel == "BEFORE":
                    dominated = True
                    new_result.append(existing)
                elif rel == "AFTER":
                    # candidate supersedes existing; drop existing
                    continue
                elif rel == "EQUAL":
                    dominated = True
                    new_result.append(existing)
                else:
                    # concurrent -- keep both
                    new_result.append(existing)
            if not dominated:
                new_result.append(candidate)
            result = new_result
        return result

    def _read_repair(self, key: str, latest: list[KVEntry], target_nodes: list[str]) -> None:
        """Push the latest version(s) to replicas that returned stale data."""
        for entry in latest:
            for node_id in target_nodes:
                node = self.nodes[node_id]
                if node.alive:
                    node.local_put(entry)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _print_section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _print_step(step: str) -> None:
    print(f"\n--- {step} ---")


def demo() -> None:
    """Run a comprehensive demonstration of the distributed KV store."""

    _print_section("Distributed Key-Value Store Demo")

    # ---- Setup ----
    _print_step("1. Creating cluster with 5 nodes (N=3, R=2, W=2)")
    store = DistributedKVStore(
        num_nodes=5,
        replication_factor=3,
        read_quorum=2,
        write_quorum=2,
    )
    print(f"Cluster created: {len(store.nodes)} nodes")
    print(f"Ring: {store.ring}")
    print(f"Quorum config: N={store.n}, R={store.r}, W={store.w}")

    # ---- Basic put/get ----
    _print_step("2. Basic put/get operations")
    vc1 = store.put("user:1001", {"name": "Alice", "email": "alice@example.com"})
    print(f"PUT user:1001 -> version: {vc1}")

    vc2 = store.put("user:1002", {"name": "Bob", "email": "bob@example.com"})
    print(f"PUT user:1002 -> version: {vc2}")

    result = store.get("user:1001")
    print(f"GET user:1001 -> {len(result)} version(s):")
    for val, vc in result:
        print(f"  value={val}, version={vc}")

    # ---- Update with context ----
    _print_step("3. Update with vector clock context")
    read_result = store.get("user:1001")
    _, read_vc = read_result[0]
    updated_vc = store.put(
        "user:1001",
        {"name": "Alice", "email": "alice_new@example.com"},
        context=read_vc,
    )
    print(f"Updated user:1001 -> version: {updated_vc}")
    result = store.get("user:1001")
    for val, vc in result:
        print(f"  value={val}, version={vc}")

    # ---- Consistent hashing ----
    _print_step("4. Consistent hashing - key distribution")
    keys = [f"product:{i}" for i in range(20)]
    node_counts: dict[str, int] = defaultdict(int)
    for k in keys:
        primary = store.ring.get_node(k)
        if primary:
            node_counts[primary] += 1
    print("Key distribution across nodes (primary only):")
    for node_id in sorted(node_counts):
        bar = "#" * node_counts[node_id]
        print(f"  {node_id}: {node_counts[node_id]:2d} keys {bar}")

    # ---- Preference list (replicas) ----
    _print_step("5. Replication - preference list for a key")
    pref = store.ring.get_preference_list("user:1001", store.n)
    print(f"Preference list for 'user:1001' (N={store.n}): {pref}")

    # ---- Node failure + hinted handoff ----
    _print_step("6. Node failure and hinted handoff")
    fail_node = pref[0]
    print(f"Taking down {fail_node} (primary for user:1001)")
    store.node_down(fail_node)
    print(f"Cluster status: {store.cluster_status()}")

    vc_during_failure = store.put(
        "user:1001",
        {"name": "Alice", "email": "alice_failover@example.com"},
        context=updated_vc,
    )
    print(f"PUT during failure -> version: {vc_during_failure}")

    result = store.get("user:1001")
    print(f"GET during failure -> {len(result)} version(s)")
    for val, vc in result:
        print(f"  value={val}, version={vc}")

    # ---- Node recovery ----
    _print_step("7. Node recovery and hint replay")
    print(f"Bringing {fail_node} back up (with hinted handoff replay)")
    store.node_up(fail_node)
    print(f"Cluster status: {store.cluster_status()}")

    result = store.get("user:1001")
    print(f"GET after recovery -> {len(result)} version(s)")
    for val, vc in result:
        print(f"  value={val}, version={vc}")

    # ---- Conflict detection (concurrent writes) ----
    _print_step("8. Conflict detection with vector clocks")

    base_vc = store.put("config:theme", "dark")
    print(f"Initial write: config:theme='dark' -> {base_vc}")

    # Simulate concurrent writes from two different coordinators
    vc_a = base_vc.copy()
    vc_a.increment("node_0")
    entry_a = KVEntry(key="config:theme", value="blue", vector_clock=vc_a)

    vc_b = base_vc.copy()
    vc_b.increment("node_1")
    entry_b = KVEntry(key="config:theme", value="green", vector_clock=vc_b)

    # Write both directly to overlapping replicas to create a conflict
    pref_config = store.ring.get_preference_list("config:theme", store.n)
    for nid in pref_config:
        store.nodes[nid].local_put(entry_a)
    for nid in pref_config:
        store.nodes[nid].local_put(entry_b)

    result = store.get("config:theme")
    print(f"After concurrent writes -> {len(result)} version(s) (siblings):")
    for val, vc in result:
        print(f"  value='{val}', version={vc}")

    if len(result) > 1:
        print("Conflict detected! Resolving with merge...")
        merged_vc = result[0][1].merge(result[1][1])
        resolved_vc = store.put("config:theme", "blue-green", context=merged_vc)
        result = store.get("config:theme")
        print(f"After resolution -> {len(result)} version(s):")
        for val, vc in result:
            print(f"  value='{val}', version={vc}")

    # ---- Delete ----
    _print_step("9. Delete operation (tombstone)")
    del_vc = store.put("temp:session", "abc123")
    print(f"PUT temp:session -> {del_vc}")
    result = store.get("temp:session")
    print(f"GET temp:session -> {result[0][0]}")

    store.delete("temp:session", context=del_vc)
    result = store.get("temp:session")
    print(f"GET after delete -> {len(result)} version(s) (empty = deleted)")

    # ---- Vector clock comparison demo ----
    _print_step("10. Vector clock comparison examples")
    vc_x = VectorClock({"A": 2, "B": 1})
    vc_y = VectorClock({"A": 3, "B": 1})
    print(f"{vc_x} vs {vc_y} -> {vc_x.compare(vc_y)}")

    vc_x = VectorClock({"A": 2, "B": 1})
    vc_y = VectorClock({"A": 1, "B": 2})
    print(f"{vc_x} vs {vc_y} -> {vc_x.compare(vc_y)}")

    vc_x = VectorClock({"A": 2, "B": 3})
    vc_y = VectorClock({"A": 2, "B": 3})
    print(f"{vc_x} vs {vc_y} -> {vc_x.compare(vc_y)}")

    vc_x = VectorClock({"A": 3, "B": 2})
    vc_y = VectorClock({"A": 2, "B": 1})
    print(f"{vc_x} vs {vc_y} -> {vc_x.compare(vc_y)}")

    # ---- Gossip protocol ----
    _print_step("11. Gossip protocol simulation")
    gossip = store.gossip
    for nid in list(store.nodes.keys())[:3]:
        gossip.heartbeat(nid)

    health = gossip.check_health()
    print("Node health after selective heartbeats:")
    for nid in sorted(health):
        print(f"  {nid}: {health[nid].value}")

    # ---- Cluster scaling ----
    _print_step("12. Adding a new node to the cluster")
    store.add_node("node_5")
    print(f"Added node_5. Cluster size: {len(store.nodes)}")
    print(f"Ring now has {len(store.ring.ring)} virtual nodes")

    new_pref = store.ring.get_preference_list("user:1001", store.n)
    print(f"Preference list for user:1001 after scaling: {new_pref}")

    # ---- Quorum failure scenario ----
    _print_step("13. Quorum failure scenario")
    print("Taking down 5 of 6 nodes to break quorum (R=2)...")
    store.node_down("node_0")
    store.node_down("node_1")
    store.node_down("node_2")
    store.node_down("node_3")
    store.node_down("node_4")
    print(f"Cluster status: {store.cluster_status()}")

    try:
        store.get("user:1001")
        print("GET succeeded (unexpected)")
    except RuntimeError as e:
        print(f"GET failed as expected: {e}")

    # Restore nodes
    for i in range(5):
        store.node_up(f"node_{i}")
    print("All nodes restored.")

    # ---- Summary ----
    _print_section("Demo Complete")
    print("Features demonstrated:")
    print("  * Consistent hashing with virtual nodes")
    print("  * Quorum-based reads and writes (N=3, R=2, W=2)")
    print("  * Vector clocks for conflict detection")
    print("  * Hinted handoff during node failures")
    print("  * Node recovery with hint replay")
    print("  * Concurrent write conflict detection and resolution")
    print("  * Delete via tombstone")
    print("  * Gossip-based health monitoring")
    print("  * Dynamic cluster scaling")
    print("  * Quorum failure handling")


if __name__ == "__main__":
    demo()
