"""
Chat System (WhatsApp / Messenger) -- Simulation

A working simulation of a real-time chat system demonstrating:
- User management with online/offline presence tracking
- 1:1 and group chat rooms
- Message sending, receiving, and delivery status tracking (sent -> delivered -> read)
- Offline message queuing and sync on reconnect
- Media message support (images, videos, files)
- Typing indicators

Architecture (simulated):
    ChatService    -- Central coordinator (simulates server)
    ChatRoom       -- Conversation (direct or group)
    User           -- Client with inbox and presence
    Message        -- Content with delivery status tracking
    MessageStatus  -- Per-recipient delivery state machine: sent -> delivered -> read

Time Complexity:
    - Send message: O(M) where M = number of members in conversation
    - Deliver to user: O(1) per message
    - Fetch history: O(K) where K = number of messages requested

Space Complexity:
    - Messages stored per conversation: O(N) where N = total messages
    - User inbox (offline queue): O(P) where P = pending messages
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class DeliveryStatus(Enum):
    """Message delivery state machine: SENT -> DELIVERED -> READ."""
    SENT = "sent"
    DELIVERED = "delivered"
    READ = "read"


class UserStatus(Enum):
    """User presence states."""
    ONLINE = "online"
    OFFLINE = "offline"
    AWAY = "away"


class ContentType(Enum):
    """Supported message content types."""
    TEXT = "text"
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    FILE = "file"


class RoomType(Enum):
    """Conversation types."""
    DIRECT = "direct"
    GROUP = "group"


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

@dataclass
class MessageStatus:
    """Tracks delivery status of a message for a specific recipient."""
    user_id: str
    status: DeliveryStatus = DeliveryStatus.SENT
    updated_at: float = field(default_factory=time.time)

    def advance_to(self, new_status: DeliveryStatus) -> bool:
        """Advance status if the new status is later in the state machine."""
        order = {DeliveryStatus.SENT: 0, DeliveryStatus.DELIVERED: 1, DeliveryStatus.READ: 2}
        if order[new_status] > order[self.status]:
            self.status = new_status
            self.updated_at = time.time()
            return True
        return False


@dataclass
class Message:
    """A single chat message with per-recipient delivery tracking."""
    message_id: str
    conversation_id: str
    sender_id: str
    content: str
    content_type: ContentType = ContentType.TEXT
    media_url: Optional[str] = None
    client_msg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=time.time)
    seq_no: int = 0
    # Per-recipient delivery status
    status_map: dict[str, MessageStatus] = field(default_factory=dict)

    def set_status_for(self, user_id: str, status: DeliveryStatus) -> None:
        if user_id not in self.status_map:
            self.status_map[user_id] = MessageStatus(user_id=user_id, status=status)
        else:
            self.status_map[user_id].advance_to(status)

    def get_status_for(self, user_id: str) -> DeliveryStatus:
        if user_id in self.status_map:
            return self.status_map[user_id].status
        return DeliveryStatus.SENT

    def get_aggregate_status(self) -> DeliveryStatus:
        """Return the minimum status across all recipients (for sender display)."""
        if not self.status_map:
            return DeliveryStatus.SENT
        order = {DeliveryStatus.SENT: 0, DeliveryStatus.DELIVERED: 1, DeliveryStatus.READ: 2}
        min_status = DeliveryStatus.READ
        for ms in self.status_map.values():
            if order[ms.status] < order[min_status]:
                min_status = ms.status
        return min_status

    def __repr__(self) -> str:
        status = self.get_aggregate_status().value
        type_tag = "" if self.content_type == ContentType.TEXT else f" [{self.content_type.value}]"
        return f"Message(seq={self.seq_no}, from={self.sender_id}, status={status}{type_tag}): {self.content}"


@dataclass
class User:
    """A chat user with presence tracking and offline message queue."""
    user_id: str
    display_name: str
    status: UserStatus = UserStatus.OFFLINE
    last_seen: float = field(default_factory=time.time)
    # Offline inbox -- messages queued while user was offline
    offline_inbox: list[Message] = field(default_factory=list)
    # Delivered inbox -- messages received while online
    received_messages: list[Message] = field(default_factory=list)
    # Typing state per conversation
    typing_in: Optional[str] = None

    def go_online(self) -> None:
        self.status = UserStatus.ONLINE
        self.last_seen = time.time()

    def go_offline(self) -> None:
        self.status = UserStatus.OFFLINE
        self.last_seen = time.time()
        self.typing_in = None

    def is_online(self) -> bool:
        return self.status == UserStatus.ONLINE

    def receive_message(self, message: Message) -> None:
        """Receive a message -- queue offline or deliver immediately."""
        if self.is_online():
            self.received_messages.append(message)
        else:
            self.offline_inbox.append(message)

    def sync_offline_messages(self) -> list[Message]:
        """Drain offline inbox when user comes back online."""
        messages = list(self.offline_inbox)
        self.received_messages.extend(messages)
        self.offline_inbox.clear()
        return messages

    def __repr__(self) -> str:
        return (
            f"User({self.display_name}, {self.status.value}, "
            f"inbox={len(self.received_messages)}, offline_queue={len(self.offline_inbox)})"
        )


class ChatRoom:
    """
    A conversation (direct or group) with message history and member management.

    Maintains an ordered message log with monotonically increasing sequence numbers.
    """

    def __init__(self, room_id: str, room_type: RoomType, name: Optional[str] = None):
        self.room_id = room_id
        self.room_type = room_type
        self.name = name or room_id
        self.members: dict[str, str] = {}  # user_id -> role ('admin' | 'member')
        self.messages: list[Message] = []
        self._seq_counter: int = 0
        self.created_at: float = time.time()

    def add_member(self, user_id: str, role: str = "member") -> None:
        self.members[user_id] = role

    def remove_member(self, user_id: str) -> bool:
        if user_id in self.members:
            del self.members[user_id]
            return True
        return False

    def is_member(self, user_id: str) -> bool:
        return user_id in self.members

    def next_seq(self) -> int:
        self._seq_counter += 1
        return self._seq_counter

    def add_message(self, message: Message) -> None:
        message.seq_no = self.next_seq()
        self.messages.append(message)

    def get_history(self, limit: int = 50, before_seq: Optional[int] = None) -> list[Message]:
        """Fetch message history with pagination."""
        msgs = self.messages
        if before_seq is not None:
            msgs = [m for m in msgs if m.seq_no < before_seq]
        return msgs[-limit:]

    def get_member_ids(self) -> list[str]:
        return list(self.members.keys())

    def __repr__(self) -> str:
        return (
            f"ChatRoom({self.name}, type={self.room_type.value}, "
            f"members={len(self.members)}, messages={len(self.messages)})"
        )


class ChatService:
    """
    Central chat service coordinating users, conversations, and message delivery.

    Simulates the server-side components:
    - User registry and presence management
    - Conversation creation and management
    - Message routing with delivery status tracking
    - Offline message queuing
    - Typing indicators
    """

    def __init__(self):
        self.users: dict[str, User] = {}
        self.rooms: dict[str, ChatRoom] = {}
        # Index: user_id -> set of room_ids (for fast lookup)
        self._user_rooms: dict[str, set[str]] = {}
        # Deduplication: client_msg_id -> message_id
        self._seen_client_ids: dict[str, str] = {}
        # Stats
        self.total_messages_sent: int = 0
        self.total_messages_delivered: int = 0

    # -- User Management --

    def register_user(self, user_id: str, display_name: str) -> User:
        user = User(user_id=user_id, display_name=display_name)
        self.users[user_id] = user
        self._user_rooms[user_id] = set()
        return user

    def get_user(self, user_id: str) -> Optional[User]:
        return self.users.get(user_id)

    def set_user_online(self, user_id: str) -> list[Message]:
        """Bring user online and deliver any queued offline messages."""
        user = self.users.get(user_id)
        if not user:
            return []
        user.go_online()
        # Sync offline messages
        synced = user.sync_offline_messages()
        # Mark synced messages as delivered
        for msg in synced:
            msg.set_status_for(user_id, DeliveryStatus.DELIVERED)
            self.total_messages_delivered += 1
        return synced

    def set_user_offline(self, user_id: str) -> None:
        user = self.users.get(user_id)
        if user:
            user.go_offline()

    def get_user_presence(self, user_id: str) -> tuple[UserStatus, float]:
        user = self.users.get(user_id)
        if user:
            return user.status, user.last_seen
        return UserStatus.OFFLINE, 0.0

    # -- Conversation Management --

    def create_direct_chat(self, user_id_1: str, user_id_2: str) -> ChatRoom:
        """Create a 1:1 direct conversation between two users."""
        room_id = f"dm:{min(user_id_1, user_id_2)}:{max(user_id_1, user_id_2)}"
        if room_id in self.rooms:
            return self.rooms[room_id]

        name_1 = self.users[user_id_1].display_name
        name_2 = self.users[user_id_2].display_name
        room = ChatRoom(room_id=room_id, room_type=RoomType.DIRECT, name=f"{name_1} <-> {name_2}")
        room.add_member(user_id_1, "member")
        room.add_member(user_id_2, "member")
        self.rooms[room_id] = room
        self._user_rooms[user_id_1].add(room_id)
        self._user_rooms[user_id_2].add(room_id)
        return room

    def create_group_chat(self, creator_id: str, name: str, member_ids: list[str]) -> ChatRoom:
        """Create a group conversation."""
        room_id = f"grp:{uuid.uuid4().hex[:8]}"
        room = ChatRoom(room_id=room_id, room_type=RoomType.GROUP, name=name)
        room.add_member(creator_id, "admin")
        self._user_rooms[creator_id].add(room_id)

        for uid in member_ids:
            if uid != creator_id and uid in self.users:
                room.add_member(uid, "member")
                self._user_rooms[uid].add(room_id)

        self.rooms[room_id] = room
        return room

    def add_group_member(self, room_id: str, user_id: str) -> bool:
        room = self.rooms.get(room_id)
        if not room or room.room_type != RoomType.GROUP:
            return False
        if user_id not in self.users:
            return False
        room.add_member(user_id, "member")
        self._user_rooms[user_id].add(room_id)
        return True

    # -- Message Sending --

    def send_message(
        self,
        sender_id: str,
        room_id: str,
        content: str,
        content_type: ContentType = ContentType.TEXT,
        media_url: Optional[str] = None,
        client_msg_id: Optional[str] = None,
    ) -> Optional[Message]:
        """
        Send a message to a conversation.

        Flow:
        1. Validate sender and room membership
        2. Check for duplicate (idempotent delivery via client_msg_id)
        3. Create message with unique ID and sequence number
        4. Persist to conversation history
        5. Fan out to all recipients (deliver or queue)
        6. Return message with delivery status
        """
        room = self.rooms.get(room_id)
        if not room or not room.is_member(sender_id):
            return None

        # Idempotency check
        cmid = client_msg_id or str(uuid.uuid4())
        if cmid in self._seen_client_ids:
            # Return existing message (duplicate send)
            existing_id = self._seen_client_ids[cmid]
            for msg in reversed(room.messages):
                if msg.message_id == existing_id:
                    return msg
            return None

        # Create message
        message = Message(
            message_id=str(uuid.uuid4()),
            conversation_id=room_id,
            sender_id=sender_id,
            content=content,
            content_type=content_type,
            media_url=media_url,
            client_msg_id=cmid,
        )

        # Persist
        room.add_message(message)
        self._seen_client_ids[cmid] = message.message_id
        self.total_messages_sent += 1

        # Fan-out to recipients
        recipients = [uid for uid in room.get_member_ids() if uid != sender_id]
        for recipient_id in recipients:
            user = self.users.get(recipient_id)
            if not user:
                continue

            # Initialize status tracking for this recipient
            message.set_status_for(recipient_id, DeliveryStatus.SENT)

            # Deliver or queue
            if user.is_online():
                user.receive_message(message)
                message.set_status_for(recipient_id, DeliveryStatus.DELIVERED)
                self.total_messages_delivered += 1
            else:
                user.receive_message(message)  # queues in offline_inbox

        return message

    # -- Read Receipts --

    def mark_as_read(self, user_id: str, room_id: str) -> int:
        """Mark all messages in a conversation as read by this user."""
        room = self.rooms.get(room_id)
        if not room or not room.is_member(user_id):
            return 0

        count = 0
        for msg in room.messages:
            if msg.sender_id != user_id:
                old = msg.get_status_for(user_id)
                msg.set_status_for(user_id, DeliveryStatus.READ)
                if msg.get_status_for(user_id) != old:
                    count += 1
        return count

    # -- Typing Indicators --

    def set_typing(self, user_id: str, room_id: str, is_typing: bool) -> list[str]:
        """Set typing indicator; returns list of online members who should be notified."""
        user = self.users.get(user_id)
        room = self.rooms.get(room_id)
        if not user or not room or not room.is_member(user_id):
            return []

        user.typing_in = room_id if is_typing else None

        # Return online members to notify (except the typer)
        notified = []
        for uid in room.get_member_ids():
            if uid != user_id:
                other = self.users.get(uid)
                if other and other.is_online():
                    notified.append(uid)
        return notified

    # -- History --

    def get_message_history(
        self, room_id: str, limit: int = 50, before_seq: Optional[int] = None
    ) -> list[Message]:
        room = self.rooms.get(room_id)
        if not room:
            return []
        return room.get_history(limit=limit, before_seq=before_seq)

    def get_user_conversations(self, user_id: str) -> list[ChatRoom]:
        room_ids = self._user_rooms.get(user_id, set())
        return [self.rooms[rid] for rid in room_ids if rid in self.rooms]

    # -- Stats --

    def get_stats(self) -> dict:
        return {
            "total_users": len(self.users),
            "total_rooms": len(self.rooms),
            "total_messages_sent": self.total_messages_sent,
            "total_messages_delivered": self.total_messages_delivered,
            "online_users": sum(1 for u in self.users.values() if u.is_online()),
        }

    def __repr__(self) -> str:
        stats = self.get_stats()
        return (
            f"ChatService(users={stats['total_users']}, rooms={stats['total_rooms']}, "
            f"msgs={stats['total_messages_sent']}, online={stats['online_users']})"
        )


# ===========================================================================
# Demo / Simulation
# ===========================================================================

def print_header(text: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}")


def print_section(text: str) -> None:
    print(f"\n--- {text} ---")


def run_demo() -> None:
    """Run a full simulation of the chat system."""

    service = ChatService()

    # ------------------------------------------------------------------
    print_header("CHAT SYSTEM SIMULATION")
    # ------------------------------------------------------------------

    # 1. Register users
    print_section("1. Registering Users")
    alice = service.register_user("alice", "Alice")
    bob = service.register_user("bob", "Bob")
    charlie = service.register_user("charlie", "Charlie")
    diana = service.register_user("diana", "Diana")
    print(f"  Registered: {alice.display_name}, {bob.display_name}, "
          f"{charlie.display_name}, {diana.display_name}")

    # 2. Bring users online
    print_section("2. Users Coming Online")
    service.set_user_online("alice")
    service.set_user_online("bob")
    service.set_user_online("charlie")
    # Diana stays offline
    for uid in ["alice", "bob", "charlie", "diana"]:
        status, _ = service.get_user_presence(uid)
        print(f"  {uid}: {status.value}")

    # 3. Create direct chat
    print_section("3. Creating Direct Chat (Alice <-> Bob)")
    dm = service.create_direct_chat("alice", "bob")
    print(f"  Created: {dm}")

    # 4. Send messages in direct chat
    print_section("4. Alice Sends Messages to Bob")
    msg1 = service.send_message("alice", dm.room_id, "Hey Bob! How are you?")
    msg2 = service.send_message("alice", dm.room_id, "Want to grab lunch?")
    if msg1:
        print(f"  {msg1}")
        print(f"    -> Status for bob: {msg1.get_status_for('bob').value}")
    if msg2:
        print(f"  {msg2}")

    # 5. Bob replies
    print_section("5. Bob Replies")
    msg3 = service.send_message("bob", dm.room_id, "Hey Alice! Doing great!")
    msg4 = service.send_message("bob", dm.room_id, "Sure, lunch sounds good!")
    if msg3:
        print(f"  {msg3}")
    if msg4:
        print(f"  {msg4}")

    # 6. Read receipts
    print_section("6. Bob Reads Alice's Messages")
    read_count = service.mark_as_read("bob", dm.room_id)
    print(f"  Bob marked {read_count} message(s) as read")
    if msg1:
        print(f"  msg1 status for bob: {msg1.get_status_for('bob').value}")
        print(f"  msg1 aggregate status: {msg1.get_aggregate_status().value}")

    # 7. Create group chat
    print_section("7. Creating Group Chat")
    group = service.create_group_chat(
        "alice", "Project Team", ["bob", "charlie", "diana"]
    )
    print(f"  Created: {group}")

    # 8. Group messaging (Diana is offline)
    print_section("8. Group Messages (Diana is OFFLINE)")
    gm1 = service.send_message("alice", group.room_id, "Welcome to the project team!")
    gm2 = service.send_message("bob", group.room_id, "Thanks for adding me!")
    gm3 = service.send_message("charlie", group.room_id, "Excited to be here!")
    if gm1:
        print(f"  {gm1}")
        print(f"    -> bob: {gm1.get_status_for('bob').value}, "
              f"charlie: {gm1.get_status_for('charlie').value}, "
              f"diana: {gm1.get_status_for('diana').value}")
    if gm2:
        print(f"  {gm2}")
    if gm3:
        print(f"  {gm3}")

    # 9. Diana comes online -- receives offline messages
    print_section("9. Diana Comes Online (Offline Sync)")
    print(f"  Diana's offline queue: {len(diana.offline_inbox)} message(s)")
    synced = service.set_user_online("diana")
    print(f"  Synced {len(synced)} message(s) to Diana")
    for msg in synced:
        print(f"    -> seq={msg.seq_no}: '{msg.content}' "
              f"(status: {msg.get_status_for('diana').value})")

    # 10. Media sharing
    print_section("10. Media Sharing")
    media_msg = service.send_message(
        "alice", group.room_id,
        "project-screenshot.png",
        content_type=ContentType.IMAGE,
        media_url="https://storage.example.com/media/abc123.png"
    )
    if media_msg:
        print(f"  {media_msg}")
        print(f"    -> media_url: {media_msg.media_url}")

    # 11. Typing indicators
    print_section("11. Typing Indicators")
    notified = service.set_typing("alice", group.room_id, True)
    print(f"  Alice is typing in '{group.name}'")
    print(f"  Notified users: {notified}")
    service.set_typing("alice", group.room_id, False)
    print(f"  Alice stopped typing")

    # 12. Idempotent delivery test
    print_section("12. Idempotent Delivery (Duplicate Check)")
    dup_client_id = "client-msg-dup-test-001"
    orig = service.send_message("bob", dm.room_id, "Test idempotency", client_msg_id=dup_client_id)
    dup = service.send_message("bob", dm.room_id, "Test idempotency", client_msg_id=dup_client_id)
    if orig and dup:
        print(f"  Original message_id: {orig.message_id}")
        print(f"  Duplicate message_id: {dup.message_id}")
        print(f"  Same message? {orig.message_id == dup.message_id} [OK]")

    # 13. Message history
    print_section("13. Message History (Direct Chat)")
    history = service.get_message_history(dm.room_id, limit=10)
    for msg in history:
        print(f"  seq={msg.seq_no} [{msg.sender_id}]: {msg.content}")

    # 14. User goes offline
    print_section("14. Bob Goes Offline")
    service.set_user_offline("bob")
    status, last_seen = service.get_user_presence("bob")
    print(f"  Bob status: {status.value}")

    # Send message while Bob is offline
    offline_msg = service.send_message("alice", dm.room_id, "Bob, are you there?")
    if offline_msg:
        print(f"  Alice sent: '{offline_msg.content}'")
        print(f"  Status for bob: {offline_msg.get_status_for('bob').value}")
        print(f"  Bob's offline queue: {len(bob.offline_inbox)} message(s)")

    # Bob reconnects
    synced_bob = service.set_user_online("bob")
    print(f"  Bob reconnected -- synced {len(synced_bob)} message(s)")
    if offline_msg:
        print(f"  Status for bob now: {offline_msg.get_status_for('bob').value}")

    # 15. Conversation listing
    print_section("15. Alice's Conversations")
    convos = service.get_user_conversations("alice")
    for room in convos:
        print(f"  * {room.name} ({room.room_type.value}, {len(room.messages)} msgs)")

    # 16. Service stats
    print_section("16. Service Stats")
    stats = service.get_stats()
    for key, val in stats.items():
        print(f"  {key}: {val}")

    # ------------------------------------------------------------------
    print_header("SIMULATION COMPLETE -- ALL FEATURES DEMONSTRATED")
    # ------------------------------------------------------------------
    print(f"\n{service}")


if __name__ == "__main__":
    run_demo()
