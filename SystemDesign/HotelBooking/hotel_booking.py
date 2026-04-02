"""
Hotel Booking System (Booking.com-style)
=========================================

Simulates core booking platform functionality:
- Hotel and room management
- Per-date inventory tracking
- Concurrent booking with optimistic locking (version column)
- Search by location and date range
- Booking state machine (PENDING -> CONFIRMED -> CANCELLED, etc.)

Key patterns demonstrated:
- Optimistic concurrency control for double-booking prevention
- Per-date inventory model (room_inventory tracks availability per date)
- Thread-safe booking under concurrent access

Usage:
    python hotel_booking.py
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Domain Models
# ---------------------------------------------------------------------------

class BookingStatus(Enum):
    """Booking state machine states."""
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    CHECKED_IN = "CHECKED_IN"
    CHECKED_OUT = "CHECKED_OUT"
    CANCELLED = "CANCELLED"


# Valid state transitions
VALID_TRANSITIONS: dict[BookingStatus, list[BookingStatus]] = {
    BookingStatus.PENDING: [BookingStatus.CONFIRMED, BookingStatus.CANCELLED],
    BookingStatus.CONFIRMED: [BookingStatus.CHECKED_IN, BookingStatus.CANCELLED],
    BookingStatus.CHECKED_IN: [BookingStatus.CHECKED_OUT],
    BookingStatus.CHECKED_OUT: [],
    BookingStatus.CANCELLED: [],
}


@dataclass
class Hotel:
    """Represents a hotel property."""
    hotel_id: str
    name: str
    city: str
    star_rating: int
    amenities: list[str] = field(default_factory=list)

    def __repr__(self) -> str:
        return f"Hotel({self.name}, {self.city}, {self.star_rating}-star)"


@dataclass
class RoomType:
    """A category of rooms within a hotel (e.g., Deluxe King, Standard Twin)."""
    room_type_id: str
    hotel_id: str
    name: str
    max_guests: int
    base_price: float
    total_rooms: int

    def __repr__(self) -> str:
        return f"RoomType({self.name}, max_guests={self.max_guests}, ${self.base_price}/night)"


@dataclass
class InventoryRecord:
    """Per-date availability for a room type. Uses version for optimistic locking."""
    room_type_id: str
    date: date
    total_rooms: int
    booked_rooms: int = 0
    price: float = 0.0
    version: int = 0

    @property
    def available(self) -> int:
        return self.total_rooms - self.booked_rooms

    def __repr__(self) -> str:
        return (
            f"Inventory({self.date}: "
            f"{self.available}/{self.total_rooms} avail, v{self.version})"
        )


@dataclass
class Booking:
    """A reservation record."""
    booking_id: str
    hotel_id: str
    room_type_id: str
    guest_name: str
    check_in: date
    check_out: date
    total_price: float
    status: BookingStatus = BookingStatus.PENDING
    created_at: float = field(default_factory=time.time)

    def transition(self, new_status: BookingStatus) -> None:
        """Transition to a new state, enforcing the state machine."""
        if new_status not in VALID_TRANSITIONS[self.status]:
            raise ValueError(
                f"Invalid transition: {self.status.value} -> {new_status.value}"
            )
        self.status = new_status

    def __repr__(self) -> str:
        return (
            f"Booking({self.booking_id[:8]}..., {self.guest_name}, "
            f"{self.check_in}->{self.check_out}, {self.status.value})"
        )


@dataclass
class Review:
    """A guest review for a hotel."""
    review_id: str
    hotel_id: str
    booking_id: str
    guest_name: str
    rating: int  # 1-10
    comment: str


# ---------------------------------------------------------------------------
# Inventory Manager (per-date availability with optimistic locking)
# ---------------------------------------------------------------------------

class InventoryManager:
    """Manages room inventory with optimistic concurrency control.

    Storage layout:
        _inventory[(room_type_id, date)] -> InventoryRecord

    Thread safety is provided by a global lock that simulates the database
    transaction boundary. Optimistic locking is demonstrated via the version
    column: a booking attempt reads the version, then conditionally updates
    only if the version has not changed.
    """

    def __init__(self) -> None:
        self._inventory: dict[tuple[str, date], InventoryRecord] = {}
        self._lock = threading.Lock()

    def initialize(self, room_type: RoomType, start: date, days: int) -> None:
        """Pre-populate inventory records for a date range."""
        for i in range(days):
            d = start + timedelta(days=i)
            key = (room_type.room_type_id, d)
            self._inventory[key] = InventoryRecord(
                room_type_id=room_type.room_type_id,
                date=d,
                total_rooms=room_type.total_rooms,
                booked_rooms=0,
                price=room_type.base_price,
                version=0,
            )

    def check_availability(
        self, room_type_id: str, check_in: date, check_out: date
    ) -> list[InventoryRecord]:
        """Return inventory records for the date range, or empty if any date is unavailable."""
        records: list[InventoryRecord] = []
        current = check_in
        while current < check_out:
            key = (room_type_id, current)
            rec = self._inventory.get(key)
            if rec is None or rec.available <= 0:
                return []
            records.append(rec)
            current += timedelta(days=1)
        return records

    def reserve(
        self, room_type_id: str, check_in: date, check_out: date
    ) -> tuple[bool, str]:
        """Attempt to reserve one room across all dates using optimistic locking.

        Returns (success, message).
        """
        with self._lock:
            # Step 1: Read current inventory + versions
            records = self.check_availability(room_type_id, check_in, check_out)
            if not records:
                return False, "No availability for requested dates"

            # Capture expected versions
            expected_versions = {rec.date: rec.version for rec in records}

            # Step 2: Optimistic update -- verify versions and increment
            for rec in records:
                if rec.version != expected_versions[rec.date]:
                    return False, f"Optimistic lock conflict on {rec.date}"
                if rec.booked_rooms >= rec.total_rooms:
                    return False, f"Room sold out on {rec.date}"

            # Step 3: Apply the reservation
            for rec in records:
                rec.booked_rooms += 1
                rec.version += 1

            return True, "Inventory reserved"

    def release(
        self, room_type_id: str, check_in: date, check_out: date
    ) -> None:
        """Release one room across all dates (used for cancellation)."""
        with self._lock:
            current = check_in
            while current < check_out:
                key = (room_type_id, current)
                rec = self._inventory.get(key)
                if rec and rec.booked_rooms > 0:
                    rec.booked_rooms -= 1
                    rec.version += 1
                current += timedelta(days=1)

    def get_price(
        self, room_type_id: str, check_in: date, check_out: date
    ) -> float:
        """Calculate total price for a stay."""
        total = 0.0
        current = check_in
        while current < check_out:
            key = (room_type_id, current)
            rec = self._inventory.get(key)
            if rec:
                total += rec.price
            current += timedelta(days=1)
        return total


# ---------------------------------------------------------------------------
# Booking Service
# ---------------------------------------------------------------------------

class BookingService:
    """Orchestrates hotel search, booking, and cancellation.

    Demonstrates:
    - Search by location and date range
    - Booking with inventory reservation (optimistic locking)
    - Cancellation with inventory release
    - Booking state machine
    - Review submission
    """

    def __init__(self) -> None:
        self.hotels: dict[str, Hotel] = {}
        self.room_types: dict[str, RoomType] = {}
        self.inventory = InventoryManager()
        self.bookings: dict[str, Booking] = {}
        self.reviews: list[Review] = []
        # Map hotel_id -> list of room_type_ids
        self._hotel_rooms: dict[str, list[str]] = {}

    # -- Hotel & Room Management --

    def add_hotel(self, hotel: Hotel) -> None:
        self.hotels[hotel.hotel_id] = hotel
        self._hotel_rooms.setdefault(hotel.hotel_id, [])

    def add_room_type(
        self, room_type: RoomType, inventory_start: date, inventory_days: int = 90
    ) -> None:
        """Register a room type and initialize its inventory."""
        self.room_types[room_type.room_type_id] = room_type
        self._hotel_rooms.setdefault(room_type.hotel_id, []).append(
            room_type.room_type_id
        )
        self.inventory.initialize(room_type, inventory_start, inventory_days)

    # -- Search --

    def search_hotels(
        self,
        city: str,
        check_in: date,
        check_out: date,
        guests: int = 1,
    ) -> list[dict]:
        """Search for available hotels in a city for given dates and guest count.

        Returns a list of dicts with hotel info and available room types.
        """
        results: list[dict] = []
        for hotel in self.hotels.values():
            if hotel.city.lower() != city.lower():
                continue

            available_rooms: list[dict] = []
            for rt_id in self._hotel_rooms.get(hotel.hotel_id, []):
                rt = self.room_types[rt_id]
                if rt.max_guests < guests:
                    continue
                records = self.inventory.check_availability(
                    rt_id, check_in, check_out
                )
                if records:
                    min_avail = min(r.available for r in records)
                    total_price = self.inventory.get_price(
                        rt_id, check_in, check_out
                    )
                    available_rooms.append({
                        "room_type_id": rt_id,
                        "name": rt.name,
                        "max_guests": rt.max_guests,
                        "price_per_night": rt.base_price,
                        "total_price": total_price,
                        "available_count": min_avail,
                    })

            if available_rooms:
                results.append({
                    "hotel_id": hotel.hotel_id,
                    "name": hotel.name,
                    "city": hotel.city,
                    "star_rating": hotel.star_rating,
                    "amenities": hotel.amenities,
                    "rooms": available_rooms,
                })

        return results

    # -- Booking --

    def create_booking(
        self,
        hotel_id: str,
        room_type_id: str,
        guest_name: str,
        check_in: date,
        check_out: date,
    ) -> tuple[Optional[Booking], str]:
        """Create a booking, reserving inventory atomically.

        Returns (booking, message). Booking is None on failure.
        """
        if hotel_id not in self.hotels:
            return None, "Hotel not found"
        if room_type_id not in self.room_types:
            return None, "Room type not found"
        if check_in >= check_out:
            return None, "check_in must be before check_out"

        # Attempt inventory reservation with optimistic locking
        success, msg = self.inventory.reserve(room_type_id, check_in, check_out)
        if not success:
            return None, msg

        total_price = self.inventory.get_price(room_type_id, check_in, check_out)
        booking = Booking(
            booking_id=str(uuid.uuid4()),
            hotel_id=hotel_id,
            room_type_id=room_type_id,
            guest_name=guest_name,
            check_in=check_in,
            check_out=check_out,
            total_price=total_price,
            status=BookingStatus.PENDING,
        )
        # In production: payment would happen here (saga step 2)
        booking.transition(BookingStatus.CONFIRMED)
        self.bookings[booking.booking_id] = booking
        return booking, "Booking confirmed"

    def cancel_booking(self, booking_id: str) -> tuple[bool, str]:
        """Cancel a booking and release inventory."""
        booking = self.bookings.get(booking_id)
        if not booking:
            return False, "Booking not found"

        try:
            booking.transition(BookingStatus.CANCELLED)
        except ValueError as e:
            return False, str(e)

        # Release inventory (compensating transaction)
        self.inventory.release(
            booking.room_type_id, booking.check_in, booking.check_out
        )
        return True, f"Booking {booking_id[:8]}... cancelled, refund ${booking.total_price:.2f}"

    # -- Reviews --

    def submit_review(
        self,
        hotel_id: str,
        booking_id: str,
        guest_name: str,
        rating: int,
        comment: str,
    ) -> Review:
        """Submit a review for a completed stay."""
        if rating < 1 or rating > 10:
            raise ValueError("Rating must be between 1 and 10")
        review = Review(
            review_id=str(uuid.uuid4()),
            hotel_id=hotel_id,
            booking_id=booking_id,
            guest_name=guest_name,
            rating=rating,
            comment=comment,
        )
        self.reviews.append(review)
        return review

    def get_hotel_reviews(self, hotel_id: str) -> list[Review]:
        return [r for r in self.reviews if r.hotel_id == hotel_id]


# ---------------------------------------------------------------------------
# Concurrent Booking Demo
# ---------------------------------------------------------------------------

def _concurrent_booking_worker(
    service: BookingService,
    hotel_id: str,
    room_type_id: str,
    guest_name: str,
    check_in: date,
    check_out: date,
    results: list,
    idx: int,
) -> None:
    """Worker function for concurrent booking threads."""
    booking, msg = service.create_booking(
        hotel_id, room_type_id, guest_name, check_in, check_out
    )
    results[idx] = (guest_name, booking is not None, msg)


def run_concurrent_booking_demo(
    service: BookingService,
    hotel_id: str,
    room_type_id: str,
    check_in: date,
    check_out: date,
    num_attempts: int,
) -> None:
    """Simulate multiple users trying to book the last room(s) concurrently."""
    print("\n" + "=" * 60)
    print("CONCURRENT BOOKING DEMO")
    print("=" * 60)
    print(f"  {num_attempts} users trying to book simultaneously...")

    results: list[Optional[tuple]] = [None] * num_attempts
    threads: list[threading.Thread] = []

    for i in range(num_attempts):
        t = threading.Thread(
            target=_concurrent_booking_worker,
            args=(
                service, hotel_id, room_type_id,
                f"Guest_{i+1}", check_in, check_out,
                results, i,
            ),
        )
        threads.append(t)

    # Start all threads at roughly the same time
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    successes = 0
    failures = 0
    for guest_name, success, msg in results:
        status = "OK" if success else "FAIL"
        if success:
            successes += 1
        else:
            failures += 1
        print(f"  [{status}] {guest_name}: {msg}")

    print(f"\n  Results: {successes} succeeded, {failures} rejected (no double booking!)")


# ---------------------------------------------------------------------------
# Main Demo
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 60)
    print("HOTEL BOOKING SYSTEM - DEMO")
    print("=" * 60)

    service = BookingService()
    today = date.today()
    check_in = today + timedelta(days=7)
    check_out = today + timedelta(days=10)  # 3-night stay

    # --- Setup hotels ---
    hotel1 = Hotel("h1", "Grand Palace Hotel", "New York", 5, ["wifi", "pool", "spa"])
    hotel2 = Hotel("h2", "City View Inn", "New York", 3, ["wifi", "parking"])
    hotel3 = Hotel("h3", "Seaside Resort", "Miami", 4, ["wifi", "pool", "beach"])
    for h in [hotel1, hotel2, hotel3]:
        service.add_hotel(h)

    # --- Setup room types ---
    rt1 = RoomType("rt1", "h1", "Deluxe King", max_guests=2, base_price=250.0, total_rooms=3)
    rt2 = RoomType("rt2", "h1", "Standard Twin", max_guests=2, base_price=150.0, total_rooms=5)
    rt3 = RoomType("rt3", "h2", "Economy Double", max_guests=2, base_price=80.0, total_rooms=10)
    rt4 = RoomType("rt4", "h3", "Ocean Suite", max_guests=4, base_price=400.0, total_rooms=2)

    for rt in [rt1, rt2, rt3, rt4]:
        service.add_room_type(rt, today, inventory_days=30)

    # --- 1. Search ---
    print("\n--- Search: Hotels in New York ---")
    results = service.search_hotels("New York", check_in, check_out, guests=2)
    for r in results:
        print(f"\n  {r['name']} ({r['star_rating']}-star)")
        print(f"    Amenities: {', '.join(r['amenities'])}")
        for rm in r["rooms"]:
            print(
                f"    - {rm['name']}: ${rm['price_per_night']}/night, "
                f"total ${rm['total_price']}, {rm['available_count']} available"
            )

    # --- 2. Make a booking ---
    print("\n--- Booking: Reserve a Deluxe King ---")
    booking, msg = service.create_booking("h1", "rt1", "Alice Smith", check_in, check_out)
    print(f"  {msg}")
    if booking:
        print(f"  {booking}")
        print(f"  Total: ${booking.total_price:.2f}")

    # --- 3. Verify availability decreased ---
    print("\n--- Availability after booking ---")
    results = service.search_hotels("New York", check_in, check_out, guests=2)
    for r in results:
        if r["hotel_id"] == "h1":
            for rm in r["rooms"]:
                if rm["room_type_id"] == "rt1":
                    print(f"  Deluxe King: {rm['available_count']} remaining (was 3)")

    # --- 4. Cancel booking ---
    print("\n--- Cancel booking ---")
    if booking:
        success, msg = service.cancel_booking(booking.booking_id)
        print(f"  {msg}")
        print(f"  Status: {booking.status.value}")

    # --- 5. Verify availability restored ---
    print("\n--- Availability after cancellation ---")
    results = service.search_hotels("New York", check_in, check_out, guests=2)
    for r in results:
        if r["hotel_id"] == "h1":
            for rm in r["rooms"]:
                if rm["room_type_id"] == "rt1":
                    print(f"  Deluxe King: {rm['available_count']} available (restored)")

    # --- 6. Submit review ---
    print("\n--- Submit Review ---")
    booking2, _ = service.create_booking("h1", "rt2", "Bob Jones", check_in, check_out)
    if booking2:
        review = service.submit_review(
            "h1", booking2.booking_id, "Bob Jones", 9, "Excellent stay, great service!"
        )
        print(f"  Review by {review.guest_name}: {review.rating}/10")
        print(f"  \"{review.comment}\"")

    # --- 7. Invalid state transition ---
    print("\n--- Invalid State Transition ---")
    if booking:
        try:
            booking.transition(BookingStatus.CONFIRMED)
        except ValueError as e:
            print(f"  Caught: {e}")

    # --- 8. Concurrent booking (flash sale scenario) ---
    # Create a room type with only 2 rooms, and have 5 users try to book
    flash_hotel = Hotel("h_flash", "Flash Deal Hotel", "Las Vegas", 4, ["wifi"])
    service.add_hotel(flash_hotel)
    flash_rt = RoomType(
        "rt_flash", "h_flash", "Flash Room",
        max_guests=2, base_price=99.0, total_rooms=2,
    )
    service.add_room_type(flash_rt, today, inventory_days=30)

    run_concurrent_booking_demo(
        service, "h_flash", "rt_flash", check_in, check_out, num_attempts=5
    )

    # --- 9. Search with no results ---
    print("\n--- Search: Hotels in London (none) ---")
    results = service.search_hotels("London", check_in, check_out)
    print(f"  Results: {len(results)} hotels found")

    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
