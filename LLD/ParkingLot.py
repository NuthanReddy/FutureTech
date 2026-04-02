"""
Parking Lot System -- Low-Level Design

Design a multi-floor parking lot that supports different vehicle/spot types,
ticket-based entry/exit, hourly fee calculation, and availability display.

Key design decisions:
 - Enums for vehicle and spot types enforce type safety.
 - A spot-compatibility mapping lets larger spots hold smaller vehicles.
 - FeeCalculator is a standalone class (Strategy pattern ready).
 - ParkingLot is the facade; floors handle spot-level logic internally.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class VehicleType(Enum):
    """Types of vehicles the lot accepts."""
    MOTORCYCLE = "MOTORCYCLE"
    CAR = "CAR"
    TRUCK = "TRUCK"


class SpotType(Enum):
    """Parking spot sizes -- ordered smallest -> largest."""
    COMPACT = "COMPACT"
    REGULAR = "REGULAR"
    LARGE = "LARGE"


# Spot compatibility: vehicle_type -> eligible spot types (preference order)
SPOT_COMPATIBILITY: Dict[VehicleType, List[SpotType]] = {
    VehicleType.MOTORCYCLE: [SpotType.COMPACT, SpotType.REGULAR, SpotType.LARGE],
    VehicleType.CAR: [SpotType.REGULAR, SpotType.LARGE],
    VehicleType.TRUCK: [SpotType.LARGE],
}


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

class Vehicle:
    """Represents a vehicle entering the parking lot."""

    def __init__(self, license_plate: str, vehicle_type: VehicleType) -> None:
        self.license_plate = license_plate
        self.vehicle_type = vehicle_type

    def __repr__(self) -> str:
        return f"Vehicle({self.license_plate!r}, {self.vehicle_type.value})"


class ParkingSpot:
    """A single parking spot on a floor."""

    def __init__(self, spot_id: str, spot_type: SpotType, floor: int) -> None:
        self.spot_id = spot_id
        self.spot_type = spot_type
        self.floor = floor
        self.is_occupied: bool = False
        self.parked_vehicle: Optional[Vehicle] = None

    def park(self, vehicle: Vehicle) -> None:
        """Occupy this spot with *vehicle*."""
        if self.is_occupied:
            raise ValueError(f"Spot {self.spot_id} is already occupied")
        self.is_occupied = True
        self.parked_vehicle = vehicle

    def unpark(self) -> Optional[Vehicle]:
        """Free this spot and return the vehicle that was parked."""
        vehicle = self.parked_vehicle
        self.is_occupied = False
        self.parked_vehicle = None
        return vehicle

    def __repr__(self) -> str:
        status = "OCCUPIED" if self.is_occupied else "FREE"
        return f"ParkingSpot({self.spot_id}, {self.spot_type.value}, {status})"


class ParkingTicket:
    """Issued when a vehicle enters; settled on exit."""

    def __init__(self, vehicle: Vehicle, spot: ParkingSpot) -> None:
        self.ticket_id: str = str(uuid.uuid4())[:8].upper()
        self.vehicle = vehicle
        self.spot = spot
        self.entry_time: datetime = datetime.now()
        self.exit_time: Optional[datetime] = None
        self.fee: float = 0.0

    def __repr__(self) -> str:
        return (
            f"Ticket({self.ticket_id}, {self.vehicle.license_plate}, "
            f"spot={self.spot.spot_id})"
        )


# ---------------------------------------------------------------------------
# Floor
# ---------------------------------------------------------------------------

class ParkingFloor:
    """Manages all spots on a single floor."""

    def __init__(self, floor_number: int, spots: List[ParkingSpot]) -> None:
        self.floor_number = floor_number
        self.spots = spots

    def find_available_spot(self, vehicle_type: VehicleType) -> Optional[ParkingSpot]:
        """Return the first free spot compatible with *vehicle_type*, or None."""
        eligible_types = SPOT_COMPATIBILITY[vehicle_type]
        for preferred_type in eligible_types:
            for spot in self.spots:
                if spot.spot_type == preferred_type and not spot.is_occupied:
                    return spot
        return None

    def park_vehicle(self, vehicle: Vehicle) -> Optional[ParkingSpot]:
        """Park *vehicle* in the best available spot; return the spot or None."""
        spot = self.find_available_spot(vehicle.vehicle_type)
        if spot:
            spot.park(vehicle)
        return spot

    def unpark_vehicle(self, spot: ParkingSpot) -> Optional[Vehicle]:
        """Remove a vehicle from *spot*."""
        return spot.unpark()

    def availability(self) -> Dict[SpotType, Dict[str, int]]:
        """Return {SpotType: {'total': n, 'available': m}} for this floor."""
        counts: Dict[SpotType, Dict[str, int]] = {}
        for st in SpotType:
            total = [s for s in self.spots if s.spot_type == st]
            free = [s for s in total if not s.is_occupied]
            counts[st] = {"total": len(total), "available": len(free)}
        return counts


# ---------------------------------------------------------------------------
# Fee calculation
# ---------------------------------------------------------------------------

class FeeCalculator:
    """Calculates parking fees based on vehicle type and duration."""

    # Hourly rates in currency units
    RATES: Dict[VehicleType, float] = {
        VehicleType.MOTORCYCLE: 10.0,
        VehicleType.CAR: 20.0,
        VehicleType.TRUCK: 30.0,
    }

    @staticmethod
    def calculate_fee(ticket: ParkingTicket) -> float:
        """Compute the fee for a completed ticket (minimum 1 hour charge)."""
        if ticket.exit_time is None:
            raise ValueError("Ticket has no exit time; vehicle hasn't exited yet")
        duration = ticket.exit_time - ticket.entry_time
        hours = max(1.0, duration.total_seconds() / 3600)
        rate = FeeCalculator.RATES[ticket.vehicle.vehicle_type]
        return round(rate * hours, 2)


# ---------------------------------------------------------------------------
# Parking Lot -- main facade
# ---------------------------------------------------------------------------

class ParkingLot:
    """Top-level facade for the parking lot system."""

    def __init__(self, name: str, floors: List[ParkingFloor]) -> None:
        self.name = name
        self.floors = floors
        self._active_tickets: Dict[str, ParkingTicket] = {}  # ticket_id -> ticket

    # -- entry / exit ---------------------------------------------------------

    def entry(self, vehicle: Vehicle) -> Optional[ParkingTicket]:
        """Attempt to park *vehicle*; return a ticket or None if lot is full."""
        for floor in self.floors:
            spot = floor.park_vehicle(vehicle)
            if spot:
                ticket = ParkingTicket(vehicle, spot)
                self._active_tickets[ticket.ticket_id] = ticket
                return ticket
        return None

    def exit(self, ticket: ParkingTicket) -> float:
        """Process exit: unpark, calculate fee, settle ticket."""
        ticket.exit_time = datetime.now()
        fee = FeeCalculator.calculate_fee(ticket)
        ticket.fee = fee

        # Find the floor that owns this spot and unpark
        for floor in self.floors:
            if ticket.spot in floor.spots:
                floor.unpark_vehicle(ticket.spot)
                break

        self._active_tickets.pop(ticket.ticket_id, None)
        return fee

    # -- display --------------------------------------------------------------

    def display_availability(self) -> None:
        """Print a formatted availability report."""
        print(f"\n{'=' * 55}")
        print(f"  {self.name} -- Availability")
        print(f"{'=' * 55}")
        for floor in self.floors:
            counts = floor.availability()
            print(f"  Floor {floor.floor_number}:")
            for st in SpotType:
                info = counts[st]
                print(f"    {st.value:<10} {info['available']:>3} / {info['total']:>3} free")
        print(f"{'=' * 55}\n")


# ---------------------------------------------------------------------------
# Builder helper
# ---------------------------------------------------------------------------

def build_parking_lot(
    name: str,
    num_floors: int,
    compact_per_floor: int,
    regular_per_floor: int,
    large_per_floor: int,
) -> ParkingLot:
    """Convenience factory to create a uniform parking lot."""
    floors: List[ParkingFloor] = []
    for f in range(1, num_floors + 1):
        spots: List[ParkingSpot] = []
        counter = 1
        for _ in range(compact_per_floor):
            spots.append(ParkingSpot(f"F{f}-C{counter}", SpotType.COMPACT, f))
            counter += 1
        for _ in range(regular_per_floor):
            spots.append(ParkingSpot(f"F{f}-R{counter}", SpotType.REGULAR, f))
            counter += 1
        for _ in range(large_per_floor):
            spots.append(ParkingSpot(f"F{f}-L{counter}", SpotType.LARGE, f))
            counter += 1
        floors.append(ParkingFloor(f, spots))
    return ParkingLot(name, floors)


# ---------------------------------------------------------------------------
# Demo / simulation
# ---------------------------------------------------------------------------

def _simulate() -> None:
    """Run a full parking-lot simulation demonstrating all features."""

    # 1. Create a 3-floor lot: 5 compact, 10 regular, 3 large per floor
    lot = build_parking_lot(
        name="Downtown Parking Garage",
        num_floors=3,
        compact_per_floor=5,
        regular_per_floor=10,
        large_per_floor=3,
    )
    print("[OK] Created parking lot with 3 floors")

    # 2. Park various vehicles
    vehicles = [
        Vehicle("MOTO-001", VehicleType.MOTORCYCLE),
        Vehicle("MOTO-002", VehicleType.MOTORCYCLE),
        Vehicle("CAR-001", VehicleType.CAR),
        Vehicle("CAR-002", VehicleType.CAR),
        Vehicle("CAR-003", VehicleType.CAR),
        Vehicle("TRUCK-001", VehicleType.TRUCK),
        Vehicle("TRUCK-002", VehicleType.TRUCK),
    ]

    tickets: List[ParkingTicket] = []
    for v in vehicles:
        ticket = lot.entry(v)
        if ticket:
            print(f"  Parked {v} -> {ticket}")
            tickets.append(ticket)
        else:
            print(f"  [X] No spot for {v}")

    # 3. Show availability after parking
    lot.display_availability()

    # 4. Fill all large spots to show overflow / rejection
    print("-- Filling all remaining LARGE spots with trucks ...")
    overflow_trucks: List[ParkingTicket] = []
    for i in range(3, 20):
        t = lot.entry(Vehicle(f"TRUCK-{i:03}", VehicleType.TRUCK))
        if t:
            overflow_trucks.append(t)
            print(f"  Parked TRUCK-{i:03} -> spot {t.spot.spot_id}")
        else:
            print(f"  [X] TRUCK-{i:03} rejected -- no LARGE spots left")
            break

    lot.display_availability()

    # 5. Park a motorcycle into a larger spot when compact is full
    print("-- Filling all COMPACT spots with motorcycles ...")
    moto_tickets: List[ParkingTicket] = []
    for i in range(3, 25):
        t = lot.entry(Vehicle(f"MOTO-{i:03}", VehicleType.MOTORCYCLE))
        if t:
            moto_tickets.append(t)
            if t.spot.spot_type != SpotType.COMPACT:
                print(
                    f"  MOTO-{i:03} parked in {t.spot.spot_type.value} spot "
                    f"{t.spot.spot_id} (compact full)"
                )
        else:
            print(f"  [X] MOTO-{i:03} rejected -- lot completely full")
            break

    lot.display_availability()

    # 6. Exit vehicles and calculate fees (simulate earlier entry for non-zero fee)
    print("-- Exiting first batch of vehicles ...")
    for ticket in tickets:
        # Backdate entry for a meaningful fee
        ticket.entry_time = datetime.now() - timedelta(hours=2, minutes=30)
        fee = lot.exit(ticket)
        print(
            f"  {ticket.vehicle.license_plate:<12} "
            f"exited | duration ~ 2.5 h | fee = ${fee:.2f}"
        )

    # 7. Show updated availability
    lot.display_availability()


if __name__ == "__main__":
    _simulate()


