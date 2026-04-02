"""
Ride Sharing System Simulation
==============================

Simulates core components of a ride-sharing platform (Uber/Lyft):
- GeoHash-based proximity search for driver matching
- Trip state machine with validated transitions
- Fare calculator with surge pricing
- Ride matching service orchestrating the flow

Uses only the Python standard library.
"""

from __future__ import annotations

import math
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Domain Models
# ---------------------------------------------------------------------------

class VehicleType(Enum):
    ECONOMY = "economy"
    PREMIUM = "premium"
    XL = "xl"


class DriverStatus(Enum):
    AVAILABLE = "AVAILABLE"
    BUSY = "BUSY"
    OFFLINE = "OFFLINE"


class TripStatus(Enum):
    REQUESTED = "REQUESTED"
    MATCHING = "MATCHING"
    MATCHED = "MATCHED"
    DRIVER_EN_ROUTE = "DRIVER_EN_ROUTE"
    DRIVER_ARRIVED = "DRIVER_ARRIVED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    NO_DRIVERS = "NO_DRIVERS"


@dataclass
class Location:
    """Geographic coordinate with Haversine distance calculation."""

    lat: float
    lng: float

    def distance_km(self, other: Location) -> float:
        """Haversine formula for great-circle distance in km."""
        R = 6371.0  # Earth radius in km
        lat1, lat2 = math.radians(self.lat), math.radians(other.lat)
        dlat = math.radians(other.lat - self.lat)
        dlng = math.radians(other.lng - self.lng)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlng / 2) ** 2
        )
        return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def __repr__(self) -> str:
        return f"Location({self.lat:.4f}, {self.lng:.4f})"


@dataclass
class Driver:
    driver_id: str
    name: str
    vehicle_type: VehicleType
    status: DriverStatus = DriverStatus.OFFLINE
    location: Optional[Location] = None
    rating: float = 5.0
    total_ratings: int = 0

    def go_online(self, location: Location) -> None:
        self.status = DriverStatus.AVAILABLE
        self.location = location

    def go_offline(self) -> None:
        self.status = DriverStatus.OFFLINE

    def update_location(self, location: Location) -> None:
        self.location = location

    def add_rating(self, score: float) -> None:
        total = self.rating * self.total_ratings + score
        self.total_ratings += 1
        self.rating = round(total / self.total_ratings, 2)


@dataclass
class Rider:
    rider_id: str
    name: str
    rating: float = 5.0
    total_ratings: int = 0
    trip_history: List[str] = field(default_factory=list)

    def add_rating(self, score: float) -> None:
        total = self.rating * self.total_ratings + score
        self.total_ratings += 1
        self.rating = round(total / self.total_ratings, 2)


# ---------------------------------------------------------------------------
# GeoHash Index -- Simulates Redis GEORADIUS
# ---------------------------------------------------------------------------

class GeoHashIndex:
    """
    In-memory geospatial index using GeoHash-style grid bucketing.

    Encodes (lat, lng) into a geohash string of configurable precision.
    Nearby drivers share a common geohash prefix, enabling fast proximity
    lookups by scanning the target cell and its 8 neighbors.
    """

    BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"

    def __init__(self, precision: int = 6) -> None:
        self.precision = precision
        # geohash_cell -> set of driver_ids
        self._cells: Dict[str, set] = {}
        # driver_id -> (geohash, Location)
        self._drivers: Dict[str, Tuple[str, Location]] = {}

    @classmethod
    def encode(cls, lat: float, lng: float, precision: int = 6) -> str:
        """Encode a lat/lng pair into a geohash string."""
        lat_range = (-90.0, 90.0)
        lng_range = (-180.0, 180.0)
        bits = 0
        bit_count = 0
        geohash_chars: List[str] = []
        is_lng = True

        while len(geohash_chars) < precision:
            if is_lng:
                mid = (lng_range[0] + lng_range[1]) / 2
                if lng >= mid:
                    bits = bits * 2 + 1
                    lng_range = (mid, lng_range[1])
                else:
                    bits = bits * 2
                    lng_range = (lng_range[0], mid)
            else:
                mid = (lat_range[0] + lat_range[1]) / 2
                if lat >= mid:
                    bits = bits * 2 + 1
                    lat_range = (mid, lat_range[1])
                else:
                    bits = bits * 2
                    lat_range = (lat_range[0], mid)
            is_lng = not is_lng
            bit_count += 1

            if bit_count == 5:
                geohash_chars.append(cls.BASE32[bits])
                bits = 0
                bit_count = 0

        return "".join(geohash_chars)

    @classmethod
    def decode(cls, geohash: str) -> Tuple[float, float]:
        """Decode a geohash string back to approximate (lat, lng)."""
        lat_range = [-90.0, 90.0]
        lng_range = [-180.0, 180.0]
        is_lng = True

        for ch in geohash:
            idx = cls.BASE32.index(ch)
            for bit_pos in range(4, -1, -1):
                bit = (idx >> bit_pos) & 1
                if is_lng:
                    mid = (lng_range[0] + lng_range[1]) / 2
                    if bit:
                        lng_range[0] = mid
                    else:
                        lng_range[1] = mid
                else:
                    mid = (lat_range[0] + lat_range[1]) / 2
                    if bit:
                        lat_range[0] = mid
                    else:
                        lat_range[1] = mid
                is_lng = not is_lng

        lat = (lat_range[0] + lat_range[1]) / 2
        lng = (lng_range[0] + lng_range[1]) / 2
        return lat, lng

    @classmethod
    def neighbors(cls, geohash: str) -> List[str]:
        """Return the 8 neighboring geohash cells plus the cell itself."""
        lat, lng = cls.decode(geohash)
        precision = len(geohash)

        # Step by full cell width to reach centers of adjacent cells
        lat_bits = (precision * 5) // 2
        lng_bits = (precision * 5 + 1) // 2
        lat_step = 180.0 / (2 ** lat_bits)
        lng_step = 360.0 / (2 ** lng_bits)

        result = set()
        for dlat in (-lat_step, 0, lat_step):
            for dlng in (-lng_step, 0, lng_step):
                result.add(cls.encode(lat + dlat, lng + dlng, precision))
        return list(result)

    def add_driver(self, driver_id: str, location: Location) -> None:
        """Add or update a driver's position in the index."""
        # Remove from old cell if present
        self.remove_driver(driver_id)
        ghash = self.encode(location.lat, location.lng, self.precision)
        self._cells.setdefault(ghash, set()).add(driver_id)
        self._drivers[driver_id] = (ghash, location)

    def remove_driver(self, driver_id: str) -> None:
        """Remove a driver from the index."""
        if driver_id in self._drivers:
            old_hash, _ = self._drivers.pop(driver_id)
            if old_hash in self._cells:
                self._cells[old_hash].discard(driver_id)
                if not self._cells[old_hash]:
                    del self._cells[old_hash]

    def find_nearby(
        self, location: Location, radius_km: float, limit: int = 10
    ) -> List[Tuple[str, float]]:
        """
        Find drivers within radius_km of location.

        Returns list of (driver_id, distance_km) sorted by distance.
        Mimics Redis GEORADIUS behavior.
        """
        center_hash = self.encode(location.lat, location.lng, self.precision)
        candidate_cells = self.neighbors(center_hash)

        results: List[Tuple[str, float]] = []
        for cell in candidate_cells:
            for driver_id in self._cells.get(cell, set()):
                _, driver_loc = self._drivers[driver_id]
                dist = location.distance_km(driver_loc)
                if dist <= radius_km:
                    results.append((driver_id, dist))

        results.sort(key=lambda x: x[1])
        return results[:limit]


# ---------------------------------------------------------------------------
# Trip State Machine
# ---------------------------------------------------------------------------

# Valid transitions: from_state -> set of allowed to_states
VALID_TRANSITIONS: Dict[TripStatus, set] = {
    TripStatus.REQUESTED: {TripStatus.MATCHING, TripStatus.CANCELLED},
    TripStatus.MATCHING: {TripStatus.MATCHED, TripStatus.NO_DRIVERS, TripStatus.CANCELLED},
    TripStatus.NO_DRIVERS: {TripStatus.CANCELLED},
    TripStatus.MATCHED: {TripStatus.DRIVER_EN_ROUTE, TripStatus.CANCELLED},
    TripStatus.DRIVER_EN_ROUTE: {TripStatus.DRIVER_ARRIVED, TripStatus.CANCELLED},
    TripStatus.DRIVER_ARRIVED: {TripStatus.IN_PROGRESS, TripStatus.CANCELLED},
    TripStatus.IN_PROGRESS: {TripStatus.COMPLETED},
    TripStatus.COMPLETED: set(),
    TripStatus.CANCELLED: set(),
}


@dataclass
class Trip:
    """Represents a ride with state machine lifecycle."""

    trip_id: str
    rider_id: str
    pickup: Location
    dropoff: Location
    vehicle_type: VehicleType
    status: TripStatus = TripStatus.REQUESTED
    driver_id: Optional[str] = None
    estimated_fare: float = 0.0
    actual_fare: float = 0.0
    surge_multiplier: float = 1.0
    distance_km: float = 0.0
    duration_min: float = 0.0
    requested_at: float = field(default_factory=time.time)
    matched_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    events: List[Dict] = field(default_factory=list)

    def transition_to(self, new_status: TripStatus) -> None:
        """Validate and execute a state transition, recording the event."""
        allowed = VALID_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise InvalidTransitionError(
                f"Cannot transition from {self.status.value} to {new_status.value}. "
                f"Allowed: {[s.value for s in allowed]}"
            )
        old_status = self.status
        self.status = new_status
        self.events.append({
            "from": old_status.value,
            "to": new_status.value,
            "timestamp": time.time(),
        })

        # Record timestamps for key transitions
        if new_status == TripStatus.MATCHED:
            self.matched_at = time.time()
        elif new_status == TripStatus.IN_PROGRESS:
            self.started_at = time.time()
        elif new_status == TripStatus.COMPLETED:
            self.completed_at = time.time()


class InvalidTransitionError(Exception):
    """Raised when an invalid state transition is attempted."""
    pass


# ---------------------------------------------------------------------------
# Fare Calculator with Surge Pricing
# ---------------------------------------------------------------------------

class FareCalculator:
    """
    Calculates ride fares based on distance, duration, vehicle type, and surge.

    Fare formula:
        base + (distance_km * per_km) + (duration_min * per_min)
        * surge_multiplier
        >= minimum_fare
    """

    RATES = {
        VehicleType.ECONOMY: {"base": 2.50, "per_km": 1.20, "per_min": 0.25, "minimum": 5.00},
        VehicleType.PREMIUM: {"base": 5.00, "per_km": 2.00, "per_min": 0.40, "minimum": 10.00},
        VehicleType.XL:      {"base": 4.00, "per_km": 1.80, "per_min": 0.35, "minimum": 8.00},
    }

    PLATFORM_COMMISSION = 0.25  # 25%

    @classmethod
    def estimate_fare(
        cls,
        pickup: Location,
        dropoff: Location,
        vehicle_type: VehicleType,
        surge: float = 1.0,
    ) -> Dict[str, float]:
        """Estimate fare before trip starts."""
        distance_km = pickup.distance_km(dropoff)
        # Rough road-network adjustment factor
        road_distance_km = distance_km * 1.35
        # Estimate duration: assume average 30 km/h in city
        est_duration_min = (road_distance_km / 30.0) * 60.0
        return cls._calculate(vehicle_type, road_distance_km, est_duration_min, surge)

    @classmethod
    def calculate_final_fare(
        cls,
        vehicle_type: VehicleType,
        distance_km: float,
        duration_min: float,
        surge: float = 1.0,
    ) -> Dict[str, float]:
        """Calculate final fare after trip completion."""
        return cls._calculate(vehicle_type, distance_km, duration_min, surge)

    @classmethod
    def _calculate(
        cls,
        vehicle_type: VehicleType,
        distance_km: float,
        duration_min: float,
        surge: float,
    ) -> Dict[str, float]:
        rates = cls.RATES[vehicle_type]
        subtotal = rates["base"] + distance_km * rates["per_km"] + duration_min * rates["per_min"]
        fare = max(subtotal * surge, rates["minimum"])
        fare = round(fare, 2)
        platform_fee = round(fare * cls.PLATFORM_COMMISSION, 2)
        driver_payout = round(fare - platform_fee, 2)
        return {
            "fare": fare,
            "distance_km": round(distance_km, 2),
            "duration_min": round(duration_min, 2),
            "surge_multiplier": surge,
            "platform_fee": platform_fee,
            "driver_payout": driver_payout,
        }


# ---------------------------------------------------------------------------
# Surge Pricing Engine
# ---------------------------------------------------------------------------

class SurgePricingEngine:
    """
    Computes surge multiplier based on supply/demand ratio in a region.

    Uses a piecewise linear curve capped at 3.0x with exponential smoothing.
    """

    def __init__(self) -> None:
        # region_key -> previous surge multiplier (for smoothing)
        self._previous_surge: Dict[str, float] = {}

    def compute_surge(self, region: str, demand: int, supply: int) -> float:
        """Compute smoothed surge multiplier for a region."""
        if supply == 0:
            raw_surge = 3.0
        else:
            ratio = demand / supply
            raw_surge = self._surge_curve(ratio)

        # Exponential smoothing
        prev = self._previous_surge.get(region, 1.0)
        smoothed = round(0.7 * raw_surge + 0.3 * prev, 2)
        self._previous_surge[region] = smoothed
        return smoothed

    @staticmethod
    def _surge_curve(ratio: float) -> float:
        """Piecewise linear surge pricing curve."""
        if ratio < 1.0:
            return 1.0
        elif ratio <= 1.5:
            return 1.0 + 0.25 * (ratio - 1.0)
        elif ratio <= 2.5:
            return 1.125 + 0.5 * (ratio - 1.5)
        elif ratio <= 4.0:
            return 1.625 + 0.5 * (ratio - 2.5)
        else:
            return min(3.0, 2.375 + 0.1 * (ratio - 4.0))


# ---------------------------------------------------------------------------
# Ride Matching Service
# ---------------------------------------------------------------------------

class RideMatchingService:
    """
    Orchestrates the ride-sharing system:
    - Registers drivers and riders
    - Maintains geospatial index of driver locations
    - Matches riders with nearest available drivers
    - Manages trip lifecycle through state machine
    - Calculates fares with surge pricing
    """

    SEARCH_RADII_KM = [3.0, 5.0, 8.0, 15.0]
    MAX_CANDIDATES = 5

    def __init__(self) -> None:
        self.drivers: Dict[str, Driver] = {}
        self.riders: Dict[str, Rider] = {}
        self.trips: Dict[str, Trip] = {}
        self.geo_index = GeoHashIndex(precision=6)
        self.surge_engine = SurgePricingEngine()

    # -- Driver Management --

    def register_driver(self, name: str, vehicle_type: VehicleType) -> Driver:
        driver = Driver(
            driver_id=f"drv_{uuid.uuid4().hex[:8]}",
            name=name,
            vehicle_type=vehicle_type,
        )
        self.drivers[driver.driver_id] = driver
        return driver

    def driver_go_online(self, driver_id: str, location: Location) -> None:
        driver = self.drivers[driver_id]
        driver.go_online(location)
        self.geo_index.add_driver(driver_id, location)

    def driver_go_offline(self, driver_id: str) -> None:
        driver = self.drivers[driver_id]
        driver.go_offline()
        self.geo_index.remove_driver(driver_id)

    def update_driver_location(self, driver_id: str, location: Location) -> None:
        driver = self.drivers[driver_id]
        driver.update_location(location)
        if driver.status == DriverStatus.AVAILABLE:
            self.geo_index.add_driver(driver_id, location)

    # -- Rider Management --

    def register_rider(self, name: str) -> Rider:
        rider = Rider(rider_id=f"rdr_{uuid.uuid4().hex[:8]}", name=name)
        self.riders[rider.rider_id] = rider
        return rider

    # -- Ride Flow --

    def estimate_fare(
        self,
        pickup: Location,
        dropoff: Location,
        vehicle_type: VehicleType,
        region: str = "default",
        demand: int = 10,
        supply: int = 10,
    ) -> Dict[str, float]:
        """Get a fare estimate including surge pricing."""
        surge = self.surge_engine.compute_surge(region, demand, supply)
        return FareCalculator.estimate_fare(pickup, dropoff, vehicle_type, surge)

    def request_ride(
        self,
        rider_id: str,
        pickup: Location,
        dropoff: Location,
        vehicle_type: VehicleType = VehicleType.ECONOMY,
        surge: float = 1.0,
    ) -> Trip:
        """Create a new ride request and attempt driver matching."""
        estimate = FareCalculator.estimate_fare(pickup, dropoff, vehicle_type, surge)
        trip = Trip(
            trip_id=f"trip_{uuid.uuid4().hex[:8]}",
            rider_id=rider_id,
            pickup=pickup,
            dropoff=dropoff,
            vehicle_type=vehicle_type,
            estimated_fare=estimate["fare"],
            surge_multiplier=surge,
        )
        self.trips[trip.trip_id] = trip
        return trip

    def match_driver(self, trip_id: str) -> Optional[str]:
        """
        Find and match the nearest available driver for a trip.

        Searches expanding radii, filters by vehicle type, and selects
        the closest available driver.
        """
        trip = self.trips[trip_id]
        trip.transition_to(TripStatus.MATCHING)

        for radius in self.SEARCH_RADII_KM:
            nearby = self.geo_index.find_nearby(
                trip.pickup, radius, limit=self.MAX_CANDIDATES * 2
            )

            for driver_id, dist_km in nearby:
                driver = self.drivers[driver_id]
                if (
                    driver.status == DriverStatus.AVAILABLE
                    and driver.vehicle_type == trip.vehicle_type
                ):
                    # Driver accepts (simulated)
                    trip.driver_id = driver_id
                    trip.transition_to(TripStatus.MATCHED)
                    driver.status = DriverStatus.BUSY
                    self.geo_index.remove_driver(driver_id)
                    return driver_id

        trip.transition_to(TripStatus.NO_DRIVERS)
        return None

    def start_trip(self, trip_id: str) -> None:
        """Transition trip through en-route -> arrived -> in-progress."""
        trip = self.trips[trip_id]
        trip.transition_to(TripStatus.DRIVER_EN_ROUTE)
        trip.transition_to(TripStatus.DRIVER_ARRIVED)
        trip.transition_to(TripStatus.IN_PROGRESS)

    def complete_trip(
        self, trip_id: str, actual_distance_km: float, actual_duration_min: float
    ) -> Dict[str, float]:
        """Complete a trip and calculate the final fare."""
        trip = self.trips[trip_id]
        trip.transition_to(TripStatus.COMPLETED)

        fare_details = FareCalculator.calculate_final_fare(
            trip.vehicle_type,
            actual_distance_km,
            actual_duration_min,
            trip.surge_multiplier,
        )
        trip.actual_fare = fare_details["fare"]
        trip.distance_km = actual_distance_km
        trip.duration_min = actual_duration_min

        # Free up driver
        if trip.driver_id and trip.driver_id in self.drivers:
            driver = self.drivers[trip.driver_id]
            driver.status = DriverStatus.AVAILABLE
            if driver.location:
                self.geo_index.add_driver(driver.driver_id, driver.location)

        # Add to rider history
        if trip.rider_id in self.riders:
            self.riders[trip.rider_id].trip_history.append(trip.trip_id)

        return fare_details

    def cancel_trip(self, trip_id: str) -> None:
        """Cancel a trip and free up the driver if assigned."""
        trip = self.trips[trip_id]
        trip.transition_to(TripStatus.CANCELLED)

        if trip.driver_id and trip.driver_id in self.drivers:
            driver = self.drivers[trip.driver_id]
            driver.status = DriverStatus.AVAILABLE
            if driver.location:
                self.geo_index.add_driver(driver.driver_id, driver.location)

    def rate_trip(self, trip_id: str, rider_rating: float, driver_rating: float) -> None:
        """Rate both rider and driver after trip completion."""
        trip = self.trips[trip_id]
        if trip.status != TripStatus.COMPLETED:
            raise ValueError("Can only rate completed trips")
        if trip.driver_id:
            self.drivers[trip.driver_id].add_rating(rider_rating)
        self.riders[trip.rider_id].add_rating(driver_rating)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def print_separator(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def demo() -> None:
    """Demonstrate the ride-sharing system end-to-end."""
    service = RideMatchingService()

    # ------------------------------------------------------------------
    # 1. Register drivers and riders
    # ------------------------------------------------------------------
    print_separator("1. Register Drivers and Riders")

    drivers = [
        service.register_driver("Alice (Economy)", VehicleType.ECONOMY),
        service.register_driver("Bob (Economy)", VehicleType.ECONOMY),
        service.register_driver("Charlie (Premium)", VehicleType.PREMIUM),
        service.register_driver("Diana (XL)", VehicleType.XL),
        service.register_driver("Eve (Economy)", VehicleType.ECONOMY),
    ]

    riders = [
        service.register_rider("Rider John"),
        service.register_rider("Rider Jane"),
    ]

    for d in drivers:
        print(f"  Driver: {d.name} [{d.vehicle_type.value}] id={d.driver_id}")
    for r in riders:
        print(f"  Rider:  {r.name} id={r.rider_id}")

    # ------------------------------------------------------------------
    # 2. Drivers go online at various SF locations
    # ------------------------------------------------------------------
    print_separator("2. Drivers Go Online (San Francisco)")

    sf_locations = [
        Location(37.7749, -122.4194),  # Downtown
        Location(37.7849, -122.4094),  # North
        Location(37.7649, -122.4294),  # South-West
        Location(37.7799, -122.3994),  # East
        Location(37.7700, -122.4100),  # Central
    ]

    for driver, loc in zip(drivers, sf_locations):
        service.driver_go_online(driver.driver_id, loc)
        ghash = GeoHashIndex.encode(loc.lat, loc.lng, precision=6)
        print(f"  {driver.name} online at {loc} geohash={ghash}")

    # ------------------------------------------------------------------
    # 3. GeoHash demonstration
    # ------------------------------------------------------------------
    print_separator("3. GeoHash Encoding/Decoding Demo")

    test_loc = Location(37.7749, -122.4194)
    ghash = GeoHashIndex.encode(test_loc.lat, test_loc.lng, precision=8)
    decoded_lat, decoded_lng = GeoHashIndex.decode(ghash)
    print(f"  Original:  ({test_loc.lat}, {test_loc.lng})")
    print(f"  GeoHash:   {ghash}")
    print(f"  Decoded:   ({decoded_lat:.4f}, {decoded_lng:.4f})")

    neighbors = GeoHashIndex.neighbors(ghash[:6])
    print(f"  Neighbors of {ghash[:6]}: {neighbors}")

    # ------------------------------------------------------------------
    # 4. Proximity search
    # ------------------------------------------------------------------
    print_separator("4. Proximity Search (find drivers near rider)")

    rider_location = Location(37.7760, -122.4180)
    nearby = service.geo_index.find_nearby(rider_location, radius_km=5.0, limit=10)

    print(f"  Rider at {rider_location}")
    print(f"  Found {len(nearby)} drivers within 5 km:")
    for drv_id, dist in nearby:
        drv = service.drivers[drv_id]
        print(f"    - {drv.name}: {dist:.2f} km [{drv.vehicle_type.value}]")

    # ------------------------------------------------------------------
    # 5. Surge pricing
    # ------------------------------------------------------------------
    print_separator("5. Surge Pricing Demo")

    surge_engine = SurgePricingEngine()
    scenarios = [
        ("Low demand", "downtown", 5, 20),
        ("Balanced", "downtown", 15, 15),
        ("High demand", "downtown", 30, 10),
        ("Very high demand", "downtown", 50, 8),
        ("Extreme demand", "downtown", 80, 5),
    ]

    for label, region, demand, supply in scenarios:
        surge = surge_engine.compute_surge(region, demand, supply)
        print(f"  {label:20s} | demand={demand:3d} supply={supply:3d} | surge={surge:.2f}x")

    # ------------------------------------------------------------------
    # 6. Fare estimation
    # ------------------------------------------------------------------
    print_separator("6. Fare Estimation")

    pickup = Location(37.7749, -122.4194)
    dropoff = Location(37.8044, -122.2712)  # Oakland

    for vtype in VehicleType:
        est = FareCalculator.estimate_fare(pickup, dropoff, vtype, surge=1.0)
        print(f"  {vtype.value:8s}: ${est['fare']:6.2f} "
              f"({est['distance_km']:.1f} km, ~{est['duration_min']:.0f} min)")

    print("\n  With 1.5x surge (economy):")
    est_surge = FareCalculator.estimate_fare(pickup, dropoff, VehicleType.ECONOMY, surge=1.5)
    print(f"    Fare: ${est_surge['fare']:.2f} | Platform fee: ${est_surge['platform_fee']:.2f} "
          f"| Driver payout: ${est_surge['driver_payout']:.2f}")

    # ------------------------------------------------------------------
    # 7. Full ride request -> match -> complete flow
    # ------------------------------------------------------------------
    print_separator("7. Complete Ride Flow")

    rider = riders[0]
    ride_pickup = Location(37.7760, -122.4180)
    ride_dropoff = Location(37.7900, -122.4000)

    # Request
    trip = service.request_ride(
        rider.rider_id, ride_pickup, ride_dropoff,
        VehicleType.ECONOMY, surge=1.2,
    )
    print(f"  [REQUESTED]  Trip {trip.trip_id}")
    print(f"    Pickup:  {trip.pickup}")
    print(f"    Dropoff: {trip.dropoff}")
    print(f"    Estimated fare: ${trip.estimated_fare:.2f} (surge {trip.surge_multiplier}x)")

    # Match
    matched_driver_id = service.match_driver(trip.trip_id)
    if matched_driver_id:
        driver = service.drivers[matched_driver_id]
        print(f"  [MATCHED]    Driver: {driver.name} ({matched_driver_id})")
    else:
        print("  [NO MATCH]   No driver found!")
        return

    # Start trip
    service.start_trip(trip.trip_id)
    print(f"  [IN_PROGRESS] Trip started")

    # Complete trip (simulated actual values)
    actual_dist = ride_pickup.distance_km(ride_dropoff) * 1.35
    actual_dur = (actual_dist / 25.0) * 60.0  # ~25 km/h city speed
    fare_details = service.complete_trip(trip.trip_id, actual_dist, actual_dur)

    print(f"  [COMPLETED]  Trip finished")
    print(f"    Distance:  {fare_details['distance_km']:.2f} km")
    print(f"    Duration:  {fare_details['duration_min']:.1f} min")
    print(f"    Fare:      ${fare_details['fare']:.2f}")
    print(f"    Platform:  ${fare_details['platform_fee']:.2f}")
    print(f"    Driver:    ${fare_details['driver_payout']:.2f}")

    # Rate
    service.rate_trip(trip.trip_id, rider_rating=5.0, driver_rating=4.5)
    print(f"  [RATED]      Driver rating: {driver.rating}, Rider rating: {rider.rating}")

    # ------------------------------------------------------------------
    # 8. Trip state machine events
    # ------------------------------------------------------------------
    print_separator("8. Trip Event Log (Event Sourcing)")

    for event in trip.events:
        print(f"  {event['from']:20s} -> {event['to']}")

    # ------------------------------------------------------------------
    # 9. Invalid transition test
    # ------------------------------------------------------------------
    print_separator("9. State Machine Validation")

    trip2 = service.request_ride(
        riders[1].rider_id,
        Location(37.78, -122.42),
        Location(37.79, -122.41),
    )
    print(f"  Trip {trip2.trip_id} status: {trip2.status.value}")

    try:
        trip2.transition_to(TripStatus.IN_PROGRESS)
        print("  ERROR: Should have raised InvalidTransitionError")
    except InvalidTransitionError as e:
        print(f"  Correctly blocked: {e}")

    # ------------------------------------------------------------------
    # 10. Cancellation flow
    # ------------------------------------------------------------------
    print_separator("10. Cancellation Flow")

    trip3 = service.request_ride(
        riders[1].rider_id,
        Location(37.77, -122.43),
        Location(37.80, -122.40),
    )
    matched_id = service.match_driver(trip3.trip_id)
    if matched_id:
        print(f"  Trip {trip3.trip_id} matched with driver {matched_id}")
        print(f"  Status: {trip3.status.value}")
        # Driver starts heading to pickup then rider cancels
        trip3.transition_to(TripStatus.DRIVER_EN_ROUTE)
        print(f"  Status: {trip3.status.value}")
        service.cancel_trip(trip3.trip_id)
        print(f"  Cancelled! Status: {trip3.status.value}")
        freed_driver = service.drivers[matched_id]
        print(f"  Driver {freed_driver.name} status: {freed_driver.status.value}")

    # ------------------------------------------------------------------
    # 11. Statistics
    # ------------------------------------------------------------------
    print_separator("11. System Statistics")

    total_trips = len(service.trips)
    completed = sum(1 for t in service.trips.values() if t.status == TripStatus.COMPLETED)
    cancelled = sum(1 for t in service.trips.values() if t.status == TripStatus.CANCELLED)
    available_drivers = sum(
        1 for d in service.drivers.values() if d.status == DriverStatus.AVAILABLE
    )

    print(f"  Total trips:       {total_trips}")
    print(f"  Completed:         {completed}")
    print(f"  Cancelled:         {cancelled}")
    print(f"  Available drivers: {available_drivers}")
    print(f"  Indexed drivers:   {len(service.geo_index._drivers)}")

    print(f"\n{'=' * 60}")
    print("  Ride Sharing System Demo Complete")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    demo()
