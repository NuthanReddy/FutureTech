"""
Elevator System — Low-Level Design

Design an elevator system for a multi-floor building with N elevators.
Uses the LOOK scheduling algorithm (serve requests in current direction,
reverse when no more requests ahead) and optimal elevator selection via
a scoring heuristic.

Key design decisions:
  - Strategy pattern: scheduling logic is encapsulated in ElevatorScheduler
    (easily swappable for SCAN, FCFS, etc.).
  - Single Responsibility: Elevator owns movement, Controller owns
    assignment, Building owns the public API.
  - Open/Closed: new scheduling strategies require no changes to Elevator
    or Building.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Direction(Enum):
    """Travel direction requested by a passenger."""
    UP = auto()
    DOWN = auto()


class ElevatorState(Enum):
    """Operational state of a single elevator."""
    IDLE = auto()
    MOVING_UP = auto()
    MOVING_DOWN = auto()
    DOOR_OPEN = auto()


# ---------------------------------------------------------------------------
# Request
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Request:
    """An external hall-call request.

    Attributes:
        source_floor: Floor where the passenger is waiting.
        destination_floor: Floor the passenger wants to reach.
        direction: Derived travel direction.
    """
    source_floor: int
    destination_floor: int
    direction: Direction

    @staticmethod
    def create(source: int, destination: int) -> Request:
        """Factory that derives direction automatically."""
        if source == destination:
            raise ValueError("Source and destination floors must differ")
        direction = Direction.UP if destination > source else Direction.DOWN
        return Request(source, destination, direction)


# ---------------------------------------------------------------------------
# Elevator
# ---------------------------------------------------------------------------

class Elevator:
    """A single elevator car.

    Attributes:
        id: Unique identifier.
        current_floor: Current position (1-indexed).
        state: Operational state.
        direction: Last-known travel direction (meaningful when not IDLE).
        stops: Set of floors this elevator must visit.
        capacity: Maximum passengers (informational).
    """

    def __init__(self, elevator_id: int, capacity: int = 10) -> None:
        self.id: int = elevator_id
        self.current_floor: int = 1
        self.state: ElevatorState = ElevatorState.IDLE
        self.direction: Direction = Direction.UP
        self.stops: set[int] = set()
        self.capacity: int = capacity

    # -- public API --------------------------------------------------------

    def add_stop(self, floor: int) -> None:
        """Add a destination floor to this elevator's stop set."""
        self.stops.add(floor)
        if self.state == ElevatorState.IDLE:
            self._update_direction()

    def move(self) -> None:
        """Advance the elevator by one floor in the current direction.

        Implements the LOOK algorithm: if there are no more stops ahead in
        the current direction, reverse.  If there are no stops at all,
        become IDLE.
        """
        if self.state == ElevatorState.DOOR_OPEN:
            self.close_doors()

        if not self.stops:
            self.state = ElevatorState.IDLE
            return

        self._update_direction()

        if self.direction == Direction.UP:
            self.current_floor += 1
            self.state = ElevatorState.MOVING_UP
        else:
            self.current_floor -= 1
            self.state = ElevatorState.MOVING_DOWN

        if self.should_stop():
            self.open_doors()

    def should_stop(self) -> bool:
        """Return True if the elevator must stop at the current floor."""
        return self.current_floor in self.stops

    def open_doors(self) -> None:
        """Open doors and remove the current floor from the stop set."""
        self.state = ElevatorState.DOOR_OPEN
        self.stops.discard(self.current_floor)

    def close_doors(self) -> None:
        """Close doors and resume movement or become IDLE."""
        if self.stops:
            self._update_direction()
            self.state = (
                ElevatorState.MOVING_UP
                if self.direction == Direction.UP
                else ElevatorState.MOVING_DOWN
            )
        else:
            self.state = ElevatorState.IDLE

    # -- internals ---------------------------------------------------------

    def _update_direction(self) -> None:
        """LOOK algorithm direction decision.

        Continue in the current direction if there are pending stops ahead;
        otherwise reverse.
        """
        if not self.stops:
            return

        has_up = any(f > self.current_floor for f in self.stops)
        has_down = any(f < self.current_floor for f in self.stops)

        if self.direction == Direction.UP:
            if not has_up and has_down:
                self.direction = Direction.DOWN
        else:
            if not has_down and has_up:
                self.direction = Direction.UP

    def __repr__(self) -> str:
        return (
            f"Elevator(id={self.id}, floor={self.current_floor}, "
            f"state={self.state.name}, stops={sorted(self.stops)})"
        )


# ---------------------------------------------------------------------------
# Scheduler Strategy (Strategy Pattern)
# ---------------------------------------------------------------------------

class ElevatorScheduler(ABC):
    """Abstract scheduling strategy for choosing which elevator to assign."""

    @abstractmethod
    def select_best(
        self, elevators: list[Elevator], floor: int, direction: Direction,
    ) -> Elevator:
        """Return the best elevator to handle a request at *floor*
        heading in *direction*."""


class LookScheduler(ElevatorScheduler):
    """LOOK-aware scheduler.

    Scoring heuristic (lower is better):
      1. Idle elevator — score = distance to request floor.
      2. Moving toward the request floor in the same direction — score =
         distance (small bonus for being already aligned).
      3. Moving away or in the opposite direction — score = distance + a
         large penalty so aligned elevators are preferred.
    """

    _PENALTY = 1000  # large value to de-prioritise misaligned elevators

    def select_best(
        self, elevators: list[Elevator], floor: int, direction: Direction,
    ) -> Elevator:
        def _score(elev: Elevator) -> tuple[float, int]:
            dist = abs(elev.current_floor - floor)
            load = len(elev.stops)  # tie-break: prefer less-loaded elevator

            if elev.state == ElevatorState.IDLE:
                return (dist, load)

            same_direction = elev.direction == direction
            approaching = (
                (direction == Direction.UP and elev.current_floor <= floor)
                or (direction == Direction.DOWN and elev.current_floor >= floor)
            )

            if same_direction and approaching:
                return (dist, load)

            return (dist + self._PENALTY, load)

        return min(elevators, key=_score)


# ---------------------------------------------------------------------------
# ElevatorController
# ---------------------------------------------------------------------------

class ElevatorController:
    """Coordinates multiple elevators using a pluggable scheduling strategy.

    Attributes:
        elevators: Managed elevator cars.
        pending_requests: Requests not yet fully assigned.
        scheduler: Strategy used to pick the best elevator.
    """

    def __init__(
        self,
        num_elevators: int,
        capacity: int = 10,
        scheduler: ElevatorScheduler | None = None,
    ) -> None:
        self.elevators: list[Elevator] = [
            Elevator(i + 1, capacity) for i in range(num_elevators)
        ]
        self.pending_requests: list[Request] = []
        self.scheduler: ElevatorScheduler = scheduler or LookScheduler()

    def request_elevator(self, floor: int, direction: Direction) -> Elevator:
        """Handle an external hall-call: pick the best elevator and send it
        to *floor*."""
        best = self.select_best_elevator(floor, direction)
        best.add_stop(floor)
        return best

    def select_best_elevator(
        self, floor: int, direction: Direction,
    ) -> Elevator:
        """Delegate to the scheduling strategy."""
        return self.scheduler.select_best(self.elevators, floor, direction)

    def press_floor_button(self, elevator_id: int, floor: int) -> None:
        """Internal cabin button press — add a destination to an elevator."""
        for elev in self.elevators:
            if elev.id == elevator_id:
                elev.add_stop(floor)
                return
        raise ValueError(f"No elevator with id {elevator_id}")

    def step(self) -> None:
        """Advance every elevator by one time unit."""
        for elev in self.elevators:
            elev.move()

    def status(self) -> str:
        """Human-readable snapshot of all elevators."""
        lines = [f"  {elev}" for elev in self.elevators]
        return "\n".join(lines)

    def all_idle(self) -> bool:
        """Return True when every elevator has finished its work."""
        return all(
            elev.state == ElevatorState.IDLE for elev in self.elevators
        )


# ---------------------------------------------------------------------------
# Building (top-level facade)
# ---------------------------------------------------------------------------

class Building:
    """Public interface representing a physical building.

    Attributes:
        num_floors: Total floors (1-indexed).
        controller: The elevator controller managing all cars.
    """

    def __init__(
        self,
        num_floors: int,
        num_elevators: int,
        capacity: int = 10,
        scheduler: ElevatorScheduler | None = None,
    ) -> None:
        if num_floors < 2:
            raise ValueError("Building must have at least 2 floors")
        self.num_floors: int = num_floors
        self.controller: ElevatorController = ElevatorController(
            num_elevators, capacity, scheduler,
        )

    def call_elevator(self, floor: int, direction: Direction) -> Elevator:
        """External hall-call button pressed on *floor*."""
        self._validate_floor(floor)
        return self.controller.request_elevator(floor, direction)

    def press_floor_button(self, elevator_id: int, floor: int) -> None:
        """Passenger inside elevator *elevator_id* presses *floor*."""
        self._validate_floor(floor)
        self.controller.press_floor_button(elevator_id, floor)

    def step(self) -> None:
        """Advance the simulation by one time unit."""
        self.controller.step()

    def status(self) -> str:
        """Pretty-print the current state of the building."""
        return (
            f"Building ({self.num_floors} floors):\n"
            + self.controller.status()
        )

    def _validate_floor(self, floor: int) -> None:
        if not 1 <= floor <= self.num_floors:
            raise ValueError(
                f"Floor {floor} out of range [1, {self.num_floors}]"
            )


# ---------------------------------------------------------------------------
# Simulation demo
# ---------------------------------------------------------------------------

def _print_header(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def _run_steps(building: Building, n: int, label: str = "") -> None:
    """Advance *n* steps, printing status each time."""
    if label:
        print(f"\n--- {label} ---")
    for i in range(1, n + 1):
        building.step()
        print(f"  Step {i}:")
        print(f"  {building.status()}\n")


def main() -> None:
    """Full simulation demonstrating the elevator system."""

    # ---- 1. Setup --------------------------------------------------------
    _print_header("1. Create a 10-floor building with 3 elevators")
    building = Building(num_floors=10, num_elevators=3)
    print(building.status())

    # ---- 2. External requests (hall calls) --------------------------------
    _print_header("2. External requests from various floors")

    print("Person on floor 5 presses UP")
    e1 = building.call_elevator(5, Direction.UP)
    print(f"  -> Assigned to Elevator {e1.id}")

    print("Person on floor 8 presses DOWN")
    e2 = building.call_elevator(8, Direction.DOWN)
    print(f"  -> Assigned to Elevator {e2.id}")

    print("Person on floor 3 presses UP")
    e3 = building.call_elevator(3, Direction.UP)
    print(f"  -> Assigned to Elevator {e3.id}")

    # ---- 3. Step through simulation showing positions ---------------------
    _print_header("3. Stepping through — elevators moving to pick-up floors")
    _run_steps(building, 5, "Moving toward passengers")

    # ---- 4. Internal requests (passengers select destination) -------------
    _print_header("4. Passengers board and press destination buttons")

    # By step 5, Elevator assignments have moved toward their pick-up floors.
    # Simulate passengers boarding and selecting destinations.
    print(f"Passenger in Elevator {e1.id} presses floor 9")
    building.press_floor_button(e1.id, 9)

    print(f"Passenger in Elevator {e2.id} presses floor 2")
    building.press_floor_button(e2.id, 2)

    print(f"Passenger in Elevator {e3.id} presses floor 7")
    building.press_floor_button(e3.id, 7)

    _run_steps(building, 6, "Delivering passengers to destinations")

    # ---- 5. LOOK algorithm direction changes ------------------------------
    _print_header("5. LOOK algorithm — serving stops and reversing direction")

    print("Elevator 1: adding stops at floor 10 (up) AND floor 3 (down)")
    print("  -> LOOK will go UP to 10 first, then reverse DOWN to 3\n")
    building.press_floor_button(e1.id, 10)
    building.press_floor_button(e1.id, 3)

    _run_steps(building, 12, "Observe direction changes")

    # ---- 6. Final status --------------------------------------------------
    _print_header("6. Final status")
    for _ in range(20):
        if building.controller.all_idle():
            break
        building.step()

    print(building.status())
    if building.controller.all_idle():
        print("\nAll elevators are IDLE — simulation complete.")
    else:
        print("\nSome elevators are still active.")


if __name__ == "__main__":
    main()
