"""Car Fleet.

Problem: Cars travel toward one target on a one-lane road. A faster car
cannot pass a slower car ahead; cars that meet continue as one fleet. Count
the fleets that arrive at the target.

Input: ``target`` destination, ``position`` starting positions, and
``speed`` values paired by index.
Output: the number of fleets reaching the target.
Constraints: positions are distinct and normally lie before the target;
speeds are positive; ``position`` and ``speed`` must have equal lengths.
"""

from __future__ import annotations


def car_fleet(target: int, position: list[int], speed: list[int]) -> int:
    """Return the number of fleets that arrive at *target*.

    Cars are sorted from closest to the target to farthest.  A car behind a
    fleet cannot pass it, so it joins that fleet whenever its arrival time is
    no greater than the fleet's time.  Otherwise it forms a new barrier/fleet.

    Complexity: O(n log n) time for sorting and O(n) extra space.
    """
    if len(position) != len(speed):
        raise ValueError("position and speed must have equal lengths")
    if target < 0:
        raise ValueError("target must be non-negative")

    cars = sorted(zip(position, speed), reverse=True)
    fleets = 0
    slowest_arrival = 0.0
    for car_position, car_speed in cars:
        if car_speed <= 0:
            raise ValueError("car speeds must be positive")
        arrival = (target - car_position) / car_speed
        if arrival > slowest_arrival:
            fleets += 1
            slowest_arrival = arrival
    return fleets


if __name__ == "__main__":
    assert car_fleet(12, [10, 8, 0, 5, 3], [2, 4, 1, 1, 3]) == 3
    assert car_fleet(10, [3], [3]) == 1
    assert car_fleet(10, [], []) == 0
    print("Car Fleet: all checks passed")
