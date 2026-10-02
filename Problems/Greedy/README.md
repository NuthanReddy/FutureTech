# Greedy

## Problem list

| Problem | Recognition cue -> greedy choice | Time | Extra space |
| --- | --- | --- | --- |
| [Can Place Flowers](https://leetcode.com/problems/can-place-flowers/) | Non-adjacent planting target -> plant at the earliest empty plot whose neighbors are empty. | O(n) | O(1) |

## Pattern identification steps

1. Identify the goal: maximize/minimize a result, or meet a target.
2. Find a local choice: earliest finish, smallest cost, or earliest valid position.
3. Check feasibility: the choice must respect constraints and previous choices.
4. Prove safety: replacing an optimal solution's choice with yours cannot worsen it.
5. Track only necessary state; if choices require comparing future outcomes, consider DP/backtracking.

**Can Place Flowers:** Scan left to right, planting whenever the current plot and
both neighbors are empty (missing neighbors count as empty). Record each planting
so later choices respect it; succeed once the target is met, including target zero.
Choosing the earliest valid plot leaves at least as much room to its right as
delaying that planting, so it cannot reduce the maximum count.
