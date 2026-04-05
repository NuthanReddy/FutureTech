# State-machine DP (Buy/Sell Stock)

## Pattern: How to Identify

Use state-machine DP when:
- each day/step has a small finite state
- actions trigger state transitions with reward/cost
- future choices depend on current state, not full history

Typical clues:
- stock buy/sell with constraints (cooldown, fee, max transactions)
- "can buy" vs "holding stock" states

## Base DP Values

For stock with cooldown:
- after last day, profit is `0`

Memoized DFS base:
- if `day >= n`, return `0`

Tabulation states:
- `hold`: best profit while holding stock
- `sold`: best profit just sold today
- `rest`: best profit while not holding and not selling today

Initial values:
- `hold = -inf`, `sold = -inf`, `rest = 0`

## Recurrence (Carry Forward)

Memoized transitions:
- if can buy:
  - buy now: `-price[day] + dfs(day+1, holding)`
  - skip: `dfs(day+1, can_buy)`
- if holding:
  - sell now (cooldown next day): `price[day] + dfs(day+2, can_buy)`
  - hold: `dfs(day+1, holding)`

Tabulation transitions per day price:
- `new_sold = hold + price`
- `new_hold = max(hold, rest - price)`
- `new_rest = max(rest, sold)`

## Break / Termination Condition

Memoization ends when day index passes final day.
Tabulation ends after final day is processed.

## Memoization vs Tabulation Tradeoffs

- Memoization:
  - very intuitive for action choices and cooldown jump
  - easy to encode custom constraints
  - recursion overhead
- Tabulation:
  - constant memory for fixed state machine
  - efficient for large inputs
  - transition ordering must be correct each day

## Example Problem

**Best Time to Buy and Sell Stock with Cooldown**

You may complete as many transactions as you want, but after selling a stock you cannot buy on the next day.

See `solution.py` and `demo.py`.

