"""Splitwise-like Expense Sharing System.

Low-level design for splitting expenses among users with support for
equal, exact, and percentage-based splits.  Includes debt simplification
via a greedy settle-up algorithm.

Classes
-------
SplitType          – enum for split strategies
User               – user entity
Split / EqualSplit / ExactSplit / PercentageSplit – split hierarchy
Expense            – a single expense record
Group              – collection of users + their expenses
BalanceSheet       – tracks net balances and simplifies debts
ExpenseManager     – façade that orchestrates the system
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class SplitType(Enum):
    """Supported ways to split an expense."""

    EQUAL = "equal"
    EXACT = "exact"
    PERCENTAGE = "percentage"


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------

@dataclass
class User:
    """A participant in the expense-sharing system."""

    user_id: str
    name: str
    email: str

    def __hash__(self) -> int:
        return hash(self.user_id)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, User):
            return NotImplemented
        return self.user_id == other.user_id

    def __repr__(self) -> str:
        return f"User({self.name!r})"


# ---------------------------------------------------------------------------
# Split hierarchy  (Strategy pattern)
# ---------------------------------------------------------------------------

@dataclass
class Split:
    """Base class for a user's share of an expense."""

    user: User
    amount: float = 0.0

    def compute_amount(self, total: float, num_participants: int) -> float:
        """Return the resolved dollar amount this user owes."""
        return self.amount


@dataclass
class EqualSplit(Split):
    """Split the expense equally among participants."""

    def compute_amount(self, total: float, num_participants: int) -> float:
        return round(total / num_participants, 2)


@dataclass
class ExactSplit(Split):
    """The exact amount this user owes is set explicitly."""

    def compute_amount(self, total: float, num_participants: int) -> float:
        return self.amount


@dataclass
class PercentageSplit(Split):
    """The user owes a given *percentage* of the total."""

    percentage: float = 0.0

    def compute_amount(self, total: float, num_participants: int) -> float:
        return round(total * self.percentage / 100, 2)


# ---------------------------------------------------------------------------
# Split factory
# ---------------------------------------------------------------------------

def create_splits(
    split_type: SplitType,
    participants: list[User],
    total: float,
    split_details: dict[str, float] | None = None,
) -> list[Split]:
    """Build the correct ``Split`` objects for a given split strategy.

    Parameters
    ----------
    split_type:
        How to divide the expense.
    participants:
        Users involved in the split.
    total:
        Full expense amount.
    split_details:
        Mapping of ``user_id`` → value whose meaning depends on
        *split_type* (exact amount **or** percentage).  Ignored for
        ``EQUAL`` splits.

    Returns
    -------
    list[Split]
        One ``Split`` subclass instance per participant.

    Raises
    ------
    ValueError
        If the supplied details don't add up to *total* / 100 %.
    """
    split_details = split_details or {}

    if split_type == SplitType.EQUAL:
        return [EqualSplit(user=u) for u in participants]

    if split_type == SplitType.EXACT:
        splits = [
            ExactSplit(user=u, amount=split_details.get(u.user_id, 0.0))
            for u in participants
        ]
        detail_sum = sum(s.amount for s in splits)
        if abs(detail_sum - total) > 0.01:
            raise ValueError(
                f"Exact split amounts ({detail_sum}) do not equal total ({total})"
            )
        return splits

    if split_type == SplitType.PERCENTAGE:
        splits = [
            PercentageSplit(
                user=u,
                percentage=split_details.get(u.user_id, 0.0),
            )
            for u in participants
        ]
        pct_sum = sum(s.percentage for s in splits)
        if abs(pct_sum - 100.0) > 0.01:
            raise ValueError(
                f"Percentages ({pct_sum}%) do not add up to 100%"
            )
        return splits

    raise ValueError(f"Unknown split type: {split_type}")


# ---------------------------------------------------------------------------
# Expense
# ---------------------------------------------------------------------------

@dataclass
class Expense:
    """A single expense paid by one user and split among many."""

    expense_id: str
    description: str
    amount: float
    paid_by: User
    splits: list[Split]
    split_type: SplitType
    created_at: datetime = field(default_factory=datetime.now)

    def __repr__(self) -> str:
        return (
            f"Expense({self.description!r}, ${self.amount:.2f}, "
            f"paid_by={self.paid_by.name!r})"
        )


# ---------------------------------------------------------------------------
# Group
# ---------------------------------------------------------------------------

class Group:
    """A named collection of users who share expenses."""

    def __init__(self, group_id: str, name: str, members: list[User]) -> None:
        self.group_id = group_id
        self.name = name
        self.members: list[User] = list(members)
        self.expenses: list[Expense] = []

    def add_member(self, user: User) -> None:
        if user not in self.members:
            self.members.append(user)

    def add_expense(
        self,
        description: str,
        amount: float,
        paid_by: User,
        split_type: SplitType,
        split_details: dict[str, float] | None = None,
        participants: list[User] | None = None,
    ) -> Expense:
        """Create and record an expense within this group.

        Parameters
        ----------
        description:
            Short label for the expense.
        amount:
            Total cost.
        paid_by:
            The user who fronted the money.
        split_type:
            How to divide the expense.
        split_details:
            Per-user values (amounts or percentages) keyed by ``user_id``.
        participants:
            Subset of members involved. Defaults to **all** members.

        Returns
        -------
        Expense
            The newly created expense record.
        """
        involved = participants or self.members
        splits = create_splits(split_type, involved, amount, split_details)
        expense = Expense(
            expense_id=str(uuid.uuid4())[:8],
            description=description,
            amount=amount,
            paid_by=paid_by,
            splits=splits,
            split_type=split_type,
        )
        self.expenses.append(expense)
        return expense

    def __repr__(self) -> str:
        return f"Group({self.name!r}, members={len(self.members)})"


# ---------------------------------------------------------------------------
# BalanceSheet
# ---------------------------------------------------------------------------

class BalanceSheet:
    """Tracks pair-wise net balances and can simplify debts.

    Internal storage: ``_balances[a_id][b_id] > 0`` means **a owes b** that
    amount.  The invariant ``_balances[a][b] == -_balances[b][a]`` is always
    maintained.
    """

    def __init__(self) -> None:
        self._balances: dict[str, dict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )

    # -- core mutations ------------------------------------------------

    def add_expense(self, expense: Expense) -> None:
        """Update balances based on an expense's splits."""
        payer = expense.paid_by
        num_participants = len(expense.splits)

        for split in expense.splits:
            owed = split.compute_amount(expense.amount, num_participants)
            if split.user == payer:
                continue
            # split.user owes payer *owed* dollars
            self._balances[split.user.user_id][payer.user_id] += owed
            self._balances[payer.user_id][split.user.user_id] -= owed

    # -- queries -------------------------------------------------------

    def get_balance(self, user1: User, user2: User) -> float:
        """Return how much *user1* owes *user2* (negative ⇒ user2 owes user1)."""
        return self._balances[user1.user_id][user2.user_id]

    def get_all_balances(self, user: User) -> dict[str, float]:
        """Return a dict of ``{other_user_id: amount}`` for *user*.

        Positive values mean *user* owes that person; negative means they
        owe *user*.  Zero-balances are omitted.
        """
        return {
            other_id: amt
            for other_id, amt in self._balances[user.user_id].items()
            if abs(amt) > 0.01
        }

    # -- debt simplification (greedy algorithm) ------------------------

    def simplify_debts(
        self, users: dict[str, User]
    ) -> list[tuple[User, User, float]]:
        """Return a minimal list of transactions to settle all debts.

        Uses a greedy approach: compute each person's **net** balance, then
        repeatedly match the largest creditor with the largest debtor.

        Parameters
        ----------
        users:
            Mapping ``user_id → User`` so we can return ``User`` objects.

        Returns
        -------
        list[tuple[User, User, float]]
            Each tuple is ``(payer, payee, amount)``.
        """
        # Step 1: compute net amounts (positive = net creditor)
        net: dict[str, float] = defaultdict(float)
        seen: set[tuple[str, str]] = set()
        for a_id, others in self._balances.items():
            for b_id, amt in others.items():
                pair = tuple(sorted((a_id, b_id)))
                if pair in seen:
                    continue
                seen.add(pair)
                # amt > 0 means a owes b
                if amt > 0:
                    net[a_id] -= amt  # a is a debtor
                    net[b_id] += amt  # b is a creditor
                else:
                    net[a_id] -= amt  # a is a creditor (amt is negative)
                    net[b_id] += amt  # b is a debtor

        # Step 2: split into debtors (negative net) and creditors (positive net)
        debtors: list[list[Any]] = []
        creditors: list[list[Any]] = []
        for uid, amount in net.items():
            if amount < -0.01:
                debtors.append([uid, -amount])  # store as positive owed
            elif amount > 0.01:
                creditors.append([uid, amount])

        # Step 3: greedily settle
        transactions: list[tuple[User, User, float]] = []
        debtors.sort(key=lambda x: x[1], reverse=True)
        creditors.sort(key=lambda x: x[1], reverse=True)

        i, j = 0, 0
        while i < len(debtors) and j < len(creditors):
            debtor_id, debt = debtors[i]
            creditor_id, credit = creditors[j]
            settle = round(min(debt, credit), 2)
            transactions.append((users[debtor_id], users[creditor_id], settle))
            debtors[i][1] -= settle
            creditors[j][1] -= settle
            if debtors[i][1] < 0.01:
                i += 1
            if creditors[j][1] < 0.01:
                j += 1

        return transactions


# ---------------------------------------------------------------------------
# ExpenseManager  (façade)
# ---------------------------------------------------------------------------

class ExpenseManager:
    """Top-level façade for the expense-sharing system."""

    def __init__(self) -> None:
        self.users: dict[str, User] = {}
        self.groups: dict[str, Group] = {}
        self.balance_sheet = BalanceSheet()

    # -- user management -----------------------------------------------

    def create_user(self, name: str, email: str) -> User:
        """Register a new user and return them."""
        user_id = str(uuid.uuid4())[:8]
        user = User(user_id=user_id, name=name, email=email)
        self.users[user_id] = user
        return user

    # -- group management ----------------------------------------------

    def create_group(self, name: str, members: list[User]) -> Group:
        """Create a new group containing *members*."""
        group_id = str(uuid.uuid4())[:8]
        group = Group(group_id=group_id, name=name, members=members)
        self.groups[group_id] = group
        return group

    # -- expenses ------------------------------------------------------

    def add_expense(
        self,
        group: Group,
        description: str,
        amount: float,
        paid_by: User,
        split_type: SplitType,
        split_details: dict[str, float] | None = None,
        participants: list[User] | None = None,
    ) -> Expense:
        """Add an expense to a group and update the global balance sheet."""
        expense = group.add_expense(
            description=description,
            amount=amount,
            paid_by=paid_by,
            split_type=split_type,
            split_details=split_details,
            participants=participants,
        )
        self.balance_sheet.add_expense(expense)
        return expense

    # -- balance queries -----------------------------------------------

    def show_balances(self, user: User) -> None:
        """Print all non-zero balances for *user*."""
        balances = self.balance_sheet.get_all_balances(user)
        if not balances:
            print(f"  {user.name}: all settled up!")
            return
        for other_id, amount in balances.items():
            other = self.users[other_id]
            if amount > 0:
                print(f"  {user.name} owes {other.name}: ${amount:.2f}")
            else:
                print(f"  {other.name} owes {user.name}: ${-amount:.2f}")

    def settle_up(self) -> list[tuple[User, User, float]]:
        """Simplify all debts and return minimal transactions."""
        return self.balance_sheet.simplify_debts(self.users)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def main() -> None:
    """Run a full simulation demonstrating the expense-sharing system."""
    manager = ExpenseManager()

    # 1. Create users
    print("=" * 60)
    print("1. CREATING USERS")
    print("=" * 60)
    alice = manager.create_user("Alice", "alice@example.com")
    bob = manager.create_user("Bob", "bob@example.com")
    charlie = manager.create_user("Charlie", "charlie@example.com")
    diana = manager.create_user("Diana", "diana@example.com")
    for u in (alice, bob, charlie, diana):
        print(f"  Created {u.name} (id={u.user_id}, email={u.email})")

    # 2. Create group
    print(f"\n{'=' * 60}")
    print("2. CREATING GROUP")
    print("=" * 60)
    trip = manager.create_group("Trip", [alice, bob, charlie, diana])
    print(f"  Group '{trip.name}' with {len(trip.members)} members")

    # 3a. Equal split — Dinner $100 paid by Alice
    print(f"\n{'=' * 60}")
    print("3. ADDING EXPENSES")
    print("=" * 60)

    e1 = manager.add_expense(
        group=trip,
        description="Dinner",
        amount=100.0,
        paid_by=alice,
        split_type=SplitType.EQUAL,
    )
    print(f"\n  [EQUAL] {e1}")
    print(f"    Each person owes: ${100.0 / 4:.2f}")

    # 3b. Exact split — Taxi $60 paid by Bob
    e2 = manager.add_expense(
        group=trip,
        description="Taxi",
        amount=60.0,
        paid_by=bob,
        split_type=SplitType.EXACT,
        split_details={
            bob.user_id: 0.0,
            alice.user_id: 20.0,
            charlie.user_id: 25.0,
            diana.user_id: 15.0,
        },
    )
    print(f"\n  [EXACT] {e2}")
    print(f"    Bob: $0, Alice: $20, Charlie: $25, Diana: $15")

    # 3c. Percentage split — Hotel $200 paid by Charlie (25% each)
    e3 = manager.add_expense(
        group=trip,
        description="Hotel",
        amount=200.0,
        paid_by=charlie,
        split_type=SplitType.PERCENTAGE,
        split_details={
            alice.user_id: 25.0,
            bob.user_id: 25.0,
            charlie.user_id: 25.0,
            diana.user_id: 25.0,
        },
    )
    print(f"\n  [PERCENTAGE] {e3}")
    print(f"    Each person owes 25% = ${200.0 * 0.25:.2f}")

    # 4. Show individual balances
    print(f"\n{'=' * 60}")
    print("4. INDIVIDUAL BALANCES")
    print("=" * 60)
    for user in (alice, bob, charlie, diana):
        print(f"\n  --- {user.name} ---")
        manager.show_balances(user)

    # 5. Simplify debts
    print(f"\n{'=' * 60}")
    print("5. SIMPLIFIED DEBTS (minimal transactions)")
    print("=" * 60)
    transactions = manager.settle_up()
    if not transactions:
        print("  Everyone is settled up!")
    for payer, payee, amount in transactions:
        print(f"  {payer.name} pays {payee.name}: ${amount:.2f}")

    print(f"\n{'=' * 60}")
    print(f"Total expenses in group '{trip.name}': {len(trip.expenses)}")
    print(f"Transactions needed to settle: {len(transactions)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
