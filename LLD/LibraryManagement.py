"""Library Management System — Low-Level Design.

Demonstrates clean OOP modelling for a common LLD interview question.
Covers: catalog search, borrowing / returning with due-dates, fine
calculation, reservations, and availability notifications.
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from datetime import date, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class BookStatus(Enum):
    """Lifecycle states of a physical book copy."""

    AVAILABLE = auto()
    CHECKED_OUT = auto()
    RESERVED = auto()
    LOST = auto()


class Genre(Enum):
    """Supported book genres."""

    FICTION = auto()
    NON_FICTION = auto()
    SCIENCE = auto()
    HISTORY = auto()
    TECHNOLOGY = auto()
    BIOGRAPHY = auto()
    PHILOSOPHY = auto()


# ---------------------------------------------------------------------------
# Core domain models
# ---------------------------------------------------------------------------

class Book:
    """Represents a book title (not a physical copy)."""

    def __init__(
        self,
        isbn: str,
        title: str,
        author: str,
        genre: Genre,
        publication_year: int,
    ) -> None:
        self.isbn = isbn
        self.title = title
        self.author = author
        self.genre = genre
        self.publication_year = publication_year

    def __repr__(self) -> str:
        return (
            f"Book(isbn={self.isbn!r}, title={self.title!r}, "
            f"author={self.author!r}, genre={self.genre.name})"
        )


class BookItem:
    """A physical copy of a *Book*, identified by a unique barcode."""

    def __init__(self, barcode: str, book: Book) -> None:
        self.barcode = barcode
        self.book = book
        self.status: BookStatus = BookStatus.AVAILABLE
        self.due_date: Optional[date] = None
        self.borrowed_by: Optional[Member] = None

    def checkout(self, member: Member, loan_days: int = 14) -> None:
        """Mark this copy as checked out by *member*."""
        self.status = BookStatus.CHECKED_OUT
        self.due_date = date.today() + timedelta(days=loan_days)
        self.borrowed_by = member

    def return_item(self) -> None:
        """Reset this copy back to available."""
        self.status = BookStatus.AVAILABLE
        self.due_date = None
        self.borrowed_by = None

    def is_overdue(self, return_date: Optional[date] = None) -> bool:
        """Check if the copy is overdue relative to *return_date*."""
        if self.due_date is None:
            return False
        check = return_date or date.today()
        return check > self.due_date

    def days_overdue(self, return_date: Optional[date] = None) -> int:
        """Number of days past the due date."""
        if not self.is_overdue(return_date):
            return 0
        check = return_date or date.today()
        return (check - self.due_date).days

    def __repr__(self) -> str:
        return (
            f"BookItem(barcode={self.barcode!r}, "
            f"title={self.book.title!r}, status={self.status.name})"
        )


class Member:
    """A registered library member."""

    MAX_BOOKS = 5

    def __init__(self, member_id: str, name: str, email: str) -> None:
        self.member_id = member_id
        self.name = name
        self.email = email
        self.borrowed_books: List[BookItem] = []
        self.reservations: List[Reservation] = []
        self.notifications: List[str] = []

    def can_borrow(self) -> bool:
        """Return *True* if the member hasn't reached the borrow limit."""
        return len(self.borrowed_books) < self.MAX_BOOKS

    def notify(self, message: str) -> None:
        """Send a notification to this member (stored in-memory)."""
        self.notifications.append(message)
        print(f"  [NOTIFICATION -> {self.name}] {message}")

    def __repr__(self) -> str:
        return (
            f"Member(id={self.member_id!r}, name={self.name!r}, "
            f"borrowed={len(self.borrowed_books)})"
        )


class Reservation:
    """A hold placed on a book that currently has no available copies."""

    def __init__(
        self,
        reservation_id: str,
        member: Member,
        book: Book,
    ) -> None:
        self.reservation_id = reservation_id
        self.member = member
        self.book = book
        self.created_at: date = date.today()
        self.fulfilled: bool = False

    def __repr__(self) -> str:
        return (
            f"Reservation(id={self.reservation_id!r}, "
            f"member={self.member.name!r}, book={self.book.title!r}, "
            f"fulfilled={self.fulfilled})"
        )


class Fine:
    """A fine levied for a late return."""

    RATE_PER_DAY = 1.0  # currency units per overdue day

    def __init__(
        self,
        member: Member,
        book_item: BookItem,
        days_overdue: int,
    ) -> None:
        self.member = member
        self.book_item = book_item
        self.days_overdue = days_overdue
        self.amount: float = days_overdue * self.RATE_PER_DAY
        self.paid: bool = False

    def pay(self) -> None:
        self.paid = True

    def __repr__(self) -> str:
        return (
            f"Fine(member={self.member.name!r}, "
            f"book={self.book_item.book.title!r}, "
            f"days={self.days_overdue}, amount={self.amount:.2f}, "
            f"paid={self.paid})"
        )


# ---------------------------------------------------------------------------
# Catalog — search layer
# ---------------------------------------------------------------------------

class Catalog:
    """Maintains the collection of books and their physical copies."""

    def __init__(self) -> None:
        self.books: Dict[str, Book] = {}                    # isbn -> Book
        self.items: Dict[str, List[BookItem]] = defaultdict(list)  # isbn -> copies

    def add_book(self, book: Book, num_copies: int = 1) -> List[BookItem]:
        """Register a book and create *num_copies* physical copies."""
        self.books[book.isbn] = book
        created: List[BookItem] = []
        for _ in range(num_copies):
            barcode = f"{book.isbn}-{uuid.uuid4().hex[:6]}"
            item = BookItem(barcode=barcode, book=book)
            self.items[book.isbn].append(item)
            created.append(item)
        return created

    # -- search helpers (case-insensitive substring match) ------------------

    def search_by_title(self, query: str) -> List[Book]:
        q = query.lower()
        return [b for b in self.books.values() if q in b.title.lower()]

    def search_by_author(self, query: str) -> List[Book]:
        q = query.lower()
        return [b for b in self.books.values() if q in b.author.lower()]

    def search_by_isbn(self, isbn: str) -> Optional[Book]:
        return self.books.get(isbn)

    def search_by_genre(self, genre: Genre) -> List[Book]:
        return [b for b in self.books.values() if b.genre == genre]

    def available_copies(self, isbn: str) -> List[BookItem]:
        """Return copies of *isbn* that are currently available."""
        return [
            item for item in self.items.get(isbn, [])
            if item.status == BookStatus.AVAILABLE
        ]

    def find_item_by_barcode(self, barcode: str) -> Optional[BookItem]:
        """Look up a single physical copy by its barcode."""
        for copies in self.items.values():
            for item in copies:
                if item.barcode == barcode:
                    return item
        return None


# ---------------------------------------------------------------------------
# Library — façade / main controller
# ---------------------------------------------------------------------------

class Library:
    """Top-level façade that coordinates all library operations."""

    LOAN_DAYS = 14

    def __init__(self, name: str) -> None:
        self.name = name
        self.catalog = Catalog()
        self.members: Dict[str, Member] = {}
        self.fines: List[Fine] = []
        self.reservations: Dict[str, List[Reservation]] = defaultdict(list)  # isbn -> queue
        self._member_counter = 0

    # -- member management --------------------------------------------------

    def register_member(self, name: str, email: str) -> Member:
        """Create and return a new library member."""
        self._member_counter += 1
        member_id = f"M{self._member_counter:04d}"
        member = Member(member_id=member_id, name=name, email=email)
        self.members[member_id] = member
        return member

    def _get_member(self, member_id: str) -> Member:
        member = self.members.get(member_id)
        if member is None:
            raise ValueError(f"No member found with id {member_id!r}")
        return member

    # -- checkout / return --------------------------------------------------

    def checkout(self, member_id: str, isbn: str) -> BookItem:
        """Check out an available copy of *isbn* to the member.

        Raises:
            ValueError: if the member cannot borrow or no copies are free.
        """
        member = self._get_member(member_id)

        if not member.can_borrow():
            raise ValueError(
                f"{member.name} has reached the maximum borrow limit "
                f"({Member.MAX_BOOKS})."
            )

        copies = self.catalog.available_copies(isbn)
        if not copies:
            raise ValueError(
                f"No available copies for ISBN {isbn!r}. "
                "Consider placing a reservation."
            )

        book_item = copies[0]
        book_item.checkout(member, loan_days=self.LOAN_DAYS)
        member.borrowed_books.append(book_item)
        return book_item

    def return_book(
        self,
        member_id: str,
        barcode: str,
        return_date: Optional[date] = None,
    ) -> Optional[Fine]:
        """Return a book copy and calculate a fine if overdue.

        Args:
            return_date: override for testing late returns.

        Returns:
            A *Fine* object when the return is late, otherwise *None*.
        """
        member = self._get_member(member_id)
        book_item = self.catalog.find_item_by_barcode(barcode)
        if book_item is None:
            raise ValueError(f"No book item with barcode {barcode!r}")
        if book_item not in member.borrowed_books:
            raise ValueError(
                f"Book item {barcode!r} is not borrowed by {member.name!r}"
            )

        # Calculate fine before resetting the item
        fine: Optional[Fine] = None
        overdue = book_item.days_overdue(return_date)
        if overdue > 0:
            fine = Fine(member=member, book_item=book_item, days_overdue=overdue)
            self.fines.append(fine)

        member.borrowed_books.remove(book_item)
        book_item.return_item()

        # Fulfil pending reservation if any
        self._notify_reservation(book_item)

        return fine

    # -- reservations -------------------------------------------------------

    def reserve(self, member_id: str, isbn: str) -> Reservation:
        """Place a reservation on *isbn* when no copies are available.

        Raises:
            ValueError: if available copies exist (should checkout instead).
        """
        member = self._get_member(member_id)
        book = self.catalog.search_by_isbn(isbn)
        if book is None:
            raise ValueError(f"ISBN {isbn!r} not found in catalog.")

        if self.catalog.available_copies(isbn):
            raise ValueError(
                f"Copies of {book.title!r} are available — "
                "please checkout instead of reserving."
            )

        reservation_id = f"R-{uuid.uuid4().hex[:8]}"
        reservation = Reservation(
            reservation_id=reservation_id,
            member=member,
            book=book,
        )
        self.reservations[isbn].append(reservation)
        member.reservations.append(reservation)
        return reservation

    def _notify_reservation(self, book_item: BookItem) -> None:
        """When a copy is returned, fulfil the oldest pending reservation."""
        isbn = book_item.book.isbn
        queue = self.reservations.get(isbn, [])

        # Find the first unfulfilled reservation
        for reservation in queue:
            if not reservation.fulfilled:
                reservation.fulfilled = True
                book_item.status = BookStatus.RESERVED
                reservation.member.notify(
                    f"'{book_item.book.title}' is now available for you! "
                    f"(barcode: {book_item.barcode})"
                )
                return

    # -- display helpers ----------------------------------------------------

    def display_catalog(self) -> None:
        """Print the full catalog with copy availability."""
        print(f"\n{'=' * 60}")
        print(f"  Catalog of '{self.name}'")
        print(f"{'=' * 60}")
        for isbn, book in self.catalog.books.items():
            copies = self.catalog.items[isbn]
            available = sum(
                1 for c in copies if c.status == BookStatus.AVAILABLE
            )
            print(
                f"  [{isbn}] {book.title} by {book.author} "
                f"({book.genre.name}) — "
                f"{available}/{len(copies)} available"
            )
        print()

    def display_member_info(self, member_id: str) -> None:
        """Print details about a member's account."""
        member = self._get_member(member_id)
        print(f"\n--- Member: {member.name} ({member.member_id}) ---")
        print(f"  Email       : {member.email}")
        print(f"  Borrowed    : {len(member.borrowed_books)}/{Member.MAX_BOOKS}")
        for bi in member.borrowed_books:
            print(f"    * {bi.book.title} [barcode={bi.barcode}, due={bi.due_date}]")
        if member.reservations:
            print("  Reservations:")
            for r in member.reservations:
                status = "fulfilled" if r.fulfilled else "pending"
                print(f"    * {r.book.title} ({status})")
        if member.notifications:
            print("  Notifications:")
            for n in member.notifications:
                print(f"    * {n}")
        # Show unpaid fines
        member_fines = [
            f for f in self.fines
            if f.member.member_id == member_id and not f.paid
        ]
        if member_fines:
            print("  Unpaid fines:")
            for f in member_fines:
                print(
                    f"    * {f.book_item.book.title}: "
                    f"${f.amount:.2f} ({f.days_overdue} days overdue)"
                )
        print()


# ---------------------------------------------------------------------------
# Demo / simulation
# ---------------------------------------------------------------------------

def main() -> None:
    """Run a full simulation demonstrating the Library Management System."""
    lib = Library(name="City Central Library")

    # 1. Populate catalog ---------------------------------------------------
    print("=== 1. Adding books to catalog ===")
    books_data = [
        Book("978-0-13-468599-1", "The Pragmatic Programmer", "David Thomas", Genre.TECHNOLOGY, 2019),
        Book("978-0-06-112008-4", "To Kill a Mockingbird", "Harper Lee", Genre.FICTION, 1960),
        Book("978-0-452-28423-4", "1984", "George Orwell", Genre.FICTION, 1949),
        Book("978-0-07-246357-5", "A Brief History of Time", "Stephen Hawking", Genre.SCIENCE, 1988),
        Book("978-0-14-028329-7", "The Republic", "Plato", Genre.PHILOSOPHY, -380),
    ]
    for book in books_data:
        copies = 3 if book.genre == Genre.FICTION else 2
        lib.catalog.add_book(book, num_copies=copies)
        print(f"  Added '{book.title}' — {copies} copies")

    lib.display_catalog()

    # 2. Register members ---------------------------------------------------
    print("=== 2. Registering members ===")
    alice = lib.register_member("Alice Johnson", "alice@example.com")
    bob = lib.register_member("Bob Smith", "bob@example.com")
    carol = lib.register_member("Carol White", "carol@example.com")
    print(f"  Registered: {alice}, {bob}, {carol}\n")

    # 3. Search catalog -----------------------------------------------------
    print("=== 3. Searching catalog ===")
    print(f"  By title 'pragmatic' : {lib.catalog.search_by_title('pragmatic')}")
    print(f"  By author 'orwell'   : {lib.catalog.search_by_author('orwell')}")
    print(f"  By genre FICTION      : {lib.catalog.search_by_genre(Genre.FICTION)}")
    print(f"  By ISBN '978-0-14…'  : {lib.catalog.search_by_isbn('978-0-14-028329-7')}")
    print()

    # 4. Checkout books -----------------------------------------------------
    print("=== 4. Checking out books ===")
    isbn_1984 = "978-0-452-28423-4"
    isbn_pragmatic = "978-0-13-468599-1"

    item1 = lib.checkout(alice.member_id, isbn_1984)
    print(f"  Alice checked out: {item1}")
    item2 = lib.checkout(bob.member_id, isbn_1984)
    print(f"  Bob checked out  : {item2}")
    item3 = lib.checkout(carol.member_id, isbn_1984)
    print(f"  Carol checked out: {item3}")
    item4 = lib.checkout(alice.member_id, isbn_pragmatic)
    print(f"  Alice checked out: {item4}")
    print()

    # 5. Exceed borrow limit ------------------------------------------------
    print("=== 5. Testing borrow limit ===")
    isbn_mockingbird = "978-0-06-112008-4"
    isbn_history = "978-0-07-246357-5"
    isbn_republic = "978-0-14-028329-7"

    lib.checkout(alice.member_id, isbn_mockingbird)
    lib.checkout(alice.member_id, isbn_history)
    lib.checkout(alice.member_id, isbn_republic)
    print(f"  Alice now has {len(alice.borrowed_books)} books (limit={Member.MAX_BOOKS})")
    try:
        lib.checkout(alice.member_id, isbn_mockingbird)
    except ValueError as exc:
        print(f"  Borrow rejected: {exc}")
    print()

    # 6. Late return -> fine -------------------------------------------------
    print("=== 6. Returning a book late ===")
    late_date = date.today() + timedelta(days=20)  # 6 days overdue
    fine = lib.return_book(
        alice.member_id,
        item1.barcode,
        return_date=late_date,
    )
    if fine:
        print(f"  Fine issued: {fine}")
    else:
        print("  No fine — returned on time.")
    print()

    # 7. Reserve a book (all copies checked out) ----------------------------
    print("=== 7. Reserving a fully checked-out book ===")
    # All 3 copies of 1984 were checked out; Alice returned hers but it may
    # have gone to a reservation. Let's check out the returned copy so all
    # are taken, then reserve.
    available_1984 = lib.catalog.available_copies(isbn_1984)
    for copy in available_1984:
        # Check out remaining copies to exhaust availability
        lib.checkout(alice.member_id, isbn_1984)

    # Now Alice wants to reserve it again if none are available
    available_1984 = lib.catalog.available_copies(isbn_1984)
    if not available_1984:
        reservation = lib.reserve(alice.member_id, isbn_1984)
        print(f"  Reservation placed: {reservation}")
    else:
        print("  Copies still available — no reservation needed.")
    print()

    # 8. Return triggers reservation notification ---------------------------
    print("=== 8. Returning book -> reservation fulfilled ===")
    fine2 = lib.return_book(bob.member_id, item2.barcode)
    if fine2:
        print(f"  Fine issued: {fine2}")
    else:
        print("  No fine for Bob.")
    print()

    # 9. Final state --------------------------------------------------------
    print("=== 9. Final state ===")
    lib.display_catalog()
    lib.display_member_info(alice.member_id)
    lib.display_member_info(bob.member_id)
    lib.display_member_info(carol.member_id)

    print("=== Fines ledger ===")
    for f in lib.fines:
        print(f"  {f}")
    print("\nDone.")


if __name__ == "__main__":
    main()

