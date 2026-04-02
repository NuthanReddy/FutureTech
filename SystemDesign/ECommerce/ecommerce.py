"""
E-Commerce Platform (Amazon-like) - Core Implementation

Demonstrates:
- Product catalog with search
- Shopping cart management (session + persistent)
- Inventory service with reservation and TTL
- Checkout service using saga-like orchestration
- Order lifecycle management

Architectural patterns used:
- Saga pattern for checkout (reserve -> pay -> confirm, with compensations)
- CQRS-inspired search (separate index from source of truth)
- Event-driven inventory updates
"""

from __future__ import annotations

import uuid
import time
import threading
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Domain enums
# ---------------------------------------------------------------------------

class OrderStatus(Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


class PaymentStatus(Enum):
    PENDING = "PENDING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"


class ReservationStatus(Enum):
    ACTIVE = "ACTIVE"
    CONFIRMED = "CONFIRMED"
    RELEASED = "RELEASED"


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

@dataclass
class Product:
    product_id: str
    title: str
    description: str
    price: float
    category: str
    brand: str
    stock: int = 0
    avg_rating: float = 0.0
    review_count: int = 0
    attributes: dict = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"Product({self.title}, ${self.price:.2f}, stock={self.stock})"


@dataclass
class CartItem:
    product_id: str
    quantity: int
    price_at_add: float


@dataclass
class Cart:
    user_id: str
    items: list[CartItem] = field(default_factory=list)
    updated_at: float = field(default_factory=time.time)

    @property
    def total(self) -> float:
        return sum(item.price_at_add * item.quantity for item in self.items)

    @property
    def item_count(self) -> int:
        return sum(item.quantity for item in self.items)


@dataclass
class OrderItem:
    product_id: str
    quantity: int
    unit_price: float

    @property
    def subtotal(self) -> float:
        return self.unit_price * self.quantity


@dataclass
class Order:
    order_id: str
    user_id: str
    items: list[OrderItem]
    total_amount: float
    status: OrderStatus = OrderStatus.PENDING
    payment_status: PaymentStatus = PaymentStatus.PENDING
    created_at: float = field(default_factory=time.time)

    def __repr__(self) -> str:
        return (
            f"Order({self.order_id[:8]}.. "
            f"status={self.status.value}, "
            f"${self.total_amount:.2f})"
        )


@dataclass
class InventoryReservation:
    reservation_id: str
    product_id: str
    order_id: str
    quantity: int
    status: ReservationStatus = ReservationStatus.ACTIVE
    expires_at: float = 0.0


@dataclass
class Review:
    product_id: str
    user_id: str
    rating: int  # 1-5
    text: str
    created_at: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Product Catalog Service
# ---------------------------------------------------------------------------

class ProductCatalog:
    """In-memory product catalog with basic CRUD and search index."""

    def __init__(self) -> None:
        self._products: dict[str, Product] = {}
        # Simple inverted index: token -> set of product_ids
        self._search_index: dict[str, set[str]] = {}

    def add_product(self, product: Product) -> Product:
        self._products[product.product_id] = product
        self._index_product(product)
        return product

    def get_product(self, product_id: str) -> Optional[Product]:
        return self._products.get(product_id)

    def update_product(self, product_id: str, **kwargs) -> Optional[Product]:
        product = self._products.get(product_id)
        if not product:
            return None
        for key, value in kwargs.items():
            if hasattr(product, key):
                setattr(product, key, value)
        self._index_product(product)
        return product

    def search(
        self,
        query: str = "",
        category: Optional[str] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        sort_by: str = "relevance",
    ) -> list[Product]:
        """CQRS-inspired search using the inverted index."""
        if query:
            tokens = query.lower().split()
            candidate_ids: Optional[set[str]] = None
            for token in tokens:
                matching = set()
                for index_token, pids in self._search_index.items():
                    if token in index_token:
                        matching |= pids
                if candidate_ids is None:
                    candidate_ids = matching
                else:
                    candidate_ids &= matching
            results = [
                self._products[pid]
                for pid in (candidate_ids or set())
                if pid in self._products
            ]
        else:
            results = list(self._products.values())

        # Apply filters
        if category:
            results = [p for p in results if p.category.lower() == category.lower()]
        if min_price is not None:
            results = [p for p in results if p.price >= min_price]
        if max_price is not None:
            results = [p for p in results if p.price <= max_price]

        # Sort
        if sort_by == "price_asc":
            results.sort(key=lambda p: p.price)
        elif sort_by == "price_desc":
            results.sort(key=lambda p: p.price, reverse=True)
        elif sort_by == "rating":
            results.sort(key=lambda p: p.avg_rating, reverse=True)

        return results

    def _index_product(self, product: Product) -> None:
        """Build a simple inverted index for full-text search."""
        tokens = set()
        for text in [product.title, product.description, product.category, product.brand]:
            tokens.update(text.lower().split())
        for token in tokens:
            self._search_index.setdefault(token, set()).add(product.product_id)


# ---------------------------------------------------------------------------
# Cart Service
# ---------------------------------------------------------------------------

class CartService:
    """Cart management with session (guest) and persistent (logged-in) carts."""

    def __init__(self, catalog: ProductCatalog) -> None:
        self._carts: dict[str, Cart] = {}
        self._catalog = catalog

    def get_cart(self, user_id: str) -> Cart:
        if user_id not in self._carts:
            self._carts[user_id] = Cart(user_id=user_id)
        return self._carts[user_id]

    def add_item(self, user_id: str, product_id: str, quantity: int = 1) -> Cart:
        product = self._catalog.get_product(product_id)
        if not product:
            raise ValueError(f"Product {product_id} not found")
        if quantity <= 0:
            raise ValueError("Quantity must be positive")

        cart = self.get_cart(user_id)

        # Update quantity if item already in cart
        for item in cart.items:
            if item.product_id == product_id:
                item.quantity += quantity
                cart.updated_at = time.time()
                return cart

        cart.items.append(
            CartItem(product_id=product_id, quantity=quantity, price_at_add=product.price)
        )
        cart.updated_at = time.time()
        return cart

    def update_item_quantity(self, user_id: str, product_id: str, quantity: int) -> Cart:
        cart = self.get_cart(user_id)
        for item in cart.items:
            if item.product_id == product_id:
                if quantity <= 0:
                    cart.items.remove(item)
                else:
                    item.quantity = quantity
                cart.updated_at = time.time()
                return cart
        raise ValueError(f"Product {product_id} not in cart")

    def remove_item(self, user_id: str, product_id: str) -> Cart:
        return self.update_item_quantity(user_id, product_id, 0)

    def clear_cart(self, user_id: str) -> Cart:
        cart = self.get_cart(user_id)
        cart.items.clear()
        cart.updated_at = time.time()
        return cart

    def merge_carts(self, guest_id: str, user_id: str) -> Cart:
        """Merge guest session cart into the persistent user cart on login."""
        guest_cart = self._carts.get(guest_id)
        if not guest_cart or not guest_cart.items:
            return self.get_cart(user_id)

        user_cart = self.get_cart(user_id)
        user_products = {item.product_id: item for item in user_cart.items}

        for guest_item in guest_cart.items:
            if guest_item.product_id in user_products:
                existing = user_products[guest_item.product_id]
                existing.quantity = max(existing.quantity, guest_item.quantity)
            else:
                user_cart.items.append(guest_item)

        user_cart.updated_at = time.time()
        del self._carts[guest_id]
        return user_cart


# ---------------------------------------------------------------------------
# Inventory Service
# ---------------------------------------------------------------------------

class InventoryService:
    """Inventory management with reservation support and TTL-based expiry."""

    DEFAULT_RESERVATION_TTL = 600  # 10 minutes

    def __init__(self) -> None:
        self._stock: dict[str, int] = {}           # product_id -> total_stock
        self._reserved: dict[str, int] = {}         # product_id -> reserved_stock
        self._reservations: dict[str, InventoryReservation] = {}
        self._lock = threading.Lock()

    def set_stock(self, product_id: str, quantity: int) -> None:
        with self._lock:
            self._stock[product_id] = quantity
            self._reserved.setdefault(product_id, 0)

    def get_available(self, product_id: str) -> int:
        with self._lock:
            total = self._stock.get(product_id, 0)
            reserved = self._reserved.get(product_id, 0)
            return total - reserved

    def reserve(
        self, product_id: str, order_id: str, quantity: int, ttl: Optional[float] = None
    ) -> InventoryReservation:
        """Atomically reserve stock. Raises ValueError if insufficient."""
        ttl = ttl or self.DEFAULT_RESERVATION_TTL
        with self._lock:
            available = self._stock.get(product_id, 0) - self._reserved.get(product_id, 0)
            if available < quantity:
                raise ValueError(
                    f"Insufficient stock for {product_id}: "
                    f"available={available}, requested={quantity}"
                )
            self._reserved[product_id] = self._reserved.get(product_id, 0) + quantity
            reservation = InventoryReservation(
                reservation_id=str(uuid.uuid4()),
                product_id=product_id,
                order_id=order_id,
                quantity=quantity,
                expires_at=time.time() + ttl,
            )
            self._reservations[reservation.reservation_id] = reservation
            return reservation

    def confirm_reservation(self, reservation_id: str) -> None:
        """Convert reservation to confirmed: deduct from total stock."""
        with self._lock:
            res = self._reservations.get(reservation_id)
            if not res or res.status != ReservationStatus.ACTIVE:
                raise ValueError(f"Reservation {reservation_id} not active")
            res.status = ReservationStatus.CONFIRMED
            self._stock[res.product_id] -= res.quantity
            self._reserved[res.product_id] -= res.quantity

    def release_reservation(self, reservation_id: str) -> None:
        """Release a reservation, making stock available again."""
        with self._lock:
            res = self._reservations.get(reservation_id)
            if not res or res.status != ReservationStatus.ACTIVE:
                return  # already released or confirmed
            res.status = ReservationStatus.RELEASED
            self._reserved[res.product_id] -= res.quantity

    def cleanup_expired(self) -> int:
        """Release all expired reservations. Returns count of released."""
        now = time.time()
        released = 0
        with self._lock:
            for res in list(self._reservations.values()):
                if res.status == ReservationStatus.ACTIVE and res.expires_at < now:
                    res.status = ReservationStatus.RELEASED
                    self._reserved[res.product_id] -= res.quantity
                    released += 1
        return released

    def get_stock_info(self, product_id: str) -> dict:
        with self._lock:
            total = self._stock.get(product_id, 0)
            reserved = self._reserved.get(product_id, 0)
            return {
                "product_id": product_id,
                "total_stock": total,
                "reserved_stock": reserved,
                "available_stock": total - reserved,
            }


# ---------------------------------------------------------------------------
# Payment Service (simulated)
# ---------------------------------------------------------------------------

class PaymentService:
    """Simulated payment processing."""

    def __init__(self) -> None:
        self._payments: dict[str, dict] = {}

    def process_payment(self, order_id: str, amount: float, method: str = "CREDIT_CARD") -> dict:
        payment_id = str(uuid.uuid4())
        # Simulate payment: succeed if amount < 10000, fail otherwise (for demo)
        success = amount < 10000
        payment = {
            "payment_id": payment_id,
            "order_id": order_id,
            "amount": amount,
            "method": method,
            "status": PaymentStatus.COMPLETED if success else PaymentStatus.FAILED,
            "transaction_ref": f"TXN-{payment_id[:8]}" if success else None,
        }
        self._payments[payment_id] = payment
        return payment

    def refund(self, payment_id: str) -> dict:
        payment = self._payments.get(payment_id)
        if not payment:
            raise ValueError(f"Payment {payment_id} not found")
        payment["status"] = PaymentStatus.REFUNDED
        return payment


# ---------------------------------------------------------------------------
# Review Service
# ---------------------------------------------------------------------------

class ReviewService:
    """Simple review system with aggregate rating updates."""

    def __init__(self, catalog: ProductCatalog) -> None:
        self._reviews: dict[str, list[Review]] = {}
        self._catalog = catalog

    def add_review(self, product_id: str, user_id: str, rating: int, text: str) -> Review:
        if not 1 <= rating <= 5:
            raise ValueError("Rating must be between 1 and 5")
        product = self._catalog.get_product(product_id)
        if not product:
            raise ValueError(f"Product {product_id} not found")

        review = Review(product_id=product_id, user_id=user_id, rating=rating, text=text)
        self._reviews.setdefault(product_id, []).append(review)

        # Update aggregate rating
        all_reviews = self._reviews[product_id]
        product.avg_rating = round(
            sum(r.rating for r in all_reviews) / len(all_reviews), 1
        )
        product.review_count = len(all_reviews)
        return review

    def get_reviews(self, product_id: str) -> list[Review]:
        return self._reviews.get(product_id, [])


# ---------------------------------------------------------------------------
# Checkout Service (Saga Orchestrator)
# ---------------------------------------------------------------------------

class CheckoutService:
    """
    Orchestrates the checkout saga:
      1. Reserve inventory
      2. Process payment
      3. Confirm order (finalize inventory)
    Each step has a compensating action on failure.
    """

    def __init__(
        self,
        catalog: ProductCatalog,
        cart_service: CartService,
        inventory_service: InventoryService,
        payment_service: PaymentService,
    ) -> None:
        self._catalog = catalog
        self._cart_service = cart_service
        self._inventory = inventory_service
        self._payment = payment_service
        self._orders: dict[str, Order] = {}
        self._event_log: list[dict] = []

    def checkout(self, user_id: str) -> Order:
        """Execute the full checkout saga."""
        cart = self._cart_service.get_cart(user_id)
        if not cart.items:
            raise ValueError("Cart is empty")

        order_id = str(uuid.uuid4())
        order_items = []
        for ci in cart.items:
            product = self._catalog.get_product(ci.product_id)
            if not product:
                raise ValueError(f"Product {ci.product_id} no longer exists")
            order_items.append(
                OrderItem(product_id=ci.product_id, quantity=ci.quantity, unit_price=product.price)
            )

        total = sum(oi.subtotal for oi in order_items)
        order = Order(
            order_id=order_id, user_id=user_id, items=order_items, total_amount=total
        )
        self._orders[order_id] = order
        self._emit("OrderCreated", order_id=order_id, user_id=user_id)

        # --- Saga Step 1: Reserve Inventory ---
        reservations: list[InventoryReservation] = []
        try:
            for oi in order_items:
                res = self._inventory.reserve(oi.product_id, order_id, oi.quantity)
                reservations.append(res)
            self._emit("InventoryReserved", order_id=order_id)
        except ValueError as exc:
            # Compensate: release all reservations made so far
            for res in reservations:
                self._inventory.release_reservation(res.reservation_id)
            order.status = OrderStatus.CANCELLED
            self._emit("InventoryReservationFailed", order_id=order_id, error=str(exc))
            raise ValueError(f"Checkout failed at inventory reservation: {exc}") from exc

        # --- Saga Step 2: Process Payment ---
        payment_result = self._payment.process_payment(order_id, total)
        if payment_result["status"] == PaymentStatus.FAILED:
            # Compensate: release all reservations
            for res in reservations:
                self._inventory.release_reservation(res.reservation_id)
            order.status = OrderStatus.CANCELLED
            order.payment_status = PaymentStatus.FAILED
            self._emit("PaymentFailed", order_id=order_id)
            raise ValueError("Checkout failed at payment processing")

        order.payment_status = PaymentStatus.COMPLETED
        self._emit("PaymentCompleted", order_id=order_id, txn=payment_result["transaction_ref"])

        # --- Saga Step 3: Confirm Order ---
        for res in reservations:
            self._inventory.confirm_reservation(res.reservation_id)
        order.status = OrderStatus.CONFIRMED
        self._emit("OrderConfirmed", order_id=order_id)

        # Clear cart after successful checkout
        self._cart_service.clear_cart(user_id)
        self._emit("CartCleared", user_id=user_id)

        return order

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def get_user_orders(self, user_id: str) -> list[Order]:
        return [o for o in self._orders.values() if o.user_id == user_id]

    def cancel_order(self, order_id: str) -> Order:
        order = self._orders.get(order_id)
        if not order:
            raise ValueError(f"Order {order_id} not found")
        if order.status != OrderStatus.CONFIRMED:
            raise ValueError(f"Cannot cancel order in {order.status.value} state")
        order.status = OrderStatus.CANCELLED
        self._emit("OrderCancelled", order_id=order_id)
        return order

    def get_event_log(self) -> list[dict]:
        return list(self._event_log)

    def _emit(self, event_type: str, **data) -> None:
        event = {"event": event_type, "timestamp": time.time(), **data}
        self._event_log.append(event)


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def create_sample_products(catalog: ProductCatalog, inventory: InventoryService) -> list[Product]:
    """Seed the catalog with sample products."""
    products_data = [
        ("Laptop Pro 16", "High-performance laptop with 32GB RAM", 1299.99, "Electronics", "TechBrand", 50),
        ("Wireless Mouse", "Ergonomic wireless mouse with USB-C", 29.99, "Electronics", "PeriphCo", 200),
        ("Python Cookbook", "Advanced Python recipes and patterns", 49.99, "Books", "TechPress", 100),
        ("Running Shoes X1", "Lightweight running shoes for marathon", 119.99, "Sports", "AthleteFit", 75),
        ("Coffee Maker Deluxe", "12-cup programmable coffee maker", 89.99, "Kitchen", "BrewMaster", 30),
        ("Noise Cancelling Headphones", "Over-ear ANC headphones", 249.99, "Electronics", "AudioMax", 40),
        ("Organic Green Tea", "100 bags of premium green tea", 24.99, "Grocery", "TeaLeaf", 500),
        ("Standing Desk", "Electric height adjustable desk", 599.99, "Furniture", "ErgoDesk", 20),
    ]
    products = []
    for title, desc, price, cat, brand, stock in products_data:
        product = Product(
            product_id=str(uuid.uuid4()),
            title=title,
            description=desc,
            price=price,
            category=cat,
            brand=brand,
            stock=stock,
        )
        catalog.add_product(product)
        inventory.set_stock(product.product_id, stock)
        products.append(product)
    return products


def run_demo() -> None:
    """Run a full e-commerce platform demo."""
    print("=" * 70)
    print("  E-COMMERCE PLATFORM DEMO (Amazon-like)")
    print("=" * 70)

    # Initialize services
    catalog = ProductCatalog()
    inventory = InventoryService()
    cart_service = CartService(catalog)
    payment_service = PaymentService()
    review_service = ReviewService(catalog)
    checkout_service = CheckoutService(catalog, cart_service, inventory, payment_service)

    # Seed products
    products = create_sample_products(catalog, inventory)
    print(f"\n[Catalog] Loaded {len(products)} products")

    # --- Search Demo ---
    print("\n" + "-" * 70)
    print("  SEARCH DEMO")
    print("-" * 70)

    results = catalog.search("laptop")
    print(f"\nSearch 'laptop': {len(results)} result(s)")
    for p in results:
        print(f"  -> {p.title} - ${p.price:.2f}")

    results = catalog.search(category="Electronics", sort_by="price_asc")
    print(f"\nBrowse Electronics (sorted by price):")
    for p in results:
        print(f"  -> {p.title} - ${p.price:.2f}")

    results = catalog.search(min_price=50, max_price=200)
    print(f"\nPrice range $50-$200: {len(results)} result(s)")
    for p in results:
        print(f"  -> {p.title} - ${p.price:.2f}")

    # --- Cart Demo ---
    print("\n" + "-" * 70)
    print("  CART DEMO")
    print("-" * 70)

    user_id = "user-alice"
    laptop = products[0]
    mouse = products[1]
    book = products[2]

    cart_service.add_item(user_id, laptop.product_id, 1)
    cart_service.add_item(user_id, mouse.product_id, 2)
    cart_service.add_item(user_id, book.product_id, 1)
    cart = cart_service.get_cart(user_id)
    print(f"\nAlice's cart: {cart.item_count} items, total=${cart.total:.2f}")
    for item in cart.items:
        p = catalog.get_product(item.product_id)
        print(f"  -> {p.title} x{item.quantity} = ${item.price_at_add * item.quantity:.2f}")

    # Guest cart merge
    guest_id = "guest-session-xyz"
    cart_service.add_item(guest_id, mouse.product_id, 3)
    cart_service.add_item(guest_id, products[3].product_id, 1)  # Running shoes
    print(f"\nGuest cart has {cart_service.get_cart(guest_id).item_count} items")
    merged = cart_service.merge_carts(guest_id, user_id)
    print(f"After merge: Alice's cart = {merged.item_count} items, total=${merged.total:.2f}")

    # --- Inventory Demo ---
    print("\n" + "-" * 70)
    print("  INVENTORY DEMO")
    print("-" * 70)

    info = inventory.get_stock_info(laptop.product_id)
    print(f"\nLaptop stock: total={info['total_stock']}, "
          f"reserved={info['reserved_stock']}, available={info['available_stock']}")

    # --- Checkout (Saga) Demo ---
    print("\n" + "-" * 70)
    print("  CHECKOUT SAGA DEMO")
    print("-" * 70)

    # Reset cart for clean checkout
    cart_service.clear_cart(user_id)
    cart_service.add_item(user_id, laptop.product_id, 1)
    cart_service.add_item(user_id, mouse.product_id, 2)
    cart = cart_service.get_cart(user_id)
    print(f"\nCheckout cart: {cart.item_count} items, total=${cart.total:.2f}")

    print("\nExecuting checkout saga...")
    order = checkout_service.checkout(user_id)
    print(f"  Step 1: Inventory reserved [OK]")
    print(f"  Step 2: Payment processed  [OK]")
    print(f"  Step 3: Order confirmed    [OK]")
    print(f"\nOrder: {order}")
    print(f"  Items: {len(order.items)}")
    for oi in order.items:
        p = catalog.get_product(oi.product_id)
        print(f"    -> {p.title} x{oi.quantity} @ ${oi.unit_price:.2f} = ${oi.subtotal:.2f}")

    # Verify inventory deducted
    info = inventory.get_stock_info(laptop.product_id)
    print(f"\nLaptop stock after order: total={info['total_stock']}, "
          f"reserved={info['reserved_stock']}, available={info['available_stock']}")

    # Verify cart cleared
    cart_after = cart_service.get_cart(user_id)
    print(f"Cart after checkout: {cart_after.item_count} items")

    # --- Failed Checkout Demo (Insufficient Stock) ---
    print("\n" + "-" * 70)
    print("  FAILED CHECKOUT DEMO (Insufficient Stock)")
    print("-" * 70)

    user_bob = "user-bob"
    standing_desk = products[7]  # stock = 20
    cart_service.add_item(user_bob, standing_desk.product_id, 25)
    print(f"\nBob tries to buy 25 standing desks (only 20 in stock)...")
    try:
        checkout_service.checkout(user_bob)
    except ValueError as e:
        print(f"  Checkout failed: {e}")
        print("  Saga compensation: all reservations released")

    # Stock should be unchanged
    info = inventory.get_stock_info(standing_desk.product_id)
    print(f"  Standing desk stock unchanged: available={info['available_stock']}")

    # --- Reservation TTL Demo ---
    print("\n" + "-" * 70)
    print("  RESERVATION TTL DEMO")
    print("-" * 70)

    coffee = products[4]
    res = inventory.reserve(coffee.product_id, "test-order", 5, ttl=0.1)
    print(f"\nReserved 5 coffee makers (TTL=0.1s)")
    info = inventory.get_stock_info(coffee.product_id)
    print(f"  Available: {info['available_stock']} (5 reserved)")

    time.sleep(0.2)
    released = inventory.cleanup_expired()
    print(f"  After TTL expiry: cleaned up {released} reservation(s)")
    info = inventory.get_stock_info(coffee.product_id)
    print(f"  Available: {info['available_stock']} (stock restored)")

    # --- Reviews Demo ---
    print("\n" + "-" * 70)
    print("  REVIEWS DEMO")
    print("-" * 70)

    review_service.add_review(laptop.product_id, "user-alice", 5, "Amazing laptop!")
    review_service.add_review(laptop.product_id, "user-charlie", 4, "Great but pricey.")
    review_service.add_review(laptop.product_id, "user-dave", 5, "Best laptop I have owned.")

    reviews = review_service.get_reviews(laptop.product_id)
    product_info = catalog.get_product(laptop.product_id)
    print(f"\n{product_info.title}: {product_info.avg_rating}/5.0 "
          f"({product_info.review_count} reviews)")
    for r in reviews:
        print(f"  [{r.rating}/5] {r.user_id}: {r.text}")

    # --- Event Log ---
    print("\n" + "-" * 70)
    print("  EVENT LOG (Saga Events)")
    print("-" * 70)
    for event in checkout_service.get_event_log():
        evt_type = event["event"]
        details = {k: v for k, v in event.items() if k not in ("event", "timestamp")}
        detail_str = ", ".join(f"{k}={v}" for k, v in details.items())
        # Truncate long UUIDs for readability
        if "order_id" in details:
            detail_str = detail_str.replace(str(details["order_id"]), str(details["order_id"])[:8] + "..")
        print(f"  [{evt_type}] {detail_str}")

    # --- Order History ---
    print("\n" + "-" * 70)
    print("  ORDER HISTORY")
    print("-" * 70)
    alice_orders = checkout_service.get_user_orders("user-alice")
    print(f"\nAlice's orders: {len(alice_orders)}")
    for o in alice_orders:
        print(f"  {o}")

    print("\n" + "=" * 70)
    print("  DEMO COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    run_demo()
