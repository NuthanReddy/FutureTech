"""
Payment System (Stripe-like) - Implementation

Demonstrates core payment processing patterns:
- PaymentGateway: entry point with idempotency key handling
- Ledger: double-entry bookkeeping (every txn has balanced debits/credits)
- PaymentProcessor: state machine for payment lifecycle
- Refund support with ledger reversals
- Provider adapters with simulated external calls

Usage:
    python payment_system.py
"""

from __future__ import annotations

import enum
import time
import uuid
import random
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Value Objects
# ---------------------------------------------------------------------------

class PaymentStatus(enum.Enum):
    CREATED = "created"
    AUTHORIZED = "authorized"
    CAPTURED = "captured"
    SETTLED = "settled"
    FAILED = "failed"
    PARTIALLY_REFUNDED = "partially_refunded"
    FULLY_REFUNDED = "fully_refunded"


class TransactionType(enum.Enum):
    AUTHORIZATION = "authorization"
    CAPTURE = "capture"
    REFUND = "refund"
    VOID = "void"


class EntryType(enum.Enum):
    DEBIT = "debit"
    CREDIT = "credit"


# Valid state transitions for the payment state machine
VALID_TRANSITIONS: dict[PaymentStatus, set[PaymentStatus]] = {
    PaymentStatus.CREATED: {PaymentStatus.AUTHORIZED, PaymentStatus.FAILED},
    PaymentStatus.AUTHORIZED: {PaymentStatus.CAPTURED, PaymentStatus.FAILED},
    PaymentStatus.CAPTURED: {
        PaymentStatus.SETTLED,
        PaymentStatus.PARTIALLY_REFUNDED,
    },
    PaymentStatus.SETTLED: {PaymentStatus.PARTIALLY_REFUNDED},
    PaymentStatus.PARTIALLY_REFUNDED: {
        PaymentStatus.PARTIALLY_REFUNDED,
        PaymentStatus.FULLY_REFUNDED,
    },
}


# ---------------------------------------------------------------------------
# Domain Entities
# ---------------------------------------------------------------------------

@dataclass
class PaymentMethod:
    id: str
    method_type: str  # card, bank_account, wallet
    token: str
    last_four: str
    brand: str

    @staticmethod
    def create(method_type: str, last_four: str, brand: str) -> PaymentMethod:
        return PaymentMethod(
            id=f"pm_{uuid.uuid4().hex[:12]}",
            method_type=method_type,
            token=f"tok_{uuid.uuid4().hex[:16]}",
            last_four=last_four,
            brand=brand,
        )


@dataclass
class Payment:
    id: str
    merchant_id: str
    amount: int  # cents
    currency: str
    status: PaymentStatus
    payment_method_id: str
    idempotency_key: Optional[str] = None
    description: str = ""
    captured_amount: int = 0
    refunded_amount: int = 0
    failure_code: Optional[str] = None
    failure_message: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    version: int = 1


@dataclass
class Transaction:
    id: str
    payment_id: str
    txn_type: TransactionType
    amount: int
    status: str  # pending, success, failed
    provider: str = ""
    provider_txn_id: str = ""
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class LedgerEntry:
    id: str
    transaction_id: str
    account_id: str
    entry_type: EntryType
    amount: int  # always positive
    currency: str
    balance_after: int
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class WebhookEvent:
    id: str
    event_type: str
    payment_id: str
    payload: dict
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# Ledger Service (Double-Entry Bookkeeping)
# ---------------------------------------------------------------------------

class Ledger:
    """
    Append-only double-entry ledger.
    Every financial movement creates balanced debit + credit entries.
    Invariant: for any transaction, SUM(debits) == SUM(credits).
    """

    def __init__(self) -> None:
        self._entries: list[LedgerEntry] = []
        self._balances: dict[str, int] = {}  # account_id -> balance (cents)
        self._lock = threading.Lock()

    def record_payment(
        self,
        transaction_id: str,
        customer_account: str,
        merchant_account: str,
        amount: int,
        currency: str,
        fee: int = 0,
    ) -> list[LedgerEntry]:
        """Record a payment capture with optional platform fee."""
        entries: list[LedgerEntry] = []
        with self._lock:
            # Debit customer (money leaves customer)
            entries.append(self._create_entry(
                transaction_id, customer_account, EntryType.DEBIT, amount, currency
            ))
            # Credit merchant (net of fees)
            merchant_amount = amount - fee
            entries.append(self._create_entry(
                transaction_id, merchant_account, EntryType.CREDIT, merchant_amount, currency
            ))
            # Credit platform fees if any
            if fee > 0:
                entries.append(self._create_entry(
                    transaction_id, "platform:fees", EntryType.CREDIT, fee, currency
                ))

        self._verify_balance(transaction_id)
        return entries

    def record_refund(
        self,
        transaction_id: str,
        customer_account: str,
        merchant_account: str,
        amount: int,
        currency: str,
    ) -> list[LedgerEntry]:
        """Record a refund: reverse the payment flow."""
        entries: list[LedgerEntry] = []
        with self._lock:
            # Debit merchant (money leaves merchant)
            entries.append(self._create_entry(
                transaction_id, merchant_account, EntryType.DEBIT, amount, currency
            ))
            # Credit customer (money returns to customer)
            entries.append(self._create_entry(
                transaction_id, customer_account, EntryType.CREDIT, amount, currency
            ))
        self._verify_balance(transaction_id)
        return entries

    def get_balance(self, account_id: str) -> int:
        """Get current balance for an account."""
        return self._balances.get(account_id, 0)

    def get_entries(self, account_id: Optional[str] = None) -> list[LedgerEntry]:
        """Get ledger entries, optionally filtered by account."""
        if account_id:
            return [e for e in self._entries if e.account_id == account_id]
        return list(self._entries)

    def _create_entry(
        self,
        transaction_id: str,
        account_id: str,
        entry_type: EntryType,
        amount: int,
        currency: str,
    ) -> LedgerEntry:
        if entry_type == EntryType.DEBIT:
            self._balances[account_id] = self._balances.get(account_id, 0) - amount
        else:
            self._balances[account_id] = self._balances.get(account_id, 0) + amount

        entry = LedgerEntry(
            id=f"le_{uuid.uuid4().hex[:12]}",
            transaction_id=transaction_id,
            account_id=account_id,
            entry_type=entry_type,
            amount=amount,
            currency=currency,
            balance_after=self._balances[account_id],
        )
        self._entries.append(entry)
        return entry

    def _verify_balance(self, transaction_id: str) -> None:
        """Verify debits == credits for a transaction."""
        txn_entries = [e for e in self._entries if e.transaction_id == transaction_id]
        total_debits = sum(e.amount for e in txn_entries if e.entry_type == EntryType.DEBIT)
        total_credits = sum(e.amount for e in txn_entries if e.entry_type == EntryType.CREDIT)
        if total_debits != total_credits:
            raise ValueError(
                f"Ledger imbalance for txn {transaction_id}: "
                f"debits={total_debits}, credits={total_credits}"
            )


# ---------------------------------------------------------------------------
# Provider Adapter (simulates external payment providers)
# ---------------------------------------------------------------------------

class ProviderResponse:
    def __init__(self, success: bool, provider_txn_id: str = "",
                 error_code: str = "", error_message: str = "") -> None:
        self.success = success
        self.provider_txn_id = provider_txn_id
        self.error_code = error_code
        self.error_message = error_message


class PaymentProviderAdapter:
    """Simulates an external payment provider (Visa, Mastercard, etc.)."""

    def __init__(self, name: str, success_rate: float = 0.95) -> None:
        self.name = name
        self.success_rate = success_rate

    def authorize(self, amount: int, currency: str, token: str) -> ProviderResponse:
        """Simulate authorization call to card network."""
        if random.random() < self.success_rate:
            return ProviderResponse(
                success=True,
                provider_txn_id=f"prv_{uuid.uuid4().hex[:16]}",
            )
        return ProviderResponse(
            success=False,
            error_code="card_declined",
            error_message="Insufficient funds",
        )

    def capture(self, provider_txn_id: str, amount: int) -> ProviderResponse:
        """Simulate capture call."""
        return ProviderResponse(success=True, provider_txn_id=provider_txn_id)

    def refund(self, provider_txn_id: str, amount: int) -> ProviderResponse:
        """Simulate refund call."""
        return ProviderResponse(
            success=True,
            provider_txn_id=f"ref_{uuid.uuid4().hex[:16]}",
        )


def call_provider_with_retry(
    provider: PaymentProviderAdapter,
    operation: str,
    max_retries: int = 3,
    **kwargs: object,
) -> ProviderResponse:
    """Call provider with exponential backoff and jitter."""
    for attempt in range(max_retries):
        try:
            func = getattr(provider, operation)
            response: ProviderResponse = func(**kwargs)
            if response.success or response.error_code == "card_declined":
                return response
        except Exception:
            pass

        delay = min(2 ** attempt + random.uniform(0, 1), 30)
        time.sleep(delay * 0.01)  # shortened for demo

    return ProviderResponse(
        success=False,
        error_code="max_retries_exceeded",
        error_message="Provider unreachable after retries",
    )


# ---------------------------------------------------------------------------
# Webhook Service (simplified)
# ---------------------------------------------------------------------------

class WebhookService:
    """Collects and delivers webhook events to merchants."""

    def __init__(self) -> None:
        self._events: list[WebhookEvent] = []

    def emit(self, event_type: str, payment_id: str, payload: dict) -> WebhookEvent:
        event = WebhookEvent(
            id=f"evt_{uuid.uuid4().hex[:12]}",
            event_type=event_type,
            payment_id=payment_id,
            payload=payload,
        )
        self._events.append(event)
        return event

    def get_events(self, payment_id: Optional[str] = None) -> list[WebhookEvent]:
        if payment_id:
            return [e for e in self._events if e.payment_id == payment_id]
        return list(self._events)


# ---------------------------------------------------------------------------
# Payment Processor (State Machine)
# ---------------------------------------------------------------------------

class PaymentProcessor:
    """
    Manages the payment lifecycle through a strict state machine.
    Coordinates between providers, ledger, and webhook services.
    """

    PLATFORM_FEE_BPS = 290  # 2.9% like Stripe

    def __init__(
        self,
        ledger: Ledger,
        webhook_service: WebhookService,
        provider: Optional[PaymentProviderAdapter] = None,
    ) -> None:
        self.ledger = ledger
        self.webhook_service = webhook_service
        self.provider = provider or PaymentProviderAdapter("default_provider", success_rate=1.0)
        self._payments: dict[str, Payment] = {}
        self._transactions: list[Transaction] = []
        self._payment_methods: dict[str, PaymentMethod] = {}

    def register_payment_method(self, pm: PaymentMethod) -> None:
        self._payment_methods[pm.id] = pm

    def _transition(self, payment: Payment, new_status: PaymentStatus) -> None:
        """Enforce state machine transitions."""
        valid = VALID_TRANSITIONS.get(payment.status, set())
        if new_status not in valid:
            raise ValueError(
                f"Invalid transition: {payment.status.value} -> {new_status.value}"
            )
        payment.status = new_status
        payment.updated_at = datetime.now(timezone.utc)
        payment.version += 1

    def create_payment(
        self,
        merchant_id: str,
        amount: int,
        currency: str,
        payment_method_id: str,
        description: str = "",
        idempotency_key: Optional[str] = None,
    ) -> Payment:
        """Create a new payment in CREATED state."""
        payment = Payment(
            id=f"pay_{uuid.uuid4().hex[:12]}",
            merchant_id=merchant_id,
            amount=amount,
            currency=currency,
            status=PaymentStatus.CREATED,
            payment_method_id=payment_method_id,
            idempotency_key=idempotency_key,
            description=description,
        )
        self._payments[payment.id] = payment
        return payment

    def authorize(self, payment_id: str) -> Payment:
        """Authorize a payment with the provider."""
        payment = self._payments[payment_id]
        pm = self._payment_methods.get(payment.payment_method_id)
        token = pm.token if pm else "tok_unknown"

        response = call_provider_with_retry(
            self.provider,
            "authorize",
            amount=payment.amount,
            currency=payment.currency,
            token=token,
        )

        txn = Transaction(
            id=f"txn_{uuid.uuid4().hex[:12]}",
            payment_id=payment_id,
            txn_type=TransactionType.AUTHORIZATION,
            amount=payment.amount,
            status="success" if response.success else "failed",
            provider=self.provider.name,
            provider_txn_id=response.provider_txn_id,
            error_code=response.error_code or None,
            error_message=response.error_message or None,
        )
        self._transactions.append(txn)

        if response.success:
            self._transition(payment, PaymentStatus.AUTHORIZED)
        else:
            self._transition(payment, PaymentStatus.FAILED)
            payment.failure_code = response.error_code
            payment.failure_message = response.error_message

        return payment

    def capture(self, payment_id: str, amount: Optional[int] = None) -> Payment:
        """Capture an authorized payment and create ledger entries."""
        payment = self._payments[payment_id]
        capture_amount = amount or payment.amount

        # Find the authorization transaction for this payment
        auth_txn = next(
            (t for t in self._transactions
             if t.payment_id == payment_id
             and t.txn_type == TransactionType.AUTHORIZATION
             and t.status == "success"),
            None,
        )
        if not auth_txn:
            raise ValueError(f"No successful authorization found for {payment_id}")

        response = call_provider_with_retry(
            self.provider,
            "capture",
            provider_txn_id=auth_txn.provider_txn_id,
            amount=capture_amount,
        )

        txn = Transaction(
            id=f"txn_{uuid.uuid4().hex[:12]}",
            payment_id=payment_id,
            txn_type=TransactionType.CAPTURE,
            amount=capture_amount,
            status="success" if response.success else "failed",
            provider=self.provider.name,
            provider_txn_id=response.provider_txn_id,
        )
        self._transactions.append(txn)

        if response.success:
            self._transition(payment, PaymentStatus.CAPTURED)
            payment.captured_amount = capture_amount

            # Double-entry ledger: debit customer, credit merchant (minus fee)
            fee = (capture_amount * self.PLATFORM_FEE_BPS) // 10000
            customer_account = f"customer:{payment.merchant_id}"
            merchant_account = f"merchant:{payment.merchant_id}"
            self.ledger.record_payment(
                transaction_id=txn.id,
                customer_account=customer_account,
                merchant_account=merchant_account,
                amount=capture_amount,
                currency=payment.currency,
                fee=fee,
            )

            self.webhook_service.emit(
                "payment.captured",
                payment.id,
                {"amount": capture_amount, "currency": payment.currency},
            )

        return payment

    def authorize_and_capture(
        self,
        merchant_id: str,
        amount: int,
        currency: str,
        payment_method_id: str,
        description: str = "",
        idempotency_key: Optional[str] = None,
    ) -> Payment:
        """Convenience: create, authorize, and capture in one call."""
        payment = self.create_payment(
            merchant_id, amount, currency, payment_method_id,
            description, idempotency_key,
        )
        payment = self.authorize(payment.id)
        if payment.status == PaymentStatus.AUTHORIZED:
            payment = self.capture(payment.id)
        return payment

    def refund(self, payment_id: str, amount: Optional[int] = None) -> Payment:
        """Process a full or partial refund."""
        payment = self._payments[payment_id]
        refund_amount = amount or (payment.captured_amount - payment.refunded_amount)

        if refund_amount <= 0:
            raise ValueError("Refund amount must be positive")
        if payment.refunded_amount + refund_amount > payment.captured_amount:
            raise ValueError(
                f"Refund of {refund_amount} exceeds remaining "
                f"{payment.captured_amount - payment.refunded_amount}"
            )

        # Find original capture transaction
        capture_txn = next(
            (t for t in self._transactions
             if t.payment_id == payment_id
             and t.txn_type == TransactionType.CAPTURE
             and t.status == "success"),
            None,
        )
        if not capture_txn:
            raise ValueError(f"No successful capture found for {payment_id}")

        response = call_provider_with_retry(
            self.provider,
            "refund",
            provider_txn_id=capture_txn.provider_txn_id,
            amount=refund_amount,
        )

        txn = Transaction(
            id=f"txn_{uuid.uuid4().hex[:12]}",
            payment_id=payment_id,
            txn_type=TransactionType.REFUND,
            amount=refund_amount,
            status="success" if response.success else "failed",
            provider=self.provider.name,
            provider_txn_id=response.provider_txn_id,
        )
        self._transactions.append(txn)

        if response.success:
            payment.refunded_amount += refund_amount
            remaining = payment.captured_amount - payment.refunded_amount
            if remaining == 0:
                self._transition(payment, PaymentStatus.FULLY_REFUNDED)
            else:
                self._transition(payment, PaymentStatus.PARTIALLY_REFUNDED)

            # Ledger reversal: debit merchant, credit customer
            customer_account = f"customer:{payment.merchant_id}"
            merchant_account = f"merchant:{payment.merchant_id}"
            self.ledger.record_refund(
                transaction_id=txn.id,
                customer_account=customer_account,
                merchant_account=merchant_account,
                amount=refund_amount,
                currency=payment.currency,
            )

            self.webhook_service.emit(
                "refund.completed",
                payment.id,
                {"amount": refund_amount, "currency": payment.currency},
            )

        return payment

    def get_payment(self, payment_id: str) -> Optional[Payment]:
        return self._payments.get(payment_id)

    def get_transactions(self, payment_id: str) -> list[Transaction]:
        return [t for t in self._transactions if t.payment_id == payment_id]


# ---------------------------------------------------------------------------
# Payment Gateway (API Layer with Idempotency)
# ---------------------------------------------------------------------------

class PaymentGateway:
    """
    Entry point for payment operations. Handles:
    - Idempotency key deduplication
    - Request routing to PaymentProcessor
    - Response caching for idempotent replay
    """

    def __init__(self, processor: PaymentProcessor) -> None:
        self.processor = processor
        self._idempotency_store: dict[str, Payment] = {}
        self._lock = threading.Lock()

    def process_payment(
        self,
        merchant_id: str,
        amount: int,
        currency: str,
        payment_method_id: str,
        description: str = "",
        idempotency_key: Optional[str] = None,
    ) -> Payment:
        """Process a payment with idempotency key deduplication."""
        if idempotency_key:
            with self._lock:
                cached = self._idempotency_store.get(idempotency_key)
                if cached is not None:
                    return cached

        payment = self.processor.authorize_and_capture(
            merchant_id=merchant_id,
            amount=amount,
            currency=currency,
            payment_method_id=payment_method_id,
            description=description,
            idempotency_key=idempotency_key,
        )

        if idempotency_key:
            with self._lock:
                self._idempotency_store[idempotency_key] = payment

        return payment

    def process_refund(
        self,
        payment_id: str,
        amount: Optional[int] = None,
        idempotency_key: Optional[str] = None,
    ) -> Payment:
        """Process a refund with idempotency."""
        if idempotency_key:
            with self._lock:
                cached = self._idempotency_store.get(idempotency_key)
                if cached is not None:
                    return cached

        payment = self.processor.refund(payment_id, amount)

        if idempotency_key:
            with self._lock:
                self._idempotency_store[idempotency_key] = payment

        return payment


# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

def _fmt_cents(cents: int) -> str:
    """Format cents as dollar string."""
    return f"${cents / 100:.2f}"


def run_demo() -> None:
    print("=" * 70)
    print("  PAYMENT SYSTEM (Stripe-like) - Demo")
    print("=" * 70)

    # --- Bootstrap ---
    ledger = Ledger()
    webhooks = WebhookService()
    provider = PaymentProviderAdapter("visa_network", success_rate=1.0)
    processor = PaymentProcessor(ledger, webhooks, provider)
    gateway = PaymentGateway(processor)

    # Register a payment method
    card = PaymentMethod.create("card", "4242", "visa")
    processor.register_payment_method(card)
    print(f"\n[Setup] Registered card ...{card.last_four} ({card.brand})")

    # --- 1. Process a payment ---
    print("\n--- 1. Process Payment ---")
    payment = gateway.process_payment(
        merchant_id="merch_acme",
        amount=5000,  # $50.00
        currency="usd",
        payment_method_id=card.id,
        description="Order #1234",
        idempotency_key="idem_pay_001",
    )
    print(f"Payment {payment.id}: {payment.status.value} for {_fmt_cents(payment.amount)}")
    fee = (payment.captured_amount * PaymentProcessor.PLATFORM_FEE_BPS) // 10000
    print(f"  Platform fee: {_fmt_cents(fee)}, Merchant net: {_fmt_cents(payment.captured_amount - fee)}")

    # --- 2. Idempotency: same key returns cached result ---
    print("\n--- 2. Idempotency Check ---")
    duplicate = gateway.process_payment(
        merchant_id="merch_acme",
        amount=5000,
        currency="usd",
        payment_method_id=card.id,
        description="Order #1234",
        idempotency_key="idem_pay_001",
    )
    assert duplicate.id == payment.id, "Idempotency failed!"
    print(f"Duplicate request returned same payment: {duplicate.id} (idempotency works)")

    # --- 3. Partial refund ---
    print("\n--- 3. Partial Refund ---")
    payment = gateway.process_refund(
        payment_id=payment.id,
        amount=2000,  # $20.00
        idempotency_key="idem_ref_001",
    )
    print(f"After partial refund: status={payment.status.value}")
    print(f"  Captured: {_fmt_cents(payment.captured_amount)}, Refunded: {_fmt_cents(payment.refunded_amount)}")

    # --- 4. Full refund of remainder ---
    print("\n--- 4. Full Refund of Remainder ---")
    payment = gateway.process_refund(
        payment_id=payment.id,
        amount=3000,  # remaining $30.00
        idempotency_key="idem_ref_002",
    )
    print(f"After full refund: status={payment.status.value}")
    print(f"  Captured: {_fmt_cents(payment.captured_amount)}, Refunded: {_fmt_cents(payment.refunded_amount)}")

    # --- 5. Ledger balances ---
    print("\n--- 5. Ledger Balances ---")
    for acct in ["customer:merch_acme", "merchant:merch_acme", "platform:fees"]:
        balance = ledger.get_balance(acct)
        print(f"  {acct:30s} = {_fmt_cents(abs(balance)):>10s} ({'debit' if balance < 0 else 'credit'})")

    # --- 6. Ledger entries ---
    print("\n--- 6. Ledger Entries (all) ---")
    for entry in ledger.get_entries():
        print(
            f"  {entry.entry_type.value:6s}  {entry.account_id:30s}  "
            f"{_fmt_cents(entry.amount):>10s}  bal={_fmt_cents(abs(entry.balance_after))}"
        )

    # --- 7. Transaction history ---
    print("\n--- 7. Transaction History ---")
    for txn in processor.get_transactions(payment.id):
        print(
            f"  {txn.txn_type.value:15s}  {_fmt_cents(txn.amount):>10s}  "
            f"status={txn.status}  provider_txn={txn.provider_txn_id[:20]}"
        )

    # --- 8. Webhook events ---
    print("\n--- 8. Webhook Events ---")
    for evt in webhooks.get_events():
        print(f"  {evt.event_type:25s}  payment={evt.payment_id}  payload={evt.payload}")

    # --- 9. State machine violation test ---
    print("\n--- 9. State Machine Enforcement ---")
    test_pay = processor.create_payment("merch_test", 1000, "usd", card.id)
    try:
        processor.capture(test_pay.id)  # cannot capture without authorization
        print("  ERROR: should have raised")
    except ValueError as e:
        print(f"  Correctly rejected invalid transition: {e}")

    # --- 10. Second payment for ledger variety ---
    print("\n--- 10. Second Payment ---")
    payment2 = gateway.process_payment(
        merchant_id="merch_beta",
        amount=15000,  # $150.00
        currency="usd",
        payment_method_id=card.id,
        description="Subscription renewal",
        idempotency_key="idem_pay_002",
    )
    print(f"Payment {payment2.id}: {payment2.status.value} for {_fmt_cents(payment2.amount)}")

    # --- Summary ---
    print("\n" + "=" * 70)
    print("  Summary")
    print("=" * 70)
    all_entries = ledger.get_entries()
    total_debits = sum(e.amount for e in all_entries if e.entry_type == EntryType.DEBIT)
    total_credits = sum(e.amount for e in all_entries if e.entry_type == EntryType.CREDIT)
    print(f"  Total ledger debits:  {_fmt_cents(total_debits)}")
    print(f"  Total ledger credits: {_fmt_cents(total_credits)}")
    print(f"  Balanced: {total_debits == total_credits}")
    print(f"  Total webhook events: {len(webhooks.get_events())}")
    print(f"  Idempotency keys cached: {len(gateway._idempotency_store)}")
    print("=" * 70)


if __name__ == "__main__":
    run_demo()
