"""Explicit order and payment status transitions.

Historical values (completed, paid, shipped, cancelled/canceled) stay valid.
Unknown historical statuses are left unchanged rather than rewritten.
"""

from __future__ import annotations

from typing import FrozenSet, Optional, Tuple

# Normalized fulfillment-facing states used by admin mutations.
ORDER_PAID = "paid"
ORDER_SHIPPED = "shipped"
ORDER_DELIVERED = "delivered"
ORDER_CANCELLED = "cancelled"
ORDER_PENDING = "pending"
ORDER_COMPLETED = "completed"  # historical paid/complete

_CANCELLED = frozenset({"cancelled", "canceled"})

# Explicitly supported admin transitions (from_norm, to_norm).
# shipped → paid is the existing unfulfill control (ops correction).
ALLOWED_ORDER_TRANSITIONS: FrozenSet[Tuple[str, str]] = frozenset(
    {
        (ORDER_PAID, ORDER_SHIPPED),
        (ORDER_COMPLETED, ORDER_SHIPPED),
        (ORDER_PENDING, ORDER_SHIPPED),
        (ORDER_SHIPPED, ORDER_PAID),  # explicit unfulfill
        (ORDER_SHIPPED, ORDER_DELIVERED),
        (ORDER_PAID, ORDER_CANCELLED),
        (ORDER_COMPLETED, ORDER_CANCELLED),
        (ORDER_PENDING, ORDER_CANCELLED),
    }
)


class InvalidStatusTransition(ValueError):
    """Requested status change is not allowed."""


def normalize_order_status(status: Optional[str], fulfillment_status: Optional[str] = None) -> str:
    st = (status or "").strip().lower()
    ff = (fulfillment_status or "").strip().lower()
    if st in _CANCELLED or ff in _CANCELLED:
        return ORDER_CANCELLED
    if st == ORDER_DELIVERED or ff == ORDER_DELIVERED:
        return ORDER_DELIVERED
    if st == ORDER_SHIPPED or ff == ORDER_SHIPPED:
        return ORDER_SHIPPED
    if st in (ORDER_PAID, ORDER_COMPLETED) or ff in (ORDER_PAID, ORDER_COMPLETED, ""):
        if ff == ORDER_SHIPPED:
            return ORDER_SHIPPED
        return ORDER_PAID if st in (ORDER_PAID, ORDER_COMPLETED, "") else st or ORDER_PAID
    if st == ORDER_PENDING:
        return ORDER_PENDING
    return st or ORDER_PAID


def order_transition_allowed(from_status: str, from_fulfillment: Optional[str], to_shipped: bool) -> bool:
    current = normalize_order_status(from_status, from_fulfillment)
    target = ORDER_SHIPPED if to_shipped else ORDER_PAID
    if current == target:
        return True
    return (current, target) in ALLOWED_ORDER_TRANSITIONS


def assert_order_fulfillment_transition(
    from_status: str, from_fulfillment: Optional[str], *, fulfill: bool
) -> None:
    if order_transition_allowed(from_status, from_fulfillment, fulfill):
        return
    current = normalize_order_status(from_status, from_fulfillment)
    target = ORDER_SHIPPED if fulfill else ORDER_PAID
    raise InvalidStatusTransition(f"order_transition_not_allowed:{current}->{target}")


PAYMENT_PENDING = "pending"
PAYMENT_SUCCEEDED = "succeeded"
PAYMENT_FAILED = "failed"
PAYMENT_REFUNDED = "refunded"

ALLOWED_PAYMENT_TRANSITIONS: FrozenSet[Tuple[str, str]] = frozenset(
    {
        (PAYMENT_PENDING, PAYMENT_PENDING),
        (PAYMENT_PENDING, PAYMENT_SUCCEEDED),
        (PAYMENT_PENDING, PAYMENT_FAILED),
        (PAYMENT_SUCCEEDED, PAYMENT_SUCCEEDED),  # idempotent replay
        (PAYMENT_FAILED, PAYMENT_FAILED),
        (PAYMENT_FAILED, PAYMENT_SUCCEEDED),  # later paid
        (PAYMENT_REFUNDED, PAYMENT_REFUNDED),
        # succeeded → refunded is reserved for a future refund implementation
        (PAYMENT_SUCCEEDED, PAYMENT_REFUNDED),
    }
)


def normalize_payment_status(status: Optional[str]) -> str:
    st = (status or "").strip().lower()
    if st in ("paid", "complete", "completed"):
        return PAYMENT_SUCCEEDED
    if st in ("fail", "failure"):
        return PAYMENT_FAILED
    if st in (PAYMENT_PENDING, PAYMENT_SUCCEEDED, PAYMENT_FAILED, PAYMENT_REFUNDED):
        return st
    return st or PAYMENT_PENDING


def payment_transition_allowed(from_status: Optional[str], to_status: str) -> bool:
    current = normalize_payment_status(from_status)
    target = normalize_payment_status(to_status)
    if current == target:
        return True
    return (current, target) in ALLOWED_PAYMENT_TRANSITIONS


def assert_payment_transition(from_status: Optional[str], to_status: str) -> None:
    if payment_transition_allowed(from_status, to_status):
        return
    raise InvalidStatusTransition(
        f"payment_transition_not_allowed:{normalize_payment_status(from_status)}->{normalize_payment_status(to_status)}"
    )
