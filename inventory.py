"""Transactional inventory ledger. All quantity changes go through movements.

Callers must use a connection that already holds a write lock (BEGIN IMMEDIATE)
for concurrent reservation safety under SQLite.
"""

from __future__ import annotations

import logging
import os
import sqlite3
from typing import List, Optional

log = logging.getLogger("licorice.database")

MOVEMENT_TYPES = (
    "RECEIPT",
    "SALE",
    "RETURN",
    "DAMAGE",
    "ADJUSTMENT",
    "TRANSFER",
    "RESERVATION",
    "RELEASE",
    "PRODUCTION_CONSUMPTION",
    "PRODUCTION_OUTPUT",
)


class InventoryError(Exception):
    """Inventory operation rejected."""


class InsufficientStock(InventoryError):
    """Reservation or sale exceeds available quantity."""


def inventory_enforced() -> bool:
    return (os.environ.get("INVENTORY_ENFORCE") or "").strip().lower() in ("1", "true", "yes")


def available_qty(on_hand: int, reserved: int) -> int:
    return int(on_hand) - int(reserved)


def _item(db: sqlite3.Connection, item_id: int) -> sqlite3.Row:
    row = db.execute(
        "SELECT * FROM inventory_items WHERE id = ?",
        (item_id,),
    ).fetchone()
    if not row:
        raise InventoryError("inventory_item_not_found")
    return row


def item_for_variant(
    db: sqlite3.Connection, variant_id: int, location_id: Optional[int] = None
) -> Optional[sqlite3.Row]:
    if location_id is not None:
        return db.execute(
            """
            SELECT * FROM inventory_items
            WHERE variant_id = ? AND location_id = ?
            LIMIT 1
            """,
            (variant_id, location_id),
        ).fetchone()
    return db.execute(
        """
        SELECT * FROM inventory_items
        WHERE variant_id = ?
        ORDER BY id
        LIMIT 1
        """,
        (variant_id,),
    ).fetchone()


def is_stock_managed(db: sqlite3.Connection, item: sqlite3.Row) -> bool:
    if int(item["quantity_on_hand"] or 0) or int(item["quantity_reserved"] or 0):
        return True
    n = db.execute(
        "SELECT COUNT(*) AS c FROM inventory_movements WHERE inventory_item_id = ?",
        (int(item["id"]),),
    ).fetchone()["c"]
    return int(n) > 0


def _move(
    db: sqlite3.Connection,
    *,
    item_id: int,
    movement_type: str,
    quantity: int,
    reason: str = "",
    reference_type: str = "",
    reference_id: Optional[int] = None,
    created_by_id: Optional[int] = None,
) -> None:
    if movement_type not in MOVEMENT_TYPES:
        raise InventoryError("invalid_movement_type")
    if int(quantity) == 0:
        raise InventoryError("quantity_must_be_nonzero")
    db.execute(
        """
        INSERT INTO inventory_movements (
            inventory_item_id, movement_type, quantity, reference_type, reference_id, reason, created_by_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (item_id, movement_type, int(quantity), reference_type, reference_id, reason, created_by_id),
    )


def reserve_inventory(
    db: sqlite3.Connection,
    item_id: int,
    quantity: int,
    *,
    reason: str = "",
    reference_type: str = "",
    reference_id: Optional[int] = None,
    allow_backorder: bool = False,
) -> None:
    qty = int(quantity)
    if qty < 1:
        raise InventoryError("quantity_must_be_positive")
    _item(db, item_id)
    if allow_backorder:
        db.execute(
            """
            UPDATE inventory_items
            SET quantity_reserved = quantity_reserved + ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            (qty, item_id),
        )
    else:
        cur = db.execute(
            """
            UPDATE inventory_items
            SET quantity_reserved = quantity_reserved + ?, updated_at = datetime('now')
            WHERE id = ?
              AND (quantity_on_hand - quantity_reserved) >= ?
            """,
            (qty, item_id, qty),
        )
        if cur.rowcount != 1:
            raise InsufficientStock("insufficient_available_stock")
    _move(
        db,
        item_id=item_id,
        movement_type="RESERVATION",
        quantity=qty,
        reason=reason or "reserve",
        reference_type=reference_type,
        reference_id=reference_id,
    )


def release_inventory(
    db: sqlite3.Connection,
    item_id: int,
    quantity: int,
    *,
    reason: str = "",
    reference_type: str = "",
    reference_id: Optional[int] = None,
) -> None:
    qty = int(quantity)
    if qty < 1:
        raise InventoryError("quantity_must_be_positive")
    item = _item(db, item_id)
    reserved = int(item["quantity_reserved"] or 0)
    if reserved < qty:
        raise InventoryError("release_exceeds_reserved")
    db.execute(
        """
        UPDATE inventory_items
        SET quantity_reserved = quantity_reserved - ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (qty, item_id),
    )
    _move(
        db,
        item_id=item_id,
        movement_type="RELEASE",
        quantity=qty,
        reason=reason or "release",
        reference_type=reference_type,
        reference_id=reference_id,
    )


def receive_inventory(
    db: sqlite3.Connection,
    item_id: int,
    quantity: int,
    *,
    reason: str = "",
    reference_type: str = "",
    reference_id: Optional[int] = None,
) -> None:
    qty = int(quantity)
    if qty < 1:
        raise InventoryError("quantity_must_be_positive")
    keyed = bool(reference_type and reference_id is not None)
    if keyed:
        try:
            _move(
                db,
                item_id=item_id,
                movement_type="RECEIPT",
                quantity=qty,
                reason=reason or "receipt",
                reference_type=reference_type,
                reference_id=reference_id,
            )
        except sqlite3.IntegrityError:
            return
    _item(db, item_id)
    db.execute(
        """
        UPDATE inventory_items
        SET quantity_on_hand = quantity_on_hand + ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (qty, item_id),
    )
    if not keyed:
        _move(
            db,
            item_id=item_id,
            movement_type="RECEIPT",
            quantity=qty,
            reason=reason or "receipt",
            reference_type=reference_type,
            reference_id=reference_id,
        )


def consume_inventory_sale(
    db: sqlite3.Connection,
    item_id: int,
    quantity: int,
    *,
    reason: str = "",
    reference_type: str = "order",
    reference_id: Optional[int] = None,
    allow_backorder: bool = False,
) -> None:
    """Deduct on-hand for a completed sale. Idempotent per (item, order) SALE movement."""
    qty = int(quantity)
    if qty < 1:
        raise InventoryError("quantity_must_be_positive")
    order_keyed = bool(reference_type == "order" and reference_id is not None)
    if order_keyed:
        try:
            _move(
                db,
                item_id=item_id,
                movement_type="SALE",
                quantity=qty,
                reason=reason or "sale",
                reference_type=reference_type,
                reference_id=reference_id,
            )
        except sqlite3.IntegrityError:
            return
    item = _item(db, item_id)
    on_hand = int(item["quantity_on_hand"] or 0)
    reserved = int(item["quantity_reserved"] or 0)
    if on_hand < qty and not allow_backorder:
        raise InsufficientStock("insufficient_on_hand_for_sale")
    new_reserved = reserved - qty if reserved >= qty else reserved
    if allow_backorder:
        db.execute(
            """
            UPDATE inventory_items
            SET quantity_on_hand = MAX(0, quantity_on_hand - ?),
                quantity_reserved = ?,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (qty, new_reserved, item_id),
        )
    else:
        cur = db.execute(
            """
            UPDATE inventory_items
            SET quantity_on_hand = quantity_on_hand - ?,
                quantity_reserved = ?,
                updated_at = datetime('now')
            WHERE id = ?
              AND quantity_on_hand >= ?
            """,
            (qty, new_reserved, item_id, qty),
        )
        if cur.rowcount != 1:
            raise InsufficientStock("insufficient_on_hand_for_sale")
    if not order_keyed:
        _move(
            db,
            item_id=item_id,
            movement_type="SALE",
            quantity=qty,
            reason=reason or "sale",
            reference_type=reference_type,
            reference_id=reference_id,
        )


def return_inventory(
    db: sqlite3.Connection,
    item_id: int,
    quantity: int,
    *,
    reason: str = "",
    reference_type: str = "",
    reference_id: Optional[int] = None,
) -> None:
    qty = int(quantity)
    if qty < 1:
        raise InventoryError("quantity_must_be_positive")
    keyed = bool(reference_type and reference_id is not None)
    if keyed:
        try:
            _move(
                db,
                item_id=item_id,
                movement_type="RETURN",
                quantity=qty,
                reason=reason or "return",
                reference_type=reference_type,
                reference_id=reference_id,
            )
        except sqlite3.IntegrityError:
            return
    _item(db, item_id)
    db.execute(
        """
        UPDATE inventory_items
        SET quantity_on_hand = quantity_on_hand + ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (qty, item_id),
    )
    if not keyed:
        _move(
            db,
            item_id=item_id,
            movement_type="RETURN",
            quantity=qty,
            reason=reason or "return",
            reference_type=reference_type,
            reference_id=reference_id,
        )


def adjust_inventory(
    db: sqlite3.Connection,
    item_id: int,
    delta: int,
    *,
    reason: str,
    created_by_id: Optional[int] = None,
) -> None:
    if not (reason or "").strip():
        raise InventoryError("adjustment_requires_reason")
    d = int(delta)
    if d == 0:
        raise InventoryError("quantity_must_be_nonzero")
    item = _item(db, item_id)
    on_hand = int(item["quantity_on_hand"] or 0)
    if on_hand + d < 0:
        raise InventoryError("adjustment_would_go_negative")
    db.execute(
        """
        UPDATE inventory_items
        SET quantity_on_hand = quantity_on_hand + ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (d, item_id),
    )
    _move(
        db,
        item_id=item_id,
        movement_type="ADJUSTMENT",
        quantity=d,
        reason=reason.strip(),
        created_by_id=created_by_id,
    )


def transfer_inventory(
    db: sqlite3.Connection,
    source_item_id: int,
    dest_item_id: int,
    quantity: int,
    *,
    reason: str = "",
) -> None:
    qty = int(quantity)
    if qty < 1:
        raise InventoryError("quantity_must_be_positive")
    if source_item_id == dest_item_id:
        raise InventoryError("transfer_same_item")
    src = _item(db, source_item_id)
    _item(db, dest_item_id)
    on_hand = int(src["quantity_on_hand"] or 0)
    reserved = int(src["quantity_reserved"] or 0)
    if available_qty(on_hand, reserved) < qty:
        raise InsufficientStock("insufficient_available_for_transfer")
    db.execute(
        """
        UPDATE inventory_items
        SET quantity_on_hand = quantity_on_hand - ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (qty, source_item_id),
    )
    db.execute(
        """
        UPDATE inventory_items
        SET quantity_on_hand = quantity_on_hand + ?, updated_at = datetime('now')
        WHERE id = ?
        """,
        (qty, dest_item_id),
    )
    _move(
        db,
        item_id=source_item_id,
        movement_type="TRANSFER",
        quantity=-qty,
        reason=reason or "transfer_out",
        reference_type="inventory_item",
        reference_id=dest_item_id,
    )
    _move(
        db,
        item_id=dest_item_id,
        movement_type="TRANSFER",
        quantity=qty,
        reason=reason or "transfer_in",
        reference_type="inventory_item",
        reference_id=source_item_id,
    )


def reconstructed_on_hand(db: sqlite3.Connection, item_id: int) -> int:
    """Rebuild on-hand from the ledger. Does not write. Opening quantity is zero at item creation."""
    total = 0
    rows = db.execute(
        """
        SELECT movement_type, quantity FROM inventory_movements
        WHERE inventory_item_id = ?
        ORDER BY id
        """,
        (int(item_id),),
    ).fetchall()
    for r in rows:
        t = str(r["movement_type"] or "")
        q = int(r["quantity"] or 0)
        if t in ("RECEIPT", "RETURN", "PRODUCTION_OUTPUT"):
            total += abs(q)
        elif t in ("SALE", "PRODUCTION_CONSUMPTION", "DAMAGE"):
            total -= abs(q)
        elif t in ("ADJUSTMENT", "TRANSFER"):
            total += q
        elif t in ("RESERVATION", "RELEASE"):
            continue
        else:
            total += q
    return total


def ledger_discrepancies(db: sqlite3.Connection, *, limit: int = 50) -> List[dict]:
    """Read-only: on-hand that does not match reconstructed ledger. Never auto-corrects."""
    out: List[dict] = []
    rows = db.execute(
        "SELECT id, quantity_on_hand FROM inventory_items ORDER BY id"
    ).fetchall()
    for r in rows:
        iid = int(r["id"])
        stored = int(r["quantity_on_hand"] or 0)
        recon = reconstructed_on_hand(db, iid)
        if stored != recon:
            out.append(
                {
                    "inventory_item_id": iid,
                    "stored_on_hand": stored,
                    "ledger_on_hand": recon,
                    "delta": stored - recon,
                }
            )
            if len(out) >= limit:
                break
    return out


def apply_order_sale_if_managed(
    db: sqlite3.Connection,
    *,
    variant_id: Optional[int],
    quantity: int,
    order_id: int,
) -> None:
    """Deduct stock for a paid order when the SKU is managed. Never fails an unpaid-gap: logs instead.

    Unmanaged zero-stock SKUs (no movements, all zeros) skip deduction so production
    can keep selling until inventory is actually operated. Set INVENTORY_ENFORCE=1 to require stock.
    """
    if not variant_id:
        return
    item = item_for_variant(db, int(variant_id))
    if not item:
        return
    managed = is_stock_managed(db, item)
    enforce = inventory_enforced()
    if not managed and not enforce:
        return
    try:
        consume_inventory_sale(
            db,
            int(item["id"]),
            int(quantity),
            reason="order_sale",
            reference_type="order",
            reference_id=int(order_id),
            allow_backorder=not enforce,
        )
    except InsufficientStock:
        log.warning(
            "inventory_sale_skipped operation=sale entity=order entity_id=%s variant_id=%s error_type=InsufficientStock",
            order_id,
            variant_id,
        )
        try:
            from db import insert_business_event

            insert_business_event(
                db,
                event_type="INVENTORY_LOW",
                entity_type="product_variant",
                entity_id=int(variant_id),
                payload={"order_id": order_id, "quantity": quantity},
            )
        except Exception:
            log.exception("inventory_low_event_failed entity_id=%s", order_id)
        if enforce:
            raise
