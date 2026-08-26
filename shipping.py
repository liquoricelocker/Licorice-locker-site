"""Database-backed shipping pricing for Liquorice Locker.

The database is the source of truth. Checkout never hard-codes a shipping price.
Historical orders keep the cents they were charged; they are not recalculated
from the current rate table.

Store currency is NZD integer cents. This module does not convert currencies.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

log = logging.getLogger("licorice.shipping")

STORE_CURRENCY = "NZD"
COUNTRY_RE = re.compile(r"^[A-Z]{2}$")
SETTING_PACKAGING_WEIGHT = "packaging_weight_grams"

COUNTRY_LABELS = {
    "NZ": "New Zealand",
    "AU": "Australia",
    "US": "United States",
    "GB": "United Kingdom",
    "CA": "Canada",
    "DE": "Germany",
    "FR": "France",
    "IT": "Italy",
    "ES": "Spain",
    "NL": "Netherlands",
    "BE": "Belgium",
    "IE": "Ireland",
    "AT": "Austria",
    "CH": "Switzerland",
    "SE": "Sweden",
    "NO": "Norway",
    "DK": "Denmark",
    "FI": "Finland",
    "PT": "Portugal",
    "JP": "Japan",
    "SG": "Singapore",
    "HK": "Hong Kong",
}
STRIPE_CHECKOUT_COUNTRIES: Tuple[str, ...] = (
    "NZ",
    "AU",
    "US",
    "GB",
    "CA",
    "DE",
    "FR",
    "IT",
    "ES",
    "NL",
    "BE",
    "IE",
    "AT",
    "CH",
    "SE",
    "NO",
    "DK",
    "FI",
    "PT",
    "JP",
    "SG",
    "HK",
)

ERROR_EMPTY_CART = "empty_cart"
ERROR_INVALID_QUANTITY = "invalid_quantity"
ERROR_UNKNOWN_PRODUCT = "unknown_product"
ERROR_INVALID_COUNTRY = "invalid_country"
ERROR_METHOD_INACTIVE = "method_inactive"
ERROR_METHOD_UNKNOWN = "method_unknown"
ERROR_NO_ZONE = "no_zone"
ERROR_NO_RATE = "no_rate"
ERROR_INTERNATIONAL_CONTACT = "international_contact"
ERROR_AMBIGUOUS_RATE = "ambiguous_rate"
ERROR_WEIGHT_REQUIRED = "weight_required"

AUTOMATED_SHIPPING_COUNTRIES = frozenset({"NZ", "AU"})

_ERROR_MESSAGES = {
    ERROR_EMPTY_CART: "Your cart is empty.",
    ERROR_INVALID_QUANTITY: "A cart quantity is invalid.",
    ERROR_UNKNOWN_PRODUCT: "A product in the cart could not be found.",
    ERROR_INVALID_COUNTRY: "That destination country is not valid.",
    ERROR_METHOD_INACTIVE: "That shipping method is not available.",
    ERROR_METHOD_UNKNOWN: "That shipping method was not found.",
    ERROR_NO_ZONE: "International shipping — Contact us.",
    ERROR_NO_RATE: "No shipping method is available for this order and destination.",
    ERROR_INTERNATIONAL_CONTACT: "International shipping — Contact us.",
    ERROR_AMBIGUOUS_RATE: "Shipping could not be calculated because two rates match this order. Contact us.",
    ERROR_WEIGHT_REQUIRED: "This order cannot be quoted until product shipping weights are set.",
}


def store_currency() -> str:
    """Liquorice Locker charges in NZD. No conversion is applied."""
    return STORE_CURRENCY


def normalize_country_code(value: Any) -> Optional[str]:
    raw = str(value or "").strip().upper()
    if not COUNTRY_RE.match(raw):
        return None
    return raw


def normalize_currency(value: Any) -> Optional[str]:
    raw = str(value or "").strip().upper()
    if raw != STORE_CURRENCY:
        return None
    return raw


def user_message_for_error(code: str) -> str:
    return _ERROR_MESSAGES.get(code, ERROR_NO_RATE)


def checkout_unavailable_message(country: Any, quote_error: Optional[str] = None) -> str:
    """Customer-facing copy when checkout cannot quote shipping."""
    code = normalize_country_code(country)
    if code and code not in AUTOMATED_SHIPPING_COUNTRIES:
        return user_message_for_error(ERROR_INTERNATIONAL_CONTACT)
    return user_message_for_error(quote_error or ERROR_NO_RATE)


def format_shipping_price(cents: Optional[int]) -> str:
    if cents is None:
        return "—"
    n = int(cents)
    if n == 0:
        return "FREE"
    return f"${n / 100:.2f}"


def stripe_payable_total_cents(subtotal_cents: int, shipping_cents: int) -> int:
    """Stripe Checkout amount = product subtotal + server-calculated shipping. Integer cents only."""
    if int(subtotal_cents) < 0 or int(shipping_cents) < 0:
        raise ValueError("stripe_amounts_cannot_be_negative")
    return int(subtotal_cents) + int(shipping_cents)


@dataclass
class CartLine:
    product_id: int
    quantity: int
    weight_grams: Optional[int] = None
    name: str = ""


@dataclass
class ShippingQuote:
    ok: bool
    error: Optional[str] = None
    shipping_method: Optional[str] = None
    shipping_method_name: Optional[str] = None
    shipping_zone: Optional[str] = None
    shipping_zone_name: Optional[str] = None
    price_cents: Optional[int] = None
    currency: str = STORE_CURRENCY
    estimated_weight_grams: Optional[int] = None
    rule_id: Optional[int] = None
    estimated_min_days: Optional[int] = None
    estimated_max_days: Optional[int] = None
    missing_weight: bool = False
    missing_weight_product_ids: List[int] = field(default_factory=list)
    packaging_weight_grams: int = 0

    def user_message(self) -> str:
        if self.ok:
            return ""
        return user_message_for_error(self.error or ERROR_NO_RATE)


def _fail(code: str, **kwargs: Any) -> ShippingQuote:
    return ShippingQuote(ok=False, error=code, **kwargs)


def get_setting_int(db: sqlite3.Connection, key: str, default: int = 0) -> int:
    try:
        row = db.execute(
            "SELECT value FROM shipping_settings WHERE key = ?",
            (key,),
        ).fetchone()
    except sqlite3.Error:
        return default
    if not row:
        return default
    try:
        return int(row["value"] if isinstance(row, sqlite3.Row) else row[0])
    except (TypeError, ValueError):
        return default


def set_setting(db: sqlite3.Connection, key: str, value: str) -> None:
    db.execute(
        """
        INSERT INTO shipping_settings (key, value, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')
        """,
        (key, str(value)),
    )


def packaging_weight_grams(db: sqlite3.Connection) -> int:
    n = get_setting_int(db, SETTING_PACKAGING_WEIGHT, 0)
    return max(0, n)


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _int_or_none(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    return int(value)


def ranges_overlap(
    a_min: Optional[int],
    a_max: Optional[int],
    b_min: Optional[int],
    b_max: Optional[int],
) -> bool:
    """Inclusive integer ranges. None min = unbounded low; None max = unbounded high."""
    a_below = a_max is not None and b_min is not None and int(a_max) < int(b_min)
    b_below = b_max is not None and a_min is not None and int(b_max) < int(a_min)
    return not (a_below or b_below)


def value_in_range(value: int, min_v: Optional[int], max_v: Optional[int]) -> bool:
    if min_v is not None and value < int(min_v):
        return False
    if max_v is not None and value > int(max_v):
        return False
    return True


def _product_row(db: sqlite3.Connection, product_id: int) -> Optional[sqlite3.Row]:
    return db.execute("SELECT * FROM products WHERE id = ?", (int(product_id),)).fetchone()


def _default_variant(db: sqlite3.Connection, product_id: int) -> Optional[sqlite3.Row]:
    return db.execute(
        """
        SELECT * FROM product_variants
        WHERE product_id = ? AND deleted_at IS NULL
        ORDER BY is_default DESC, id ASC
        LIMIT 1
        """,
        (int(product_id),),
    ).fetchone()


def normalize_cart(db: sqlite3.Connection, cart: Sequence[Any]) -> Tuple[List[CartLine], Optional[str], bool, List[int]]:
    """Turn checkout cart rows into CartLine values. Does not invent missing weights."""
    lines: List[CartLine] = []
    missing_ids: List[int] = []
    saw_negative = False
    for raw in cart or []:
        if isinstance(raw, CartLine):
            pid = int(raw.product_id)
            qty = int(raw.quantity)
            weight = raw.weight_grams
            name = raw.name
        elif isinstance(raw, dict):
            product = raw.get("product")
            if product is not None and not isinstance(product, dict):
                try:
                    pid = int(product["id"])
                    name = str(product["name"] or "")
                except (KeyError, TypeError, ValueError):
                    return [], ERROR_UNKNOWN_PRODUCT, False, []
            else:
                try:
                    pid = int(raw.get("product_id") or raw.get("id") or 0)
                except (TypeError, ValueError):
                    return [], ERROR_UNKNOWN_PRODUCT, False, []
                name = str(raw.get("name") or "")
            try:
                qty = int(raw.get("quantity", 0))
            except (TypeError, ValueError):
                return [], ERROR_INVALID_QUANTITY, False, []
            weight = raw.get("weight_grams")
            if weight is not None and weight != "":
                try:
                    weight = int(weight)
                except (TypeError, ValueError):
                    weight = None
            else:
                weight = None
        else:
            return [], ERROR_UNKNOWN_PRODUCT, False, []
        if qty < 0:
            saw_negative = True
            continue
        if qty == 0:
            continue
        if pid < 1:
            return [], ERROR_UNKNOWN_PRODUCT, False, []
        product = _product_row(db, pid)
        if product is None:
            return [], ERROR_UNKNOWN_PRODUCT, False, []
        variant = _default_variant(db, pid)
        resolved_weight: Optional[int] = weight
        if resolved_weight is None and variant is not None:
            vg = variant["weight_grams"] if "weight_grams" in variant.keys() else None
            if vg is not None:
                resolved_weight = int(vg)
        if resolved_weight is None:
            missing_ids.append(pid)
        if not name:
            name = str(product["name"] or "")
        lines.append(
            CartLine(
                product_id=pid,
                quantity=qty,
                weight_grams=resolved_weight,
                name=name,
            )
        )
    if saw_negative and not lines:
        return [], ERROR_INVALID_QUANTITY, False, []
    if saw_negative:
        return [], ERROR_INVALID_QUANTITY, False, []
    missing_weight = bool(missing_ids)
    return lines, None, missing_weight, missing_ids


def order_value_cents(db: sqlite3.Connection, lines: Sequence[CartLine]) -> int:
    total = 0
    for line in lines:
        product = _product_row(db, line.product_id)
        if product is None:
            continue
        total += int(product["price_cents"]) * int(line.quantity)
    return total


def billable_weight_grams(
    lines: Sequence[CartLine],
    *,
    packaging_grams: int = 0,
    method_packaging_grams: int = 0,
) -> Optional[int]:
    """Sum of quantity × variant weight, plus packaging. None if any line has no weight."""
    product_total = 0
    for line in lines:
        if line.weight_grams is None:
            return None
        if int(line.weight_grams) < 0:
            return None
        product_total += int(line.weight_grams) * int(line.quantity)
    extra = max(0, int(packaging_grams)) + max(0, int(method_packaging_grams))
    return product_total + extra


def zones_for_country(db: sqlite3.Connection, country_code: str) -> List[sqlite3.Row]:
    specific = db.execute(
        """
        SELECT z.*
        FROM shipping_zones z
        INNER JOIN shipping_zone_countries c ON c.shipping_zone_id = z.id
        WHERE z.active = 1 AND c.country_code = ?
        ORDER BY z.id
        """,
        (country_code,),
    ).fetchall()
    if specific:
        return list(specific)
    catch_all = db.execute(
        """
        SELECT z.* FROM shipping_zones z
        WHERE z.active = 1 AND z.catch_all = 1
        ORDER BY z.id
        """
    ).fetchall()
    return list(catch_all)


def _method_by_code(db: sqlite3.Connection, code: str) -> Optional[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM shipping_methods WHERE code = ?",
        (str(code or "").strip(),),
    ).fetchone()


def _rates_for_method_zones(
    db: sqlite3.Connection, method_id: int, zone_ids: Sequence[int]
) -> List[sqlite3.Row]:
    if not zone_ids:
        return []
    placeholders = ",".join("?" for _ in zone_ids)
    return list(
        db.execute(
            f"""
            SELECT r.*, z.code AS zone_code, z.name AS zone_name
            FROM shipping_rates r
            INNER JOIN shipping_zones z ON z.id = r.shipping_zone_id
            WHERE r.shipping_method_id = ?
              AND r.active = 1
              AND z.active = 1
              AND r.shipping_zone_id IN ({placeholders})
            ORDER BY r.priority DESC, r.id ASC
            """,
            (int(method_id), *[int(z) for z in zone_ids]),
        ).fetchall()
    )


def _rate_matches(
    rate: sqlite3.Row,
    *,
    order_cents: int,
    weight_grams: Optional[int],
) -> bool:
    if not value_in_range(
        order_cents,
        _int_or_none(rate["min_order_value_cents"]),
        _int_or_none(rate["max_order_value_cents"]),
    ):
        return False
    min_w = _int_or_none(rate["min_weight_grams"])
    max_w = _int_or_none(rate["max_weight_grams"])
    if min_w is None and max_w is None:
        return True
    if weight_grams is None:
        return False
    return value_in_range(weight_grams, min_w, max_w)


def calculate_shipping(
    db: sqlite3.Connection,
    cart: Sequence[Any],
    destination_country: Any,
    shipping_method: Any,
) -> ShippingQuote:
    """Single source of truth: cart + destination + method → database rate.

    Never trusts a client-submitted price. Returns ok=False instead of charging $0
    when no rate matches.
    """
    if not _table_exists(db, "shipping_rates"):
        return _fail(ERROR_NO_RATE)

    country = normalize_country_code(destination_country)
    missing_weight = False
    missing_ids: List[int] = []
    if country is None:
        return _fail(ERROR_INVALID_COUNTRY)

    lines, err, missing_weight, missing_ids = normalize_cart(db, cart)
    if err:
        return _fail(err, missing_weight=missing_weight, missing_weight_product_ids=missing_ids)
    if not lines:
        return _fail(ERROR_EMPTY_CART)

    method_code = str(shipping_method or "").strip()
    if not method_code:
        return _fail(ERROR_METHOD_UNKNOWN, missing_weight=missing_weight, missing_weight_product_ids=missing_ids)

    method = _method_by_code(db, method_code)
    if method is None:
        return _fail(ERROR_METHOD_UNKNOWN, missing_weight=missing_weight, missing_weight_product_ids=missing_ids)
    if int(method["active"] or 0) != 1:
        return _fail(
            ERROR_METHOD_INACTIVE,
            shipping_method=str(method["code"]),
            shipping_method_name=str(method["name"] or method["code"]),
            missing_weight=missing_weight,
            missing_weight_product_ids=missing_ids,
        )

    zones = zones_for_country(db, country)
    if not zones:
        return _fail(
            ERROR_NO_ZONE,
            shipping_method=str(method["code"]),
            shipping_method_name=str(method["name"] or method["code"]),
            missing_weight=missing_weight,
            missing_weight_product_ids=missing_ids,
        )

    pack = packaging_weight_grams(db)
    method_pack = int(method["packaging_weight_grams"] or 0) if "packaging_weight_grams" in method.keys() else 0
    weight = billable_weight_grams(lines, packaging_grams=pack, method_packaging_grams=method_pack)
    order_cents = order_value_cents(db, lines)

    rates = _rates_for_method_zones(db, int(method["id"]), [int(z["id"]) for z in zones])
    matched = [r for r in rates if _rate_matches(r, order_cents=order_cents, weight_grams=weight)]
    if not matched:
        return _fail(
            ERROR_NO_RATE,
            shipping_method=str(method["code"]),
            shipping_method_name=str(method["name"] or method["code"]),
            shipping_zone=str(zones[0]["code"]),
            shipping_zone_name=str(zones[0]["name"] or zones[0]["code"]),
            currency=str(method["currency"] or STORE_CURRENCY),
            estimated_weight_grams=weight,
            missing_weight=missing_weight,
            missing_weight_product_ids=missing_ids,
            packaging_weight_grams=pack + method_pack,
        )

    top_priority = max(int(r["priority"] or 0) for r in matched)
    top = [r for r in matched if int(r["priority"] or 0) == top_priority]
    if len(top) > 1:
        log.warning(
            "shipping_ambiguous method=%s country=%s rate_ids=%s",
            method_code,
            country,
            [int(r["id"]) for r in top],
        )
        return _fail(
            ERROR_AMBIGUOUS_RATE,
            shipping_method=str(method["code"]),
            shipping_method_name=str(method["name"] or method["code"]),
            estimated_weight_grams=weight,
            missing_weight=missing_weight,
            missing_weight_product_ids=missing_ids,
            packaging_weight_grams=pack + method_pack,
        )

    rate = top[0]
    price = int(rate["price_cents"])
    currency = str(rate["currency"] or method["currency"] or STORE_CURRENCY).upper()
    return ShippingQuote(
        ok=True,
        shipping_method=str(method["code"]),
        shipping_method_name=str(method["name"] or method["code"]),
        shipping_zone=str(rate["zone_code"]),
        shipping_zone_name=str(rate["zone_name"] or rate["zone_code"]),
        price_cents=price,
        currency=currency,
        estimated_weight_grams=weight,
        rule_id=int(rate["id"]),
        estimated_min_days=_int_or_none(rate["estimated_min_days"]),
        estimated_max_days=_int_or_none(rate["estimated_max_days"]),
        missing_weight=missing_weight,
        missing_weight_product_ids=missing_ids,
        packaging_weight_grams=pack + method_pack,
    )


def list_quotes_for_destination(
    db: sqlite3.Connection,
    cart: Sequence[Any],
    destination_country: Any,
) -> Tuple[List[ShippingQuote], Optional[str], bool, List[int]]:
    """Active methods that have a matching rate for this cart and country."""
    country = normalize_country_code(destination_country)
    lines, err, missing_weight, missing_ids = normalize_cart(db, cart)
    if err:
        return [], err, missing_weight, missing_ids
    if not lines:
        return [], ERROR_EMPTY_CART, missing_weight, missing_ids
    if country is None:
        return [], ERROR_INVALID_COUNTRY, missing_weight, missing_ids
    if not zones_for_country(db, country):
        return [], ERROR_NO_ZONE, missing_weight, missing_ids

    methods = db.execute(
        """
        SELECT * FROM shipping_methods
        WHERE active = 1
        ORDER BY sort_order ASC, id ASC
        """
    ).fetchall()
    quotes: List[ShippingQuote] = []
    for method in methods:
        quote = calculate_shipping(db, lines, country, method["code"])
        if quote.ok:
            quotes.append(quote)
    return quotes, None, missing_weight, missing_ids


def checkout_countries(db: sqlite3.Connection) -> List[str]:
    """Countries customers can select. Includes Stripe-supported catch-all destinations."""
    codes = set()
    if _table_exists(db, "shipping_zone_countries"):
        for row in db.execute("SELECT country_code FROM shipping_zone_countries"):
            c = normalize_country_code(row["country_code"])
            if c:
                codes.add(c)
    has_catch_all = False
    if _table_exists(db, "shipping_zones"):
        has_catch_all = bool(
            db.execute(
                "SELECT 1 FROM shipping_zones WHERE active = 1 AND catch_all = 1 LIMIT 1"
            ).fetchone()
        )
    if has_catch_all:
        codes.update(STRIPE_CHECKOUT_COUNTRIES)
    if not codes:
        codes.update(STRIPE_CHECKOUT_COUNTRIES)
    return sorted(codes)


def country_label(code: str) -> str:
    c = (code or "").strip().upper()
    return COUNTRY_LABELS.get(c, c)


def quote_to_dict(quote: ShippingQuote) -> Dict[str, Any]:
    return {
        "ok": quote.ok,
        "error": quote.error,
        "shipping_method": quote.shipping_method,
        "shipping_method_name": quote.shipping_method_name,
        "shipping_zone": quote.shipping_zone,
        "price_cents": quote.price_cents,
        "currency": quote.currency,
        "estimated_weight_grams": quote.estimated_weight_grams,
        "rule_id": quote.rule_id,
        "estimated_min_days": quote.estimated_min_days,
        "estimated_max_days": quote.estimated_max_days,
        "missing_weight": quote.missing_weight,
    }


def snapshot_from_quote(quote: ShippingQuote) -> Dict[str, Any]:
    if not quote.ok or quote.price_cents is None:
        raise ValueError("cannot_snapshot_failed_quote")
    return {
        "shipping_cents": int(quote.price_cents),
        "shipping_method_code": quote.shipping_method,
        "shipping_method_name": quote.shipping_method_name,
        "shipping_rate_id": quote.rule_id,
        "shipping_currency": quote.currency or STORE_CURRENCY,
        "shipping_weight_grams": quote.estimated_weight_grams,
        "shipping_zone_code": quote.shipping_zone,
    }


def snapshot_from_checkout_metadata(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Read the server-written Stripe metadata snapshot. Ignore any client price field."""
    try:
        shipping_cents = max(0, int(meta.get("shipping_cents") or 0))
    except (TypeError, ValueError):
        shipping_cents = 0
    rate_id = None
    try:
        raw_rate = meta.get("shipping_rate_id")
        if raw_rate not in (None, ""):
            rate_id = int(raw_rate)
    except (TypeError, ValueError):
        rate_id = None
    weight = None
    try:
        raw_w = meta.get("shipping_weight_grams")
        if raw_w not in (None, ""):
            weight = int(raw_w)
    except (TypeError, ValueError):
        weight = None
    return {
        "shipping_cents": shipping_cents,
        "shipping_method_code": (meta.get("shipping_method") or "").strip() or None,
        "shipping_method_name": (meta.get("shipping_method_name") or "").strip() or None,
        "shipping_rate_id": rate_id,
        "shipping_currency": ((meta.get("shipping_currency") or STORE_CURRENCY).strip().upper() or STORE_CURRENCY),
        "shipping_weight_grams": weight,
        "shipping_zone_code": (meta.get("shipping_zone") or "").strip() or None,
        "destination_country": normalize_country_code(meta.get("destination_country")),
    }


def apply_order_shipping_snapshot(db: sqlite3.Connection, order_id: int, snapshot: Dict[str, Any]) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(orders)").fetchall()}
    assignments = []
    values: List[Any] = []
    mapping = {
        "shipping_method_code": snapshot.get("shipping_method_code"),
        "shipping_method_name": snapshot.get("shipping_method_name"),
        "shipping_rate_id": snapshot.get("shipping_rate_id"),
        "shipping_currency": snapshot.get("shipping_currency") or STORE_CURRENCY,
        "shipping_weight_grams": snapshot.get("shipping_weight_grams"),
        "shipping_zone_code": snapshot.get("shipping_zone_code"),
    }
    for col, val in mapping.items():
        if col in cols:
            assignments.append(f"{col} = ?")
            values.append(val)
    if not assignments:
        return
    values.append(int(order_id))
    db.execute(
        f"UPDATE orders SET {', '.join(assignments)} WHERE id = ?",
        values,
    )


def rate_conflict_with_existing(
    db: sqlite3.Connection,
    *,
    method_id: int,
    zone_id: int,
    min_order: Optional[int],
    max_order: Optional[int],
    min_weight: Optional[int],
    max_weight: Optional[int],
    priority: int,
    exclude_rate_id: Optional[int] = None,
    active: int = 1,
) -> List[int]:
    """Active rates that would both match the same cart at the same priority."""
    if int(active) != 1:
        return []
    rows = db.execute(
        """
        SELECT * FROM shipping_rates
        WHERE shipping_method_id = ?
          AND shipping_zone_id = ?
          AND active = 1
          AND priority = ?
        """,
        (int(method_id), int(zone_id), int(priority)),
    ).fetchall()
    conflicts: List[int] = []
    for row in rows:
        rid = int(row["id"])
        if exclude_rate_id is not None and rid == int(exclude_rate_id):
            continue
        if ranges_overlap(
            min_order,
            max_order,
            _int_or_none(row["min_order_value_cents"]),
            _int_or_none(row["max_order_value_cents"]),
        ) and ranges_overlap(
            min_weight,
            max_weight,
            _int_or_none(row["min_weight_grams"]),
            _int_or_none(row["max_weight_grams"]),
        ):
            conflicts.append(rid)
    return conflicts


def validate_rate_fields(
    *,
    price_cents: Any,
    currency: Any,
    min_order_value_cents: Any,
    max_order_value_cents: Any,
    min_weight_grams: Any,
    max_weight_grams: Any,
    priority: Any,
    estimated_min_days: Any,
    estimated_max_days: Any,
) -> Tuple[Dict[str, Any], List[str]]:
    errors: List[str] = []
    out: Dict[str, Any] = {}
    try:
        price = int(price_cents)
    except (TypeError, ValueError):
        price = -1
    if price < 0:
        errors.append("Price cannot be negative. Use 0 for free shipping.")
    out["price_cents"] = max(price, 0) if price >= 0 else 0

    cur = normalize_currency(currency)
    if cur is None:
        errors.append(f"Currency must be {STORE_CURRENCY}. Currency conversion is not supported.")
        cur = STORE_CURRENCY
    out["currency"] = cur

    def parse_opt_int(raw: Any, label: str, *, allow_zero: bool = True) -> Optional[int]:
        if raw is None or str(raw).strip() == "":
            return None
        try:
            n = int(raw)
        except (TypeError, ValueError):
            errors.append(f"{label} must be a whole number.")
            return None
        if n < 0:
            errors.append(f"{label} cannot be negative.")
            return None
        if n == 0 and not allow_zero:
            return 0
        return n

    min_o = parse_opt_int(min_order_value_cents, "Minimum order")
    max_o = parse_opt_int(max_order_value_cents, "Maximum order")
    min_w = parse_opt_int(min_weight_grams, "Minimum weight")
    max_w = parse_opt_int(max_weight_grams, "Maximum weight")
    if min_o is not None and max_o is not None and max_o < min_o:
        errors.append("Maximum order must be greater than or equal to minimum order.")
    if min_w is not None and max_w is not None and max_w < min_w:
        errors.append("Maximum weight must be greater than or equal to minimum weight.")
    out["min_order_value_cents"] = min_o
    out["max_order_value_cents"] = max_o
    out["min_weight_grams"] = min_w
    out["max_weight_grams"] = max_w

    try:
        pri = int(priority if str(priority).strip() != "" else 0)
    except (TypeError, ValueError):
        errors.append("Priority must be a whole number.")
        pri = 0
    out["priority"] = pri

    min_d = parse_opt_int(estimated_min_days, "Estimated minimum days")
    max_d = parse_opt_int(estimated_max_days, "Estimated maximum days")
    if min_d is not None and max_d is not None and max_d < min_d:
        errors.append("Estimated maximum days must be greater than or equal to minimum days.")
    out["estimated_min_days"] = min_d
    out["estimated_max_days"] = max_d
    return out, errors


def validate_country_code(value: Any) -> Tuple[Optional[str], Optional[str]]:
    code = normalize_country_code(value)
    if code is None:
        return None, "Country code must be a two-letter ISO code (for example NZ or AU)."
    return code, None


def human_rule_label(rate: Any) -> str:
    """Admin-facing description. Does not expose table names."""
    parts: List[str] = []
    min_o = _int_or_none(rate["min_order_value_cents"] if not isinstance(rate, dict) else rate.get("min_order_value_cents"))
    max_o = _int_or_none(rate["max_order_value_cents"] if not isinstance(rate, dict) else rate.get("max_order_value_cents"))
    min_w = _int_or_none(rate["min_weight_grams"] if not isinstance(rate, dict) else rate.get("min_weight_grams"))
    max_w = _int_or_none(rate["max_weight_grams"] if not isinstance(rate, dict) else rate.get("max_weight_grams"))
    if min_o is None and max_o is None:
        pass
    elif min_o is not None and max_o is None:
        parts.append(f"Orders ≥ ${min_o / 100:.2f}".replace(".00", ""))
    elif min_o is None and max_o is not None:
        parts.append(f"Orders ≤ ${max_o / 100:.2f}".replace(".00", ""))
    else:
        parts.append(
            f"Orders ${min_o / 100:.2f}–${max_o / 100:.2f}".replace(".00", "")
        )
    if min_w is not None or max_w is not None:
        parts.append(_weight_range_label(min_w, max_w))
    if not parts:
        return "All orders"
    return " · ".join(parts)


def _weight_range_label(min_w: Optional[int], max_w: Optional[int]) -> str:
    def fmt(g: int) -> str:
        if g == 0:
            return "0"
        if g % 1000 == 0:
            kg = g // 1000
            return f"{kg}kg"
        return f"{g}g"

    if min_w is None and max_w is not None:
        return f"Up to {fmt(max_w)}"
    if min_w is not None and max_w is None:
        return f"{fmt(min_w)}+"
    if min_w is not None and max_w is not None:
        return f"{fmt(min_w)}–{fmt(max_w)}"
    return ""


def variants_missing_weight(db: sqlite3.Connection) -> List[sqlite3.Row]:
    if not _table_exists(db, "product_variants"):
        return []
    return list(
        db.execute(
            """
            SELECT v.id, v.sku, v.name AS variant_name, p.name AS product_name, p.slug
            FROM product_variants v
            INNER JOIN products p ON p.id = v.product_id
            WHERE v.deleted_at IS NULL
              AND v.weight_grams IS NULL
              AND COALESCE(p.add_to_cart_enabled, 1) = 1
            ORDER BY p.sort_order, p.id, v.id
            """
        ).fetchall()
    )


def shipping_integrity(db: sqlite3.Connection) -> Dict[str, Any]:
    """Read-only. Does not repair invalid rows."""

    def _count(sql: str) -> int:
        try:
            return int(db.execute(sql).fetchone()[0])
        except sqlite3.Error:
            return -1

    if not _table_exists(db, "shipping_rates"):
        return {"available": False}

    overlapping: List[Dict[str, Any]] = []
    rows = db.execute(
        """
        SELECT * FROM shipping_rates WHERE active = 1 ORDER BY shipping_method_id, shipping_zone_id, priority, id
        """
    ).fetchall()
    seen_pairs = set()
    for i, a in enumerate(rows):
        for b in rows[i + 1 :]:
            if int(a["shipping_method_id"]) != int(b["shipping_method_id"]):
                continue
            if int(a["shipping_zone_id"]) != int(b["shipping_zone_id"]):
                continue
            if int(a["priority"] or 0) != int(b["priority"] or 0):
                continue
            if ranges_overlap(
                _int_or_none(a["min_order_value_cents"]),
                _int_or_none(a["max_order_value_cents"]),
                _int_or_none(b["min_order_value_cents"]),
                _int_or_none(b["max_order_value_cents"]),
            ) and ranges_overlap(
                _int_or_none(a["min_weight_grams"]),
                _int_or_none(a["max_weight_grams"]),
                _int_or_none(b["min_weight_grams"]),
                _int_or_none(b["max_weight_grams"]),
            ):
                pair = tuple(sorted((int(a["id"]), int(b["id"]))))
                if pair not in seen_pairs:
                    seen_pairs.add(pair)
                    overlapping.append({"rate_a": pair[0], "rate_b": pair[1], "priority": int(a["priority"] or 0)})

    return {
        "available": True,
        "rates_without_method": _count(
            """
            SELECT COUNT(*) FROM shipping_rates r
            LEFT JOIN shipping_methods m ON m.id = r.shipping_method_id
            WHERE m.id IS NULL
            """
        ),
        "rates_without_zone": _count(
            """
            SELECT COUNT(*) FROM shipping_rates r
            LEFT JOIN shipping_zones z ON z.id = r.shipping_zone_id
            WHERE z.id IS NULL
            """
        ),
        "invalid_currency": _count(
            f"""
            SELECT COUNT(*) FROM shipping_rates
            WHERE UPPER(COALESCE(currency, '')) != '{STORE_CURRENCY}'
            """
        ),
        "negative_prices": _count("SELECT COUNT(*) FROM shipping_rates WHERE price_cents < 0"),
        "invalid_order_thresholds": _count(
            """
            SELECT COUNT(*) FROM shipping_rates
            WHERE min_order_value_cents IS NOT NULL
              AND max_order_value_cents IS NOT NULL
              AND max_order_value_cents < min_order_value_cents
            """
        ),
        "invalid_weight_ranges": _count(
            """
            SELECT COUNT(*) FROM shipping_rates
            WHERE min_weight_grams IS NOT NULL
              AND max_weight_grams IS NOT NULL
              AND max_weight_grams < min_weight_grams
            """
        ),
        "negative_weight": _count(
            """
            SELECT COUNT(*) FROM shipping_rates
            WHERE COALESCE(min_weight_grams, 0) < 0 OR COALESCE(max_weight_grams, 0) < 0
            """
        ),
        "duplicate_active_overlapping_rules": len(overlapping),
        "overlapping_rate_pairs": overlapping[:50],
        "zone_countries_invalid_code": _count(
            """
            SELECT COUNT(*) FROM shipping_zone_countries
            WHERE country_code IS NULL
               OR length(trim(country_code)) != 2
               OR country_code != upper(country_code)
            """
        ),
        "variants_missing_weight": _count(
            """
            SELECT COUNT(*) FROM product_variants v
            INNER JOIN products p ON p.id = v.product_id
            WHERE v.deleted_at IS NULL
              AND v.weight_grams IS NULL
              AND COALESCE(p.add_to_cart_enabled, 1) = 1
            """
        ),
    }


def list_admin_rates(db: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = db.execute(
        """
        SELECT
            r.*,
            m.name AS method_name,
            m.code AS method_code,
            z.name AS zone_name,
            z.code AS zone_code
        FROM shipping_rates r
        INNER JOIN shipping_methods m ON m.id = r.shipping_method_id
        INNER JOIN shipping_zones z ON z.id = r.shipping_zone_id
        ORDER BY z.name, m.sort_order, r.priority DESC, r.min_order_value_cents, r.min_weight_grams, r.id
        """
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["rule_label"] = human_rule_label(r)
        d["price_label"] = format_shipping_price(int(r["price_cents"]))
        out.append(d)
    return out


def seed_reference_data(db: sqlite3.Connection) -> None:
    """Methods, zones, and country membership. Does not insert prices."""
    methods = [
        ("Standard (NZ)", "NZ_STANDARD", "New Zealand standard post", 10),
        ("Express (NZ)", "NZ_EXPRESS", "New Zealand express post", 20),
        ("International standard", "INTERNATIONAL_STANDARD", "Tracked international standard", 30),
        ("International express", "INTERNATIONAL_EXPRESS", "International express", 40),
        ("Local pickup", "LOCAL_PICKUP", "Collect from Liquorice Locker", 50),
    ]
    for name, code, desc, sort in methods:
        db.execute(
            """
            INSERT INTO shipping_methods (name, code, description, currency, active, sort_order)
            SELECT ?, ?, ?, ?, 1, ?
            WHERE NOT EXISTS (SELECT 1 FROM shipping_methods WHERE code = ?)
            """,
            (name, code, desc, STORE_CURRENCY, sort, code),
        )
    zones = [
        ("New Zealand", "NZ", "Domestic New Zealand", 0),
        ("Australia", "AUSTRALIA", "Australia", 0),
        ("Rest of world", "REST_OF_WORLD", "All other destinations", 1),
    ]
    for name, code, desc, catch_all in zones:
        db.execute(
            """
            INSERT INTO shipping_zones (name, code, description, active, catch_all)
            SELECT ?, ?, ?, 1, ?
            WHERE NOT EXISTS (SELECT 1 FROM shipping_zones WHERE code = ?)
            """,
            (name, code, desc, catch_all, code),
        )
    for zone_code, country in (("NZ", "NZ"), ("AUSTRALIA", "AU")):
        db.execute(
            """
            INSERT INTO shipping_zone_countries (shipping_zone_id, country_code)
            SELECT z.id, ?
            FROM shipping_zones z
            WHERE z.code = ?
              AND NOT EXISTS (
                  SELECT 1 FROM shipping_zone_countries c
                  WHERE c.shipping_zone_id = z.id AND c.country_code = ?
              )
            """,
            (country, zone_code, country),
        )
    db.execute(
        """
        INSERT INTO shipping_settings (key, value)
        SELECT ?, '0'
        WHERE NOT EXISTS (SELECT 1 FROM shipping_settings WHERE key = ?)
        """,
        (SETTING_PACKAGING_WEIGHT, SETTING_PACKAGING_WEIGHT),
    )


def seed_development_rates_if_empty(db: sqlite3.Connection) -> None:
    """Insert canonical rates when the table is empty (development/test fallback)."""
    if not _table_exists(db, "shipping_rates"):
        return
    n = int(db.execute("SELECT COUNT(*) AS c FROM shipping_rates").fetchone()[0])
    if n > 0:
        return
    apply_canonical_shipping_rates(db)


def apply_canonical_shipping_rates(db: sqlite3.Connection) -> None:
    """Insert store policy rates only when ``shipping_rates`` is empty.

    Never deletes or overwrites production/admin rates. Checkout still fails
    safely if no matching active rate exists (never silent $0).

    NZ Standard: $0–$149.99 → $12; $150–$299.99 → $18; $300+ → FREE
    Australia Standard: $0–$199.99 → $35; $200+ → $50
    Rest of world: no rates (checkout shows Contact us)
    """
    if not _table_exists(db, "shipping_rates"):
        return
    existing = int(db.execute("SELECT COUNT(*) AS c FROM shipping_rates").fetchone()[0])
    if existing > 0:
        log.info(
            "shipping_rates already has %s row(s); leaving database-owned prices unchanged",
            existing,
        )
        return

    def _id(table: str, code: str) -> Optional[int]:
        row = db.execute(f"SELECT id FROM {table} WHERE code = ?", (code,)).fetchone()
        return int(row["id"]) if row else None

    nz_m = _id("shipping_methods", "NZ_STANDARD")
    au_m = _id("shipping_methods", "INTERNATIONAL_STANDARD")
    z_nz = _id("shipping_zones", "NZ")
    z_au = _id("shipping_zones", "AUSTRALIA")
    if not all([nz_m, au_m, z_nz, z_au]):
        return

    rows = [
        (nz_m, z_nz, 1200, None, 14999, 3, 5),
        (nz_m, z_nz, 1800, 15000, 29999, 3, 5),
        (nz_m, z_nz, 0, 30000, None, 3, 5),
        (au_m, z_au, 3500, None, 19999, 5, 10),
        (au_m, z_au, 5000, 20000, None, 5, 10),
    ]
    for method_id, zone_id, price, min_o, max_o, dmin, dmax in rows:
        db.execute(
            """
            INSERT INTO shipping_rates (
                shipping_method_id, shipping_zone_id, price_cents, currency,
                min_order_value_cents, max_order_value_cents,
                min_weight_grams, max_weight_grams,
                active, priority, estimated_min_days, estimated_max_days
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, 1, 10, ?, ?)
            """,
            (
                method_id,
                zone_id,
                price,
                STORE_CURRENCY,
                min_o,
                max_o,
                dmin,
                dmax,
            ),
        )
    log.info("Applied canonical Liquorice Locker shipping rates")
