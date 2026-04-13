"""Order emails: confirmation and shipping. Resend (HTML) when RESEND_API_KEY is set; else SMTP."""

from __future__ import annotations

import html
import os
import smtplib
from email.message import EmailMessage
from typing import Any, List

try:
    import resend
except ImportError:  # pragma: no cover
    resend = None  # type: ignore[misc, assignment]

import db as database


def _send_smtp(to_addr: str, subject: str, body: str) -> bool:
    host = os.environ.get("SMTP_HOST")
    if not host:
        print(f"[Licorice Locker email — no SMTP_HOST] To: {to_addr}\nSubject: {subject}\n\n{body}\n")
        return True
    port = int(os.environ.get("SMTP_PORT", "587"))
    user = os.environ.get("SMTP_USER", "")
    password = os.environ.get("SMTP_PASSWORD", "")
    from_addr = os.environ.get("SMTP_FROM", user or "noreply@licoricelocker.local")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg.set_content(body)

    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls()
        if user:
            smtp.login(user, password)
        smtp.send_message(msg)
    return True


def send_order_confirmation(customer_email: str, order_number: str, lines: str, total_display: str) -> bool:
    body = (
        f"Thank you for your order from Licorice Locker.\n\n"
        f"Order: {order_number}\n"
        f"Total: {total_display}\n\n"
        f"Items:\n{lines}\n\n"
        f"We will notify you when your order ships."
    )
    try:
        return _send_smtp(customer_email, f"Order confirmed — {order_number}", body)
    except Exception as exc:
        print(f"[Licorice Locker] Order confirmation email failed: {exc}")
        return False


def send_password_reset_email(to_addr: str, reset_url: str) -> bool:
    body = (
        "You requested a password reset for your Licorice Locker Listening Room account.\n\n"
        f"Open this link to choose a new password (valid for a limited time):\n{reset_url}\n\n"
        "If you did not request this, you can ignore this email.\n"
    )
    try:
        return _send_smtp(to_addr, "Reset your Listening Room password — Licorice Locker", body)
    except Exception as exc:
        print(f"[Licorice Locker] Password reset email failed: {exc}")
        return False


def send_shipping_notification(
    customer_email: str,
    order_number: str,
    tracking_number: str,
    carrier_hint: str = "",
) -> bool:
    extra = f"\nCarrier / notes: {carrier_hint}\n" if carrier_hint else "\n"
    body = (
        f"Your Licorice Locker order has shipped.\n\n"
        f"Order: {order_number}\n"
        f"Tracking: {tracking_number}\n"
        f"{extra}"
        f"Thank you for supporting our artists and The Listening Room."
    )
    try:
        return _send_smtp(customer_email, f"Shipped — {order_number}", body)
    except Exception as exc:
        print(f"[Licorice Locker] Shipping email failed: {exc}")
        return False


def send_order_fulfilled_notification(customer_email: str, order_number: str) -> bool:
    """Customer-facing fulfilment notice; copy can be refined when site-wide email templates are finalised."""
    body = (
        f"Your Licorice Locker order is marked as fulfilled.\n\n"
        f"Order: {order_number}\n\n"
        f"Thank you for your purchase."
    )
    try:
        return _send_smtp(customer_email, f"Order fulfilled — {order_number}", body)
    except Exception as exc:
        print(f"[Licorice Locker] Fulfillment email failed: {exc}")
        return False


def _resend_from_address() -> str:
    return os.environ.get(
        "RESEND_FROM",
        "Licorice Locker <orders@licoricelocker.com>",
    ).strip()


def _admin_order_email() -> str:
    return (
        os.environ.get("ADMIN_ORDER_EMAIL")
        or os.environ.get("FOOTER_CONTACT_EMAIL")
        or "hello@licoricelocker.com"
    ).strip()


def send_resend_email(to: str, subject: str, html_body: str) -> bool:
    """Send one HTML message via Resend. Requires RESEND_API_KEY and verified `from` domain."""
    if resend is None:
        print("[Licorice Locker / Resend] Package not installed (pip install resend).")
        return False
    to = (to or "").strip()
    if not to:
        return False
    key = (os.environ.get("RESEND_API_KEY") or "").strip()
    if not key:
        return False
    resend.api_key = key
    try:
        params: dict[str, Any] = {
            "from": _resend_from_address(),
            "to": [to],
            "subject": subject,
            "html": html_body,
        }
        resend.Emails.send(params)
        print(f"[Licorice Locker / Resend] OK subject={subject!r} to={to}")
        return True
    except Exception as exc:
        print(f"[Licorice Locker / Resend] FAILED to={to!r} subject={subject!r}: {exc}")
        return False


def send_resend_post_purchase_emails(order_id: int) -> bool:
    """After Stripe payment is confirmed and the order row exists: customer + affiliate + admin.

    Uses live line items and ``affiliate_commission_cents`` from the database (same as dashboard).
    Returns True if the customer email was sent successfully. If RESEND_API_KEY is unset, returns
    False so the caller can fall back to SMTP ``send_order_confirmation``.
    """
    if resend is None or not (os.environ.get("RESEND_API_KEY") or "").strip():
        return False

    with database.get_db() as conn:
        order = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if not order:
            print(f"[Licorice Locker / Resend] No order id={order_id}; skip bundle.")
            return False
        item_rows = conn.execute(
            """
            SELECT oi.quantity, oi.unit_price_cents, p.name
            FROM order_items oi
            JOIN products p ON p.id = oi.product_id
            WHERE oi.order_id = ?
            ORDER BY p.sort_order, p.id
            """,
            (order_id,),
        ).fetchall()

        aff_id = order["affiliate_user_id"]
        aff_id_int = int(aff_id) if aff_id is not None else None
        aff_email = ""
        aff_display = ""
        if aff_id_int:
            u = conn.execute(
                "SELECT email, full_name, affiliate_slug FROM users WHERE id = ?",
                (aff_id_int,),
            ).fetchone()
            if u:
                aff_email = (u["email"] or "").strip()
                aff_display = (u["full_name"] or u["affiliate_slug"] or "").strip() or str(aff_id_int)

    order_number = str(order["order_number"] or "")
    customer_email = (order["customer_email"] or "").strip().lower()
    total_cents = int(order["total_cents"] or 0)
    total_display = database.format_money(total_cents)
    comm_cents = int(order["affiliate_commission_cents"] or 0)

    lis: List[str] = []
    for r in item_rows:
        name = html.escape(str(r["name"] or "Product"))
        qty = int(r["quantity"] or 1)
        unit = int(r["unit_price_cents"] or 0)
        line_cents = unit * qty
        line_disp = database.format_money(line_cents)
        lis.append(f"<li>{name} × {qty} — <strong>{html.escape(line_disp)}</strong></li>")
    items_block = "<ul>" + "".join(lis) + "</ul>" if lis else "<p>(no line items)</p>"

    summary_plain = ", ".join(
        f"{r['name']} ×{r['quantity']}" for r in item_rows
    ) or "Order"

    customer_html = (
        "<h1>Order Confirmed</h1>"
        "<p>Thank you for your purchase.</p>"
        f"<p>Order <strong>{html.escape(order_number)}</strong></p>"
        f"{items_block}"
        f"<p><strong>Total: {html.escape(total_display)}</strong></p>"
        "<p>Your piece is on its way.</p>"
    )
    customer_ok = send_resend_email(
        customer_email,
        "Your order is confirmed – Licorice Locker",
        customer_html,
    )
    if not customer_ok:
        return False

    if aff_email and aff_id_int:
        aff_html = (
            "<h1>You made a sale</h1>"
            f"<p>Product: {html.escape(summary_plain)}</p>"
            f"<p>Commission earned: <strong>{html.escape(database.format_money(comm_cents))}</strong></p>"
            f"<p>Order: {html.escape(order_number)}</p>"
        )
        send_resend_email(aff_email, "You made a sale \U0001f3b5", aff_html)

    admin_to = _admin_order_email()
    affiliate_line = html.escape(aff_display) if aff_display else "None"
    admin_html = (
        "<h1>New Order</h1>"
        f"<p>Order: {html.escape(order_number)}</p>"
        f"{items_block}"
        f"<p>Total: {html.escape(total_display)}</p>"
        f"<p>Customer: {html.escape(customer_email)}</p>"
        f"<p>Affiliate: {affiliate_line}</p>"
    )
    send_resend_email(admin_to, "New Order – Licorice Locker", admin_html)

    return True
