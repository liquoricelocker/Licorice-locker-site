"""Order emails: confirmation and shipping. Uses SMTP when configured; logs otherwise."""

from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage


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
