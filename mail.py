"""Licorice Locker — all outbound email. Resend (HTML) primary; SMTP fallback. Templates live here."""

from __future__ import annotations

import html as html_module
import logging
import os
import re
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage
from typing import Any, List, Optional, Tuple

try:
    import resend
except ImportError:  # pragma: no cover
    resend = None  # type: ignore[misc, assignment]

import db as database

logger = logging.getLogger("licorice.mail")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
)


def is_valid_email(addr: str) -> bool:
    s = (addr or "").strip()
    if len(s) > 254 or "@" not in s:
        return False
    return bool(_EMAIL_RE.match(s))


def _public_site_base() -> str:
    """Base URL for absolute image links in HTML (no trailing slash)."""
    for key in ("SITE_URL", "EMAIL_PUBLIC_BASE_URL"):
        v = (os.environ.get(key) or "").strip().rstrip("/")
        if v:
            return v if v.startswith("http") else f"https://{v.lstrip('/')}"
    dom = (os.environ.get("RAILWAY_PUBLIC_DOMAIN") or "").strip().lstrip("/")
    if dom:
        return dom if dom.startswith("http") else f"https://{dom}"
    return "https://www.licoricelocker.com"


def _email_logo_url() -> str:
    return f"{_public_site_base()}/static/shop-hero-banner.png"


def _resend_from_address() -> str:
    return (
        os.environ.get("RESEND_FROM", "").strip()
        or "Licorice Locker <orders@licoricelocker.com>"
    ).strip()


def _smtp_from_address() -> str:
    return (
        os.environ.get("SMTP_FROM")
        or os.environ.get("SMTP_USER")
        or "noreply@licoricelocker.local"
    ).strip()


def _admin_order_email() -> str:
    return (
        os.environ.get("ADMIN_ORDER_EMAIL")
        or os.environ.get("FOOTER_CONTACT_EMAIL")
        or "hello@licoricelocker.com"
    ).strip()


def _resend_api_key() -> str:
    return (os.environ.get("RESEND_API_KEY") or "").strip()


def _log_resend_from_safety() -> None:
    if _resend_api_key() and not (os.environ.get("RESEND_FROM") or "").strip():
        logger.error(
            "RESEND_API_KEY is set but RESEND_FROM is unset — using built-in default; "
            "set RESEND_FROM to a verified sender in Resend for production."
        )


# ---------------------------------------------------------------------------
# Order context (DB → templates)
# ---------------------------------------------------------------------------


@dataclass
class OrderEmailContext:
    """Serializable order view for template renders (swap for Resend templates later)."""

    order_id: int
    order_number: str
    customer_email: str
    customer_first: str
    customer_last: str
    total_cents: int
    lines: List[Tuple[str, int, int]]  # product_name, qty, line_cents
    affiliate_user_id: Optional[int]
    affiliate_email: str
    affiliate_display_name: str
    commission_cents: int
    logo_url: str
    shop_url: str


def load_order_email_context(order_id: int) -> Optional[OrderEmailContext]:
    with database.get_db() as conn:
        order = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if not order:
            return None
        rows = conn.execute(
            """
            SELECT oi.quantity, oi.unit_price_cents, p.name
            FROM order_items oi
            JOIN products p ON p.id = oi.product_id
            WHERE oi.order_id = ?
            ORDER BY p.sort_order, p.id
            """,
            (order_id,),
        ).fetchall()
    lines: List[Tuple[str, int, int]] = []
    for r in rows:
        q = int(r["quantity"] or 1)
        unit = int(r["unit_price_cents"] or 0)
        lines.append((str(r["name"] or "Product"), q, unit * q))
    aff_uid = order["affiliate_user_id"]
    aff_uid_i = int(aff_uid) if aff_uid is not None else None
    aff_email = ""
    aff_name = ""
    if aff_uid_i:
        with database.get_db() as conn:
            u = conn.execute(
                "SELECT email, full_name, affiliate_slug FROM users WHERE id = ?",
                (aff_uid_i,),
            ).fetchone()
        if u:
            aff_email = (u["email"] or "").strip()
            aff_name = (u["full_name"] or u["affiliate_slug"] or "").strip() or str(aff_uid_i)
    base = _public_site_base()
    return OrderEmailContext(
        order_id=order_id,
        order_number=str(order["order_number"] or ""),
        customer_email=(order["customer_email"] or "").strip().lower(),
        customer_first=str(order["customer_first"] or "").strip(),
        customer_last=str(order["customer_last"] or "").strip(),
        total_cents=int(order["total_cents"] or 0),
        lines=lines,
        affiliate_user_id=aff_uid_i,
        affiliate_email=aff_email,
        affiliate_display_name=aff_name,
        commission_cents=int(order["affiliate_commission_cents"] or 0),
        logo_url=_email_logo_url(),
        shop_url=base,
    )


# ---------------------------------------------------------------------------
# HTML layout (inline CSS, email-safe)
# ---------------------------------------------------------------------------


def _wrap_brand_html(title: str, inner_html: str) -> str:
    """Shared shell: Ghost White field, white card, Coffee Bean type — matches site. Inline styles only."""
    esc_title = html_module.escape(title)
    base = html_module.escape(_public_site_base())
    font_stack = (
        "Inter,-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Helvetica,Arial,sans-serif"
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width" /><title>{esc_title}</title></head>
<body style="margin:0;padding:0;background-color:#f1f3f9;font-family:{font_stack};">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color:#f1f3f9;padding:28px 14px;">
    <tr><td align="center">
      <table role="presentation" width="100%" style="max-width:560px;background-color:#ffffff;border-radius:2px;overflow:hidden;border:1px solid rgba(180,184,171,0.55);">
        <tr><td style="padding:28px 28px 12px 28px;text-align:center;">
          <p style="margin:0 0 10px 0;font-size:11px;letter-spacing:0.22em;font-weight:700;text-transform:uppercase;color:#25181d;">Licorice Locker</p>
          <img src="{html_module.escape(_email_logo_url())}" alt="" width="200" height="auto" style="max-width:200px;height:auto;display:block;margin:0 auto;border:0;" />
        </td></tr>
        <tr><td style="padding:8px 28px 32px 28px;color:#25181d;font-size:16px;line-height:1.55;">
{inner_html}
        </td></tr>
        <tr><td style="padding:16px 28px 24px 28px;border-top:1px solid rgba(180,184,171,0.45);color:rgba(37,24,29,0.58);font-size:12px;line-height:1.5;text-align:center;">
          <p style="margin:0;">The Listening Room</p>
          <p style="margin:10px 0 0 0;"><a href="{base}" style="color:#25181d;text-decoration:underline;text-underline-offset:2px;">Visit the shop</a></p>
        </td></tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


def render_customer_email(order: OrderEmailContext) -> Tuple[str, str]:
    """HTML + plain text for customer order confirmation."""
    total = database.format_money(order.total_cents)
    rows_html = []
    plain_lines = []
    for name, qty, line_c in order.lines:
        ld = database.format_money(line_c)
        rows_html.append(
            f'<tr><td style="padding:10px 0;border-bottom:1px solid rgba(180,184,171,0.45);">'
            f'<span style="color:#25181d;">{html_module.escape(name)}</span> '
            f'<span style="color:rgba(37,24,29,0.55);">× {qty}</span></td>'
            f'<td style="padding:10px 0;border-bottom:1px solid rgba(180,184,171,0.45);text-align:right;color:#25181d;font-weight:600;">{html_module.escape(ld)}</td></tr>'
        )
        plain_lines.append(f"  - {name} x{qty}  {ld}")
    table = (
        f'<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="margin:16px 0;">'
        f'{"".join(rows_html)}'
        f'<tr><td colspan="2" style="padding-top:16px;font-size:18px;font-weight:700;color:#25181d;">Total {html_module.escape(total)}</td></tr>'
        f"</table>"
    )
    inner = f"""
          <h1 style="margin:0 0 12px 0;font-size:22px;font-weight:600;color:#25181d;letter-spacing:0.02em;">Order confirmed</h1>
          <p style="margin:0 0 16px 0;color:rgba(37,24,29,0.62);">Thank you — your music-inspired piece is on its way from our studio.</p>
          <p style="margin:0 0 8px 0;color:rgba(37,24,29,0.55);font-size:13px;text-transform:uppercase;letter-spacing:0.08em;">Order number</p>
          <p style="margin:0 0 20px 0;font-size:18px;color:#25181d;font-weight:600;">{html_module.escape(order.order_number)}</p>
          {table}
          <p style="margin:20px 0 0 0;color:rgba(37,24,29,0.62);">We&apos;ll notify you when your order ships.</p>
"""
    plain = (
        f"Licorice Locker — Order confirmed\n\n"
        f"Order: {order.order_number}\n"
        f"Total: {total}\n\n"
        f"Items:\n" + "\n".join(plain_lines) + "\n\n"
        f"We will notify you when your order ships.\n"
        f"{order.shop_url}\n"
    )
    return _wrap_brand_html("Order confirmed", inner), plain


def render_admin_email(order: OrderEmailContext) -> Tuple[str, str]:
    total = database.format_money(order.total_cents)
    aff = order.affiliate_display_name or "None"
    rows = "".join(
        f"<li>{html_module.escape(n)} × {q} — {html_module.escape(database.format_money(lc))}</li>"
        for n, q, lc in order.lines
    )
    inner = f"""
          <h1 style="margin:0 0 12px 0;font-size:20px;color:#25181d;">New order</h1>
          <p style="margin:0;color:rgba(37,24,29,0.62);"><strong>Order</strong> {html_module.escape(order.order_number)} · <strong>Total</strong> {html_module.escape(total)}</p>
          <ul style="margin:16px 0;padding-left:20px;color:#25181d;">{rows or "<li>(no lines)</li>"}</ul>
          <p style="margin:8px 0;color:rgba(37,24,29,0.62);"><strong>Customer</strong> {html_module.escape(order.customer_email)}</p>
          <p style="margin:8px 0;color:rgba(37,24,29,0.62);"><strong>Affiliate</strong> {html_module.escape(aff)}</p>
"""
    plain = (
        f"New order {order.order_number}\nTotal: {total}\nCustomer: {order.customer_email}\nAffiliate: {aff}\n"
    )
    return _wrap_brand_html("New order", inner), plain


def render_affiliate_email(order: OrderEmailContext) -> Tuple[str, str]:
    subj = "You made a sale \U0001f3b5"
    comm = database.format_money(order.commission_cents)
    summary = ", ".join(f"{n} ×{q}" for n, q, _ in order.lines) or "Order"
    inner = f"""
          <h1 style="margin:0 0 12px 0;font-size:20px;color:#25181d;">You made a sale</h1>
          <p style="margin:0 0 12px 0;color:rgba(37,24,29,0.62);">Someone ordered through your Listening Room.</p>
          <p style="margin:0;color:#25181d;"><strong>{html_module.escape(summary)}</strong></p>
          <p style="margin:16px 0 0 0;font-size:18px;color:#25181d;">Commission earned: <strong>{html_module.escape(comm)}</strong></p>
          <p style="margin:12px 0 0 0;color:rgba(37,24,29,0.55);font-size:14px;">Order {html_module.escape(order.order_number)}</p>
"""
    plain = f"You made a sale\n\n{summary}\nCommission: {comm}\nOrder: {order.order_number}\n"
    return _wrap_brand_html("You made a sale", inner), plain


# ---------------------------------------------------------------------------
# Delivery: Resend → SMTP, with logging
# ---------------------------------------------------------------------------


def _log_attempt(
    outcome: str,
    provider: str,
    to_addr: str,
    subject: str,
    detail: str = "",
) -> None:
    extra = f" {detail}" if detail else ""
    logger.info(
        "email_attempt outcome=%s provider=%s to=%s subject=%r%s",
        outcome,
        provider,
        to_addr,
        subject,
        extra,
    )


def _send_via_resend(to_addr: str, subject: str, html_body: str, text_plain: str) -> bool:
    _log_resend_from_safety()
    if resend is None:
        _log_attempt("FAIL", "resend", to_addr, subject, "reason=package_missing")
        return False
    key = _resend_api_key()
    if not key:
        _log_attempt("FAIL", "resend", to_addr, subject, "reason=no_api_key")
        return False
    from_addr = _resend_from_address()
    resend.api_key = key
    try:
        params: dict[str, Any] = {
            "from": from_addr,
            "to": [to_addr],
            "subject": subject,
            "html": html_body,
            "text": text_plain,
        }
        resend.Emails.send(params)
        _log_attempt("SUCCESS", "resend", to_addr, subject)
        return True
    except Exception as exc:
        _log_attempt("FAIL", "resend", to_addr, subject, f"reason={exc!r}")
        return False


def _send_via_smtp_html(to_addr: str, subject: str, html_body: str, text_plain: str) -> bool:
    host = (os.environ.get("SMTP_HOST") or "").strip()
    if not host:
        logger.warning(
            "email_attempt outcome=DEV_SKIP provider=smtp to=%s subject=%r (no SMTP_HOST; printing excerpt)",
            to_addr,
            subject,
        )
        print(
            f"[Licorice Locker email — no SMTP_HOST] To: {to_addr}\nSubject: {subject}\n\n{text_plain}\n",
            flush=True,
        )
        return True
    try:
        port = int(os.environ.get("SMTP_PORT", "587"))
        user = os.environ.get("SMTP_USER", "")
        password = os.environ.get("SMTP_PASSWORD", "")
        from_addr = _smtp_from_address()

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg.set_content(text_plain)
        msg.add_alternative(html_body, subtype="html")

        with smtplib.SMTP(host, port) as smtp:
            smtp.starttls()
            if user:
                smtp.login(user, password)
            smtp.send_message(msg)
        _log_attempt("SUCCESS", "smtp", to_addr, subject)
        return True
    except Exception as exc:
        _log_attempt("FAIL", "smtp", to_addr, subject, f"reason={exc!r}")
        return False


def send_html_email(
    to_addr: str,
    subject: str,
    html_body: str,
    text_plain: str,
    *,
    purpose: str = "transactional",
) -> bool:
    """Resend first if configured; otherwise SMTP HTML. Never raises."""
    to_addr = (to_addr or "").strip()
    if not is_valid_email(to_addr):
        logger.warning(
            "email_attempt outcome=SKIP provider=none to=%r subject=%r reason=invalid_email purpose=%s",
            to_addr,
            subject,
            purpose,
        )
        return False
    try:
        if _resend_api_key():
            ok = _send_via_resend(to_addr, subject, html_body, text_plain)
            if ok:
                return True
            logger.info("email_fallback smtp_after_resend_failure to=%s purpose=%s", to_addr, purpose)
        return _send_via_smtp_html(to_addr, subject, html_body, text_plain)
    except Exception as exc:
        logger.exception("email_send_unexpected_error purpose=%s to=%s: %s", purpose, to_addr, exc)
        return False


# ---------------------------------------------------------------------------
# Public API — orders
# ---------------------------------------------------------------------------


def send_post_purchase_order_emails(order_id: int) -> bool:
    """
    Customer + affiliate + admin after order exists and commissions are applied.
    Returns True if the customer confirmation was delivered (Resend or SMTP).
    Idempotent: if receipt_sent already, logs and returns True without re-sending.
    """
    try:
        with database.get_db() as conn:
            order = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if not order:
            logger.error("post_purchase_emails abort reason=order_not_found order_id=%s", order_id)
            return False
        if int(order["receipt_sent"] or 0) == 1:
            logger.info(
                "post_purchase_emails SKIP order_id=%s reason=receipt_already_sent (idempotent)",
                order_id,
            )
            return True

        ctx = load_order_email_context(order_id)
        if not ctx:
            logger.error("post_purchase_emails abort reason=context_load_failed order_id=%s", order_id)
            return False

        cust_subj = "Your order is confirmed – Licorice Locker"
        html_c, plain_c = render_customer_email(ctx)
        if not is_valid_email(ctx.customer_email):
            logger.error(
                "post_purchase_emails FAIL order_id=%s reason=invalid_customer_email value=%r",
                order_id,
                ctx.customer_email,
            )
            return False

        customer_ok = send_html_email(
            ctx.customer_email,
            cust_subj,
            html_c,
            plain_c,
            purpose="post_purchase_customer",
        )
        if not customer_ok:
            return False

        if ctx.affiliate_user_id and ctx.affiliate_email and is_valid_email(ctx.affiliate_email):
            ha, pa = render_affiliate_email(ctx)
            send_html_email(
                ctx.affiliate_email,
                "You made a sale \U0001f3b5",
                ha,
                pa,
                purpose="post_purchase_affiliate",
            )
        elif ctx.affiliate_user_id and not ctx.affiliate_email:
            logger.warning(
                "post_purchase_affiliate SKIP order_id=%s reason=no_affiliate_email",
                order_id,
            )

        admin_to = _admin_order_email()
        if is_valid_email(admin_to):
            ha, pa = render_admin_email(ctx)
            send_html_email(
                admin_to,
                "New Order – Licorice Locker",
                ha,
                pa,
                purpose="post_purchase_admin",
            )
        else:
            logger.error("post_purchase_admin SKIP reason=invalid_ADMIN_ORDER_EMAIL")

        return True
    except Exception as exc:
        logger.exception("post_purchase_emails fatal order_id=%s: %s", order_id, exc)
        return False


def send_order_receipt_email_fallback(order_id: int) -> bool:
    """If Resend bundle path failed early, retry customer email only (same HTML template)."""
    try:
        ctx = load_order_email_context(order_id)
        if not ctx or not is_valid_email(ctx.customer_email):
            return False
        html_c, plain_c = render_customer_email(ctx)
        return send_html_email(
            ctx.customer_email,
            "Your order is confirmed – Licorice Locker",
            html_c,
            plain_c,
            purpose="receipt_fallback",
        )
    except Exception as exc:
        logger.exception("receipt_fallback failed order_id=%s: %s", order_id, exc)
        return False


# Backwards-compatible name / signature for older call sites
def send_order_confirmation(
    customer_email: str,
    order_number: str,
    lines: str,
    total_display: str,
) -> bool:
    """Legacy plain-text path; prefer send_order_receipt_email_fallback(order_id). Builds minimal HTML."""
    if not is_valid_email(customer_email):
        logger.warning("send_order_confirmation SKIP invalid email=%r", customer_email)
        return False
    esc = html_module.escape
    inner = f"""
          <h1 style="margin:0 0 12px 0;font-size:22px;color:#25181d;">Order confirmed</h1>
          <p style="margin:0 0 8px 0;color:#25181d;">Order <strong>{esc(order_number)}</strong></p>
          <pre style="white-space:pre-wrap;font-family:inherit;color:rgba(37,24,29,0.62);">{esc(lines)}</pre>
          <p style="margin:16px 0 0 0;color:#25181d;"><strong>Total {esc(total_display)}</strong></p>
"""
    html_b = _wrap_brand_html("Order confirmed", inner)
    plain = (
        f"Thank you for your order from Licorice Locker.\n\n"
        f"Order: {order_number}\nTotal: {total_display}\n\nItems:\n{lines}\n\n"
        f"We will notify you when your order ships."
    )
    return send_html_email(
        customer_email,
        f"Order confirmed — {order_number}",
        html_b,
        plain,
        purpose="legacy_confirmation",
    )


# ---------------------------------------------------------------------------
# Other flows (password, shipping, fulfilled)
# ---------------------------------------------------------------------------


def send_password_reset_email(to_addr: str, reset_url: str) -> bool:
    if not is_valid_email(to_addr):
        return False
    esc = html_module.escape
    inner = f"""
          <h1 style="margin:0 0 12px 0;font-size:20px;color:#25181d;">Reset your password</h1>
          <p style="color:rgba(37,24,29,0.62);">Open the link below to choose a new password (valid for a limited time).</p>
          <p style="margin:24px 0;"><a href="{esc(reset_url)}" style="display:inline-block;padding:12px 24px;background:#25181d;color:#ffffff;text-decoration:none;border-radius:2px;font-weight:600;">Reset password</a></p>
          <p style="color:rgba(37,24,29,0.55);font-size:13px;">If you did not request this, ignore this email.</p>
"""
    html_b = _wrap_brand_html("Password reset", inner)
    plain = (
        "Password reset — Licorice Locker\n\n"
        f"{reset_url}\n\n"
        "If you did not request this, ignore this email.\n"
    )
    return send_html_email(
        to_addr,
        "Reset your Listening Room password — Licorice Locker",
        html_b,
        plain,
        purpose="password_reset",
    )


def send_shipping_notification(
    customer_email: str,
    order_number: str,
    tracking_number: str,
    carrier_hint: str = "",
) -> bool:
    if not is_valid_email(customer_email):
        return False
    esc = html_module.escape
    extra = f"<p style='color:rgba(37,24,29,0.62);'>Carrier / notes: {esc(carrier_hint)}</p>" if carrier_hint else ""
    inner = f"""
          <h1 style="margin:0 0 12px 0;font-size:20px;color:#25181d;">Your order has shipped</h1>
          <p style="color:rgba(37,24,29,0.62);">Order <strong>{esc(order_number)}</strong></p>
          <p style="font-size:18px;color:#25181d;font-weight:600;">Tracking: {esc(tracking_number)}</p>
          {extra}
          <p style="color:rgba(37,24,29,0.62);">Thank you for supporting our artists and The Listening Room.</p>
"""
    html_b = _wrap_brand_html("Shipped", inner)
    plain = (
        f"Shipped — {order_number}\nTracking: {tracking_number}\n"
        + (f"Carrier: {carrier_hint}\n" if carrier_hint else "")
        + "\nThank you.\n"
    )
    return send_html_email(
        customer_email,
        f"Shipped — {order_number}",
        html_b,
        plain,
        purpose="shipping",
    )


def send_order_fulfilled_notification(customer_email: str, order_number: str) -> bool:
    if not is_valid_email(customer_email):
        return False
    esc = html_module.escape
    inner = f"""
          <h1 style="margin:0 0 12px 0;font-size:20px;color:#25181d;">Order fulfilled</h1>
          <p style="color:rgba(37,24,29,0.62);">Order <strong>{esc(order_number)}</strong> is marked as fulfilled.</p>
          <p style="color:#25181d;">Thank you for your purchase.</p>
"""
    html_b = _wrap_brand_html("Fulfilled", inner)
    plain = f"Order fulfilled — {order_number}\n\nThank you for your purchase.\n"
    return send_html_email(
        customer_email,
        f"Order fulfilled — {order_number}",
        html_b,
        plain,
        purpose="fulfilled",
    )


# Alias for imports that expect the old Resend-only name
send_resend_post_purchase_emails = send_post_purchase_order_emails
