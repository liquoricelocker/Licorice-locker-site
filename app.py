from __future__ import annotations

"""Licorice Locker — Flask web app."""

from dotenv import load_dotenv

# Load .env before any imports that read os.environ at module load (e.g. db.py).
load_dotenv()

import copy
import json
import os
import secrets
import sqlite3
import uuid
import base64
import io
import time
from pathlib import Path
from calendar import month_name, monthrange
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from flask import (
    Flask,
    flash,
    has_request_context,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import LoginManager, UserMixin, current_user, login_required, login_user, logout_user
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

import pyotp
import qrcode
import stripe

import db as database
from tracking import client_ip_from_request, device_class_from_user_agent, geo_lookup, ip_fingerprint
from commissions import (
    COMMISSION_TIERS,
    EARNINGS_DISPLAY_NZD,
    LIST_PRICE_MINI_SERIES_NZD,
    LIST_PRICE_SOUNDWAVE_NZD,
    commission_cents_for_nth_sale,
    current_rate_for_next_sale_after,
    monthly_milestone_bonus_cents,
    next_tier_sales_threshold,
    progress_toward_next_tier_pct,
    rate_for_nth_sale,
    summarize_month,
)
from mail import (
    send_order_confirmation,
    send_order_fulfilled_notification,
    send_password_reset_email,
    send_resend_post_purchase_emails,
)


def _env_secret_clean(key: str, *fallback_keys: str) -> str:
    """Read env var, strip whitespace/newlines, optional wrapping quotes (common copy-paste issues)."""
    raw = os.environ.get(key) or ""
    for fk in fallback_keys:
        if not str(raw).strip():
            raw = os.environ.get(fk) or ""
        else:
            break
    s = str(raw).strip().replace("\r", "").replace("\n", "")
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"":
        s = s[1:-1].strip()
    return s


def _stripe_secret_key() -> str:
    return _env_secret_clean("STRIPE_SECRET_KEY", "STRIPE_API_KEY", "STRIPE_SECRET")


def _stripe_publishable_key() -> str:
    return _env_secret_clean(
        "STRIPE_PUBLIC_KEY",
        "STRIPE_PUBLISHABLE_KEY",
        "STRIPE_PUBLISHABLEKEY",
    )


app = Flask(__name__)

# Railway (and similar) sit behind a reverse proxy; fixes request.host_url / scheme for Stripe return URLs.
if (os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("TRUST_PROXY") or "").strip():
    from werkzeug.middleware.proxy_fix import ProxyFix

    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

AVATAR_UPLOAD_DIR = Path(__file__).resolve().parent / "static" / "uploads" / "avatars"
MAX_AVATAR_UPLOAD_BYTES = 3 * 1024 * 1024

BANNER_UPLOAD_DIR = Path(__file__).resolve().parent / "static" / "uploads" / "affiliate-banners"
MAX_BANNER_UPLOAD_BYTES = 8 * 1024 * 1024

CREATIVE_UPLOAD_DIR = Path(__file__).resolve().parent / "static" / "uploads" / "creative"
MAX_CREATIVE_IMAGE_BYTES = 35 * 1024 * 1024
MAX_CREATIVE_VIDEO_BYTES = 220 * 1024 * 1024
_CREATIVE_IMAGE_EXT = frozenset({".jpg", ".jpeg", ".png", ".webp", ".gif"})
_CREATIVE_VIDEO_EXT = frozenset({".mp4", ".webm", ".mov"})


def _creative_kind_from_ext(ext: str) -> Optional[str]:
    e = ext.lower()
    if e in _CREATIVE_IMAGE_EXT:
        return "image"
    if e in _CREATIVE_VIDEO_EXT:
        return "video"
    return None


def _unlink_static_relative(rel: Optional[str]) -> None:
    if not rel or not str(rel).strip():
        return
    rel = str(rel).strip().lstrip("/")
    if rel.startswith("static/"):
        rel = rel[len("static/") :]
    p = Path(__file__).resolve().parent / "static" / rel
    try:
        if p.is_file():
            p.unlink()
    except OSError:
        pass


def _store_creative_main_file(file_storage) -> Tuple[str, str]:
    """Save main asset; returns (path under static/, asset_type)."""
    raw = file_storage.read()
    fname = secure_filename(file_storage.filename or "file")
    ext = Path(fname).suffix.lower()
    kind = _creative_kind_from_ext(ext)
    if not kind:
        raise ValueError("unsupported_type")
    if kind == "image" and len(raw) > MAX_CREATIVE_IMAGE_BYTES:
        raise ValueError("image_too_large")
    if kind == "video" and len(raw) > MAX_CREATIVE_VIDEO_BYTES:
        raise ValueError("video_too_large")
    key = f"{uuid.uuid4().hex}{ext}"
    CREATIVE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = CREATIVE_UPLOAD_DIR / key
    dest.write_bytes(raw)
    return f"uploads/creative/{key}", kind


def _store_creative_thumbnail_file(file_storage) -> str:
    raw = file_storage.read()
    fname = secure_filename(file_storage.filename or "thumb.jpg")
    ext = Path(fname).suffix.lower()
    if ext not in _CREATIVE_IMAGE_EXT:
        raise ValueError("thumb_not_image")
    if len(raw) > MAX_CREATIVE_IMAGE_BYTES:
        raise ValueError("thumb_too_large")
    key = f"{uuid.uuid4().hex}_t{ext}"
    CREATIVE_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    dest = CREATIVE_UPLOAD_DIR / key
    dest.write_bytes(raw)
    return f"uploads/creative/{key}"


def _save_affiliate_display_picture(uid: int, file_storage) -> str:
    """Resize avatar to max 512px, save JPEG under static. Returns path for url_for('static', filename=...)."""
    from PIL import Image

    raw = file_storage.read()
    if len(raw) > MAX_AVATAR_UPLOAD_BYTES:
        raise ValueError("Image must be 3 MB or smaller.")
    if len(raw) < 24:
        raise ValueError("Invalid image file.")
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception:
        raise ValueError("Could not read that image. Use JPEG, PNG, WebP, or GIF.")
    try:
        resample = Image.Resampling.LANCZOS
    except AttributeError:
        resample = Image.LANCZOS  # type: ignore[attr-defined]
    img = img.convert("RGB")
    img.thumbnail((512, 512), resample)
    AVATAR_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    out_path = AVATAR_UPLOAD_DIR / f"{uid}.jpg"
    img.save(out_path, "JPEG", quality=88, optimize=True)
    return f"uploads/avatars/{uid}.jpg"


def _unlink_affiliate_avatar_file(uid: int) -> None:
    p = AVATAR_UPLOAD_DIR / f"{uid}.jpg"
    try:
        if p.is_file():
            p.unlink()
    except OSError:
        pass


def _unlink_affiliate_banner_file(uid: int) -> None:
    p = BANNER_UPLOAD_DIR / f"{uid}.jpg"
    try:
        if p.is_file():
            p.unlink()
    except OSError:
        pass


def _affiliate_banner_is_uploaded_path(url: Optional[str]) -> bool:
    u = (url or "").strip()
    return u.startswith("uploads/affiliate-banners/") and u.endswith(".jpg")


def _save_affiliate_banner(uid: int, file_storage) -> str:
    """Resize banner to max 1920px wide; save JPEG under static. Returns path for url_for(static, ...)."""
    from PIL import Image

    raw = file_storage.read()
    if len(raw) > MAX_BANNER_UPLOAD_BYTES:
        raise ValueError("Banner image must be 8 MB or smaller.")
    if len(raw) < 24:
        raise ValueError("Invalid image file.")
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception:
        raise ValueError("Could not read that image. Use JPEG, PNG, WebP, or GIF.")
    try:
        resample = Image.Resampling.LANCZOS
    except AttributeError:
        resample = Image.LANCZOS  # type: ignore[attr-defined]
    img = img.convert("RGB")
    img.thumbnail((1920, 1920), resample)
    BANNER_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    out_path = BANNER_UPLOAD_DIR / f"{uid}.jpg"
    img.save(out_path, "JPEG", quality=88, optimize=True)
    return f"uploads/affiliate-banners/{uid}.jpg"


def _listening_room_banner_url(page: Optional[sqlite3.Row]) -> str:
    """Banner image for Listening Room shop replica; falls back to default shop banner."""
    if not page:
        return url_for("static", filename="banner.png")
    raw = ""
    if "banner_image_url" in page.keys():
        raw = (page["banner_image_url"] or "").strip()
    if not raw:
        return url_for("static", filename="banner.png")
    if raw.startswith(("http://", "https://")):
        return raw
    if raw.startswith("/"):
        return raw
    return url_for("static", filename=raw.lstrip("/"))


@app.template_global()
def admin_affiliate_orders_url(code: Optional[str], dq: str = "", dsa: str = "") -> str:
    """Admin drill-down to one member's orders; optional dashboard filter passthrough."""
    c = (code or "").strip()
    if not c:
        return url_for("admin_dashboard")
    kw: Dict[str, str] = {"code": c}
    dqp = (dq or "").strip()
    dsap = (dsa or "").strip()
    if dqp:
        kw["dq"] = dqp
    if dsap:
        kw["dsa"] = dsap
    return url_for("admin_affiliate_orders", **kw)


@app.template_global()
def affiliate_media_src(url: Optional[str]) -> str:
    """Resolve display picture URL: external https, /static/..., or path under static/."""
    if not url:
        return ""
    u = url.strip()
    if u.startswith("http://") or u.startswith("https://"):
        return u
    if u.startswith("/static/"):
        return u
    fn = u.lstrip("/")
    if fn.startswith("static/"):
        fn = fn[len("static/") :]
    return url_for("static", filename=fn)


# Idempotent schema migrations (safe for gunicorn import).
database.init_db()
app.secret_key = os.environ.get("SECRET_KEY", "dev-change-me-licorice-locker")
# Same-site return from Stripe → keep cart: Lax allows cookie on top-level return; Secure on HTTPS hosts.
_use_secure_cookie = (os.environ.get("SESSION_COOKIE_SECURE") or "").strip().lower() in (
    "1",
    "true",
    "yes",
) or bool((os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RENDER") or "").strip())
app.config["SESSION_COOKIE_SECURE"] = _use_secure_cookie
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"


def _absolute_site_url(relative_path: str) -> str:
    """If SITE_URL is set (e.g. https://licorice-locker.com), return absolute URLs for header links."""
    base = os.environ.get("SITE_URL", "").rstrip("/")
    if not base:
        return relative_path
    if relative_path.startswith("/"):
        return base + relative_path
    return f"{base}/{relative_path}"


@app.context_processor
def inject_header_home() -> Dict[str, Any]:
    """Logo home: shop index unless on Listening Room shop replica or legacy /a/<slug> landing."""
    path = request.path or ""
    listening_room_code: Optional[str] = None
    if path.startswith("/listening-room/"):
        rest = path[len("/listening-room/") :].strip("/")
        if rest:
            listening_room_code = rest.split("/", 1)[0] or None
    affiliate_slug: Optional[str] = None
    if path.startswith("/a/"):
        rest = path[len("/a/") :].strip("/")
        if rest:
            affiliate_slug = rest.split("/", 1)[0] or None
    if listening_room_code:
        rel = url_for("listening_room", code=listening_room_code)
    elif affiliate_slug:
        rel = url_for("affiliate_landing", slug=affiliate_slug)
    else:
        rel = url_for("index")
    return {
        "header_home_url": _absolute_site_url(rel),
        "header_is_affiliate_context": listening_room_code is not None or affiliate_slug is not None,
    }


def _session_cart_total_qty() -> int:
    raw = session.get("cart")
    if isinstance(raw, list):
        cart_qty = 0
        for row in raw:
            if not isinstance(row, dict):
                continue
            try:
                cart_qty += max(0, int(row.get("quantity", 0)))
            except (TypeError, ValueError):
                pass
        return cart_qty
    if isinstance(raw, dict):
        cart_qty = 0
        for q in raw.values():
            try:
                cart_qty += max(0, int(q))
            except (TypeError, ValueError):
                pass
        return cart_qty
    return 0


@app.context_processor
def inject_cart() -> Dict[str, Any]:
    cart_qty = _session_cart_total_qty()
    return {"cart_item_count": cart_qty, "cart_has_items": cart_qty > 0}


# Homepage / slide-out menu order (explicit; not DB sort order).
NAV_MENU_ITEMS: Tuple[Tuple[str, str, str], ...] = (
    ("sound-wave", "Sound Wave", "sound-wave-feature-01.png"),
    ("riff", "Riff", "riff-feature-01.png"),
    ("harmony", "Harmony", "harmony-feature-01.png"),
    ("melody", "Melody", "melody-feature-01.png"),
    ("allegro", "Allegra", "allegra-feature-01.png"),
)


def _product_success_image_static(slug: Optional[str]) -> str:
    """Static path under static/ for success-page hero (matches checkout line banners)."""
    s = (slug or "").strip().lower()
    banners = {x[0]: x[2] for x in NAV_MENU_ITEMS}
    if s in banners:
        return banners[s]
    if s:
        return f"products/{s}.svg"
    return "shop-hero-banner.png"


def _order_success_product_rows(conn: sqlite3.Connection, order_id: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT p.name, p.slug, oi.quantity
        FROM order_items oi
        JOIN products p ON p.id = oi.product_id
        WHERE oi.order_id = ?
        ORDER BY p.sort_order, p.id
        """,
        (order_id,),
    ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "name": str(r["name"] or "Product"),
                "quantity": int(r["quantity"] or 1),
                "image_static": _product_success_image_static(str(r["slug"] or "")),
            }
        )
    return out


def _cart_migrate_dict_to_list(d: Dict[str, Any], conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for pid_s, qty_raw in (d or {}).items():
        try:
            pid = int(pid_s)
            qty = int(qty_raw)
        except (TypeError, ValueError):
            continue
        if qty < 1:
            continue
        p = database.product_by_id(conn, pid)
        if not p or not database.product_add_to_cart_enabled(p):
            continue
        slug = str(p["slug"] or "").strip().lower()
        rows.append(
            {
                "product_id": pid,
                "quantity": min(999, qty),
                "name": str(p["name"] or "Product"),
                "price_cents": int(p["price_cents"]),
                "image": _product_success_image_static(slug),
                "slug": slug,
            }
        )
    rows.sort(key=lambda x: x["product_id"])
    return rows


def _cart_merge_duplicate_lines(lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_pid: Dict[int, Dict[str, Any]] = {}
    for row in lines:
        pid = int(row["product_id"])
        q = int(row.get("quantity", 1))
        if pid in by_pid:
            by_pid[pid]["quantity"] = min(999, int(by_pid[pid]["quantity"]) + q)
        else:
            by_pid[pid] = dict(row)
    return [by_pid[k] for k in sorted(by_pid.keys())]


def _cart_get_list(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    """Normalized cart: list of {product_id, quantity, name, price_cents, image, slug}. Migrates legacy dict cart."""
    raw = session.get("cart")
    if not raw:
        return []
    if isinstance(raw, dict):
        lst = _cart_migrate_dict_to_list(raw, conn)
        session["cart"] = lst
        session.modified = True
        return lst
    if not isinstance(raw, list):
        session["cart"] = []
        session.modified = True
        return []
    out: List[Dict[str, Any]] = []
    dropped = False
    for entry in raw:
        if not isinstance(entry, dict):
            dropped = True
            continue
        try:
            pid = int(entry.get("product_id") or entry.get("id") or 0)
        except (TypeError, ValueError):
            dropped = True
            continue
        if pid < 1:
            dropped = True
            continue
        p = database.product_by_id(conn, pid)
        if not p or not database.product_add_to_cart_enabled(p):
            dropped = True
            continue
        try:
            q = max(1, min(999, int(entry.get("quantity", 1))))
        except (TypeError, ValueError):
            q = 1
        slug = str(p["slug"] or "").strip().lower()
        out.append(
            {
                "product_id": pid,
                "quantity": q,
                "name": str(p["name"] or "Product"),
                "price_cents": int(p["price_cents"]),
                "image": _product_success_image_static(slug),
                "slug": slug,
            }
        )
    merged = _cart_merge_duplicate_lines(out)
    if merged != raw or dropped or len(merged) != len(raw):
        session["cart"] = merged
        session.modified = True
    return merged


@app.context_processor
def inject_nav_menu() -> Dict[str, Any]:
    return {
        "nav_menu_items": [
            {"slug": slug, "name": name, "menu_image": fn} for slug, name, fn in NAV_MENU_ITEMS
        ]
    }


@app.context_processor
def inject_product_checkout_banner() -> Dict[str, Any]:
    """Hero/banner static image per product slug (checkout line thumbnails)."""
    banners = {s: fn for s, _n, fn in NAV_MENU_ITEMS}

    def product_checkout_banner(slug: Optional[str]) -> Optional[str]:
        if not slug:
            return None
        return banners.get(str(slug).strip().lower())

    return {"product_checkout_banner": product_checkout_banner}


class User(UserMixin):
    def __init__(self, row: sqlite3.Row) -> None:
        self.id = row["id"]
        self.email = row["email"]
        self.role = row["role"]
        self.affiliate_slug = row["affiliate_slug"]
        self.full_name = row["full_name"]
        self.affiliate_code = row["affiliate_code"] if "affiliate_code" in row.keys() else None
        self.display_picture_url = row["display_picture_url"] if "display_picture_url" in row.keys() else None
        self.terms_accepted_at = row["terms_accepted_at"] if "terms_accepted_at" in row.keys() else None
        if "terms_accepted" in row.keys():
            self.terms_accepted = int(row["terms_accepted"] or 0)
        else:
            self.terms_accepted = 1 if self.terms_accepted_at else 0
        self.affiliate_active = int(row["affiliate_active"]) if "affiliate_active" in row.keys() else 1


@login_manager.user_loader
def load_user(user_id: str) -> Optional[User]:
    with database.get_db() as conn:
        row = database.user_by_id(conn, int(user_id))
        if row:
            return User(row)
    return None


def _admin_owner_emails() -> frozenset:
    """Emails allowed to use /login + full admin UI even if DB role is still ``affiliate``.
    Set ``ADMIN_EMAILS`` to a comma-separated list to override or extend (default: liquoricelocker@gmail.com)."""
    raw = os.environ.get("ADMIN_EMAILS", "liquoricelocker@gmail.com")
    return frozenset(e.strip().lower() for e in raw.split(",") if e.strip())


def _user_is_effective_admin(user: Optional[User] = None) -> bool:
    u = user if user is not None else current_user
    if not u or not getattr(u, "is_authenticated", False):
        return False
    if getattr(u, "role", None) == "admin":
        return True
    email = (getattr(u, "email", None) or "").strip().lower()
    return email in _admin_owner_emails()


@app.template_global()
def user_is_effective_admin() -> bool:
    """Templates: show Admin nav and staff UI for role=admin or owner email allowlist."""
    return _user_is_effective_admin()


def _affiliate_row_public_ok(row: sqlite3.Row) -> bool:
    """Active Listening Room member with terms accepted: public link, cookie tracking, commissions."""
    if not row:
        return False
    if "affiliate_active" in row.keys() and int(row["affiliate_active"] or 0) != 1:
        return False
    if "terms_accepted" in row.keys():
        return int(row["terms_accepted"] or 0) == 1
    return bool(row["terms_accepted_at"] if "terms_accepted_at" in row.keys() else None)


def _affiliate_from_cookie(conn: sqlite3.Connection, slug: Optional[str]) -> Optional[sqlite3.Row]:
    if not slug or not str(slug).strip():
        return None
    row = database.affiliate_by_slug(conn, str(slug).strip())
    if not row:
        return None
    return row if _affiliate_row_public_ok(row) else None


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


AFFILIATE_2FA_PENDING_KEY = "affiliate_2fa_pending"
AFFILIATE_2FA_TTL_SEC = 600


def _totp_provisioning_qr_data_url(secret: str, email: str) -> str:
    totp = pyotp.TOTP(secret)
    uri = totp.provisioning_uri(name=email, issuer_name="Licorice Locker")
    img = qrcode.make(uri)
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def _clear_affiliate_2fa_session() -> None:
    session.pop(AFFILIATE_2FA_PENDING_KEY, None)


def _affiliate_2fa_pending_valid() -> Optional[Dict[str, Any]]:
    data = session.get(AFFILIATE_2FA_PENDING_KEY)
    if not data or not isinstance(data, dict):
        return None
    uid = data.get("uid")
    exp = data.get("exp")
    if uid is None or exp is None:
        _clear_affiliate_2fa_session()
        return None
    if time.time() > float(exp):
        _clear_affiliate_2fa_session()
        return None
    return data


def _parse_iso_datetime(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def affiliate_orders_in_month(
    conn: sqlite3.Connection, affiliate_id: int, year: int, month: int
) -> List[sqlite3.Row]:
    start = f"{year:04d}-{month:02d}-01 00:00:00"
    last_day = monthrange(year, month)[1]
    end = f"{year:04d}-{month:02d}-{last_day:02d} 23:59:59"
    return conn.execute(
        """
        SELECT * FROM orders
        WHERE affiliate_user_id = ?
          AND order_type = 'affiliate'
          AND status IN ('completed', 'shipped')
          AND created_at >= ? AND created_at <= ?
        ORDER BY created_at ASC
        """,
        (affiliate_id, start, end),
    ).fetchall()


def _order_affiliate_commission_cents(o: sqlite3.Row) -> int:
    if "affiliate_commission_cents" not in o.keys():
        return 0
    return int(o["affiliate_commission_cents"] or 0)


def apply_affiliate_commission_rates_for_month(
    conn: sqlite3.Connection, affiliate_id: int, year: int, month: int
) -> None:
    """Each order earns the rate for its sale position that month (1st→20%, 10th→25%,25th+→30%)."""
    orders = affiliate_orders_in_month(conn, affiliate_id, year, month)
    for i, o in enumerate(orders, start=1):
        total = int(o["total_cents"])
        rate = rate_for_nth_sale(i)
        comm = commission_cents_for_nth_sale(i, total)
        conn.execute(
            """
            UPDATE orders SET affiliate_commission_cents = ?, affiliate_commission_rate = ?
            WHERE id = ?
            """,
            (comm, rate, o["id"]),
        )


def refresh_commission_snapshot(conn: sqlite3.Connection, affiliate_id: int, year: int, month: int) -> None:
    orders = affiliate_orders_in_month(conn, affiliate_id, year, month)
    sales_count = len(orders)
    total_cents = sum(int(o["total_cents"]) for o in orders)
    commission_cents = sum(_order_affiliate_commission_cents(o) for o in orders)
    if commission_cents == 0 and sales_count:
        commission_cents = sum(
            commission_cents_for_nth_sale(i, int(o["total_cents"])) for i, o in enumerate(orders, start=1)
        )
    bonus_cents = monthly_milestone_bonus_cents(sales_count)
    rate_next = current_rate_for_next_sale_after(sales_count)
    ym = f"{year:04d}-{month:02d}"
    conn.execute(
        """
        INSERT INTO commissions (affiliate_user_id, year_month, sales_count, commission_rate,
            total_sales_cents, commission_cents, bonus_cents, payout_status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
        ON CONFLICT(affiliate_user_id, year_month) DO UPDATE SET
            sales_count = excluded.sales_count,
            commission_rate = excluded.commission_rate,
            total_sales_cents = excluded.total_sales_cents,
            commission_cents = excluded.commission_cents,
            bonus_cents = excluded.bonus_cents
        """,
        (affiliate_id, ym, sales_count, rate_next, total_cents, commission_cents, bonus_cents),
    )


def order_items_lines(conn: sqlite3.Connection, order_id: int) -> str:
    rows = conn.execute(
        """
        SELECT oi.quantity, oi.unit_price_cents, p.name
        FROM order_items oi
        JOIN products p ON p.id = oi.product_id
        WHERE oi.order_id = ?
        """,
        (order_id,),
    ).fetchall()
    parts = []
    for r in rows:
        parts.append(f"  - {r['name']} x{r['quantity']} @ {database.format_money(r['unit_price_cents'])}")
    return "\n".join(parts)


def _cart_line_items(conn: sqlite3.Connection) -> Tuple[List[Dict[str, Any]], int]:
    lst = _cart_get_list(conn)
    items: List[Dict[str, Any]] = []
    total = 0
    for row in lst:
        p = database.product_by_id(conn, row["product_id"])
        if not p or not database.product_add_to_cart_enabled(p):
            continue
        qty = int(row["quantity"])
        line = int(p["price_cents"]) * qty
        total += line
        items.append({"product": p, "quantity": qty, "line_cents": line})
    items.sort(key=lambda x: (int(x["product"]["sort_order"]), int(x["product"]["id"])))
    return items, total


_MINI_SERIES_SLUGS = frozenset({"melody", "harmony", "riff", "allegro"})


@app.route("/")
def index():
    return redirect(url_for("shop"))


@app.context_processor
def inject_footer() -> Dict[str, Any]:
    return {
        "footer_instagram_url": os.environ.get("FOOTER_INSTAGRAM_URL", "https://www.instagram.com/"),
        "footer_tiktok_url": os.environ.get("FOOTER_TIKTOK_URL", "https://www.tiktok.com/"),
        "footer_email": os.environ.get("FOOTER_CONTACT_EMAIL", "hello@licoricelocker.com"),
    }


@app.context_processor
def inject_stripe_public() -> Dict[str, Any]:
    return {"stripe_public_key": _stripe_publishable_key()}


@app.context_processor
def inject_analytics_public() -> Dict[str, Any]:
    """Storefront-only tracker: skip admin + affiliate member dashboards + staff login."""
    p = request.path or ""
    analytics_public = not (
        p.startswith("/dashboard/admin")
        or p.startswith("/dashboard/affiliate")
        or p.startswith("/login")
    )
    return {"analytics_public": analytics_public}


@app.route("/login/affiliate", methods=["GET"])
def affiliate_login():
    return redirect(url_for("shop", open_affiliate_login="1"))


@app.route("/login", methods=["GET", "POST"])
def login():
    """Staff (admin) sign-in only. Members use the footer Listening Room modal + two-factor auth."""
    if current_user.is_authenticated:
        return redirect(_dashboard_for_role())
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        password = request.form.get("password", "")
        with database.get_db() as conn:
            row = database.user_by_email(conn, email)
            if row and check_password_hash(row["password_hash"], password):
                if row["role"] != "admin" and email not in _admin_owner_emails():
                    flash("Use Listening Room sign in in the site footer to access your member account.", "error")
                    return render_template("login.html")
                login_user(User(row), remember=True)
                return redirect(_dashboard_for_role(User(row)))
        flash("Invalid email or password.", "error")
    return render_template("login.html")


@app.route("/auth/affiliate/signup", methods=["POST"])
def auth_affiliate_signup():
    """Create Listening Room member account (hashed password, unique email, generated code). Log in; redirect to dashboard."""
    ct = (request.content_type or "").lower()
    wants_json = "application/json" in ct
    if wants_json:
        data = request.get_json(silent=True) or {}
        first_name = (data.get("first_name") or "").strip()
        last_name = (data.get("last_name") or "").strip()
        email = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""
        password_confirm = data.get("password_confirm") or ""
    else:
        first_name = (request.form.get("first_name") or "").strip()
        last_name = (request.form.get("last_name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        password_confirm = request.form.get("password_confirm") or ""

    def _fail_json(code: int, err: str):
        return jsonify({"ok": False, "error": err}), code

    def _fail_form(message: str):
        flash(message, "error")
        return redirect(request.referrer or url_for("shop", open_affiliate_signup="1"))

    if not first_name or not last_name or not email or not password or not password_confirm:
        return _fail_json(400, "fields_required") if wants_json else _fail_form("Please fill in all fields.")
    if password != password_confirm:
        return _fail_json(400, "password_mismatch") if wants_json else _fail_form("Passwords do not match.")
    if len(password) < 8:
        return _fail_json(400, "password_short") if wants_json else _fail_form("Password must be at least 8 characters.")

    _method = "pbkdf2:sha256"
    pw_hash = generate_password_hash(password, method=_method)

    try:
        with database.get_db() as conn:
            if database.user_by_email(conn, email):
                return _fail_json(409, "email_exists") if wants_json else _fail_form("An account with this email already exists.")
            uid = database.create_affiliate_signup(conn, email, pw_hash, first_name, last_name)
    except sqlite3.IntegrityError:
        return _fail_json(409, "email_exists") if wants_json else _fail_form("An account with this email already exists.")
    except RuntimeError:
        return _fail_json(503, "server_busy") if wants_json else _fail_form("Could not complete signup. Please try again.")

    with database.get_db() as conn:
        row = database.user_by_id(conn, uid)
    if not row:
        return _fail_json(500, "invalid") if wants_json else _fail_form("Something went wrong. Try again.")
    login_user(User(row), remember=True)
    if wants_json:
        return jsonify({"ok": True, "redirect": url_for("affiliate_dashboard")})
    return redirect(url_for("affiliate_dashboard"))


@app.route("/auth/affiliate/step1", methods=["POST"])
def auth_affiliate_step1():
    """Verify member email + password; return TOTP setup (QR) or verification step."""
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    password = data.get("password") or ""
    if not email or not password:
        return jsonify({"ok": False, "error": "invalid_credentials"}), 400
    with database.get_db() as conn:
        row = database.user_by_email(conn, email)
    if not row or row["role"] != "affiliate":
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401
    if not check_password_hash(row["password_hash"], password):
        return jsonify({"ok": False, "error": "invalid_credentials"}), 401
    uid = int(row["id"])
    exp = time.time() + AFFILIATE_2FA_TTL_SEC
    totp_secret = row["totp_secret"]
    totp_confirmed = bool(row["totp_confirmed"])
    with database.get_db() as conn:
        if not totp_confirmed:
            if not totp_secret:
                totp_secret = pyotp.random_base32()
                database.set_user_totp_secret(conn, uid, totp_secret)
            qr = _totp_provisioning_qr_data_url(totp_secret, email)
            session[AFFILIATE_2FA_PENDING_KEY] = {"uid": uid, "exp": exp, "mode": "setup"}
            return jsonify({"ok": True, "step": "setup", "qr": qr})
        session[AFFILIATE_2FA_PENDING_KEY] = {"uid": uid, "exp": exp, "mode": "verify"}
        return jsonify({"ok": True, "step": "verify"})


@app.route("/auth/affiliate/step2", methods=["POST"])
def auth_affiliate_step2():
    """Verify TOTP code; complete Listening Room sign-in (and confirm device on first setup)."""
    data = request.get_json(silent=True) or {}
    code = (data.get("code") or "").replace(" ", "").strip()
    if not code or len(code) < 6:
        return jsonify({"ok": False, "error": "invalid_code"}), 400
    pending = _affiliate_2fa_pending_valid()
    if not pending:
        return jsonify({"ok": False, "error": "session_expired"}), 401
    uid = int(pending["uid"])
    mode = pending.get("mode", "verify")
    with database.get_db() as conn:
        row = database.user_by_id(conn, uid)
        if not row or row["role"] != "affiliate":
            _clear_affiliate_2fa_session()
            return jsonify({"ok": False, "error": "invalid"}), 400
        secret = row["totp_secret"]
    if not secret:
        return jsonify({"ok": False, "error": "invalid"}), 400
    totp = pyotp.TOTP(secret)
    if not totp.verify(code, valid_window=1):
        return jsonify({"ok": False, "error": "invalid_code"}), 400
    with database.get_db() as conn:
        if mode == "setup":
            database.confirm_user_totp(conn, uid)
        row = database.user_by_id(conn, uid)
    if not row:
        _clear_affiliate_2fa_session()
        return jsonify({"ok": False, "error": "invalid"}), 400
    login_user(User(row), remember=True)
    _clear_affiliate_2fa_session()
    return jsonify({"ok": True, "redirect": url_for("affiliate_dashboard")})


@app.route("/forgot-password", methods=["GET", "POST"])
def forgot_password():
    if request.method == "POST":
        email = request.form.get("email", "").strip().lower()
        with database.get_db() as conn:
            row = database.user_by_email(conn, email)
            if row and row["role"] == "affiliate":
                token = secrets.token_urlsafe(32)
                expires = (_now_utc() + timedelta(hours=1)).isoformat()
                database.set_password_reset(conn, int(row["id"]), token, expires)
                reset_url = _absolute_site_url(url_for("reset_password", token=token))
                send_password_reset_email(email, reset_url)
        flash("If that email is registered with The Listening Room, we sent a reset link.", "ok")
        return redirect(url_for("forgot_password"))
    return render_template("forgot_password.html")


@app.route("/reset-password/<token>", methods=["GET", "POST"])
def reset_password(token: str):
    with database.get_db() as conn:
        row = database.user_by_affiliate_reset_token(conn, token)
    if not row:
        flash("This reset link is invalid or expired.", "error")
        return redirect(url_for("shop"))
    exp_dt = _parse_iso_datetime(row["password_reset_expires"])
    if exp_dt is None or _now_utc() > exp_dt:
        flash("This reset link has expired. Request a new one.", "error")
        return redirect(url_for("forgot_password"))
    uid = int(row["id"])
    if request.method == "POST":
        pw = request.form.get("password", "")
        pw2 = request.form.get("password_confirm", "")
        if len(pw) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template("reset_password.html", token=token)
        if pw != pw2:
            flash("Passwords do not match.", "error")
            return render_template("reset_password.html", token=token)
        _method = "pbkdf2:sha256"
        h = generate_password_hash(pw, method=_method)
        with database.get_db() as conn:
            database.set_user_password_hash(conn, uid, h)
            database.clear_password_reset(conn, uid)
            database.clear_user_totp(conn, uid)
        flash("Your password was updated. Use Listening Room sign in in the footer — you will set up 2FA again on first login.", "ok")
        return redirect(url_for("shop", open_affiliate_login="1"))
    return render_template("reset_password.html", token=token)


@app.route("/design-philosophy")
def design_philosophy():
    return render_template("design_philosophy.html")


@app.route("/our-story")
def our_story():
    return render_template("our_story.html")


@app.route("/about")
def about_page():
    return render_template("about.html")


@app.route("/privacy-policy")
def privacy_policy():
    return render_template("privacy_policy.html")


@app.route("/currency-pricing")
def currency_pricing():
    return render_template("currency_pricing.html")


@app.route("/shipping-delivery")
def shipping_delivery():
    return render_template("shipping_delivery.html")


def _dashboard_for_role(u: Optional[User] = None) -> str:
    user = u or current_user
    if _user_is_effective_admin(user):
        return url_for("admin_dashboard")
    return url_for("affiliate_dashboard")


@app.route("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("shop"))


@app.route("/a/<slug>")
def affiliate_landing(slug: str):
    from flask import make_response

    with database.get_db() as conn:
        aff = database.affiliate_by_slug(conn, slug)
        if not aff or not _affiliate_row_public_ok(aff):
            flash("That Listening Room page was not found.", "error")
            return redirect(url_for("shop"))
        code = (aff["affiliate_code"] or "").strip() if "affiliate_code" in aff.keys() else ""
        if code:
            return redirect(url_for("listening_room", code=code), code=302)
        page = database.affiliate_page(conn, aff["id"])
        products = database.list_products(conn)
        aff_id = int(aff["id"])

    vid = request.cookies.get("licorice_visitor")
    set_visitor_cookie = False
    if not vid:
        vid = str(uuid.uuid4())
        set_visitor_cookie = True

    body = render_template(
        "affiliate_landing.html",
        affiliate=aff,
        page=page,
        products=products,
        format_money=database.format_money,
    )
    r = make_response(body)
    r.set_cookie(
        "licorice_affiliate_slug",
        value=slug,
        max_age=60 * 60 * 24 * 30,
        samesite="Lax",
        httponly=True,
    )
    if set_visitor_cookie:
        r.set_cookie(
            "licorice_visitor",
            vid,
            max_age=60 * 60 * 24 * 365,
            samesite="Lax",
            httponly=True,
        )

    with database.get_db() as conn:
        conn.execute(
            "INSERT INTO affiliate_visits (affiliate_user_id, visitor_id) VALUES (?, ?)",
            (aff_id, vid),
        )
    return r


@app.route("/listening-room")
def listening_room_program():
    """
    Public page: The Listening Room — culture-first conversion.
    Member shops live at /listening-room/<member_code> (separate route).
    """
    return render_template(
        "listening_room_program.html",
        signup_url=url_for("shop", open_affiliate_signup="1"),
        shop_url=url_for("shop"),
        soundwave_cents=int(LIST_PRICE_SOUNDWAVE_NZD * 100),
        mini_cents=int(LIST_PRICE_MINI_SERIES_NZD * 100),
    )


@app.route("/listening-room/<code>")
def listening_room(code: str):
    """Shop homepage layout with Listening Room hero; tracks via same cookie as /a/<slug>."""
    from flask import make_response

    with database.get_db() as conn:
        aff = database.affiliate_by_code(conn, code)
        if not aff or not _affiliate_row_public_ok(aff):
            flash("That Listening Room was not found.", "error")
            return redirect(url_for("shop"))
        slug = (aff["affiliate_slug"] or "").strip()
        if not slug:
            flash("That Listening Room was not found.", "error")
            return redirect(url_for("shop"))
        page = database.affiliate_page(conn, int(aff["id"]))
        products = database.list_products(conn)
        aff_id = int(aff["id"])

    if not page:
        flash("That Listening Room was not found.", "error")
        return redirect(url_for("shop"))

    sound_wave_product = next((p for p in products if p["slug"] == "sound-wave"), None)
    riff_product = next((p for p in products if p["slug"] == "riff"), None)
    harmony_product = next((p for p in products if p["slug"] == "harmony"), None)
    melody_product = next((p for p in products if p["slug"] == "melody"), None)
    allegro_product = next((p for p in products if p["slug"] == "allegro"), None)
    listening_room_banner_url = _listening_room_banner_url(page)

    vid = request.cookies.get("licorice_visitor")
    set_visitor_cookie = False
    if not vid:
        vid = str(uuid.uuid4())
        set_visitor_cookie = True

    body = render_template(
        "listening_room.html",
        affiliate=aff,
        page=page,
        products=products,
        sound_wave_product=sound_wave_product,
        riff_product=riff_product,
        harmony_product=harmony_product,
        melody_product=melody_product,
        allegro_product=allegro_product,
        format_money=database.format_money,
        listening_room_banner_url=listening_room_banner_url,
    )
    r = make_response(body)
    r.set_cookie(
        "licorice_affiliate_slug",
        value=slug,
        max_age=60 * 60 * 24 * 30,
        samesite="Lax",
        httponly=True,
    )
    if set_visitor_cookie:
        r.set_cookie(
            "licorice_visitor",
            vid,
            max_age=60 * 60 * 24 * 365,
            samesite="Lax",
            httponly=True,
        )

    with database.get_db() as conn:
        conn.execute(
            "INSERT INTO affiliate_visits (affiliate_user_id, visitor_id) VALUES (?, ?)",
            (aff_id, vid),
        )
    return r


@app.route("/product/<slug>")
def product_detail(slug: str):
    with database.get_db() as conn:
        product = database.product_by_slug(conn, slug)
        if not product:
            flash("That product was not found.", "error")
            return redirect(url_for("shop"))
        affiliate_slug = request.cookies.get("licorice_affiliate_slug")
        affiliate_row = _affiliate_from_cookie(conn, affiliate_slug) if affiliate_slug else None
        pid = int(product["id"])
        product_images = database.list_product_images(conn, pid)
    return render_template(
        "product_detail.html",
        product=product,
        product_images=product_images,
        format_money=database.format_money,
        affiliate=affiliate_row,
    )


@app.route("/shop")
def shop():
    if (request.args.get("cancelled") or "").strip().lower() == "true":
        flash("Checkout cancelled — your cart is unchanged.", "info")
        return redirect(url_for("shop"), code=303)

    with database.get_db() as conn:
        products = database.list_products(conn)
    sound_wave_product = next((p for p in products if p["slug"] == "sound-wave"), None)
    riff_product = next((p for p in products if p["slug"] == "riff"), None)
    harmony_product = next((p for p in products if p["slug"] == "harmony"), None)
    melody_product = next((p for p in products if p["slug"] == "melody"), None)
    allegro_product = next((p for p in products if p["slug"] == "allegro"), None)
    return render_template(
        "shop.html",
        products=products,
        sound_wave_product=sound_wave_product,
        riff_product=riff_product,
        harmony_product=harmony_product,
        melody_product=melody_product,
        allegro_product=allegro_product,
        format_money=database.format_money,
    )


@app.route("/success")
@app.route("/checkout/success")
def checkout_success():
    """Stripe redirects here after payment; fulfillment runs once, then a dedicated success screen."""
    csid = (request.args.get("session_id") or "").strip()
    if csid:
        return _stripe_process_paid_return(csid)
    payload = session.pop("checkout_success_view", None)
    if not payload:
        return redirect(url_for("shop"), code=303)
    products = payload.get("products") or []
    html = render_template(
        "checkout_success.html",
        format_money=database.format_money,
        order_number=str(payload.get("order_number") or ""),
        total_cents=int(payload.get("total_cents") or 0),
        receipt_sent=bool(payload.get("receipt_sent")),
        detail=(payload.get("detail") or "").strip(),
        is_new_order=bool(payload.get("is_new_order")),
        products=products,
    )
    session.pop("cart", None)
    session.modified = True
    return html


@app.route("/cart")
def cart_view():
    if (request.args.get("returned") or "").strip().lower() in ("1", "true", "yes"):
        flash("You're back from checkout — your cart is unchanged.", "info")
        return redirect(url_for("cart_view"), code=303)
    with database.get_db() as conn:
        items, total = _cart_line_items(conn)
    affiliate_slug = request.cookies.get("licorice_affiliate_slug")
    affiliate_row = None
    if affiliate_slug:
        with database.get_db() as conn:
            affiliate_row = _affiliate_from_cookie(conn, affiliate_slug)
    cart_has_mini_series = any(
        str(it["product"]["slug"] or "").strip().lower() in _MINI_SERIES_SLUGS for it in items
    )
    return render_template(
        "cart.html",
        items=items,
        total=total,
        format_money=database.format_money,
        affiliate=affiliate_row,
        cart_has_mini_series=cart_has_mini_series,
    )


def _cart_add_wants_json() -> bool:
    return request.headers.get("X-Requested-With") == "XMLHttpRequest" or (
        (request.form.get("ajax") or "").strip() == "1"
    )


def _cart_slug_counts_from_lines(lines: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in lines or []:
        slug = (row.get("slug") or "").strip().lower()
        if not slug:
            continue
        try:
            q = int(row.get("quantity", 0))
        except (TypeError, ValueError):
            continue
        if q < 1:
            continue
        counts[slug] = counts.get(slug, 0) + q
    return counts


def _mini_soundwave_just_completed(before: Dict[str, int], after: Dict[str, int]) -> bool:
    mb, hb = before.get("melody", 0), before.get("harmony", 0)
    ma, ha = after.get("melody", 0), after.get("harmony", 0)
    if mb >= 1 and hb >= 1:
        return False
    return ma >= 1 and ha >= 1


def _cart_upsell_for_add(
    conn: sqlite3.Connection,
    before_lines: List[Dict[str, Any]],
    after_lines: List[Dict[str, Any]],
    added_slug: str,
) -> Optional[Dict[str, Any]]:
    """Single upsell payload after add-to-cart (Mini Series system)."""
    del conn  # reserved for future DB-backed upsell rules
    s = (added_slug or "").strip().lower()
    if s not in _MINI_SERIES_SLUGS:
        return None
    before = _cart_slug_counts_from_lines(before_lines)
    after = _cart_slug_counts_from_lines(after_lines)
    ma, ha = after.get("melody", 0), after.get("harmony", 0)

    def pack(
        title: str,
        body: str,
        primary_label: str,
        primary_href: str,
        secondary_label: str,
        secondary_href: Optional[str],
        image: str,
    ) -> Dict[str, Any]:
        return {
            "title": title,
            "body": body,
            "primary_label": primary_label,
            "primary_href": primary_href,
            "secondary_label": secondary_label,
            "secondary_href": secondary_href,
            "image": image,
        }

    img_pair = url_for("static", filename="mini-soundwave-melody-harmony.png")
    img_riff = url_for("static", filename="riff-feature-01.png")
    img_allegra = url_for("static", filename="allegra-feature-01.png")

    if _mini_soundwave_just_completed(before, after) and s in ("melody", "harmony"):
        return pack(
            "Your soundwave has started",
            "Add more pieces to expand your display.",
            "Add more",
            url_for("shop"),
            "Checkout",
            url_for("checkout"),
            img_pair,
        )

    if s == "melody" and ha < 1:
        return pack(
            "Complete your mini soundwave",
            "Add Harmony to create a full mini soundwave display.",
            "Add Harmony",
            url_for("product_detail", slug="harmony"),
            "Continue",
            None,
            img_pair,
        )

    if s == "harmony" and ma < 1:
        return pack(
            "Complete your mini soundwave",
            "Add Melody to create a full mini soundwave display.",
            "Add Melody",
            url_for("product_detail", slug="melody"),
            "Continue",
            None,
            img_pair,
        )

    if s == "riff":
        return pack(
            "Build your soundwave",
            "Add another Riff to expand your display.",
            "Add another Riff",
            url_for("product_detail", slug="riff"),
            "Continue",
            None,
            img_riff,
        )

    if s == "allegro":
        return pack(
            "Build your soundwave",
            "Add another Allegra to expand your display.",
            "Add another Allegra",
            url_for("product_detail", slug="allegro"),
            "Continue",
            None,
            img_allegra,
        )

    if ma >= 1 and ha >= 1:
        return pack(
            "Your soundwave has started",
            "Add more pieces to expand your display.",
            "Add more",
            url_for("shop"),
            "Checkout",
            url_for("checkout"),
            img_pair,
        )

    return None


@app.route("/cart/add", methods=["POST"])
def cart_add():
    wants_json = _cart_add_wants_json()
    try:
        pid = int(request.form.get("product_id", 0))
    except (TypeError, ValueError):
        pid = 0
    try:
        qty = max(1, int(request.form.get("quantity", 1)))
    except (TypeError, ValueError):
        qty = 1

    with database.get_db() as conn:
        p = database.product_by_id(conn, pid)
    if not p or not database.product_add_to_cart_enabled(p):
        msg = "This product is not available for purchase."
        if wants_json:
            return jsonify({"ok": False, "message": msg}), 400
        flash(msg, "error")
        return redirect(request.referrer or url_for("shop"))

    with database.get_db() as conn:
        before_lines = copy.deepcopy(_cart_get_list(conn))
        lst = _cart_get_list(conn)
        new_list: List[Dict[str, Any]] = []
        found = False
        for row in lst:
            if int(row["product_id"]) == pid:
                new_list.append(
                    {
                        **row,
                        "quantity": min(999, int(row["quantity"]) + qty),
                    }
                )
                found = True
            else:
                new_list.append(dict(row))
        if not found:
            slug = str(p["slug"] or "").strip().lower()
            new_list.append(
                {
                    "product_id": pid,
                    "quantity": qty,
                    "name": str(p["name"] or "Product"),
                    "price_cents": int(p["price_cents"]),
                    "image": _product_success_image_static(slug),
                    "slug": slug,
                }
            )
        new_list.sort(key=lambda x: x["product_id"])
        session["cart"] = new_list
        session.modified = True
        after_lines = copy.deepcopy(_cart_get_list(conn))
        added_slug = str(p["slug"] or "").strip().lower()
        upsell = _cart_upsell_for_add(conn, before_lines, after_lines, added_slug)

    total_qty = _session_cart_total_qty()

    if wants_json:
        return jsonify(
            {
                "ok": True,
                "message": "Added to cart.",
                "cart_item_count": total_qty,
                "upsell": upsell,
            }
        )
    flash("Added to cart.", "ok")
    return redirect(request.referrer or url_for("shop"))


@app.route("/cart/update", methods=["POST"])
def cart_update():
    pid = request.form.get("product_id")
    if not pid:
        return redirect(url_for("cart_view"))
    try:
        qty = int(request.form.get("quantity", "0"))
    except ValueError:
        qty = 0
    with database.get_db() as conn:
        lst = _cart_get_list(conn)
        new_list: List[Dict[str, Any]] = []
        for row in lst:
            if str(row["product_id"]) == str(pid):
                if qty >= 1:
                    new_list.append({**row, "quantity": min(999, qty)})
            else:
                new_list.append(dict(row))
        session["cart"] = new_list
        session.modified = True
    flash("Cart updated.", "ok")
    return redirect(url_for("cart_view"))


@app.route("/cart/remove", methods=["POST"])
def cart_remove():
    pid = request.form.get("product_id")
    with database.get_db() as conn:
        lst = _cart_get_list(conn)
        new_list = [dict(r) for r in lst if str(r["product_id"]) != str(pid)]
        session["cart"] = new_list
        session.modified = True
    return redirect(url_for("cart_view"))


_STRIPE_SHIPPING_COUNTRIES = [
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
]


def _as_stripe_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, dict):
        return obj
    return {}


def _stripe_identity_from_checkout_session(raw_cs: Dict[str, Any], csid: str) -> Optional[Dict[str, str]]:
    """Map Stripe Checkout Session shipping + customer payloads into DB-ready fields.

    Merges ``shipping_details.address`` with ``customer_details.address``, uses session-level
    ``customer_email`` when needed, and applies safe fallbacks for regions that omit postal or city
    (common source of false 'incomplete details' errors).
    """
    ship = _as_stripe_dict(raw_cs.get("shipping_details"))
    addr_s = _as_stripe_dict(ship.get("address"))
    cd = _as_stripe_dict(raw_cs.get("customer_details"))
    addr_c = _as_stripe_dict(cd.get("address"))

    def pick_addr(key: str) -> str:
        a = addr_s.get(key)
        b = addr_c.get(key)
        sa = str(a).strip() if a is not None else ""
        sb = str(b).strip() if b is not None else ""
        return sa or sb

    line1 = pick_addr("line1")
    line2 = pick_addr("line2")
    city = pick_addr("city")
    postal = pick_addr("postal_code")
    region = pick_addr("state")
    ctry_raw = pick_addr("country") or "NZ"
    country = str(ctry_raw).strip().upper()[:2]
    if len(country) != 2:
        country = "NZ"

    ship_name = str(ship.get("name") or "").strip()
    email = str(cd.get("email") or "").strip().lower()
    if not email:
        email = str(raw_cs.get("customer_email") or "").strip().lower()
    phone = str(cd.get("phone") or "").strip()
    cust_name = str(cd.get("name") or "").strip() or ship_name

    fallbacks: List[str] = []
    if line1 and city and not postal:
        postal = "n/a"
        fallbacks.append("postal_code_placeholder")
    if line1 and not city:
        city = (region or country or "—")[:120]
        fallbacks.append("city_fallback")
    if line1 and not postal:
        postal = "n/a"
        fallbacks.append("postal_placeholder_after_city")

    if not email:
        app.logger.warning(
            "stripe_checkout_identity_incomplete session_id=%s missing=email",
            (csid or "")[:32],
        )
        return None
    if not line1:
        app.logger.warning(
            "stripe_checkout_identity_incomplete session_id=%s missing=line1 shipping_addr_keys=%s customer_addr_keys=%s",
            (csid or "")[:32],
            list(addr_s.keys()),
            list(addr_c.keys()),
        )
        return None

    if fallbacks:
        app.logger.info(
            "stripe_checkout_address_fallbacks session_id=%s %s",
            (csid or "")[:32],
            ",".join(fallbacks),
        )

    parts_nm = cust_name.split(None, 1) if cust_name else []
    first = parts_nm[0] if parts_nm else "Customer"
    last = parts_nm[1] if len(parts_nm) > 1 else ""
    shipping_name = ship_name or f"{first} {last}".strip()

    return {
        "email": email,
        "phone": phone,
        "first": first,
        "last": last,
        "shipping_name": shipping_name,
        "line1": line1,
        "line2": line2,
        "city": city,
        "region": region,
        "postal": postal,
        "country": country,
    }


def _stripe_checkout_base_url() -> str:
    """Public origin for Stripe success/cancel URLs.

    Uses the same host the customer used to start checkout when possible, so a
    mis-set or not-yet-live SITE_URL (e.g. custom domain without DNS) does not
    send them to an unreachable host after Stripe.

    Precedence:
    1. STRIPE_PUBLIC_BASE_URL or STRIPE_CHECKOUT_BASE_URL — explicit override.
    2. Request Host / X-Forwarded-Host (non-local only) — matches the live tab URL.
    3. SITE_URL — canonical fallback when no usable request host.
    4. RAILWAY_PUBLIC_DOMAIN — platform default.
    5. request.host_url — last resort.
    """
    explicit = (
        os.environ.get("STRIPE_PUBLIC_BASE_URL") or os.environ.get("STRIPE_CHECKOUT_BASE_URL") or ""
    ).strip().rstrip("/")
    if explicit:
        return explicit

    if has_request_context():
        fwd = (request.headers.get("X-Forwarded-Host") or "").split(",")[0].strip()
        host = fwd or (request.host or "").split(",")[0].strip()
        if host:
            h = host.lower()
            if h not in ("localhost", "127.0.0.1") and not h.startswith("127.") and not h.endswith(
                ".local"
            ):
                proto = (
                    (request.headers.get("X-Forwarded-Proto") or request.scheme or "https")
                    .split(",")[0]
                    .strip()
                    .lower()
                )
                if proto not in ("http", "https"):
                    proto = "https"
                return f"{proto}://{host}".rstrip("/")

    base = (os.environ.get("SITE_URL") or "").strip().rstrip("/")
    if base:
        return base

    domain = (os.environ.get("RAILWAY_PUBLIC_DOMAIN") or "").strip()
    if domain:
        domain = domain.lstrip("/")
        if domain.lower().startswith("http://") or domain.lower().startswith("https://"):
            return domain.rstrip("/")
        return f"https://{domain}".rstrip("/")

    if has_request_context():
        return (request.host_url or "").rstrip("/")
    return ""


def _stripe_process_paid_return(csid: str):
    """Verify Stripe Checkout session, record order once, then redirect to /checkout/success (PRG)."""
    secret = _stripe_secret_key()
    if not secret:
        app.logger.error("stripe_paid_return_aborted reason=missing_stripe_secret")
        flash("Payments are not configured.", "error")
        return redirect(url_for("shop"), code=303)
    stripe.api_key = secret

    try:
        cs = stripe.checkout.Session.retrieve(csid, expand=["line_items"])
    except stripe.error.StripeError as exc:
        app.logger.warning("stripe_session_retrieve_failed session_id=%s error=%s", csid[:32], exc)
        flash("Could not verify your payment. Contact us with your receipt.", "error")
        return redirect(url_for("shop"), code=303)

    if (cs.payment_status or "") != "paid":
        app.logger.warning(
            "stripe_session_not_paid session_id=%s payment_status=%r",
            csid[:32],
            cs.payment_status,
        )
        flash("Payment was not completed.", "error")
        return redirect(url_for("shop"), code=303)

    with database.get_db() as conn:
        existing = conn.execute(
            "SELECT id, order_number, total_cents FROM orders WHERE stripe_checkout_session_id = ?",
            (csid,),
        ).fetchone()
        if existing:
            oid_e = int(existing["id"])
            row = conn.execute(
                "SELECT receipt_sent, total_cents FROM orders WHERE id = ?",
                (oid_e,),
            ).fetchone()
            total = int((row or existing)["total_cents"] or 0)
            prows = _order_success_product_rows(conn, oid_e)
            app.logger.info(
                "stripe_checkout_idempotent_replay order_id=%s order_number=%s session_id=%s",
                oid_e,
                existing["order_number"],
                csid[:24],
            )
            session["checkout_success_view"] = {
                "order_number": str(existing["order_number"]),
                "total_cents": total,
                "receipt_sent": bool(row and row["receipt_sent"]),
                "detail": "Thanks again — we have your order on record.",
                "is_new_order": False,
                "products": prows,
            }
            return redirect(url_for("checkout_success"), code=303)

    raw_cs = cs.to_dict()
    meta = dict(raw_cs.get("metadata") or {})
    paid_total = int(cs.amount_total or 0)

    affiliate_slug = (meta.get("affiliate_slug") or "").strip()
    guest_session_id = (meta.get("guest_session_id") or "").strip()
    affiliate_row = None
    if affiliate_slug:
        with database.get_db() as conn:
            affiliate_row = _affiliate_from_cookie(conn, affiliate_slug)

    idn = _stripe_identity_from_checkout_session(raw_cs, csid)
    if not idn:
        flash("Order could not be recorded (incomplete details). Contact us.", "error")
        return redirect(url_for("shop"), code=303)

    email = idn["email"]
    phone = idn["phone"]
    first = idn["first"]
    last = idn["last"]
    shipping_name = idn["shipping_name"]
    line1 = idn["line1"]
    line2 = idn["line2"]
    city = idn["city"]
    region = idn["region"]
    postal = idn["postal"]
    country = idn["country"]

    order_number = f"LL-{_now_utc().strftime('%Y%m%d')}-{secrets.token_hex(3).upper()}"
    aff_id = int(affiliate_row["id"]) if affiliate_row else None
    otype = "affiliate" if aff_id else "direct"
    affiliate_code = None
    if affiliate_row:
        ac = (affiliate_row["affiliate_code"] if "affiliate_code" in affiliate_row.keys() else None) or ""
        affiliate_code = ac.strip() or (affiliate_row["affiliate_slug"] or "")
    affiliate_counted = 1 if aff_id else 0
    checkout_mode = (meta.get("checkout_mode") or "").strip().lower()
    customer_notes = (meta.get("customer_notes") or "").strip()

    if checkout_mode == "cart":
        try:
            shipping_cents = max(0, int(meta.get("shipping_cents") or 0))
        except (TypeError, ValueError):
            shipping_cents = 0
        cart_spec = (meta.get("cart_lines") or "").strip()
        pairs: List[Tuple[int, int]] = []
        for seg in cart_spec.split(","):
            seg = seg.strip()
            if not seg:
                continue
            left, _, right = seg.partition(":")
            try:
                pairs.append((int(left.strip()), max(1, int(right.strip()))))
            except (TypeError, ValueError):
                app.logger.warning(
                    "stripe_order_invalid_cart session_id=%s cart_spec=%r",
                    csid[:32],
                    cart_spec[:200] if cart_spec else "",
                )
                flash("Order could not be recorded (invalid cart). Contact us.", "error")
                return redirect(url_for("shop"), code=303)
        pairs.sort(key=lambda t: t[0])
        order_lines: List[Tuple[int, int, int, Any]] = []
        subtotal_cents = 0
        with database.get_db() as conn:
            for pid, qty in pairs:
                p = database.product_by_id(conn, pid)
                if not p or not database.product_add_to_cart_enabled(p):
                    app.logger.warning(
                        "stripe_order_product_unavailable session_id=%s product_id=%s",
                        csid[:32],
                        pid,
                    )
                    flash("Order could not be recorded (product unavailable). Contact us.", "error")
                    return redirect(url_for("shop"), code=303)
                unit = int(p["price_cents"])
                subtotal_cents += unit * qty
                order_lines.append((pid, qty, unit, p))
        expected_total = subtotal_cents + shipping_cents
        if paid_total != expected_total:
            app.logger.warning(
                "stripe_amount_mismatch session_id=%s paid_total=%s expected_total=%s mode=cart",
                csid[:32],
                paid_total,
                expected_total,
            )
            flash("Payment amount mismatch. Contact us with your Stripe receipt.", "error")
            return redirect(url_for("shop"), code=303)
        total_with_shipping = paid_total
        with database.get_db() as conn:
            cur = conn.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, affiliate_user_id,
                    affiliate_code, affiliate_counted,
                    customer_first, customer_last, customer_email,
                    customer_phone,
                    guest_session_id,
                    shipping_name,
                    shipping_line1, shipping_line2, shipping_city, shipping_region,
                    shipping_postal, shipping_country,
                    subtotal_cents, shipping_cents, total_cents,
                    payment_method, customer_notes,
                    status, fulfillment_status,
                    shipping_tracking,
                    stripe_checkout_session_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_number,
                    otype,
                    aff_id,
                    affiliate_code,
                    affiliate_counted,
                    first,
                    last,
                    email,
                    phone,
                    guest_session_id,
                    shipping_name,
                    line1,
                    line2,
                    city,
                    region,
                    postal,
                    country,
                    subtotal_cents,
                    shipping_cents,
                    total_with_shipping,
                    "stripe",
                    customer_notes,
                    "completed",
                    "paid",
                    "",
                    csid,
                ),
            )
            oid = int(cur.lastrowid)
            app.logger.info(
                "order_inserted order_id=%s order_number=%s stripe_session=%s affiliate_user_id=%s total_cents=%s",
                oid,
                order_number,
                csid[:28],
                aff_id or 0,
                total_with_shipping,
            )
            checkout_ip = client_ip_from_request(request)
            geo_purchase = geo_lookup(checkout_ip)
            gc = (geo_purchase.get("country_code") or "").strip() or None
            gcity = (geo_purchase.get("city") or "").strip() or None
            conn.execute(
                "UPDATE orders SET geo_country = ?, geo_city = ? WHERE id = ?",
                (gc, (gcity[:128] if gcity else None), oid),
            )
            for pid, qty, unit, _p in order_lines:
                conn.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
                    VALUES (?, ?, ?, ?)
                    """,
                    (oid, pid, qty, unit),
                )
            if aff_id:
                dt = _now_utc()
                apply_affiliate_commission_rates_for_month(conn, aff_id, dt.year, dt.month)
                refresh_commission_snapshot(conn, aff_id, dt.year, dt.month)
            lines = order_items_lines(conn, oid)
    else:
        try:
            pid = int(meta.get("product_id") or 0)
        except (TypeError, ValueError):
            pid = 0
        try:
            qty = max(1, int(meta.get("quantity") or 1))
        except (TypeError, ValueError):
            qty = 1
        try:
            shipping_cents = max(0, int(meta.get("shipping_cents") or 0))
        except (TypeError, ValueError):
            shipping_cents = 0

        with database.get_db() as conn:
            p = database.product_by_id(conn, pid)
        if not p:
            app.logger.warning(
                "stripe_order_product_missing session_id=%s product_id=%s",
                csid[:32],
                pid,
            )
            flash("Order could not be recorded (product missing). Contact us.", "error")
            return redirect(url_for("shop"), code=303)

        unit = int(p["price_cents"])
        subtotal_cents = unit * qty
        expected_total = subtotal_cents + shipping_cents
        if paid_total != expected_total:
            app.logger.warning(
                "stripe_amount_mismatch session_id=%s paid_total=%s expected_total=%s mode=single",
                csid[:32],
                paid_total,
                expected_total,
            )
            flash("Payment amount mismatch. Contact us with your Stripe receipt.", "error")
            return redirect(url_for("shop"), code=303)

        total_with_shipping = paid_total

        with database.get_db() as conn:
            cur = conn.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, affiliate_user_id,
                    affiliate_code, affiliate_counted,
                    customer_first, customer_last, customer_email,
                    customer_phone,
                    guest_session_id,
                    shipping_name,
                    shipping_line1, shipping_line2, shipping_city, shipping_region,
                    shipping_postal, shipping_country,
                    subtotal_cents, shipping_cents, total_cents,
                    payment_method, customer_notes,
                    status, fulfillment_status,
                    shipping_tracking,
                    stripe_checkout_session_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_number,
                    otype,
                    aff_id,
                    affiliate_code,
                    affiliate_counted,
                    first,
                    last,
                    email,
                    phone,
                    guest_session_id,
                    shipping_name,
                    line1,
                    line2,
                    city,
                    region,
                    postal,
                    country,
                    subtotal_cents,
                    shipping_cents,
                    total_with_shipping,
                    "stripe",
                    "",
                    "completed",
                    "paid",
                    "",
                    csid,
                ),
            )
            oid = int(cur.lastrowid)
            app.logger.info(
                "order_inserted order_id=%s order_number=%s stripe_session=%s affiliate_user_id=%s total_cents=%s",
                oid,
                order_number,
                csid[:28],
                aff_id or 0,
                total_with_shipping,
            )
            checkout_ip = client_ip_from_request(request)
            geo_purchase = geo_lookup(checkout_ip)
            gc = (geo_purchase.get("country_code") or "").strip() or None
            gcity = (geo_purchase.get("city") or "").strip() or None
            conn.execute(
                "UPDATE orders SET geo_country = ?, geo_city = ? WHERE id = ?",
                (gc, (gcity[:128] if gcity else None), oid),
            )
            conn.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
                VALUES (?, ?, ?, ?)
                """,
                (oid, pid, qty, unit),
            )
            if aff_id:
                dt = _now_utc()
                apply_affiliate_commission_rates_for_month(conn, aff_id, dt.year, dt.month)
                refresh_commission_snapshot(conn, aff_id, dt.year, dt.month)
            lines = order_items_lines(conn, oid)

    receipt_ok = send_resend_post_purchase_emails(oid)
    if not receipt_ok:
        receipt_ok = send_order_confirmation(
            email, order_number, lines, database.format_money(total_with_shipping)
        )
    if receipt_ok:
        with database.get_db() as conn:
            database.mark_order_receipt_sent(conn, oid)
    else:
        app.logger.warning(
            "order_receipt_email_failed order_id=%s order_number=%s resend_and_smtp_failed",
            oid,
            order_number,
        )
        flash("Order confirmed, but the confirmation email could not be sent. We will follow up.", "error")

    app.logger.info(
        "checkout_flow_complete order_id=%s order_number=%s receipt_sent=%s",
        oid,
        order_number,
        receipt_ok,
    )

    detail = (
        "A confirmation email is on its way."
        if receipt_ok
        else "We will email you when your dispatch details are ready."
    )
    with database.get_db() as conn:
        prows = _order_success_product_rows(conn, oid)
    session["checkout_success_view"] = {
        "order_number": order_number,
        "total_cents": total_with_shipping,
        "receipt_sent": receipt_ok,
        "detail": detail,
        "is_new_order": True,
        "products": prows,
    }
    return redirect(url_for("checkout_success"), code=303)


@app.route("/create-checkout-session", methods=["POST"])
def create_checkout_session():
    """Start Stripe Checkout from the session cart (same flow as checkout page)."""
    notes = (request.form.get("notes") or "").strip()[:500]
    return _stripe_checkout_redirect_from_cart(notes=notes, error_endpoint="cart_view")


@app.route("/checkout/stripe/success")
def stripe_checkout_success():
    """Legacy URL: redirect into success handler with session id (bookmarkable)."""
    csid = (request.args.get("session_id") or "").strip()
    if not csid:
        flash("Missing payment session.", "error")
        return redirect(url_for("shop"), code=303)
    return redirect(url_for("checkout_success", session_id=csid), code=303)


@app.route("/checkout/stripe/cancel")
@app.route("/cancel")
def stripe_checkout_cancel():
    return redirect(url_for("cart_view", returned="1"), code=303)


def _stripe_checkout_redirect_from_cart(*, notes: str, error_endpoint: str) -> Any:
    """Create Stripe Checkout for the current session cart; customer completes address & payment on Stripe."""
    secret = _stripe_secret_key()
    if not secret:
        flash("Online payments are not configured.", "error")
        return redirect(url_for(error_endpoint))
    stripe.api_key = secret

    with database.get_db() as conn:
        items, total = _cart_line_items(conn)
    if not items:
        flash("Your cart is empty.", "error")
        return redirect(url_for(error_endpoint))

    notes_meta = (notes or "").strip()[:500]

    try:
        default_shipping_cents = max(0, int(os.environ.get("CHECKOUT_SHIPPING_CENTS_DEFAULT", "0") or 0))
    except ValueError:
        default_shipping_cents = 0

    affiliate_slug = (request.cookies.get("licorice_affiliate_slug") or "").strip()
    guest_session_id = (request.cookies.get("licorice_visitor") or "").strip()

    sorted_items = sorted(items, key=lambda x: int(x["product"]["id"]))
    cart_lines = ",".join(f"{int(it['product']['id'])}:{int(it['quantity'])}" for it in sorted_items)

    line_items: List[Dict[str, Any]] = []
    for it in sorted_items:
        p = it["product"]
        unit_amount = int(p["price_cents"])
        if unit_amount < 50:
            flash(
                "Something in your cart cannot be paid for online. Remove it or contact us to order.",
                "error",
            )
            return redirect(url_for(error_endpoint))
        slug_val = str(p["slug"] or "").strip()
        line_items.append(
            {
                "price_data": {
                    "currency": "nzd",
                    "product_data": {
                        "name": str(p["name"] or "Product"),
                        "metadata": {"product_id": str(p["id"]), "slug": slug_val},
                    },
                    "unit_amount": unit_amount,
                },
                "quantity": int(it["quantity"]),
            }
        )
    if default_shipping_cents > 0:
        line_items.append(
            {
                "price_data": {
                    "currency": "nzd",
                    "product_data": {"name": "Shipping"},
                    "unit_amount": default_shipping_cents,
                },
                "quantity": 1,
            }
        )

    base = _stripe_checkout_base_url()
    success_path = url_for("checkout_success", _external=False)
    success_url = f"{base}{success_path}?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_path = url_for("cart_view", _external=False)
    cancel_url = f"{base}{cancel_path}?returned=1"

    session.modified = True

    try:
        checkout_session = stripe.checkout.Session.create(
            line_items=line_items,
            mode="payment",
            success_url=success_url,
            cancel_url=cancel_url,
            shipping_address_collection={"allowed_countries": _STRIPE_SHIPPING_COUNTRIES},
            phone_number_collection={"enabled": True},
            metadata={
                "checkout_mode": "cart",
                "cart_lines": cart_lines,
                "shipping_cents": str(default_shipping_cents),
                "affiliate_slug": affiliate_slug,
                "guest_session_id": guest_session_id,
                "customer_notes": notes_meta,
            },
        )
    except stripe.error.StripeError as e:
        flash(f"Payment could not be started: {getattr(e, 'user_message', None) or str(e)}", "error")
        return redirect(url_for(error_endpoint))

    pay_url = checkout_session.url
    if not pay_url:
        flash("Payment could not be started.", "error")
        return redirect(url_for(error_endpoint))
    return redirect(pay_url, code=303)


def _checkout_start_stripe_redirect() -> Any:
    notes = (request.form.get("notes") or "").strip()[:500]
    return _stripe_checkout_redirect_from_cart(notes=notes, error_endpoint="checkout")


@app.route("/checkout", methods=["GET", "POST"])
def checkout():
    try:
        default_shipping_cents = max(0, int(os.environ.get("CHECKOUT_SHIPPING_CENTS_DEFAULT", "0") or 0))
    except ValueError:
        default_shipping_cents = 0

    with database.get_db() as conn:
        items, total = _cart_line_items(conn)

    affiliate_slug = request.cookies.get("licorice_affiliate_slug")
    affiliate_row = None
    if affiliate_slug:
        with database.get_db() as conn:
            affiliate_row = _affiliate_from_cookie(conn, affiliate_slug)

    if request.method == "POST":
        return _checkout_start_stripe_redirect()

    cancel_q = (request.args.get("cancelled") or "").strip().lower()
    stripe_back_from_payment = cancel_q in ("true", "1", "yes")
    if request.method == "GET" and stripe_back_from_payment:
        flash("You’re back on checkout — your basket is unchanged.", "info")

    payments_ready = bool(_stripe_secret_key())
    return render_template(
        "checkout.html",
        items=items,
        total=total,
        format_money=database.format_money,
        affiliate=affiliate_row,
        checkout_shipping_cents=default_shipping_cents,
        payments_ready=payments_ready,
        stripe_back_from_payment=stripe_back_from_payment,
    )


@app.route("/dashboard/affiliate")
@login_required
def affiliate_dashboard():
    if _user_is_effective_admin():
        return redirect(url_for("admin_dashboard"))
    if current_user.role != "affiliate":
        return redirect(url_for("admin_dashboard"))
    with database.get_db() as conn:
        creative_assets = database.list_creative_assets(conn)
        affiliate_pending_payout = database.affiliate_has_pending_commission_payout(
            conn, current_user.id
        )
    return render_template(
        "affiliate_dashboard.html",
        affiliate_nav_active="dashboard",
        tiers=COMMISSION_TIERS,
        format_money=database.format_money,
        creative_assets=creative_assets,
        earnings_display=EARNINGS_DISPLAY_NZD,
        affiliate_pending_payout=affiliate_pending_payout,
    )


@app.route("/dashboard/affiliate/delete-account", methods=["POST"])
@login_required
def affiliate_delete_account():
    if _user_is_effective_admin():
        return redirect(url_for("admin_dashboard"))
    if current_user.role != "affiliate":
        return redirect(url_for("admin_dashboard"))
    confirm = (request.form.get("confirm_text") or "").strip()
    password = request.form.get("password") or ""
    if confirm != "DELETE":
        flash("Type DELETE in the confirmation field to remove your account.", "error")
        return redirect(url_for("affiliate_dashboard"))
    uid = current_user.id
    with database.get_db() as conn:
        row = database.user_by_id(conn, uid)
        if not row or row["role"] != "affiliate":
            flash("This account could not be removed.", "error")
            return redirect(url_for("shop"))
        if not check_password_hash(row["password_hash"], password):
            flash("Incorrect password. Your account was not changed.", "error")
            return redirect(url_for("affiliate_dashboard"))
        email_for_log = (row["email"] or "").strip()
        _unlink_affiliate_banner_file(uid)
        _unlink_affiliate_avatar_file(uid)
        ok = database.delete_affiliate_user_account(conn, uid, email_for_log)
    if not ok:
        flash("Could not remove your account. Contact us if this keeps happening.", "error")
        return redirect(url_for("affiliate_dashboard"))
    logout_user()
    flash("Your Listening Room account has been removed.", "ok")
    return redirect(url_for("shop"))


@app.route("/dashboard/affiliate/terms", methods=["POST"])
@login_required
def affiliate_terms_action():
    if _user_is_effective_admin():
        return jsonify({"error": "forbidden"}), 403
    if current_user.role != "affiliate":
        return jsonify({"error": "forbidden"}), 403
    action = (request.form.get("action") or "").strip()
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest"
    uid = current_user.id
    if action == "accept":
        with database.get_db() as conn:
            conn.execute(
                "UPDATE users SET terms_accepted = 1, terms_accepted_at = ? WHERE id = ?",
                (_now_utc().isoformat(), uid),
            )
        if wants_json:
            return jsonify({"ok": True, "terms_accepted": True})
        flash("Terms accepted. Your Listening Room tools are now active.", "ok")
        return redirect(request.referrer or url_for("affiliate_dashboard"))
    if action == "decline":
        with database.get_db() as conn:
            conn.execute(
                "UPDATE users SET terms_accepted = 0, terms_accepted_at = NULL WHERE id = ?",
                (uid,),
            )
        if wants_json:
            return jsonify({"ok": True, "terms_accepted": False})
        flash("You declined the terms. Your Listening Room link and dashboard are locked until you agree.", "error")
        return redirect(request.referrer or url_for("affiliate_dashboard"))
    return jsonify({"error": "bad_request"}), 400


@app.route("/api/affiliate/stats")
@login_required
def api_affiliate_stats():
    if _user_is_effective_admin():
        return jsonify({"error": "forbidden"}), 403
    if current_user.role != "affiliate":
        return jsonify({"error": "forbidden"}), 403
    if not getattr(current_user, "terms_accepted", 0):
        return jsonify(
            {
                "error": "terms_required",
                "locked": True,
                "message": "Accept The Listening Room terms and conditions to view stats and commissions.",
            }
        ), 403
    uid = current_user.id
    now = _now_utc()
    year, month = now.year, now.month

    with database.get_db() as conn:
        urow = conn.execute(
            "SELECT affiliate_code, terms_accepted_at FROM users WHERE id = ?", (uid,)
        ).fetchone()
        lifetime_orders = database.affiliate_lifetime_order_count(conn, uid)
        visit_count = database.affiliate_visit_count(conn, uid)
        orders = affiliate_orders_in_month(conn, uid, year, month)
        orders_json = [
            {
                "order_number": o["order_number"],
                "amount_cents": o["total_cents"],
                "customer": f'{o["customer_first"]} {o["customer_last"]} · {o["customer_email"]}',
                "created_at": o["created_at"],
            }
            for o in reversed(orders)
        ]

        # Daily cumulative for current month
        days_in_month = monthrange(year, month)[1]
        by_day: Dict[str, int] = {f"{month:02d}-{d:02d}": 0 for d in range(1, days_in_month + 1)}
        for o in orders:
            created = o["created_at"][:10]
            try:
                y_s, m_s, d_s = created.split("-")
                if int(y_s) != year or int(m_s) != month:
                    continue
                d_int = int(d_s)
                key = f"{month:02d}-{d_int:02d}"
                if key in by_day:
                    by_day[key] += int(o["total_cents"])
            except (ValueError, KeyError):
                continue
        cumulative_labels: List[str] = []
        cumulative_values: List[float] = []
        run = 0
        for d in range(1, days_in_month + 1):
            key = f"{month:02d}-{d:02d}"
            run += by_day.get(key, 0)
            cumulative_labels.append(str(d))
            cumulative_values.append(run / 100.0)

        sales_count = len(orders)
        total_cents = sum(int(o["total_cents"]) for o in orders)
        orders_commission = sum(_order_affiliate_commission_cents(o) for o in orders)
        if orders_commission == 0 and sales_count:
            orders_commission = sum(
                commission_cents_for_nth_sale(i, int(o["total_cents"]))
                for i, o in enumerate(orders, start=1)
            )
        summary = summarize_month(total_cents, orders_commission, sales_count, year, month)
        next_goal = next_tier_sales_threshold(sales_count)
        progress_pct = progress_toward_next_tier_pct(sales_count)
        lifetime_earnings = database.affiliate_lifetime_earnings_cents(conn, uid)

        # Last 6 months totals (month bar / history)
        monthly: List[Dict[str, Any]] = []
        y, m = year, month
        for _ in range(6):
            ords = affiliate_orders_in_month(conn, uid, y, m)
            cnt = len(ords)
            tot = sum(int(o["total_cents"]) for o in ords)
            ord_comm = sum(_order_affiliate_commission_cents(o) for o in ords)
            if ord_comm == 0 and cnt:
                ord_comm = sum(
                    commission_cents_for_nth_sale(i, int(o["total_cents"]))
                    for i, o in enumerate(ords, start=1)
                )
            bon = monthly_milestone_bonus_cents(cnt)
            monthly.append(
                {
                    "label": f"{y}-{m:02d}",
                    "sales_count": cnt,
                    "total_dollars": tot / 100.0,
                    "rate_next_sale": current_rate_for_next_sale_after(cnt),
                    "commission_dollars": (ord_comm + bon) / 100.0,
                }
            )
            m -= 1
            if m < 1:
                m = 12
                y -= 1

    aff_code = ""
    if urow and "affiliate_code" in urow.keys() and urow["affiliate_code"]:
        aff_code = str(urow["affiliate_code"]).strip()

    return jsonify(
        {
            "sales_this_month": sales_count,
            "total_sales_cents": total_cents,
            "commission_rate": summary.current_rate_next_sale,
            "orders_commission_cents": summary.commission_from_orders_cents,
            "bonus_cents": summary.bonus_cents,
            "earnings_month_cents": summary.total_payable_cents,
            "tier_name": summary.tier_name,
            "affiliate_code": aff_code,
            "lifetime_orders": lifetime_orders,
            "lifetime_earnings_cents": lifetime_earnings,
            "terms_accepted": True,
            "payout_date": summary.payout_date.isoformat(),
            "next_tier_goal": next_goal,
            "progress_pct": round(progress_pct, 1),
            "orders": orders_json,
            "cumulative": {"labels": cumulative_labels, "values": cumulative_values},
            "monthly": list(reversed(monthly)),
            "affiliate_clicks": visit_count,
            "updated_at": _now_utc().isoformat(),
        }
    )


@app.route("/dashboard/affiliate/page", methods=["GET", "POST"])
@login_required
def affiliate_page_edit():
    if _user_is_effective_admin():
        return redirect(url_for("admin_dashboard"))
    if current_user.role != "affiliate":
        return redirect(url_for("admin_dashboard"))
    uid = current_user.id
    with database.get_db() as conn:
        urow = conn.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()

        if request.method == "POST":
            action = (request.form.get("action") or "update").strip()
            tr = conn.execute(
                "SELECT terms_accepted, terms_accepted_at FROM users WHERE id = ?",
                (uid,),
            ).fetchone()
            terms_ok = (
                int(tr["terms_accepted"] or 0) == 1
                if tr and "terms_accepted" in tr.keys()
                else bool(tr["terms_accepted_at"] if tr else None)
            )

            if not terms_ok:
                flash("You must accept the terms before updating your page.", "error")
                return redirect(url_for("affiliate_page_edit"))

            headline = request.form.get("headline", "").strip() or "Welcome"
            tagline = request.form.get("tagline", "").strip()
            description = request.form.get("description", "").strip()
            instagram = request.form.get("instagram_url", "").strip()
            tiktok = request.form.get("tiktok_url", "").strip()
            uploaded = request.files.get("display_picture")
            clear_photo = request.form.get("clear_display_picture") == "1"
            prev_row = conn.execute(
                "SELECT display_picture_url FROM users WHERE id = ?", (uid,)
            ).fetchone()
            prev_pic = (prev_row["display_picture_url"] or "").strip() if prev_row else ""

            prev_page_row = conn.execute(
                "SELECT banner_image_url FROM affiliate_pages WHERE user_id = ?", (uid,)
            ).fetchone()
            prev_banner = (prev_page_row["banner_image_url"] or "").strip() if prev_page_row else ""
            clear_banner = request.form.get("clear_banner_image") == "1"
            banner_upload = request.files.get("banner_image")
            manual_banner_url = request.form.get("banner_image_url", "").strip()

            if clear_banner:
                banner = ""
                _unlink_affiliate_banner_file(uid)
            elif banner_upload and banner_upload.filename:
                try:
                    banner = _save_affiliate_banner(uid, banner_upload)
                except ValueError as err:
                    flash(str(err), "error")
                    return redirect(url_for("affiliate_page_edit"))
            elif manual_banner_url:
                banner = manual_banner_url
                if _affiliate_banner_is_uploaded_path(prev_banner):
                    _unlink_affiliate_banner_file(uid)
            else:
                banner = prev_banner

            if clear_photo:
                avatar_path = AVATAR_UPLOAD_DIR / f"{uid}.jpg"
                if avatar_path.is_file():
                    avatar_path.unlink()
                display_picture_url = None
            elif uploaded and uploaded.filename:
                try:
                    display_picture_url = _save_affiliate_display_picture(uid, uploaded)
                except ValueError as err:
                    flash(str(err), "error")
                    return redirect(url_for("affiliate_page_edit"))
            else:
                url_in = request.form.get("display_picture_url", "").strip()
                if url_in:
                    display_picture_url = url_in
                else:
                    display_picture_url = prev_pic or None
            target = 25  # Stored for compatibility; dashboard progress uses tier milestones (10 / 25).
            now_iso = _now_utc().isoformat()
            conn.execute(
                """
                UPDATE affiliate_pages SET
                    headline = ?, tagline = ?, description = ?,
                    instagram_url = ?, tiktok_url = ?, banner_image_url = ?,
                    monthly_sales_target = ?, page_updated_at = ?
                WHERE user_id = ?
                """,
                (headline, tagline, description, instagram, tiktok, banner, target, now_iso, uid),
            )
            conn.execute(
                "UPDATE users SET display_picture_url = ? WHERE id = ?",
                (display_picture_url or None, uid),
            )
            flash("Your page was updated.", "ok")
            return redirect(url_for("affiliate_page_edit"))

        page = database.affiliate_page(conn, uid)
        if not page:
            conn.execute(
                """
                INSERT INTO affiliate_pages (user_id, headline, tagline, description, monthly_sales_target)
                VALUES (?, 'Welcome', '', '', 25)
                """,
                (uid,),
            )
            page = database.affiliate_page(conn, uid)
        u_public = conn.execute(
            "SELECT affiliate_slug, affiliate_code FROM users WHERE id = ?", (uid,)
        ).fetchone()
    slug = u_public["affiliate_slug"] if u_public else None
    acode = (u_public["affiliate_code"] or "").strip() if u_public and "affiliate_code" in u_public.keys() else ""
    base = request.host_url.rstrip("/")
    if acode:
        public_url = base + url_for("listening_room", code=acode)
    elif slug:
        public_url = base + url_for("affiliate_landing", slug=slug)
    else:
        public_url = base + url_for("shop")
    display_picture_url = ""
    if urow and "display_picture_url" in urow.keys() and urow["display_picture_url"]:
        display_picture_url = str(urow["display_picture_url"]).strip()
    return render_template(
        "affiliate_page_edit.html",
        affiliate_nav_active="page",
        page=page,
        public_url=public_url,
        display_picture_url=display_picture_url,
    )


def _admin_orders_query(conn: sqlite3.Connection, search_q: str) -> List[sqlite3.Row]:
    """All orders with affiliate + item count; optional case-insensitive partial match on name or order #."""
    base = """
        SELECT o.*,
               u.full_name AS aff_name,
               u.email AS aff_email,
               u.affiliate_code AS aff_code,
               (SELECT COALESCE(SUM(oi.quantity), 0) FROM order_items oi WHERE oi.order_id = o.id) AS item_count
        FROM orders o
        LEFT JOIN users u ON u.id = o.affiliate_user_id
    """
    q = (search_q or "").strip()
    if not q:
        return conn.execute(
            base + " ORDER BY o.created_at DESC LIMIT 1000",
        ).fetchall()
    like = f"%{q}%"
    return conn.execute(
        base
        + """
        WHERE o.order_number LIKE ? COLLATE NOCASE
           OR o.customer_first LIKE ? COLLATE NOCASE
           OR o.customer_last LIKE ? COLLATE NOCASE
           OR (o.customer_first || ' ' || o.customer_last) LIKE ? COLLATE NOCASE
        ORDER BY o.created_at DESC
        LIMIT 1000
        """,
        (like, like, like, like),
    ).fetchall()


def _successful_affiliates_current_month(conn: sqlite3.Connection, search_sa: str) -> List[sqlite3.Row]:
    """Listening Room members with ≥1 affiliate order in the current calendar month (UTC)."""
    now = _now_utc()
    y, m = now.year, now.month
    start = f"{y:04d}-{m:02d}-01 00:00:00"
    last_day = monthrange(y, m)[1]
    end = f"{y:04d}-{m:02d}-{last_day:02d} 23:59:59"
    ym = f"{y:04d}-{m:02d}"
    base = """
        SELECT
            u.id,
            u.full_name,
            u.affiliate_code,
            u.email,
            COUNT(o.id) AS sales_this_month,
            COALESCE(SUM(o.affiliate_commission_cents), 0) AS commission_cents_this_month,
            (SELECT payout_status FROM commissions WHERE affiliate_user_id = u.id AND year_month = ? LIMIT 1) AS payout_status
        FROM users u
        INNER JOIN orders o ON o.affiliate_user_id = u.id AND o.order_type = 'affiliate'
          AND o.created_at >= ? AND o.created_at <= ?
        WHERE u.role = 'affiliate'
          AND u.affiliate_code IS NOT NULL AND TRIM(u.affiliate_code) != ''
    """
    params: List[Any] = [ym, start, end]
    sq = (search_sa or "").strip()
    if sq:
        like = f"%{sq}%"
        base += " AND (u.full_name LIKE ? COLLATE NOCASE OR u.affiliate_code LIKE ? COLLATE NOCASE)"
        params.extend([like, like])
    base += """
        GROUP BY u.id
        HAVING COUNT(o.id) > 0
        ORDER BY commission_cents_this_month DESC, u.full_name COLLATE NOCASE
    """
    return conn.execute(base, tuple(params)).fetchall()


def _admin_affiliate_orders_query(
    conn: sqlite3.Connection, affiliate_user_id: int, search_q: str
) -> List[sqlite3.Row]:
    """All referral orders for one affiliate; optional filter on order # or customer name."""
    base = """
        SELECT o.*,
               (SELECT COALESCE(SUM(oi.quantity), 0) FROM order_items oi WHERE oi.order_id = o.id) AS item_count
        FROM orders o
        WHERE o.affiliate_user_id = ? AND o.order_type = 'affiliate'
    """
    q = (search_q or "").strip()
    if not q:
        return conn.execute(base + " ORDER BY o.created_at DESC LIMIT 500", (affiliate_user_id,)).fetchall()
    like = f"%{q}%"
    return conn.execute(
        base
        + """
        AND (
          o.order_number LIKE ? COLLATE NOCASE
          OR o.customer_first LIKE ? COLLATE NOCASE
          OR o.customer_last LIKE ? COLLATE NOCASE
          OR (o.customer_first || ' ' || o.customer_last) LIKE ? COLLATE NOCASE
        )
        ORDER BY o.created_at DESC
        LIMIT 500
        """,
        (affiliate_user_id, like, like, like, like),
    ).fetchall()


def _url_admin_dashboard_preserve(q_orders: str, q_sa: str) -> str:
    """Build admin dashboard URL preserving order-list and successful-affiliate search params."""
    qo = (q_orders or "").strip()
    qs = (q_sa or "").strip()
    if qo and qs:
        return url_for("admin_dashboard", q=qo, sa_q=qs)
    if qo:
        return url_for("admin_dashboard", q=qo)
    if qs:
        return url_for("admin_dashboard", sa_q=qs)
    return url_for("admin_dashboard")


@app.route("/dashboard/admin")
@login_required
def admin_dashboard():
    if not _user_is_effective_admin():
        return redirect(url_for("affiliate_dashboard"))
    search_q = (request.args.get("q") or "").strip()
    sa_q = (request.args.get("sa_q") or "").strip()
    now = _now_utc()
    ym_label = f"{month_name[now.month]} {now.year}"
    with database.get_db() as conn:
        orders = _admin_orders_query(conn, search_q)
        successful_affiliates = _successful_affiliates_current_month(conn, sa_q)
        affiliates = conn.execute(
            """
            SELECT u.id, u.email, u.full_name, u.affiliate_slug, u.affiliate_code,
                   COUNT(o.id) AS order_count,
                   COALESCE(SUM(o.total_cents), 0) AS lifetime_sales_cents
            FROM users u
            LEFT JOIN orders o ON o.affiliate_user_id = u.id AND o.order_type = 'affiliate'
            WHERE u.role = 'affiliate'
            GROUP BY u.id
            ORDER BY u.email
            """
        ).fetchall()
        creative_assets = database.list_creative_assets(conn)
    return render_template(
        "admin_dashboard.html",
        orders=orders,
        successful_affiliates=successful_affiliates,
        successful_month_label=ym_label,
        affiliates=affiliates,
        creative_assets=creative_assets,
        format_money=database.format_money,
        search_q=search_q,
        sa_q=sa_q,
    )


@app.route("/admin/affiliate/<code>")
@login_required
def admin_affiliate_alias(code: str):
    """Short URL → affiliate orders drill-down (admin only)."""
    if not _user_is_effective_admin():
        return redirect(url_for("shop"))
    return redirect(url_for("admin_affiliate_orders", code=code))


@app.route("/order/<order_number>")
@login_required
def admin_order_alias(order_number: str):
    """Short URL → admin order detail (admin only)."""
    if not _user_is_effective_admin():
        return redirect(url_for("shop"))
    return redirect(url_for("admin_order_detail_ref", order_number=order_number))


@app.route("/dashboard/admin/affiliate/<code>")
@login_required
def admin_affiliate_orders(code: str):
    """All orders for one Listening Room member (by affiliate code)."""
    if not _user_is_effective_admin():
        return redirect(url_for("affiliate_dashboard"))
    dq = (request.args.get("dq") or "").strip()
    dsa = (request.args.get("dsa") or "").strip()
    oq = (request.args.get("oq") or "").strip()
    with database.get_db() as conn:
        aff = database.affiliate_by_code(conn, code)
        if not aff:
            flash("That Listening Room member was not found.", "error")
            return redirect(_url_admin_dashboard_preserve(dq, dsa))
        uid = int(aff["id"])
        order_rows = _admin_affiliate_orders_query(conn, uid, oq)
    display_name = (aff["full_name"] or "").strip() or (aff["affiliate_code"] or "").strip() or "Member"
    back_admin = _url_admin_dashboard_preserve(dq, dsa)
    return render_template(
        "admin_affiliate_orders.html",
        affiliate=aff,
        affiliate_display_name=display_name,
        orders=order_rows,
        format_money=database.format_money,
        oq=oq,
        dash_q=dq,
        dash_sa=dsa,
        back_admin=back_admin,
    )


@app.route("/dashboard/admin/order/<int:order_id>/fulfillment", methods=["POST"])
@login_required
def admin_order_fulfillment(order_id: int):
    if not _user_is_effective_admin():
        return redirect(url_for("affiliate_dashboard"))
    fulfilled = request.form.get("fulfilled") == "1"
    search_q = (request.form.get("search_q") or "").strip()
    return_to = (request.form.get("return_to") or "").strip()
    redirect_order_number: Optional[str] = None
    with database.get_db() as conn:
        row = conn.execute(
            "SELECT id, fulfillment_status, customer_email, order_number FROM orders WHERE id = ?",
            (order_id,),
        ).fetchone()
        if not row:
            flash("Order not found.", "error")
            return redirect(url_for("admin_dashboard", q=search_q) if search_q else url_for("admin_dashboard"))
        was_fulfilled = (row["fulfillment_status"] or "").strip().lower() == "shipped"
        if fulfilled:
            conn.execute(
                """
                UPDATE orders SET fulfillment_status = 'shipped', status = 'shipped'
                WHERE id = ?
                """,
                (order_id,),
            )
        else:
            conn.execute(
                """
                UPDATE orders SET fulfillment_status = 'paid', status = 'completed'
                WHERE id = ?
                """,
                (order_id,),
            )
        if fulfilled and not was_fulfilled:
            try:
                send_order_fulfilled_notification(
                    str(row["customer_email"] or "").strip(),
                    str(row["order_number"] or ""),
                )
            except Exception:
                pass
        if return_to == "detail":
            r2 = conn.execute("SELECT order_number FROM orders WHERE id = ?", (order_id,)).fetchone()
            if r2 and r2["order_number"]:
                redirect_order_number = str(r2["order_number"])
    if return_to == "detail":
        if redirect_order_number:
            return redirect(url_for("admin_order_detail_ref", order_number=redirect_order_number))
        return redirect(url_for("admin_order_detail", order_id=order_id))
    if return_to == "affiliate":
        aff_code = (request.form.get("affiliate_code") or "").strip()
        if aff_code:
            oq = (request.form.get("affiliate_list_q") or "").strip()
            dq = (request.form.get("dash_q") or "").strip()
            dsa = (request.form.get("dash_sa") or "").strip()
            kw: Dict[str, str] = {}
            if oq:
                kw["oq"] = oq
            if dq:
                kw["dq"] = dq
            if dsa:
                kw["dsa"] = dsa
            return redirect(url_for("admin_affiliate_orders", code=aff_code, **kw))
    dest = url_for("admin_dashboard", q=search_q) if search_q else url_for("admin_dashboard")
    return redirect(dest)


@app.route("/dashboard/admin/creative-library", methods=["POST"])
@login_required
def admin_creative_library_upload():
    if not _user_is_effective_admin():
        return redirect(url_for("affiliate_dashboard"))
    main = request.files.get("file")
    if not main or not main.filename or not main.filename.strip():
        flash("Choose an image or video file to upload.", "error")
        return redirect(url_for("admin_dashboard"))
    title = (request.form.get("title") or "").strip()
    tags = (request.form.get("tags") or "").strip()
    thumb = request.files.get("thumbnail")
    thumb_path: Optional[str] = None
    file_path: Optional[str] = None
    asset_type: Optional[str] = None
    try:
        file_path, asset_type = _store_creative_main_file(main)
        if thumb and thumb.filename and thumb.filename.strip() and asset_type == "video":
            thumb_path = _store_creative_thumbnail_file(thumb)
    except ValueError as e:
        if file_path:
            _unlink_static_relative(file_path)
        err = str(e)
        msg = {
            "unsupported_type": "Unsupported file type. Use JPG, PNG, WebP, GIF, MP4, WebM, or MOV.",
            "image_too_large": "Image file is too large (max 35 MB).",
            "video_too_large": "Video file is too large (max 220 MB).",
            "thumb_not_image": "Thumbnail must be a JPG, PNG, WebP, or GIF image.",
            "thumb_too_large": "Thumbnail image is too large (max 35 MB).",
        }.get(err, "Could not upload that file.")
        flash(msg, "error")
        return redirect(url_for("admin_dashboard"))
    with database.get_db() as conn:
        database.insert_creative_asset(conn, title, file_path, thumb_path, asset_type, tags)
    flash("Creative asset added. It will appear in every member's Listening Room dashboard.", "ok")
    return redirect(url_for("admin_dashboard"))


@app.route("/dashboard/admin/creative-library/<int:asset_id>/delete", methods=["POST"])
@login_required
def admin_creative_library_delete(asset_id: int):
    if not _user_is_effective_admin():
        return redirect(url_for("affiliate_dashboard"))
    with database.get_db() as conn:
        row = database.delete_creative_asset(conn, asset_id)
    if not row:
        flash("That asset was not found.", "error")
        return redirect(url_for("admin_dashboard"))
    _unlink_static_relative(row["file_path"])
    if row["thumbnail_path"]:
        _unlink_static_relative(row["thumbnail_path"])
    flash("Creative asset removed.", "ok")
    return redirect(url_for("admin_dashboard"))


def _admin_order_detail_response(order_id: int):
    """Shared handler: order detail page (by id). GET shows breakdown; POST action=tracking saves tracking only."""
    if not _user_is_effective_admin():
        return redirect(url_for("affiliate_dashboard"))
    with database.get_db() as conn:
        order = conn.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
        if not order:
            flash("Order not found.", "error")
            return redirect(url_for("admin_dashboard"))
        if request.method == "POST" and (request.form.get("action") or "").strip() == "tracking":
            tracking = request.form.get("shipping_tracking", "").strip()
            conn.execute(
                "UPDATE orders SET shipping_tracking = ? WHERE id = ?",
                (tracking, order_id),
            )
            flash("Tracking saved.", "ok")
            rnum = conn.execute("SELECT order_number FROM orders WHERE id = ?", (order_id,)).fetchone()
            if rnum and rnum["order_number"]:
                return redirect(url_for("admin_order_detail_ref", order_number=rnum["order_number"]))
            return redirect(url_for("admin_order_detail", order_id=order_id))

        items = conn.execute(
            """
            SELECT oi.*, p.name AS product_name, p.slug
            FROM order_items oi
            JOIN products p ON p.id = oi.product_id
            WHERE oi.order_id = ?
            ORDER BY oi.id
            """,
            (order_id,),
        ).fetchall()
        aff = None
        if order["affiliate_user_id"]:
            aff = database.user_by_id(conn, order["affiliate_user_id"])

    subtotal_cents = order["subtotal_cents"]
    if subtotal_cents is None:
        subtotal_cents = int(order["total_cents"] or 0) - int(order["shipping_cents"] or 0)
    else:
        subtotal_cents = int(subtotal_cents)
    shipping_cents = int(order["shipping_cents"] or 0)
    is_fulfilled = (order["fulfillment_status"] or "").strip().lower() == "shipped"
    return render_template(
        "admin_order_detail.html",
        order=order,
        items=items,
        affiliate=aff,
        format_money=database.format_money,
        subtotal_cents=subtotal_cents,
        shipping_cents=shipping_cents,
        is_fulfilled=is_fulfilled,
    )


@app.route("/dashboard/admin/order/ref/<order_number>", methods=["GET", "POST"])
@login_required
def admin_order_detail_ref(order_number: str):
    """Pretty URL: /dashboard/admin/order/ref/LL-YYYYMMDD-HEX — same view as numeric id."""
    with database.get_db() as conn:
        row = conn.execute("SELECT id FROM orders WHERE order_number = ?", (order_number,)).fetchone()
    if not row:
        flash("Order not found.", "error")
        return redirect(url_for("admin_dashboard"))
    return _admin_order_detail_response(int(row["id"]))


@app.route("/dashboard/admin/order/<int:order_id>", methods=["GET", "POST"])
@login_required
def admin_order_detail(order_id: int):
    return _admin_order_detail_response(order_id)


@app.route("/api/admin/summary")
@login_required
def api_admin_summary():
    if not _user_is_effective_admin():
        return jsonify({"error": "forbidden"}), 403
    with database.get_db() as conn:
        order_count = conn.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"]
        aff_count = conn.execute(
            "SELECT COUNT(*) AS c FROM users WHERE role = 'affiliate'"
        ).fetchone()["c"]
    return jsonify(
        {
            "orders": order_count,
            "affiliates": aff_count,
            "updated_at": _now_utc().isoformat(),
        }
    )


ANALYTICS_INACTIVITY_SEC = 900


@app.route("/api/analytics/session/start", methods=["POST"])
def api_analytics_session_start():
    """Create or resume a behavioral session (geo from IP, device from UA)."""
    data = request.get_json(silent=True) or {}
    now = _now_utc().isoformat()
    client_sid = (data.get("session_id") or "").strip()
    aff = (data.get("affiliate") or "").strip() or None
    ip = client_ip_from_request(request)
    geo = geo_lookup(ip)
    country = geo.get("country_code") or ""
    city = geo.get("city") or ""
    fpt = ip_fingerprint(ip)
    ua = request.headers.get("User-Agent", "") or ""
    dev = device_class_from_user_agent(ua)
    with database.get_db() as conn:
        if client_sid and database.analytics_session_exists(conn, client_sid):
            database.analytics_session_touch(conn, client_sid, now)
            if aff:
                conn.execute(
                    """
                    UPDATE analytics_sessions SET affiliate_code = COALESCE(affiliate_code, ?)
                    WHERE session_id = ?
                    """,
                    (aff, client_sid),
                )
            return jsonify({"session_id": client_sid, "country": country, "city": city})
        sid = client_sid or str(uuid.uuid4())
        database.analytics_create_session(
            conn, sid, country or "", city or "", fpt, dev, ua, aff, now
        )
    return jsonify({"session_id": sid, "country": country, "city": city})


@app.route("/api/analytics/track", methods=["POST"])
def api_analytics_track():
    data = request.get_json(silent=True) or {}
    sid = (data.get("session_id") or "").strip()
    event = (data.get("event") or "").strip() or "unknown"
    page = (data.get("page") or "").strip() or (request.path or "")
    meta = data.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    now = _now_utc().isoformat()
    if not sid:
        return jsonify({"ok": False, "error": "session_id"}), 400
    try:
        meta_json = json.dumps(meta, separators=(",", ":"), ensure_ascii=False)[:8192]
    except (TypeError, ValueError):
        meta_json = "{}"
    with database.get_db() as conn:
        if not database.analytics_session_exists(conn, sid):
            return jsonify({"ok": False, "error": "unknown_session"}), 400
        database.analytics_insert_event(conn, sid, event, page, meta_json, now)
    return jsonify({"ok": True})


@app.route("/api/analytics/convert", methods=["POST"])
def api_analytics_convert():
    data = request.get_json(silent=True) or {}
    sid = (data.get("session_id") or "").strip()
    now = _now_utc().isoformat()
    if not sid:
        return jsonify({"ok": False}), 400
    with database.get_db() as conn:
        if database.analytics_session_exists(conn, sid):
            database.analytics_mark_converted(conn, sid, now)
    return jsonify({"ok": True})


def _analytics_period_start_iso(days: int) -> Optional[str]:
    if days <= 0:
        return None
    return (_now_utc() - timedelta(days=days)).isoformat()


@app.route("/api/admin/analytics", methods=["GET"])
@login_required
def api_admin_analytics():
    if not _user_is_effective_admin():
        return jsonify({"error": "forbidden"}), 403
    try:
        days = int((request.args.get("days") or "0").strip() or "0")
    except ValueError:
        days = 0
    days = max(0, min(days, 366))
    since_iso = _analytics_period_start_iso(days)
    cutoff = (_now_utc() - timedelta(seconds=ANALYTICS_INACTIVITY_SEC)).isoformat()
    with database.get_db() as conn:
        database.analytics_refresh_dropoffs(conn, cutoff)
        countries = database.analytics_summary_by_country(conn, since_iso)
        cities = database.analytics_summary_top_cities(conn, since_iso, 15)
        devices = database.analytics_summary_devices(conn, since_iso)
        dropoffs = database.analytics_summary_dropoffs(conn, since_iso)
        total, converted, rate = database.analytics_conversion_stats(conn, since_iso)
        orders_geo = database.orders_geo_summary_by_country(conn, since_iso)

    visitor_chart = [
        {"country": k, "visits": v} for k, v in sorted(countries.items(), key=lambda x: -x[1])
    ]
    order_chart = [
        {
            "country": k,
            "orders": v["orders"],
            "revenue_cents": v["revenue_cents"],
        }
        for k, v in sorted(orders_geo.items(), key=lambda x: -x[1]["orders"])
    ]
    city_chart = [{"city": k, "visits": v} for k, v in sorted(cities.items(), key=lambda x: -x[1])]

    return jsonify(
        {
            "period_days": days,
            "countries": countries,
            "cities": cities,
            "city_chart": city_chart,
            "visitor_countries_chart": visitor_chart,
            "order_countries_chart": order_chart,
            "devices": devices,
            "dropoffs": dropoffs,
            "orders_by_country": orders_geo,
            "conversion": {
                "total": total,
                "converted": converted,
                "rate": round(rate, 2),
            },
            "updated_at": _now_utc().isoformat(),
        }
    )


@app.after_request
def _affiliate_ref_query_last_click(response):
    """Last-click attribution: ?ref=AFFILIATE_CODE sets a 30-day Listening Room cookie."""
    if request.method != "GET":
        return response
    if (request.endpoint or "") == "static" or request.path.startswith("/static"):
        return response
    ref = (request.args.get("ref") or "").strip()
    if not ref:
        return response
    with database.get_db() as conn:
        aff = database.affiliate_by_code(conn, ref)
        if not aff or not _affiliate_row_public_ok(aff):
            return response
        slug = (aff["affiliate_slug"] or "").strip()
        if not slug:
            return response
    response.set_cookie(
        "licorice_affiliate_slug",
        slug,
        max_age=60 * 60 * 24 * 30,
        samesite="Lax",
        httponly=True,
    )
    return response


@app.cli.command("init-db")
def init_db_command():
    database.init_db()
    database.seed_if_empty()
    print("Database ready.")


app.logger.info(
    "Licorice Locker configuration: stripe_secret=%s resend=%s site_url=%r proxy_trust=%s "
    "(set SITE_URL to your canonical public origin, e.g. https://www.licoricelocker.com)",
    "configured" if _stripe_secret_key() else "MISSING",
    "configured" if (os.environ.get("RESEND_API_KEY") or "").strip() else "off",
    (os.environ.get("SITE_URL") or "").strip() or None,
    bool((os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("TRUST_PROXY") or "").strip()),
)


if __name__ == "__main__":
    database.seed_if_empty()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
