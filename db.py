"""SQLite database for Licorice Locker."""

from __future__ import annotations

import hashlib
import logging
import os
import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from werkzeug.security import generate_password_hash


def _resolved_db_path() -> Path:
    """Resolve SQLite file path.

    Set ``DATABASE_PATH`` on production (e.g. Railway volume: ``/data/app.db``).
    If unset, uses ``<project>/data/licorice.db`` (created on first connect).
    """
    raw = (os.environ.get("DATABASE_PATH") or "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parent / "data" / "licorice.db"


DB_PATH = _resolved_db_path()


def normalize_email(email: Optional[str]) -> str:
    """Return a single canonical form for lookups and storage.

    - Strip, NFKC, case-fold (ASCII emails are lowercased for storage and lookup).
    - ``@googlemail.com`` → ``@gmail.com`` (Google treats them as the same mailbox).
    - For Gmail addresses: strip ``+tag`` from the local part (delivery aliases) and remove
      dots from the local part (Google ignores dots). Matches how people type vs how they
      signed up.
    """
    if email is None:
        return ""
    s = str(email).strip()
    # Autofill / paste sometimes inserts BOM or zero-width characters (Safari, Word, PDFs).
    s = (
        s.replace("\ufeff", "")
        .replace("\u200b", "")
        .replace("\u200c", "")
        .replace("\u200d", "")
        .replace("\u2060", "")
    )
    # Non-breaking / narrow spaces (Safari autofill, copy-paste from web)
    s = s.replace("\u00a0", " ").replace("\u202f", " ")
    # Fullwidth @ (some IMEs / mobile keyboards)
    s = s.replace("\uff20", "@")
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.casefold()
    if "@" not in s:
        return s
    local, domain = s.rsplit("@", 1)
    local = local.strip()
    domain = domain.strip()
    if domain == "googlemail.com":
        domain = "gmail.com"
    if domain == "gmail.com":
        if "+" in local:
            local = local.split("+", 1)[0]
        local = local.replace(".", "")
    return f"{local}@{domain}"


def _migrate_normalize_user_emails(db: sqlite3.Connection) -> None:
    """Backfill: store emails in normalized form. Idempotent; logs duplicates that need manual fix."""
    log = logging.getLogger(__name__)
    rows = db.execute("SELECT id, email FROM users").fetchall()
    for r in rows:
        uid = int(r["id"])
        old = r["email"] or ""
        new = normalize_email(old)
        if not new:
            continue
        if old == new:
            continue
        try:
            db.execute("UPDATE users SET email = ? WHERE id = ?", (new, uid))
            log.info("Normalized stored email for user id=%s", uid)
        except sqlite3.IntegrityError:
            log.warning(
                "Could not normalize email for user id=%s: target %r already exists; resolve duplicate manually",
                uid,
                new,
            )


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db() -> Generator[sqlite3.Connection, None, None]:
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db() -> None:
    """Create tables and run migrations if missing. Never drops data or overwrites existing rows."""
    with get_db() as db:
        db.executescript(
            """
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL CHECK (role IN ('admin', 'affiliate')),
                affiliate_slug TEXT UNIQUE,
                full_name TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS affiliate_pages (
                user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                headline TEXT NOT NULL DEFAULT 'Welcome',
                tagline TEXT NOT NULL DEFAULT '',
                description TEXT NOT NULL DEFAULT '',
                instagram_url TEXT NOT NULL DEFAULT '',
                tiktok_url TEXT NOT NULL DEFAULT '',
                banner_image_url TEXT NOT NULL DEFAULT '',
                monthly_sales_target INTEGER NOT NULL DEFAULT 25
            );

            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                price_cents INTEGER NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                sort_order INTEGER NOT NULL DEFAULT 0,
                is_main INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_number TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                status TEXT NOT NULL DEFAULT 'completed',
                order_type TEXT NOT NULL CHECK (order_type IN ('affiliate', 'direct')),
                affiliate_user_id INTEGER REFERENCES users(id),
                customer_first TEXT NOT NULL,
                customer_last TEXT NOT NULL,
                customer_email TEXT NOT NULL,
                shipping_line1 TEXT NOT NULL,
                shipping_line2 TEXT NOT NULL DEFAULT '',
                shipping_city TEXT NOT NULL,
                shipping_region TEXT NOT NULL DEFAULT '',
                shipping_postal TEXT NOT NULL,
                shipping_country TEXT NOT NULL DEFAULT '',
                shipping_tracking TEXT NOT NULL DEFAULT '',
                total_cents INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
                product_id INTEGER NOT NULL REFERENCES products(id),
                quantity INTEGER NOT NULL,
                unit_price_cents INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS affiliate_visits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                affiliate_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                visitor_id TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE TABLE IF NOT EXISTS commissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                affiliate_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                year_month TEXT NOT NULL,
                sales_count INTEGER NOT NULL,
                commission_rate REAL NOT NULL,
                total_sales_cents INTEGER NOT NULL,
                commission_cents INTEGER NOT NULL,
                payout_status TEXT NOT NULL DEFAULT 'pending',
                UNIQUE (affiliate_user_id, year_month)
            );

            CREATE INDEX IF NOT EXISTS idx_orders_affiliate ON orders(affiliate_user_id);
            CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);
            CREATE INDEX IF NOT EXISTS idx_visits_affiliate ON affiliate_visits(affiliate_user_id);

            CREATE TABLE IF NOT EXISTS product_images (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                path TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                role TEXT NOT NULL DEFAULT 'gallery' CHECK (role IN ('banner', 'thumbnail', 'gallery'))
            );

            CREATE TABLE IF NOT EXISTS product_tags (
                product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
                tag TEXT NOT NULL COLLATE NOCASE,
                PRIMARY KEY (product_id, tag)
            );

            CREATE INDEX IF NOT EXISTS idx_product_images_product ON product_images(product_id);
            CREATE INDEX IF NOT EXISTS idx_product_tags_tag ON product_tags(tag);
            """
        )
        _migrate_product_columns(db)
        _migrate_user_columns(db)
        _migrate_product_enhanced(db)
        _ensure_core_products(db)
        _ensure_product_specs(db)
        _backfill_product_enhanced(db)
        _ensure_product_images_tags(db)
        _backfill_sound_wave_marketing(db)
        _backfill_allegra_marketing(db)
        _backfill_harmony_marketing(db)
        _backfill_melody_marketing(db)
        _backfill_riff_marketing(db)
        _migrate_order_columns(db)
        _backfill_order_columns(db)
        _migrate_order_affiliate_commission(db)
        _migrate_affiliate_profile_columns(db)
        _migrate_terms_accepted(db)
        _backfill_affiliate_profile(db)
        _migrate_creative_assets(db)
        _migrate_commissions_bonus(db)
        _migrate_analytics_tables(db)
        _migrate_analytics_geo_columns(db)
        _migrate_orders_geo_columns(db)
        _migrate_affiliate_deletion_log(db)
        _migrate_affiliate_invite_tokens(db)
        _migrate_normalize_user_emails(db)
        # Align legacy16-sale target with top milestone (25)
        db.execute(
            "UPDATE affiliate_pages SET monthly_sales_target = 25 WHERE IFNULL(monthly_sales_target, 0) = 16"
        )
        _sync_product_catalog_prices(db)


def _migrate_creative_assets(db: sqlite3.Connection) -> None:
    """Curated media for all affiliates (admin-managed)."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS creative_assets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL DEFAULT '',
            file_path TEXT NOT NULL,
            thumbnail_path TEXT,
            asset_type TEXT NOT NULL CHECK (asset_type IN ('image', 'video')),
            tags TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_creative_assets_created ON creative_assets(created_at)")


def _migrate_terms_accepted(db: sqlite3.Connection) -> None:
    """Explicit yes/no for affiliate terms; default 0. Backfill from terms_accepted_at."""
    cols = {row[1] for row in db.execute("PRAGMA table_info(users)").fetchall()}
    if "terms_accepted" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN terms_accepted INTEGER NOT NULL DEFAULT 0")
    db.execute(
        """
        UPDATE users
        SET terms_accepted = 1
        WHERE terms_accepted_at IS NOT NULL AND TRIM(terms_accepted_at) != ''
        """
    )


def _migrate_affiliate_invite_tokens(db: sqlite3.Connection) -> None:
    """Optional invite tokens for controlled Listening Room signup (open mode when table empty / env off)."""
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS affiliate_invite_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            token TEXT NOT NULL UNIQUE,
            email TEXT,
            expires_at TEXT NOT NULL,
            used_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    db.execute("CREATE INDEX IF NOT EXISTS idx_affiliate_invite_tokens_token ON affiliate_invite_tokens(token)")


def affiliate_invite_token_by_token(conn: sqlite3.Connection, token: str) -> Optional[sqlite3.Row]:
    t = (token or "").strip()
    if not t:
        return None
    return conn.execute(
        "SELECT * FROM affiliate_invite_tokens WHERE token = ? LIMIT 1",
        (t,),
    ).fetchone()


def mark_affiliate_invite_token_used(conn: sqlite3.Connection, invite_id: int) -> None:
    conn.execute(
        "UPDATE affiliate_invite_tokens SET used_at = datetime('now') WHERE id = ?",
        (invite_id,),
    )


def _migrate_affiliate_deletion_log(db: sqlite3.Connection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS affiliate_account_deletions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deleted_at TEXT NOT NULL DEFAULT (datetime('now')),
            former_user_id INTEGER NOT NULL,
            email_hash TEXT NOT NULL DEFAULT ''
        )
        """
    )


def _migrate_commissions_bonus(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(commissions)").fetchall()}
    if "bonus_cents" not in cols:
        db.execute("ALTER TABLE commissions ADD COLUMN bonus_cents INTEGER NOT NULL DEFAULT 0")


def _migrate_analytics_tables(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS analytics_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL UNIQUE,
            country TEXT NOT NULL DEFAULT '',
            device_class TEXT NOT NULL DEFAULT '',
            user_agent TEXT NOT NULL DEFAULT '',
            affiliate_code TEXT,
            started_at TEXT NOT NULL,
            last_active_at TEXT NOT NULL,
            converted INTEGER NOT NULL DEFAULT 0,
            dropped_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_analytics_sessions_last ON analytics_sessions(last_active_at);
        CREATE INDEX IF NOT EXISTS idx_analytics_sessions_conv ON analytics_sessions(converted);

        CREATE TABLE IF NOT EXISTS analytics_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            event TEXT NOT NULL,
            page TEXT NOT NULL,
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_analytics_events_session ON analytics_events(session_id, created_at);
        """
    )


def _migrate_analytics_geo_columns(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(analytics_sessions)").fetchall()}
    if "city" not in cols:
        db.execute("ALTER TABLE analytics_sessions ADD COLUMN city TEXT NOT NULL DEFAULT ''")
    if "ip_hash" not in cols:
        db.execute("ALTER TABLE analytics_sessions ADD COLUMN ip_hash TEXT NOT NULL DEFAULT ''")
    if "started_at" in cols:
        db.execute(
            "CREATE INDEX IF NOT EXISTS idx_analytics_sessions_started ON analytics_sessions(started_at)"
        )


def _migrate_orders_geo_columns(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(orders)").fetchall()}
    if "geo_country" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN geo_country TEXT")
    if "geo_city" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN geo_city TEXT")


def _sync_product_catalog_prices(db: sqlite3.Connection) -> None:
    """List prices in NZD (cents). Idempotent UPDATEs so existing DBs match the storefront."""
    for cents, slug in (
        (42900, "sound-wave"),
        (14300, "riff"),
        (14300, "allegro"),
        (14300, "harmony"),
        (14300, "melody"),
    ):
        db.execute("UPDATE products SET price_cents = ? WHERE slug = ?", (cents, slug))


def _migrate_order_affiliate_commission(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(orders)").fetchall()}
    if "affiliate_commission_cents" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN affiliate_commission_cents INTEGER")
    if "affiliate_commission_rate" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN affiliate_commission_rate REAL")


def _migrate_affiliate_profile_columns(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(users)").fetchall()}
    if "affiliate_code" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN affiliate_code TEXT")
    if "affiliate_active" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN affiliate_active INTEGER NOT NULL DEFAULT 1")
    if "terms_accepted_at" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN terms_accepted_at TEXT")
    if "display_picture_url" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN display_picture_url TEXT")
    if "admin_notes" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN admin_notes TEXT")
    if "signup_first_name" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN signup_first_name TEXT")
    if "signup_last_name" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN signup_last_name TEXT")
    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_users_affiliate_code
        ON users(affiliate_code)
        WHERE affiliate_code IS NOT NULL AND LENGTH(TRIM(affiliate_code)) > 0
        """
    )
    ap_cols = {row[1] for row in db.execute("PRAGMA table_info(affiliate_pages)").fetchall()}
    if "page_updated_at" not in ap_cols:
        db.execute("ALTER TABLE affiliate_pages ADD COLUMN page_updated_at TEXT")


def _backfill_affiliate_profile(db: sqlite3.Connection) -> None:
    """Affiliate codes from name; signup names from full_name."""
    rows = db.execute(
        "SELECT id, full_name, affiliate_slug, affiliate_code, signup_first_name FROM users WHERE role = 'affiliate'"
    ).fetchall()
    for r in rows:
        uid = int(r["id"])
        fn = (r["full_name"] or "").strip()
        existing = (r["affiliate_code"] or "").strip()
        if not existing:
            parts = fn.split()
            first = parts[0] if parts else "Partner"
            last = parts[-1] if len(parts) > 1 else first
            base_raw = f"{first}-{last}".upper().replace(" ", "-")
            base = "".join(c for c in base_raw if c.isalnum() or c == "-")[:32] or "AFFILIATE"
            unique = base
            suffix = 0
            while True:
                clash = db.execute(
                    "SELECT id FROM users WHERE affiliate_code = ? AND id != ? LIMIT 1",
                    (unique, uid),
                ).fetchone()
                if not clash:
                    break
                suffix += 1
                unique = f"{base}-{suffix}"[:40]
            db.execute("UPDATE users SET affiliate_code = ? WHERE id = ?", (unique, uid))
        if not (r["signup_first_name"] or "").strip() and fn:
            parts = fn.split()
            sf = parts[0] if parts else ""
            sl = parts[-1] if len(parts) > 1 else ""
            db.execute(
                "UPDATE users SET signup_first_name = ?, signup_last_name = ? WHERE id = ? AND (signup_first_name IS NULL OR signup_first_name = '')",
                (sf, sl, uid),
            )


def _migrate_order_columns(db: sqlite3.Connection) -> None:
    """Completed-order fields: money breakdown, affiliate audit, receipt, fulfillment, guest id."""
    cols = {row[1] for row in db.execute("PRAGMA table_info(orders)").fetchall()}
    if "subtotal_cents" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN subtotal_cents INTEGER")
    if "shipping_cents" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN shipping_cents INTEGER NOT NULL DEFAULT 0")
    if "affiliate_code" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN affiliate_code TEXT")
    if "affiliate_counted" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN affiliate_counted INTEGER NOT NULL DEFAULT 0")
    if "guest_session_id" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN guest_session_id TEXT")
    if "receipt_sent" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN receipt_sent INTEGER NOT NULL DEFAULT 0")
    if "receipt_sent_at" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN receipt_sent_at TEXT")
    if "shipping_name" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN shipping_name TEXT")
    if "payment_method" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN payment_method TEXT NOT NULL DEFAULT ''")
    if "customer_notes" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN customer_notes TEXT NOT NULL DEFAULT ''")
    if "fulfillment_status" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN fulfillment_status TEXT NOT NULL DEFAULT 'paid'")
    if "customer_phone" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN customer_phone TEXT NOT NULL DEFAULT ''")
    if "stripe_checkout_session_id" not in cols:
        db.execute("ALTER TABLE orders ADD COLUMN stripe_checkout_session_id TEXT")
    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_stripe_checkout_session
        ON orders(stripe_checkout_session_id)
        WHERE stripe_checkout_session_id IS NOT NULL AND length(trim(stripe_checkout_session_id)) > 0
        """
    )


def _backfill_order_columns(db: sqlite3.Connection) -> None:
    """Legacy rows: subtotal = total if no shipping split; affiliate flags; fulfillment state."""
    db.execute(
        """
        UPDATE orders SET subtotal_cents = total_cents
        WHERE subtotal_cents IS NULL
        """
    )
    db.execute(
        """
        UPDATE orders SET affiliate_counted = 1
        WHERE affiliate_user_id IS NOT NULL AND affiliate_counted = 0
        """
    )
    db.execute(
        """
        UPDATE orders SET fulfillment_status = 'shipped' WHERE status = 'shipped'
        """
    )
    db.execute(
        """
        UPDATE orders SET shipping_name = TRIM(customer_first || ' ' || customer_last)
        WHERE (shipping_name IS NULL OR TRIM(shipping_name) = '')
          AND (customer_first IS NOT NULL OR customer_last IS NOT NULL)
        """
    )


def _migrate_user_columns(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(users)").fetchall()}
    if "totp_secret" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN totp_secret TEXT")
    if "totp_confirmed" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN totp_confirmed INTEGER NOT NULL DEFAULT 0")
    if "password_reset_token" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN password_reset_token TEXT")
    if "password_reset_expires" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN password_reset_expires TEXT")


def _migrate_product_columns(db: sqlite3.Connection) -> None:
    cols = {row[1] for row in db.execute("PRAGMA table_info(products)").fetchall()}
    if "dimensions" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN dimensions TEXT NOT NULL DEFAULT ''")
    if "materials" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN materials TEXT NOT NULL DEFAULT ''")
    if "capacity" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN capacity TEXT NOT NULL DEFAULT ''")


def _migrate_product_enhanced(db: sqlite3.Connection) -> None:
    """SKU, timestamps, dimensions (cm), capacity count, flags, collection, related tables."""
    cols = {row[1] for row in db.execute("PRAGMA table_info(products)").fetchall()}
    if "sku" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN sku TEXT")
    if "created_at" not in cols:
        # SQLite ALTER cannot use non-constant defaults; backfill in _backfill_product_enhanced.
        db.execute("ALTER TABLE products ADD COLUMN created_at TEXT")
    if "width_cm" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN width_cm REAL")
    if "height_cm" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN height_cm REAL")
    if "depth_cm" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN depth_cm REAL")
    if "capacity_records" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN capacity_records INTEGER")
    if "add_to_cart_enabled" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN add_to_cart_enabled INTEGER NOT NULL DEFAULT 1")
    if "featured" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN featured INTEGER NOT NULL DEFAULT 0")
    if "collection" not in cols:
        db.execute("ALTER TABLE products ADD COLUMN collection TEXT NOT NULL DEFAULT ''")

    db.execute("CREATE INDEX IF NOT EXISTS idx_products_collection ON products(collection)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_products_featured ON products(featured)")


# W × H × D in millimetres (from product copy); stored as cm in DB.
_PRODUCT_DIMS_MM: Dict[str, Tuple[float, float, float]] = {
    "sound-wave": (312.0, 198.0, 94.0),
    "allegro": (210.0, 120.0, 38.0),
    "melody": (240.0, 160.0, 52.0),
    "harmony": (280.0, 200.0, 64.0),
    "riff": (190.0, 110.0, 42.0),
}

_PRODUCT_CAPACITY: Dict[str, int] = {
    "sound-wave": 40,
    "allegro": 12,
    "melody": 24,
    "harmony": 32,
    "riff": 8,
}

_PRODUCT_COLLECTION: Dict[str, str] = {
    "sound-wave": "Clear Collection",
    "allegro": "Clear Collection",
    "melody": "Clear Collection",
    "harmony": "Clear Collection",
    "riff": "Clear Collection",
}

_PRODUCT_TAGS: Dict[str, Tuple[str, ...]] = {}

_SOUND_WAVE_DESCRIPTION = (
    "Sound Wave is a sculptural vinyl record display designed to turn a record collection into a physical expression of music itself.\n\n"
    "Inspired by the visual form of an audio waveform, Sound Wave arranges records in a gentle rise and fall — creating rhythm, movement, and balance in space. From a distance it reads as a single object; up close, each record becomes part of a larger composition.\n\n"
    "Rather than storing records vertically or hiding them away, Sound Wave places them on display as art. Albums float in sequence, evenly spaced, allowing cover artwork to remain visible while maintaining a sense of lightness and restraint.\n\n"
    "The clear acrylic structure is deliberately minimal. It recedes visually, allowing the records to take centre stage, while maintaining strength and precision. The result is a display that feels architectural rather than decorative — considered, calm, and intentional.\n\n"
    "Sound Wave is designed for collectors who don't just listen to music, but live with it. It transforms a personal collection into a sculptural object that evolves as records are added, removed, and rearranged.\n\n"
    "Each configuration becomes unique to the owner — a visual snapshot of taste, memory, and sound."
)

_ALLEGRA_DESCRIPTION = (
    "Allegra is a vinyl record display defined by lightness and pace.\n\n"
    "Where other pieces in the collection emphasise balance, cohesion, or repetition, Allegra introduces a sense of movement. Records are arranged in a subtle forward rhythm, creating a gentle progression that feels active without being loud. The display carries energy, but remains composed.\n\n"
    "The form is deliberately minimal. Clear acrylic supports the records with precision, allowing each sleeve to appear suspended, evenly spaced, and unobstructed. The structure recedes almost entirely, leaving the collection to define the visual experience. From different angles, the arrangement shifts slightly, giving the display a quiet dynamism.\n\n"
    "Allegra is well suited to collectors who enjoy interaction — those who frequently move between records, rotate selections, and engage physically with their collection. It keeps albums accessible and visible, encouraging use rather than storage.\n\n"
    "Like the rest of the Liquorice Locker system, Allegra is modular. Additional units can be placed alongside one another as a collection expands, extending the display while maintaining its clarity and proportion. Growth feels fluid rather than fixed, adaptable rather than imposed.\n\n"
    "Allegra is for those who value momentum and ease. It turns a vinyl collection into an active presence in the room — one that reflects the pace and flow of listening itself."
)

_HARMONY_DESCRIPTION = (
    "Harmony is a vinyl record display built around cohesion and balance.\n\n"
    "Rather than focusing on scale or rotation, Harmony brings records together into a single, composed arrangement. Albums are held upright and closely aligned, forming a unified presence that feels settled and complete. The display reads as one object first, a collection second.\n\n"
    "The structure is deliberately restrained. Clear acrylic supports the records with minimal visual interruption, allowing cover artwork and edges to align in a calm, ordered sequence. The result is a display that feels architectural — measured, grounded, and quietly confident.\n\n"
    "Harmony works especially well in shared living spaces, where records are part of the room rather than the centre of it. It offers a way to keep a meaningful selection visible without introducing visual noise. The collection feels integrated into the space, not placed on top of it.\n\n"
    "Designed to grow gradually, Harmony can be extended by placing additional units alongside one another. As collections expand, the display maintains its clarity and proportion, preserving the sense of balance that defines the piece.\n\n"
    "Harmony is for collectors who value consistency and composure. It turns a group of records into a resolved arrangement — one that brings order to variety and calm to the ritual of listening."
)

_MELODY_DESCRIPTION = (
    "Melody is a vinyl record display defined by balance and repetition.\n\n"
    "Designed to hold a modest selection of records, Melody presents albums in a tight, upright formation — calm, ordered, and quietly expressive. It's a piece that doesn't demand attention, but rewards it, revealing detail and intention the longer it's lived with.\n\n"
    "The design focuses on rhythm rather than scale. Each record is evenly spaced, creating a consistent visual cadence that mirrors the idea of a musical melody — simple, memorable, and sustained. Album artwork remains visible, while the clear acrylic structure recedes almost entirely, allowing the collection itself to become the object.\n\n"
    "Melody is well suited to listening spaces where records are in regular rotation. It keeps a select group of albums close at hand, encouraging interaction rather than accumulation. The display feels deliberate and resolved, offering a sense of completion even with a small number of records.\n\n"
    "Like the rest of the Liquorice Locker system, Melody is designed to adapt. Multiple units can be placed together to extend the display over time, allowing the composition to grow without losing its clarity or proportion. Each addition feels intentional — a continuation rather than an interruption.\n\n"
    "Melody is for collectors who appreciate structure, repetition, and restraint. It turns a small collection into a considered arrangement, where music and space move together."
)

_RIFF_DESCRIPTION = (
    "Riff is a compact vinyl record display designed for smaller, carefully curated collections.\n\n"
    "Where Sound Wave expresses music through scale and rhythm, Riff focuses on selection. It holds a tight edit of records — albums chosen deliberately, returned to often, and kept close at hand. The result is a display that feels personal rather than expansive, quiet rather than performative.\n\n"
    "Records are held upright and evenly spaced, allowing cover artwork to remain visible while maintaining a sense of order and clarity. The clear acrylic structure fades into the background, creating the impression that the records themselves are floating in space. Nothing competes for attention; the music leads.\n\n"
    "Riff is designed to live comfortably beside a turntable, listening chair, or shelf — a small footprint with a strong presence. It's ideal for collectors who rotate their records frequently or prefer to keep a focused selection on display.\n\n"
    "As a collection grows, Riff grows with it. Each unit is designed to sit seamlessly beside another, allowing multiple displays to be placed side by side. Over time, individual pieces can be added to form a longer, continuous arrangement — evolving naturally with the collection rather than demanding a single, fixed solution.\n\n"
    "Riff is not about quantity. It's about curation, adaptability, and living with music in a way that feels intentional and considered."
)


# Static paths under /static; role order: banner, thumbnail, gallery…
_PRODUCT_IMAGE_SEEDS: Dict[str, Tuple[Tuple[str, int, str], ...]] = {
    "sound-wave": (
        ("sound-wave-feature-01.png", 0, "banner"),
        ("sound-wave-feature-02.png", 1, "gallery"),
        ("sound-wave-feature-03.png", 2, "gallery"),
        ("sound-wave-feature-04.png", 3, "gallery"),
        ("sound-wave-feature-05.png", 4, "gallery"),
        ("sound-wave-feature-06.png", 5, "gallery"),
        ("sound-wave-feature-07.png", 6, "gallery"),
        ("sound-wave-feature-08.png", 7, "gallery"),
        ("sound-wave-feature-09.png", 8, "gallery"),
        ("sound-wave-feature-10.png", 9, "gallery"),
        ("sound-wave-feature-11.png", 10, "gallery"),
        ("sound-wave-feature-12.png", 11, "gallery"),
        ("sound-wave-feature-13.png", 12, "gallery"),
        ("sound-wave-feature-14.png", 13, "gallery"),
    ),
    "riff": (
        ("riff-feature-01.png", 0, "banner"),
        ("riff-feature-02.png", 1, "gallery"),
        ("riff-feature-03.png", 2, "gallery"),
        ("riff-feature-04.png", 3, "gallery"),
        ("riff-feature-05.png", 4, "gallery"),
        ("riff-feature-06.png", 5, "gallery"),
        ("riff-feature-07.png", 6, "gallery"),
    ),
    "harmony": (
        ("harmony-feature-01.png", 0, "banner"),
        ("harmony-feature-02.png", 1, "gallery"),
        ("harmony-feature-03.png", 2, "gallery"),
        ("harmony-feature-04.png", 3, "gallery"),
        ("harmony-feature-05.png", 4, "gallery"),
        ("harmony-feature-06.png", 5, "gallery"),
    ),
    "melody": (
        ("melody-feature-01.png", 0, "banner"),
        ("melody-feature-02.png", 1, "gallery"),
        ("melody-feature-03.png", 2, "gallery"),
        ("melody-feature-04.png", 3, "gallery"),
        ("melody-feature-05.png", 4, "gallery"),
        ("melody-feature-06.png", 5, "gallery"),
        ("melody-feature-07.png", 6, "gallery"),
        ("melody-feature-08.png", 7, "gallery"),
        ("melody-feature-09.png", 8, "gallery"),
        ("melody-feature-10.png", 9, "gallery"),
        ("melody-feature-11.png", 10, "gallery"),
        ("melody-feature-12.png", 11, "gallery"),
    ),
    "allegro": (
        ("allegra-feature-01.png", 0, "banner"),
        ("allegra-feature-02.png", 1, "gallery"),
        ("allegra-feature-03.png", 2, "gallery"),
        ("allegra-feature-04.png", 3, "gallery"),
        ("allegra-feature-05.png", 4, "gallery"),
        ("allegra-feature-06.png", 5, "gallery"),
        ("allegra-feature-07.png", 6, "gallery"),
        ("allegra-feature-08.png", 7, "gallery"),
        ("allegra-feature-09.png", 8, "gallery"),
        ("allegra-feature-10.png", 9, "gallery"),
        ("allegra-feature-11.png", 10, "gallery"),
    ),
}


def _core_catalog_product_rows() -> List[Tuple[str, str, int, str, int, int]]:
    """Single source of truth for storefront SKUs (matches seed_if_empty)."""
    return [
        ("sound-wave", "Sound Wave", 42900, _SOUND_WAVE_DESCRIPTION.split("\n\n")[0], 0, 1),
        ("allegro", "Allegra", 14300, _ALLEGRA_DESCRIPTION.split("\n\n")[0], 1, 0),
        ("melody", "Melody", 14300, _MELODY_DESCRIPTION.split("\n\n")[0], 2, 0),
        ("harmony", "Harmony", 14300, _HARMONY_DESCRIPTION.split("\n\n")[0], 3, 0),
        ("riff", "Riff", 14300, _RIFF_DESCRIPTION.split("\n\n")[0], 4, 0),
    ]


def _ensure_core_products(db: sqlite3.Connection) -> None:
    """Insert core products if missing (e.g. gunicorn on Railway never runs seed_if_empty). Idempotent."""
    for slug, name, cents, desc, sort_order, is_main in _core_catalog_product_rows():
        db.execute(
            """
            INSERT OR IGNORE INTO products (slug, name, price_cents, description, sort_order, is_main)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (slug, name, cents, desc, sort_order, is_main),
        )


def _backfill_product_enhanced(db: sqlite3.Connection) -> None:
    """SKU, cm dimensions, capacity count, featured, collection; unique index on sku."""
    db.execute(
        """
        UPDATE products SET featured = 1
        WHERE is_main = 1 AND (featured IS NULL OR featured = 0)
        """
    )
    db.execute(
        "UPDATE products SET created_at = datetime('now') WHERE created_at IS NULL OR trim(created_at) = ''"
    )
    rows = db.execute("SELECT id, slug, sku, width_cm FROM products").fetchall()
    for r in rows:
        pid = int(r["id"])
        slug = str(r["slug"])
        sku = (r["sku"] or "").strip()
        if not sku:
            sku = f"LL-{slug.upper()}"
            db.execute("UPDATE products SET sku = ? WHERE id = ?", (sku, pid))

        if r["width_cm"] is None and slug in _PRODUCT_DIMS_MM:
            w_mm, h_mm, d_mm = _PRODUCT_DIMS_MM[slug]
            db.execute(
                "UPDATE products SET width_cm = ?, height_cm = ?, depth_cm = ? WHERE id = ?",
                (w_mm / 10.0, h_mm / 10.0, d_mm / 10.0, pid),
            )

        cap = db.execute("SELECT capacity_records FROM products WHERE id = ?", (pid,)).fetchone()
        if cap and cap["capacity_records"] is None and slug in _PRODUCT_CAPACITY:
            db.execute(
                "UPDATE products SET capacity_records = ? WHERE id = ?",
                (_PRODUCT_CAPACITY[slug], pid),
            )

        coll = db.execute("SELECT collection FROM products WHERE id = ?", (pid,)).fetchone()
        if coll and (coll["collection"] or "").strip() == "" and slug in _PRODUCT_COLLECTION:
            db.execute(
                "UPDATE products SET collection = ? WHERE id = ?",
                (_PRODUCT_COLLECTION[slug], pid),
            )

    db.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_products_sku ON products(sku)
        WHERE sku IS NOT NULL AND LENGTH(TRIM(sku)) > 0
        """
    )


def _ensure_product_images_tags(db: sqlite3.Connection) -> None:
    """Idempotent seeds for gallery images and tags (linked by product_id)."""
    for slug, paths in _PRODUCT_IMAGE_SEEDS.items():
        row = db.execute("SELECT id FROM products WHERE slug = ?", (slug,)).fetchone()
        if not row:
            continue
        pid = int(row["id"])
        for path, sort_order, role in paths:
            exists = db.execute(
                "SELECT 1 FROM product_images WHERE product_id = ? AND path = ? LIMIT 1",
                (pid, path),
            ).fetchone()
            if not exists:
                db.execute(
                    """
                    INSERT INTO product_images (product_id, path, sort_order, role)
                    VALUES (?, ?, ?, ?)
                    """,
                    (pid, path, sort_order, role),
                )
        if slug in _PRODUCT_TAGS:
            for tag in _PRODUCT_TAGS[slug]:
                db.execute(
                    "INSERT OR IGNORE INTO product_tags (product_id, tag) VALUES (?, ?)",
                    (pid, tag),
                )


def _backfill_sound_wave_marketing(db: sqlite3.Connection) -> None:
    """Sound Wave product copy, specs, and gallery images (idempotent; keeps storefront in sync)."""
    row = db.execute("SELECT id FROM products WHERE slug = 'sound-wave'").fetchone()
    if not row:
        return
    pid = int(row["id"])
    db.execute("DELETE FROM product_tags WHERE product_id = ?", (pid,))
    db.execute(
        "UPDATE products SET description = ? WHERE slug = 'sound-wave'",
        (_SOUND_WAVE_DESCRIPTION,),
    )
    db.execute(
        """
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?
        WHERE slug = 'sound-wave'
        """,
        (
            "312 × 198 × 94 mm (W × D × H)",
            "Clear acrylic structure — minimal, precision-cut supports.",
            "Holds up to 40 LPs on display; cover artwork stays visible in sequence.",
        ),
    )
    db.execute("DELETE FROM product_images WHERE product_id = ?", (pid,))
    for path, sort_order, role in _PRODUCT_IMAGE_SEEDS["sound-wave"]:
        db.execute(
            """
            INSERT INTO product_images (product_id, path, sort_order, role)
            VALUES (?, ?, ?, ?)
            """,
            (pid, path, sort_order, role),
        )


def _backfill_allegra_marketing(db: sqlite3.Connection) -> None:
    """Allegra (slug allegro): name, copy, specs, gallery; slug stays allegro for URLs and cart."""
    row = db.execute("SELECT id FROM products WHERE slug = 'allegro'").fetchone()
    if not row:
        return
    pid = int(row["id"])
    db.execute("DELETE FROM product_tags WHERE product_id = ?", (pid,))
    db.execute(
        """
        UPDATE products SET name = 'Allegra', description = ? WHERE slug = 'allegro'
        """,
        (_ALLEGRA_DESCRIPTION,),
    )
    db.execute(
        """
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?
        WHERE slug = 'allegro'
        """,
        (
            "210 × 120 × 38 mm (W × D × H)",
            "Clear acrylic — minimal supports; precision spacing.",
            "Holds up to 12 LPs on display; albums stay accessible and visible.",
        ),
    )
    db.execute("DELETE FROM product_images WHERE product_id = ?", (pid,))
    for path, sort_order, role in _PRODUCT_IMAGE_SEEDS["allegro"]:
        db.execute(
            """
            INSERT INTO product_images (product_id, path, sort_order, role)
            VALUES (?, ?, ?, ?)
            """,
            (pid, path, sort_order, role),
        )


def _backfill_harmony_marketing(db: sqlite3.Connection) -> None:
    """Harmony product copy, specs, gallery images (idempotent)."""
    row = db.execute("SELECT id FROM products WHERE slug = 'harmony'").fetchone()
    if not row:
        return
    pid = int(row["id"])
    db.execute("DELETE FROM product_tags WHERE product_id = ?", (pid,))
    db.execute(
        "UPDATE products SET description = ? WHERE slug = 'harmony'",
        (_HARMONY_DESCRIPTION,),
    )
    db.execute(
        """
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?
        WHERE slug = 'harmony'
        """,
        (
            "280 × 200 × 64 mm (W × D × H)",
            "Clear acrylic — minimal supports; aligned, cohesive rows.",
            "Holds up to 32 LPs on display; unified presence, calm order.",
        ),
    )
    db.execute("DELETE FROM product_images WHERE product_id = ?", (pid,))
    for path, sort_order, role in _PRODUCT_IMAGE_SEEDS["harmony"]:
        db.execute(
            """
            INSERT INTO product_images (product_id, path, sort_order, role)
            VALUES (?, ?, ?, ?)
            """,
            (pid, path, sort_order, role),
        )


def _backfill_melody_marketing(db: sqlite3.Connection) -> None:
    """Melody product copy, specs, gallery images (idempotent)."""
    row = db.execute("SELECT id FROM products WHERE slug = 'melody'").fetchone()
    if not row:
        return
    pid = int(row["id"])
    db.execute("DELETE FROM product_tags WHERE product_id = ?", (pid,))
    db.execute(
        "UPDATE products SET description = ?, collection = ? WHERE slug = 'melody'",
        (_MELODY_DESCRIPTION, "Clear Collection"),
    )
    db.execute(
        """
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?
        WHERE slug = 'melody'
        """,
        (
            "240 × 160 × 52 mm (W × D × H)",
            "Clear acrylic — minimal supports; even, rhythmic spacing.",
            "Holds up to 24 LPs on display; modest selection, deliberate arrangement.",
        ),
    )
    db.execute("DELETE FROM product_images WHERE product_id = ?", (pid,))
    for path, sort_order, role in _PRODUCT_IMAGE_SEEDS["melody"]:
        db.execute(
            """
            INSERT INTO product_images (product_id, path, sort_order, role)
            VALUES (?, ?, ?, ?)
            """,
            (pid, path, sort_order, role),
        )


def _backfill_riff_marketing(db: sqlite3.Connection) -> None:
    """Riff product copy, specs, gallery images (idempotent)."""
    row = db.execute("SELECT id FROM products WHERE slug = 'riff'").fetchone()
    if not row:
        return
    pid = int(row["id"])
    db.execute("DELETE FROM product_tags WHERE product_id = ?", (pid,))
    db.execute(
        "UPDATE products SET description = ?, collection = ? WHERE slug = 'riff'",
        (_RIFF_DESCRIPTION, "Clear Collection"),
    )
    db.execute(
        """
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?
        WHERE slug = 'riff'
        """,
        (
            "190 × 110 × 42 mm (W × D × H)",
            "Clear acrylic — compact footprint; records appear to float.",
            "Holds up to 8 LPs on display; curated selection, side-by-side modular growth.",
        ),
    )
    db.execute("DELETE FROM product_images WHERE product_id = ?", (pid,))
    for path, sort_order, role in _PRODUCT_IMAGE_SEEDS["riff"]:
        db.execute(
            """
            INSERT INTO product_images (product_id, path, sort_order, role)
            VALUES (?, ?, ?, ?)
            """,
            (pid, path, sort_order, role),
        )


def _ensure_product_specs(db: sqlite3.Connection) -> None:
    """Copy for product detail pages (idempotent)."""
    specs: Dict[str, Tuple[str, str, str]] = {
        "sound-wave": (
            "312 × 198 × 94 mm (W × D × H)",
            "Clear acrylic structure — minimal, precision-cut supports.",
            "Holds up to 40 LPs on display; cover artwork stays visible in sequence.",
        ),
        "allegro": (
            "210 × 120 × 38 mm (W × D × H)",
            "Clear acrylic — minimal supports; precision spacing.",
            "Holds up to 12 LPs on display; albums stay accessible and visible.",
        ),
        "melody": (
            "240 × 160 × 52 mm (W × D × H)",
            "Clear acrylic — minimal supports; even, rhythmic spacing.",
            "Holds up to 24 LPs on display; modest selection, deliberate arrangement.",
        ),
        "harmony": (
            "280 × 200 × 64 mm (W × D × H)",
            "Clear acrylic — minimal supports; aligned, cohesive rows.",
            "Holds up to 32 LPs on display; unified presence, calm order.",
        ),
        "riff": (
            "190 × 110 × 42 mm (W × D × H)",
            "Clear acrylic — compact footprint; records appear to float.",
            "Holds up to 8 LPs on display; curated selection, side-by-side modular growth.",
        ),
    }
    for slug, (dim, mat, cap) in specs.items():
        db.execute(
            """
            UPDATE products SET dimensions = ?, materials = ?, capacity = ?
            WHERE slug = ? AND (dimensions = '' OR dimensions IS NULL)
            """,
            (dim, mat, cap, slug),
        )


def seed_if_empty() -> None:
    with get_db() as db:
        n = db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        if n > 0:
            return

        _method = "pbkdf2:sha256"
        admin_hash = generate_password_hash(os.environ.get("ADMIN_PASSWORD", "admin123"), method=_method)
        aff_hash = generate_password_hash(os.environ.get("AFFILIATE_PASSWORD", "affiliate123"), method=_method)

        db.execute(
            """
            INSERT INTO users (email, password_hash, role, affiliate_slug, full_name)
            VALUES (?, ?, 'admin', NULL, 'Site Admin')
            """,
            ("admin@licoricelocker.local", admin_hash),
        )
        db.execute(
            """
            INSERT INTO users (email, password_hash, role, affiliate_slug, full_name)
            VALUES (?, ?, 'affiliate', 'sound-partner', 'Jordan Keys')
            """,
            ("partner@licoricelocker.local", aff_hash),
        )
        aff_id = db.execute("SELECT id FROM users WHERE email = ?", ("partner@licoricelocker.local",)).fetchone()[
            "id"
        ]
        db.execute(
            """
            INSERT INTO affiliate_pages (user_id, headline, tagline, description, monthly_sales_target)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                aff_id,
                "Sound that travels with you",
                "Licorice Locker — curated audio gear",
                "I only share gear I use. Every purchase supports independent sound design.",
                25,
            ),
        )

        _ensure_core_products(db)
        _ensure_product_specs(db)
        _backfill_product_enhanced(db)
        _ensure_product_images_tags(db)
        _backfill_sound_wave_marketing(db)
        _backfill_allegra_marketing(db)
        _backfill_harmony_marketing(db)
        _backfill_melody_marketing(db)
        _backfill_riff_marketing(db)
        _migrate_order_columns(db)
        _backfill_order_columns(db)
        _migrate_order_affiliate_commission(db)
        _migrate_affiliate_profile_columns(db)
        _backfill_affiliate_profile(db)
        _migrate_affiliate_deletion_log(db)


def user_by_id(db: sqlite3.Connection, uid: int) -> Optional[sqlite3.Row]:
    return db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()


def list_creative_assets(db: sqlite3.Connection) -> List[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM creative_assets ORDER BY datetime(created_at) DESC, id DESC"
    ).fetchall()


def creative_asset_by_id(db: sqlite3.Connection, asset_id: int) -> Optional[sqlite3.Row]:
    return db.execute("SELECT * FROM creative_assets WHERE id = ?", (asset_id,)).fetchone()


def insert_creative_asset(
    db: sqlite3.Connection,
    title: str,
    file_path: str,
    thumbnail_path: Optional[str],
    asset_type: str,
    tags: str,
) -> int:
    cur = db.execute(
        """
        INSERT INTO creative_assets (title, file_path, thumbnail_path, asset_type, tags)
        VALUES (?, ?, ?, ?, ?)
        """,
        (title, file_path, thumbnail_path, asset_type, tags),
    )
    return int(cur.lastrowid)


def delete_creative_asset(db: sqlite3.Connection, asset_id: int) -> Optional[sqlite3.Row]:
    row = creative_asset_by_id(db, asset_id)
    if not row:
        return None
    db.execute("DELETE FROM creative_assets WHERE id = ?", (asset_id,))
    return row


def user_by_email(db: sqlite3.Connection, email: str) -> Optional[sqlite3.Row]:
    norm = normalize_email(email)
    if not norm:
        return None
    row = db.execute("SELECT * FROM users WHERE email = ?", (norm,)).fetchone()
    if row:
        return row
    # Legacy: mixed-case / whitespace-only differences
    row = db.execute(
        "SELECT * FROM users WHERE lower(trim(email)) = ?",
        (norm.lower(),),
    ).fetchone()
    if row:
        return row
    # Legacy: rows stored before Gmail/googlemail or dot/plus canonicalization — compare
    # canonical forms without loading unbounded columns (users table stays small).
    for r in db.execute("SELECT * FROM users").fetchall():
        if normalize_email(r["email"] or "") == norm:
            return r
    return None


def allocate_unique_affiliate_code_and_slug(
    conn: sqlite3.Connection, first_name: str, last_name: str
) -> Tuple[str, str]:
    """Return a unique (affiliate_code, affiliate_slug) pair; both use the same string for new signups."""
    raw = (first_name.strip() + last_name.strip()).lower()
    raw = unicodedata.normalize("NFKD", raw)
    raw = "".join(c for c in raw if not unicodedata.combining(c))
    base = re.sub(r"[^a-z0-9]", "", raw)
    if not base:
        base = "affiliate"
    base = base[:32]
    n = 0
    while True:
        candidate = base if n == 0 else f"{base}{n}"
        candidate = candidate[:40]
        clash = conn.execute(
            """
            SELECT 1 FROM users
            WHERE affiliate_slug = ?
               OR (affiliate_code IS NOT NULL AND TRIM(affiliate_code) != '' AND affiliate_code = ?)
            LIMIT 1
            """,
            (candidate, candidate),
        ).fetchone()
        if not clash:
            return candidate, candidate
        n += 1


def create_affiliate_signup(
    conn: sqlite3.Connection,
    email: str,
    password_hash: str,
    first_name: str,
    last_name: str,
) -> int:
    """Insert affiliate user + default affiliate_pages row. Retries on slug/code race. Returns new user id."""
    email_norm = normalize_email(email)
    full_name = f"{first_name.strip()} {last_name.strip()}".strip()
    fn = first_name.strip()
    ln = last_name.strip()
    for _ in range(24):
        code, slug = allocate_unique_affiliate_code_and_slug(conn, fn, ln)
        try:
            cur = conn.execute(
                """
                INSERT INTO users (
                    email, password_hash, role, affiliate_slug, full_name,
                    affiliate_code, affiliate_active, terms_accepted_at, terms_accepted,
                    signup_first_name, signup_last_name, totp_secret, totp_confirmed
                ) VALUES (?, ?, 'affiliate', ?, ?, ?, 1, NULL, 0, ?, ?, NULL, 0)
                """,
                (email_norm, password_hash, slug, full_name, code, fn, ln),
            )
            uid = int(cur.lastrowid)
            conn.execute(
                """
                INSERT INTO affiliate_pages (user_id, headline, tagline, description, monthly_sales_target)
                VALUES (?, 'Welcome', '', '', 16)
                """,
                (uid,),
            )
            return uid
        except sqlite3.IntegrityError as e:
            err = str(e)
            if "users.email" in err:
                raise
            continue
    raise RuntimeError("Could not allocate a unique affiliate code")


def set_user_totp_secret(db: sqlite3.Connection, user_id: int, secret: str) -> None:
    db.execute(
        "UPDATE users SET totp_secret = ?, totp_confirmed = 0 WHERE id = ?",
        (secret, user_id),
    )


def confirm_user_totp(db: sqlite3.Connection, user_id: int) -> None:
    db.execute("UPDATE users SET totp_confirmed = 1 WHERE id = ?", (user_id,))


def set_password_reset(db: sqlite3.Connection, user_id: int, token: str, expires_iso: str) -> None:
    db.execute(
        "UPDATE users SET password_reset_token = ?, password_reset_expires = ? WHERE id = ?",
        (token, expires_iso, user_id),
    )


def clear_password_reset(db: sqlite3.Connection, user_id: int) -> None:
    db.execute(
        "UPDATE users SET password_reset_token = NULL, password_reset_expires = NULL WHERE id = ?",
        (user_id,),
    )


def user_by_affiliate_reset_token(db: sqlite3.Connection, token: str) -> Optional[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM users WHERE password_reset_token = ? AND role = 'affiliate'",
        (token,),
    ).fetchone()


def set_user_password_hash(db: sqlite3.Connection, user_id: int, password_hash: str) -> None:
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (password_hash, user_id))


def clear_user_totp(db: sqlite3.Connection, user_id: int) -> None:
    db.execute("UPDATE users SET totp_secret = NULL, totp_confirmed = 0 WHERE id = ?", (user_id,))


def affiliate_visit_count(db: sqlite3.Connection, affiliate_user_id: int) -> int:
    row = db.execute(
        "SELECT COUNT(*) AS c FROM affiliate_visits WHERE affiliate_user_id = ?",
        (affiliate_user_id,),
    ).fetchone()
    return int(row["c"]) if row else 0


def affiliate_has_pending_commission_payout(db: sqlite3.Connection, affiliate_user_id: int) -> bool:
    row = db.execute(
        """
        SELECT 1 FROM commissions
        WHERE affiliate_user_id = ?
          AND LOWER(TRIM(payout_status)) = 'pending'
          AND (commission_cents + IFNULL(bonus_cents, 0)) > 0
        LIMIT 1
        """,
        (affiliate_user_id,),
    ).fetchone()
    return row is not None


def delete_affiliate_user_account(db: sqlite3.Connection, user_id: int, email_for_log: str) -> bool:
    """Log deletion, detach orders, delete affiliate user (CASCADE removes page, visits, commissions)."""
    row = db.execute("SELECT id, role FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row or row["role"] != "affiliate":
        return False
    normalized_email = (email_for_log or "").strip().lower()
    email_hash = (
        hashlib.sha256(normalized_email.encode("utf-8")).hexdigest() if normalized_email else ""
    )
    db.execute(
        "INSERT INTO affiliate_account_deletions (former_user_id, email_hash) VALUES (?, ?)",
        (user_id, email_hash),
    )
    db.execute("UPDATE orders SET affiliate_user_id = NULL WHERE affiliate_user_id = ?", (user_id,))
    cur = db.execute("DELETE FROM users WHERE id = ? AND role = 'affiliate'", (user_id,))
    return cur.rowcount > 0


def affiliate_by_slug(db: sqlite3.Connection, slug: str) -> Optional[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM users WHERE affiliate_slug = ? AND role = 'affiliate'",
        (slug.strip(),),
    ).fetchone()


def affiliate_by_code(db: sqlite3.Connection, code: str) -> Optional[sqlite3.Row]:
    """Match affiliate row by affiliate_code (case-insensitive). Used for /listening-room/<code>."""
    c = (code or "").strip()
    if not c:
        return None
    return db.execute(
        """
        SELECT * FROM users
        WHERE role = 'affiliate'
          AND affiliate_code IS NOT NULL
          AND TRIM(affiliate_code) != ''
          AND LOWER(TRIM(affiliate_code)) = LOWER(?)
        LIMIT 1
        """,
        (c,),
    ).fetchone()


def affiliate_page(db: sqlite3.Connection, user_id: int) -> Optional[sqlite3.Row]:
    return db.execute("SELECT * FROM affiliate_pages WHERE user_id = ?", (user_id,)).fetchone()


def affiliate_lifetime_order_count(db: sqlite3.Connection, affiliate_user_id: int) -> int:
    row = db.execute(
        """
        SELECT COUNT(*) AS c FROM orders
        WHERE affiliate_user_id = ? AND order_type = 'affiliate'
          AND status IN ('completed', 'shipped')
        """,
        (affiliate_user_id,),
    ).fetchone()
    return int(row["c"]) if row else 0


def affiliate_lifetime_earnings_cents(db: sqlite3.Connection, affiliate_user_id: int) -> int:
    """Sum of recorded order commissions plus paid-in-snapshot milestone bonuses (all time)."""
    order_row = db.execute(
        """
        SELECT COALESCE(SUM(affiliate_commission_cents), 0) AS s FROM orders
        WHERE affiliate_user_id = ? AND order_type = 'affiliate'
          AND status IN ('completed', 'shipped')
        """,
        (affiliate_user_id,),
    ).fetchone()
    bonus_row = db.execute(
        """
        SELECT COALESCE(SUM(bonus_cents), 0) AS s FROM commissions
        WHERE affiliate_user_id = ?
        """,
        (affiliate_user_id,),
    ).fetchone()
    orders_part = int(order_row["s"] or 0) if order_row else 0
    bonus_part = int(bonus_row["s"] or 0) if bonus_row else 0
    return orders_part + bonus_part


def analytics_session_touch(db: sqlite3.Connection, session_id: str, now_iso: str) -> None:
    db.execute(
        "UPDATE analytics_sessions SET last_active_at = ? WHERE session_id = ?",
        (now_iso, session_id),
    )


def analytics_session_exists(db: sqlite3.Connection, session_id: str) -> bool:
    row = db.execute(
        "SELECT 1 AS o FROM analytics_sessions WHERE session_id = ? LIMIT 1",
        (session_id,),
    ).fetchone()
    return row is not None


def analytics_create_session(
    db: sqlite3.Connection,
    session_id: str,
    country: str,
    city: str,
    ip_hash: str,
    device_class: str,
    user_agent: str,
    affiliate_code: Optional[str],
    now_iso: str,
) -> None:
    aff = (affiliate_code or "").strip() or None
    db.execute(
        """
        INSERT INTO analytics_sessions (
            session_id, country, city, ip_hash, device_class, user_agent, affiliate_code,
            started_at, last_active_at, converted, dropped_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL)
        """,
        (
            session_id,
            country[:16],
            (city or "")[:128],
            (ip_hash or "")[:32],
            device_class[:32],
            user_agent[:512],
            aff,
            now_iso,
            now_iso,
        ),
    )


def analytics_insert_event(
    db: sqlite3.Connection,
    session_id: str,
    event: str,
    page: str,
    meta_json: str,
    now_iso: str,
) -> None:
    db.execute(
        """
        INSERT INTO analytics_events (session_id, event, page, meta_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (session_id, event[:128], page[:512], meta_json[:8192], now_iso),
    )
    analytics_session_touch(db, session_id, now_iso)


def analytics_mark_converted(db: sqlite3.Connection, session_id: str, now_iso: str) -> None:
    db.execute(
        """
        UPDATE analytics_sessions
        SET converted = 1, dropped_at = NULL, last_active_at = ?
        WHERE session_id = ?
        """,
        (now_iso, session_id),
    )


def analytics_refresh_dropoffs(db: sqlite3.Connection, cutoff_iso: str) -> int:
    """Sessions inactive since cutoff, not converted, no drop yet → dropped_at = last event name."""
    rows = db.execute(
        """
        SELECT session_id FROM analytics_sessions
        WHERE converted = 0 AND dropped_at IS NULL AND last_active_at < ?
        """,
        (cutoff_iso,),
    ).fetchall()
    n = 0
    for r in rows:
        sid = str(r["session_id"])
        ev = db.execute(
            """
            SELECT event FROM analytics_events WHERE session_id = ?
            ORDER BY created_at DESC, id DESC LIMIT 1
            """,
            (sid,),
        ).fetchone()
        drop_at = str(ev["event"]) if ev else "unknown"
        db.execute(
            "UPDATE analytics_sessions SET dropped_at = ? WHERE session_id = ?",
            (drop_at[:128], sid),
        )
        n += 1
    return n


def analytics_summary_devices(
    db: sqlite3.Connection, since_iso: Optional[str] = None
) -> Dict[str, int]:
    q = "SELECT device_class, COUNT(*) AS c FROM analytics_sessions WHERE 1=1"
    params: List[Any] = []
    if since_iso:
        q += " AND started_at >= ?"
        params.append(since_iso)
    q += " GROUP BY device_class ORDER BY c DESC"
    rows = db.execute(q, tuple(params)).fetchall()
    return {str(r["device_class"] or "—"): int(r["c"]) for r in rows}


def analytics_summary_by_country(
    db: sqlite3.Connection, since_iso: Optional[str] = None
) -> Dict[str, int]:
    q = "SELECT country, COUNT(*) AS c FROM analytics_sessions WHERE 1=1"
    params: List[Any] = []
    if since_iso:
        q += " AND started_at >= ?"
        params.append(since_iso)
    q += " GROUP BY country ORDER BY c DESC"
    rows = db.execute(q, tuple(params)).fetchall()
    out: Dict[str, int] = {}
    for r in rows:
        k = (r["country"] or "").strip() or "—"
        out[k] = int(r["c"])
    return out


def analytics_summary_top_cities(
    db: sqlite3.Connection, since_iso: Optional[str] = None, limit: int = 12
) -> Dict[str, int]:
    q = """
        SELECT city, COUNT(*) AS c FROM analytics_sessions
        WHERE TRIM(COALESCE(city, '')) != ''
    """
    params: List[Any] = []
    if since_iso:
        q += " AND started_at >= ?"
        params.append(since_iso)
    q += " GROUP BY city ORDER BY c DESC LIMIT ?"
    params.append(limit)
    rows = db.execute(q, tuple(params)).fetchall()
    return {str(r["city"]): int(r["c"]) for r in rows}


def analytics_summary_dropoffs(
    db: sqlite3.Connection, since_iso: Optional[str] = None
) -> Dict[str, int]:
    q = """
        SELECT dropped_at, COUNT(*) AS c FROM analytics_sessions
        WHERE dropped_at IS NOT NULL AND dropped_at != ''
    """
    params: List[Any] = []
    if since_iso:
        q += " AND started_at >= ?"
        params.append(since_iso)
    q += " GROUP BY dropped_at ORDER BY c DESC"
    rows = db.execute(q, tuple(params)).fetchall()
    return {str(r["dropped_at"]): int(r["c"]) for r in rows}


def analytics_conversion_stats(
    db: sqlite3.Connection, since_iso: Optional[str] = None
) -> Tuple[int, int, float]:
    q = "SELECT COUNT(*) AS c FROM analytics_sessions WHERE 1=1"
    params: List[Any] = []
    if since_iso:
        q += " AND started_at >= ?"
        params.append(since_iso)
    total = int(db.execute(q, tuple(params)).fetchone()["c"])
    q2 = q + " AND converted = 1"
    converted = int(db.execute(q2, tuple(params)).fetchone()["c"])
    rate = (converted / total * 100.0) if total else 0.0
    return total, converted, rate


def orders_geo_summary_by_country(
    db: sqlite3.Connection, since_iso: Optional[str] = None
) -> Dict[str, Dict[str, int]]:
    """
    Orders grouped by buyer geo (IP at checkout), falling back to shipping country.
    Returns { country_code: {"orders": n, "revenue_cents": x} }.
    """
    q = """
        SELECT
            UPPER(TRIM(COALESCE(
                NULLIF(TRIM(geo_country), ''),
                NULLIF(TRIM(shipping_country), ''),
                '—'
            ))) AS cc,
            COUNT(*) AS c,
            COALESCE(SUM(total_cents), 0) AS rev
        FROM orders
        WHERE status IN ('completed', 'shipped')
    """
    params: List[Any] = []
    if since_iso:
        q += " AND created_at >= ?"
        params.append(since_iso)
    q += " GROUP BY cc ORDER BY c DESC"
    rows = db.execute(q, tuple(params)).fetchall()
    out: Dict[str, Dict[str, int]] = {}
    for r in rows:
        cc = str(r["cc"] or "—")
        out[cc] = {"orders": int(r["c"]), "revenue_cents": int(r["rev"] or 0)}
    return out


def list_products(
    db: sqlite3.Connection,
    collection: Optional[str] = None,
    featured_only: bool = False,
) -> List[sqlite3.Row]:
    q = "SELECT * FROM products WHERE 1=1"
    params: List[Any] = []
    if collection is not None:
        q += " AND collection = ?"
        params.append(collection)
    if featured_only:
        q += " AND featured = 1"
    q += " ORDER BY sort_order, id"
    return db.execute(q, tuple(params)).fetchall()


def product_add_to_cart_enabled(row: sqlite3.Row) -> bool:
    if "add_to_cart_enabled" not in row.keys():
        return True
    v = row["add_to_cart_enabled"]
    if v is None:
        return True
    return int(v) == 1


def list_product_images(db: sqlite3.Connection, product_id: int) -> List[sqlite3.Row]:
    return db.execute(
        "SELECT * FROM product_images WHERE product_id = ? ORDER BY sort_order, id",
        (product_id,),
    ).fetchall()


def list_product_tags(db: sqlite3.Connection, product_id: int) -> List[str]:
    rows = db.execute(
        "SELECT tag FROM product_tags WHERE product_id = ? ORDER BY tag COLLATE NOCASE",
        (product_id,),
    ).fetchall()
    return [str(r["tag"]) for r in rows]


def product_by_id(db: sqlite3.Connection, pid: int) -> Optional[sqlite3.Row]:
    return db.execute("SELECT * FROM products WHERE id = ?", (pid,)).fetchone()


def product_by_slug(db: sqlite3.Connection, slug: str) -> Optional[sqlite3.Row]:
    return db.execute("SELECT * FROM products WHERE slug = ?", (slug.strip(),)).fetchone()


def mark_order_receipt_sent(db: sqlite3.Connection, order_id: int) -> None:
    db.execute(
        """
        UPDATE orders SET receipt_sent = 1, receipt_sent_at = datetime('now')
        WHERE id = ?
        """,
        (order_id,),
    )


def format_money(cents: int) -> str:
    return f"${cents / 100:.2f}"
