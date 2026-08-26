"""SQLite database for Liquorice Locker."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import unicodedata
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

from werkzeug.security import generate_password_hash

from database_config import (
    CORE_TABLES,
    DATABASE_STATE_DEVELOPMENT,
    DATABASE_STATE_EMPTY_VOLUME_BOOTSTRAP,
    DATABASE_STATE_EXISTING,
    DATABASE_STATE_TEST,
    ProductionDatabaseError,
    assert_production_database_file,
    assert_production_path_on_volume,
    classify_production_database_state,
    configured_volume_root,
    detect_environment,
    display_database_path,
    expected_volume_root,
    get_database_path,
    is_production,
    last_database_state,
    log_database_config,
    log_startup_success,
    path_is_inside,
    raise_for_production_state,
    railway_diagnostics,
    railway_volume_mount,
    set_last_database_state,
    sqlite_header_ok,
    sqlite_uri,
)

# Re-export for app.py and tests
__all__ = []  # keep module-style access: database.get_database_path


def _resolved_db_path() -> Path:
    """Deprecated alias — use get_database_path()."""
    return get_database_path()


class _DatabasePathProxy:
    """Lazy path so dotenv can load before first use. Do not cache at import time."""

    def __str__(self) -> str:
        return str(get_database_path())

    def __fspath__(self) -> str:
        return str(get_database_path())

    def is_file(self) -> bool:
        return get_database_path().is_file()

    def exists(self) -> bool:
        return get_database_path().exists()

    @property
    def parent(self) -> Path:
        return get_database_path().parent


DB_PATH = _DatabasePathProxy()


class PersistVerificationError(Exception):
    """Read-back check failed after a write; raise inside ``get_db()`` so the transaction rolls back."""


def _norm_text(val: Any) -> str:
    return (val if val is not None else "").strip()


def ensure_affiliate_page_for_user(db: sqlite3.Connection, user_id: int) -> None:
    """Ensure ``affiliate_pages`` has a row for this member (repairs legacy gaps; safe if row exists)."""
    uid = int(user_id)
    if db.execute("SELECT 1 FROM affiliate_pages WHERE user_id = ?", (uid,)).fetchone():
        return
    try:
        db.execute(
            """
            INSERT INTO affiliate_pages (user_id, headline, tagline, description, monthly_sales_target)
            VALUES (?, 'Welcome', '', '', 25)
            """,
            (uid,),
        )
    except sqlite3.IntegrityError:
        if db.execute("SELECT 1 FROM affiliate_pages WHERE user_id = ?", (uid,)).fetchone():
            return
        raise

# Only these normalized emails may have role ``admin`` (enforced at bootstrap + login).
ADMIN_EMAIL_ALLOWLIST: Tuple[str, ...] = (
    "joshuafrenchdesign@gmail.com",
    "davidbootnz@gmail.com",
    "accounts@evcity.kiwi",
)


def admin_allowlist_normalized() -> frozenset[str]:
    return frozenset(normalize_email(e) for e in ADMIN_EMAIL_ALLOWLIST if str(e).strip())


def sync_admin_allowlist_users(
    conn: sqlite3.Connection,
    *,
    admin_password_plain: Optional[str],
    log: Optional[logging.Logger] = None,
) -> None:
    """Create missing allowlisted admins, promote allowlisted users to admin, demote any other admin.

    New rows use ``admin_password_plain`` (hashed); existing users keep their password hash.
    If the password is unset, missing allowlisted users are skipped (logged).
    """
    lg = log or logging.getLogger(__name__)
    allow = admin_allowlist_normalized()
    method = "pbkdf2:sha256"
    pw = (admin_password_plain or "").strip()
    admin_hash: Optional[str] = generate_password_hash(pw, method=method) if pw else None

    for email in sorted(allow):
        row = user_by_email(conn, email)
        if row is None:
            if not admin_hash:
                lg.warning(
                    "admin_bootstrap skip_create reason=ADMIN_PASSWORD_unset normalized_email=%s",
                    email,
                )
                continue
            local = email.split("@", 1)[0] if "@" in email else email
            conn.execute(
                """
                INSERT INTO users (email, password_hash, role, affiliate_slug, full_name)
                VALUES (?, ?, 'admin', NULL, ?)
                """,
                (email, admin_hash, f"Admin ({local})"),
            )
            lg.info("admin_bootstrap created normalized_email=%s", email)
            continue
        uid = int(row["id"])
        if (row["role"] or "") != "admin":
            conn.execute(
                """
                UPDATE users
                SET role = 'admin', affiliate_slug = NULL, affiliate_code = NULL
                WHERE id = ?
                """,
                (uid,),
            )
            lg.info("admin_bootstrap promoted_to_admin user_id=%s normalized_email=%s", uid, email)

    for row in conn.execute("SELECT id, email FROM users WHERE role = 'admin'"):
        em = normalize_email(row["email"] or "")
        if em not in allow:
            _demote_non_allowlisted_admin(conn, int(row["id"]), em, lg)


def _demote_non_allowlisted_admin(
    conn: sqlite3.Connection, user_id: int, normalized_email: str, lg: logging.Logger
) -> None:
    """Force role to affiliate and ensure Listening Room row exists (legacy stray admins)."""
    code, slug = allocate_unique_affiliate_code_and_slug(conn, "Staff", "User")
    conn.execute(
        """
        UPDATE users
        SET role = 'affiliate',
            affiliate_slug = ?,
            affiliate_code = ?,
            affiliate_active = 1
        WHERE id = ?
        """,
        (slug, code, user_id),
    )
    has_page = conn.execute("SELECT 1 FROM affiliate_pages WHERE user_id = ?", (user_id,)).fetchone()
    if not has_page:
        conn.execute(
            """
            INSERT INTO affiliate_pages (user_id, headline, tagline, description, monthly_sales_target)
            VALUES (?, 'Welcome', '', '', 25)
            """,
            (user_id,),
        )
    lg.warning(
        "admin_bootstrap demoted_non_allowlist_admin user_id=%s normalized_email=%s",
        user_id,
        normalized_email,
    )


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
    path = get_database_path()
    if is_production():
        assert_production_path_on_volume(path)
        assert_production_database_file(path)
        conn = _open_sqlite(path, create=False)
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = _open_sqlite(path, create=True)
    return conn


def _apply_connection_pragmas(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    try:
        conn.execute("PRAGMA journal_mode = WAL")
    except sqlite3.OperationalError:
        logging.getLogger("licorice.database").warning("Could not enable WAL journal mode")
    try:
        conn.execute("PRAGMA synchronous = NORMAL")
    except sqlite3.OperationalError:
        pass


def _open_sqlite(path: Path, *, create: bool) -> sqlite3.Connection:
    if create:
        conn = sqlite3.connect(str(path), check_same_thread=False, timeout=30.0)
    else:
        conn = sqlite3.connect(
            sqlite_uri(path, mode="rw"),
            uri=True,
            check_same_thread=False,
            timeout=30.0,
        )
    _apply_connection_pragmas(conn)
    return conn


@contextmanager
def get_db(*, immediate: bool = False, commit: bool = True) -> Generator[sqlite3.Connection, None, None]:
    """One connection strategy: open, optional IMMEDIATE lock, commit or rollback, always close."""
    conn = get_connection()
    try:
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        yield conn
        if commit:
            conn.commit()
        else:
            conn.rollback()
    except Exception as exc:
        conn.rollback()
        skip_log = (sqlite3.IntegrityError, ValueError, ProductionDatabaseError)
        try:
            from inventory import InventoryError

            skip_log = skip_log + (InventoryError,)
        except Exception:
            pass
        if not isinstance(exc, skip_log):
            logging.getLogger("licorice.database").exception(
                "database_transaction_failed operation=get_db error_type=%s",
                type(exc).__name__,
            )
        raise
    finally:
        conn.close()


def apply_baseline_schema(db: sqlite3.Connection) -> None:
    """Create original tables and additive columns. Never overwrites catalogue rows."""
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
    _backfill_product_enhanced(db)
    _ensure_product_specs(db)
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
    db.execute(
        "UPDATE affiliate_pages SET monthly_sales_target = 25 WHERE IFNULL(monthly_sales_target, 0) = 16"
    )
    _migrate_brand_spelling(db)


def _backup_database_file(path: Path) -> Optional[Path]:
    if not path.is_file():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    dest = path.parent / f"licorice-pre-migration-{stamp}.db"
    src = sqlite3.connect(str(path))
    try:
        dst = sqlite3.connect(str(dest))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()
    logging.getLogger("licorice.database").info(
        "Wrote pre-migration backup next to the live database (licorice-pre-migration-%s.db)",
        stamp,
    )
    return dest


def restore_sqlite_backup(*, backup_path: Path, destination: Path) -> None:
    """Copy a SQLite backup into destination using the backup API. Never overwrites production live path."""
    src_path = Path(backup_path)
    dest_path = Path(destination)
    if not src_path.is_file():
        raise FileNotFoundError("backup_not_found")
    if is_production() and dest_path.resolve() == get_database_path().resolve():
        raise ProductionDatabaseError(
            "Refusing to restore over the live production database from application code."
        )
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(str(src_path))
    try:
        dst = sqlite3.connect(str(dest_path))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def _production_core_tables_ok(db: sqlite3.Connection) -> List[str]:
    missing = []
    for name in CORE_TABLES:
        row = db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
            (name,),
        ).fetchone()
        if not row:
            missing.append(name)
    return missing


def _assert_production_database_healthy(db: sqlite3.Connection, path: Path) -> None:
    shown = display_database_path(path)
    missing = _production_core_tables_ok(db)
    if missing:
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            f"Missing core tables: {', '.join(missing)}\n"
            "Refusing to create a replacement database."
        )
    products = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
    users = int(db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"])
    orders = int(db.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"])
    if products == 0 and users == 0 and orders == 0:
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The database is unexpectedly empty (no products, users, or orders).\n"
            "Refusing to treat this as a valid production database."
        )


def _assert_production_migrations_table(db: sqlite3.Connection, path: Path) -> None:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = 'schema_migrations' LIMIT 1"
    ).fetchone()
    if not row:
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The schema_migrations table is missing after startup."
        )


def _sqlite_integrity_ok(db: sqlite3.Connection) -> bool:
    try:
        row = db.execute("PRAGMA integrity_check").fetchone()
        return bool(row) and str(row[0]).lower() == "ok"
    except sqlite3.Error:
        return False


def _sqlite_fk_ok(db: sqlite3.Connection) -> bool:
    try:
        rows = db.execute("PRAGMA foreign_key_check").fetchall()
        return len(rows) == 0
    except sqlite3.Error:
        return False


def _cleanup_sqlite_sidecars(path: Path) -> None:
    for extra in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            extra.unlink()
        except FileNotFoundError:
            pass


def seed_production_first_boot(db: sqlite3.Connection) -> None:
    """Official catalogue and shipping config only. No customers, orders, users, or payments."""
    n = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
    if n == 0:
        logging.getLogger("licorice.database").info(
            "Production first-boot: inserting official Liquorice Locker catalogue"
        )
        _ensure_core_products(db)
        _backfill_product_enhanced(db)
        _ensure_product_specs(db)
        _ensure_product_images_tags(db)
        _apply_initial_marketing_copy(db)
        ensure_default_variants(db)
        _ensure_inventory_rows_for_variants(db)
    from shipping import apply_canonical_shipping_rates, seed_local_pickup_rate_if_missing

    apply_canonical_shipping_rates(db)
    seed_local_pickup_rate_if_missing(db)


def _create_verified_volume_database(path: Path) -> None:
    """Create SQLite on a verified empty Volume via a temp file, then atomically replace."""
    parent = path.parent
    if not parent.is_dir():
        parent.mkdir(parents=True, exist_ok=True)
    tmp = parent / f".{path.name}.bootstrap-tmp"
    _cleanup_sqlite_sidecars(tmp)
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = _open_sqlite(tmp, create=True)
        from migrations.runner import run_migrations

        applied = run_migrations(conn)
        if applied:
            logging.getLogger("licorice.database").info(
                "First-boot migrations applied: %s", ", ".join(applied)
            )
        seed_production_first_boot(conn)
        conn.commit()
        shown = display_database_path(path)
        if not _sqlite_integrity_ok(conn):
            raise ProductionDatabaseError(
                "CRITICAL DATABASE SAFETY ERROR\n"
                "First-boot bootstrap failed.\n"
                f"DATABASE_PATH={shown}\n"
                "SQLite integrity_check did not return ok."
            )
        if not _sqlite_fk_ok(conn):
            raise ProductionDatabaseError(
                "CRITICAL DATABASE SAFETY ERROR\n"
                "First-boot bootstrap failed.\n"
                f"DATABASE_PATH={shown}\n"
                "PRAGMA foreign_key_check found violations."
            )
        _assert_production_database_healthy(conn, path)
        _assert_production_migrations_table(conn, path)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass
        conn.commit()
        conn.close()
        conn = None
        os.replace(str(tmp), str(path))
        _cleanup_sqlite_sidecars(tmp)
    except Exception:
        if conn is not None:
            try:
                conn.close()
            except sqlite3.Error:
                pass
        _cleanup_sqlite_sidecars(tmp)
        raise


@contextmanager
def _startup_migration_lock(path: Path) -> Generator[None, None, None]:
    """Serialize schema work across Gunicorn workers. Idempotent migrations remain the real safety net."""
    lock_path = path.parent / ".licorice-migrate.lock"
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    except OSError:
        yield
        return
    try:
        try:
            import fcntl
        except ImportError:
            yield
            return
        fcntl.flock(fd, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        os.close(fd)


def bootstrap() -> None:
    """Resolve path, refuse unsafe production states, migrate, never overwrite catalogue."""
    env = detect_environment()
    path = get_database_path()
    existed = path.is_file()
    first_boot = False
    state = DATABASE_STATE_TEST if env == "test" else DATABASE_STATE_DEVELOPMENT
    log_database_config(exists=existed, created_new=False)

    if env == "production":
        state = classify_production_database_state(path)
        set_last_database_state(state)
        raise_for_production_state(state, path)
        if state == DATABASE_STATE_EMPTY_VOLUME_BOOTSTRAP:
            first_boot = True
            logging.getLogger("licorice.database").info(
                "Database state: EMPTY VOLUME BOOTSTRAP — creating %s on verified Volume",
                display_database_path(path),
            )
            _create_verified_volume_database(path)
            existed = True
        elif state == DATABASE_STATE_EXISTING:
            assert_production_path_on_volume(path)
            assert_production_database_file(path)
        else:
            raise_for_production_state(state, path)
    else:
        set_last_database_state(state)

    from migrations.runner import current_version, has_pending_migrations_readonly, run_migrations

    with _startup_migration_lock(path):
        if existed and not first_boot and has_pending_migrations_readonly(path):
            _backup_database_file(path)

        with get_db() as db:
            if env == "production" and not first_boot:
                _assert_production_database_healthy(db, path)
            applied = run_migrations(db)
            if applied:
                logging.getLogger("licorice.database").info(
                    "Migrations applied: %s", ", ".join(applied)
                )
            if env == "production":
                _assert_production_database_healthy(db, path)
                _assert_production_migrations_table(db, path)
            if env != "production":
                seed_development_if_empty(db)
                from shipping import seed_development_rates_if_empty

                seed_development_rates_if_empty(db)
            _warn_inventory_enforce_unpopulated(db)
            journal = str(db.execute("PRAGMA journal_mode").fetchone()[0])
            fk = bool(db.execute("PRAGMA foreign_keys").fetchone()[0])
            integrity_ok = _sqlite_integrity_ok(db)
            fk_ok = _sqlite_fk_ok(db)
            if env == "production" and not integrity_ok:
                shown = display_database_path(path)
                raise ProductionDatabaseError(
                    "CRITICAL DATABASE SAFETY ERROR\n"
                    "Production database could not be verified.\n"
                    f"DATABASE_PATH={shown}\n"
                    "SQLite integrity_check did not return ok."
                )
            if env == "production" and not fk_ok:
                shown = display_database_path(path)
                raise ProductionDatabaseError(
                    "CRITICAL DATABASE SAFETY ERROR\n"
                    "Production database could not be verified.\n"
                    f"DATABASE_PATH={shown}\n"
                    "PRAGMA foreign_key_check found violations."
                )
            log_startup_success(
                path=path,
                journal_mode=journal,
                foreign_keys=fk,
                migration=current_version(db),
                integrity_ok=integrity_ok and fk_ok,
                database_state=last_database_state(),
            )


def init_db() -> None:
    """Back-compat entry: run bootstrap (schema migrations only; no catalogue overwrite)."""
    bootstrap()


def _unmanaged_sellable_sku_count(db: sqlite3.Connection) -> int:
    try:
        row = db.execute(
            """
            SELECT COUNT(*) AS c
            FROM product_variants v
            INNER JOIN products p ON p.id = v.product_id
            LEFT JOIN inventory_items i ON i.variant_id = v.id
            WHERE COALESCE(p.add_to_cart_enabled, 1) = 1
              AND v.deleted_at IS NULL
              AND COALESCE(i.quantity_on_hand, 0) = 0
              AND COALESCE(i.quantity_reserved, 0) = 0
              AND NOT EXISTS (
                    SELECT 1 FROM inventory_movements m
                    WHERE i.id IS NOT NULL AND m.inventory_item_id = i.id
              )
            """
        ).fetchone()
        return int(row["c"] if row else 0)
    except sqlite3.Error:
        return -1


def _warn_inventory_enforce_unpopulated(db: sqlite3.Connection) -> None:
    from inventory import inventory_enforced

    if not inventory_enforced():
        return
    n = _unmanaged_sellable_sku_count(db)
    logging.getLogger("licorice.database").warning(
        "INVENTORY_ENFORCE is ENABLED. Unmanaged/zero-stock sellable SKUs=%s. "
        "Enabling enforcement before loading inventory can block checkout. "
        "This process will not invent stock.",
        n,
    )


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


def _migrate_brand_spelling(db: sqlite3.Connection) -> None:
    """Update stored affiliate copy from Licorice Locker to Liquorice Locker."""
    for col in ("headline", "tagline", "description"):
        db.execute(
            f"UPDATE affiliate_pages SET {col} = REPLACE({col}, 'Licorice Locker', 'Liquorice Locker') "
            f"WHERE {col} LIKE '%Licorice Locker%'"
        )


def _sync_product_catalog_prices(db: sqlite3.Connection) -> None:
    """Intentionally disabled. Catalogue prices are owned by SQLite, not Python constants."""
    raise RuntimeError(
        "Refusing to overwrite product prices from Python constants. "
        "The database is the source of truth."
    )


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
    if "affiliate_country" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN affiliate_country TEXT NOT NULL DEFAULT ''")
    if "affiliate_bank_details" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN affiliate_bank_details TEXT NOT NULL DEFAULT ''")
    if "affiliate_bank_details_updated_at" not in cols:
        db.execute("ALTER TABLE users ADD COLUMN affiliate_bank_details_updated_at TEXT")
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
    "sound-wave": 50,
    "allegro": 14,
    "melody": 14,
    "harmony": 14,
    "riff": 14,
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
        ("allegro", "Allegra", 9999, _ALLEGRA_DESCRIPTION.split("\n\n")[0], 1, 0),
        ("melody", "Melody", 9999, _MELODY_DESCRIPTION.split("\n\n")[0], 2, 0),
        ("harmony", "Harmony", 9999, _HARMONY_DESCRIPTION.split("\n\n")[0], 3, 0),
        ("riff", "Riff", 9999, _RIFF_DESCRIPTION.split("\n\n")[0], 4, 0),
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
    """SKU, cm dimensions, capacity count, collection; unique index on sku.

    Fills NULL/empty fields only. Does not re-apply featured flags or prices.
    """
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
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?, capacity_records = ?
        WHERE slug = 'sound-wave'
        """,
        (
            "312 × 198 × 94 mm (W × D × H)",
            "Clear acrylic structure — minimal, precision-cut supports.",
            "Holds up to 50 LPs on display; cover artwork stays visible in sequence.",
            50,
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
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?, capacity_records = ?
        WHERE slug = 'allegro'
        """,
        (
            "210 × 120 × 38 mm (W × D × H)",
            "Clear acrylic — minimal supports; precision spacing.",
            "Holds up to 14 LPs on display; albums stay accessible and visible.",
            14,
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
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?, capacity_records = ?
        WHERE slug = 'harmony'
        """,
        (
            "280 × 200 × 64 mm (W × D × H)",
            "Clear acrylic — minimal supports; aligned, cohesive rows.",
            "Holds up to 14 LPs on display; unified presence, calm order.",
            14,
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
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?, capacity_records = ?
        WHERE slug = 'melody'
        """,
        (
            "240 × 160 × 52 mm (W × D × H)",
            "Clear acrylic — minimal supports; even, rhythmic spacing.",
            "Holds up to 14 LPs on display; modest selection, deliberate arrangement.",
            14,
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
        UPDATE products SET dimensions = ?, materials = ?, capacity = ?, capacity_records = ?
        WHERE slug = 'riff'
        """,
        (
            "190 × 110 × 42 mm (W × D × H)",
            "Clear acrylic — compact footprint; records appear to float.",
            "Holds up to 14 LPs on display; curated selection, side-by-side modular growth.",
            14,
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
            "Holds up to 50 LPs on display; cover artwork stays visible in sequence.",
        ),
        "allegro": (
            "210 × 120 × 38 mm (W × D × H)",
            "Clear acrylic — minimal supports; precision spacing.",
            "Holds up to 14 LPs on display; albums stay accessible and visible.",
        ),
        "melody": (
            "240 × 160 × 52 mm (W × D × H)",
            "Clear acrylic — minimal supports; even, rhythmic spacing.",
            "Holds up to 14 LPs on display; modest selection, deliberate arrangement.",
        ),
        "harmony": (
            "280 × 200 × 64 mm (W × D × H)",
            "Clear acrylic — minimal supports; aligned, cohesive rows.",
            "Holds up to 14 LPs on display; unified presence, calm order.",
        ),
        "riff": (
            "190 × 110 × 42 mm (W × D × H)",
            "Clear acrylic — compact footprint; records appear to float.",
            "Holds up to 14 LPs on display; curated selection, side-by-side modular growth.",
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


def seed_development_if_empty(db: sqlite3.Connection) -> None:
    """Insert catalogue only when the database has no products. Never runs in production."""
    if is_production():
        return
    n = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
    if n > 0:
        return
    logging.getLogger("licorice.database").info("Seeding empty development catalogue")
    _ensure_core_products(db)
    _backfill_product_enhanced(db)
    _ensure_product_specs(db)
    _ensure_product_images_tags(db)
    _apply_initial_marketing_copy(db)
    ensure_default_variants(db)
    _ensure_inventory_rows_for_variants(db)


def seed_if_empty() -> None:
    """Back-compat. Production is a no-op. Development seeds only if users and products are empty."""
    if is_production():
        logging.getLogger("licorice.database").warning("seed_if_empty refused: production")
        return
    with get_db() as db:
        users = int(db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"])
        seed_development_if_empty(db)
        if users > 0:
            return
        _seed_dev_affiliate(db)


def _seed_dev_affiliate(db: sqlite3.Connection) -> None:
    _method = "pbkdf2:sha256"
    aff_hash = generate_password_hash(os.environ.get("AFFILIATE_PASSWORD", "affiliate123"), method=_method)
    db.execute(
        """
        INSERT INTO users (email, password_hash, role, affiliate_slug, full_name)
        VALUES (?, ?, 'affiliate', 'sound-partner', 'Jordan Keys')
        """,
        ("partner@licoricelocker.local", aff_hash),
    )
    aff_id = db.execute("SELECT id FROM users WHERE email = ?", ("partner@licoricelocker.local",)).fetchone()["id"]
    db.execute(
        """
        INSERT INTO affiliate_pages (user_id, headline, tagline, description, monthly_sales_target)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            aff_id,
            "Sound that travels with you",
            "Liquorice Locker — curated audio gear",
            "I only share gear I use. Every purchase supports independent sound design.",
            25,
        ),
    )
    _backfill_affiliate_profile(db)


def _apply_initial_marketing_copy(db: sqlite3.Connection) -> None:
    """Full descriptions for a brand-new catalogue only (caller already verified empty products)."""
    mapping = {
        "sound-wave": _SOUND_WAVE_DESCRIPTION,
        "allegro": _ALLEGRA_DESCRIPTION,
        "harmony": _HARMONY_DESCRIPTION,
        "melody": _MELODY_DESCRIPTION,
        "riff": _RIFF_DESCRIPTION,
    }
    for slug, desc in mapping.items():
        db.execute("UPDATE products SET description = ? WHERE slug = ?", (desc, slug))


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
    if email_norm in admin_allowlist_normalized():
        raise ValueError("email reserved for staff admin allowlist")
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
    """Clear legacy TOTP columns (affiliate 2FA removed; kept for password-reset hygiene)."""
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


def verify_affiliate_page_profile_saved(
    db: sqlite3.Connection,
    user_id: int,
    headline: str,
    tagline: str,
    description: str,
    instagram_url: str,
    tiktok_url: str,
    banner_image_url: str,
    affiliate_country: str,
    display_picture_url: Optional[str],
) -> None:
    """Confirm landing-page + profile fields match what we just wrote; raises PersistVerificationError if not."""
    uid = int(user_id)
    ap = db.execute(
        """
        SELECT headline, tagline, description, instagram_url, tiktok_url, banner_image_url
        FROM affiliate_pages WHERE user_id = ?
        """,
        (uid,),
    ).fetchone()
    ur = db.execute(
        "SELECT display_picture_url, affiliate_country FROM users WHERE id = ?",
        (uid,),
    ).fetchone()
    if not ap or not ur:
        raise PersistVerificationError("missing_row")
    if _norm_text(ap["headline"]) != _norm_text(headline):
        raise PersistVerificationError("headline")
    if _norm_text(ap["tagline"]) != _norm_text(tagline):
        raise PersistVerificationError("tagline")
    if _norm_text(ap["description"]) != _norm_text(description):
        raise PersistVerificationError("description")
    if _norm_text(ap["instagram_url"]) != _norm_text(instagram_url):
        raise PersistVerificationError("instagram_url")
    if _norm_text(ap["tiktok_url"]) != _norm_text(tiktok_url):
        raise PersistVerificationError("tiktok_url")
    if _norm_text(ap["banner_image_url"]) != _norm_text(banner_image_url):
        raise PersistVerificationError("banner_image_url")
    if _norm_text(ur["affiliate_country"]) != _norm_text(affiliate_country):
        raise PersistVerificationError("affiliate_country")
    stored_dp = ur["display_picture_url"]
    want = ((display_picture_url or "").strip() or None)
    got = ((stored_dp or "").strip() or None)
    if want != got:
        raise PersistVerificationError("display_picture_url")


def verify_affiliate_bank_details_saved(
    db: sqlite3.Connection,
    user_id: int,
    bank_details: str,
) -> None:
    uid = int(user_id)
    row = db.execute(
        "SELECT affiliate_bank_details FROM users WHERE id = ?",
        (uid,),
    ).fetchone()
    if not row:
        raise PersistVerificationError("missing_row")
    if _norm_text(row["affiliate_bank_details"]) != _norm_text(bank_details):
        raise PersistVerificationError("affiliate_bank_details")


def mark_affiliate_commission_paid(
    db: sqlite3.Connection,
    affiliate_user_id: int,
    year_month: str,
) -> bool:
    """Set payout_status to paid for one affiliate/month. Returns True if a row was updated."""
    ym = (year_month or "").strip()
    if not ym:
        return False
    cur = db.execute(
        """
        UPDATE commissions
        SET payout_status = 'paid'
        WHERE affiliate_user_id = ? AND year_month = ?
          AND LOWER(TRIM(payout_status)) != 'paid'
        """,
        (int(affiliate_user_id), ym),
    )
    return cur.rowcount > 0


def list_fellow_affiliates_for_dashboard(db: sqlite3.Connection, exclude_user_id: int) -> List[sqlite3.Row]:
    """Other Listening Room members (terms accepted, active) for community / collab directory."""
    return db.execute(
        """
        SELECT
            u.id,
            u.email,
            u.full_name,
            u.affiliate_slug,
            u.affiliate_code,
            COALESCE(NULLIF(TRIM(u.affiliate_country), ''), '') AS affiliate_country,
            u.display_picture_url,
            ap.instagram_url,
            ap.tiktok_url
        FROM users u
        INNER JOIN affiliate_pages ap ON ap.user_id = u.id
        WHERE u.role = 'affiliate'
          AND u.id != ?
          AND COALESCE(u.affiliate_active, 1) = 1
          AND (
                COALESCE(u.terms_accepted, 0) = 1
             OR (u.terms_accepted_at IS NOT NULL AND TRIM(COALESCE(u.terms_accepted_at, '')) != '')
          )
        ORDER BY
            LOWER(COALESCE(NULLIF(TRIM(u.full_name), ''), u.affiliate_slug, u.email))
        """,
        (int(exclude_user_id),),
    ).fetchall()


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


def attach_commerce_records_for_new_order(
    db: sqlite3.Connection,
    *,
    order_id: int,
    csid: str,
    email: str,
    first: str,
    last: str,
    phone: str,
    total_cents: int,
    payment_method: str,
) -> None:
    if int(total_cents) < 0:
        raise ValueError("payment_amount_cannot_be_negative")
    cid = get_or_create_customer(
        db, email=email, first_name=first, last_name=last, phone=phone
    )
    db.execute("UPDATE orders SET customer_id = ? WHERE id = ?", (cid, order_id))
    exists = db.execute(
        "SELECT id, status FROM payments WHERE provider_session_id = ? LIMIT 1",
        (csid,),
    ).fetchone()
    if exists:
        from commerce_state import assert_payment_transition, InvalidStatusTransition

        try:
            assert_payment_transition(str(exists["status"] or ""), "succeeded")
        except InvalidStatusTransition:
            logging.getLogger("licorice.database").warning(
                "payment_replay_blocked operation=payment entity=payment entity_id=%s error_type=InvalidStatusTransition",
                exists["id"],
            )
    else:
        db.execute(
            """
            INSERT INTO payments (
                order_id, provider, provider_session_id, amount_cents, currency,
                status, payment_method, paid_at
            ) VALUES (?, 'stripe', ?, ?, 'NZD', 'succeeded', ?, datetime('now'))
            """,
            (order_id, csid, int(total_cents), payment_method),
        )
    insert_audit_log(
        db,
        action="ORDER_CREATED",
        entity_type="order",
        entity_id=order_id,
        after={"order_id": order_id, "stripe_session": csid, "total_cents": total_cents},
    )
    insert_business_event(
        db,
        event_type="ORDER_CREATED",
        entity_type="order",
        entity_id=order_id,
        payload={"stripe_session": csid, "total_cents": total_cents},
    )
    insert_business_event(
        db,
        event_type="PAYMENT_RECEIVED",
        entity_type="order",
        entity_id=order_id,
        payload={"provider": "stripe", "amount_cents": total_cents},
    )
    rows = db.execute(
        "SELECT variant_id, quantity FROM order_items WHERE order_id = ?",
        (order_id,),
    ).fetchall()
    from inventory import apply_order_sale_if_managed

    for row in rows:
        apply_order_sale_if_managed(
            db,
            variant_id=int(row["variant_id"]) if row["variant_id"] is not None else None,
            quantity=int(row["quantity"]),
            order_id=order_id,
        )


def format_money(cents: int) -> str:
    return f"${cents / 100:.2f}"


def ensure_default_variants(db: sqlite3.Connection) -> None:
    db.execute(
        """
        INSERT INTO product_variants (
            product_id, name, sku, status, price_cents, currency,
            width_cm, height_cm, depth_cm, is_default
        )
        SELECT
            p.id,
            'Default',
            p.sku,
            'active',
            p.price_cents,
            'NZD',
            p.width_cm,
            p.height_cm,
            p.depth_cm,
            1
        FROM products p
        WHERE NOT EXISTS (
            SELECT 1 FROM product_variants v WHERE v.product_id = p.id
        )
        """
    )


def _ensure_inventory_rows_for_variants(db: sqlite3.Connection) -> None:
    loc = db.execute("SELECT id FROM inventory_locations ORDER BY id LIMIT 1").fetchone()
    if not loc:
        return
    lid = int(loc["id"])
    db.execute(
        """
        INSERT INTO inventory_items (variant_id, location_id, quantity_on_hand, quantity_reserved)
        SELECT v.id, ?, 0, 0
        FROM product_variants v
        WHERE NOT EXISTS (
            SELECT 1 FROM inventory_items i
            WHERE i.variant_id = v.id AND i.location_id = ?
        )
        """,
        (lid, lid),
    )


def default_variant_for_product(db: sqlite3.Connection, product_id: int) -> Optional[sqlite3.Row]:
    return db.execute(
        """
        SELECT * FROM product_variants
        WHERE product_id = ? AND deleted_at IS NULL
        ORDER BY is_default DESC, id ASC
        LIMIT 1
        """,
        (product_id,),
    ).fetchone()


def backfill_customers_from_orders(db: sqlite3.Connection) -> None:
    rows = db.execute(
        """
        SELECT id, customer_email, customer_first, customer_last, customer_phone,
               shipping_line1, shipping_line2, shipping_city, shipping_region,
               shipping_postal, shipping_country, shipping_name
        FROM orders
        WHERE customer_id IS NULL
        ORDER BY id
        """
    ).fetchall()
    for r in rows:
        email = (r["customer_email"] or "").strip()
        norm = normalize_email(email)
        if not norm:
            continue
        existing = db.execute(
            "SELECT id FROM customers WHERE email_normalized = ?",
            (norm,),
        ).fetchone()
        if existing:
            cid = int(existing["id"])
        else:
            cur = db.execute(
                """
                INSERT INTO customers (
                    email, email_normalized, first_name, last_name, phone
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    email,
                    norm,
                    (r["customer_first"] or "").strip(),
                    (r["customer_last"] or "").strip(),
                    (r["customer_phone"] or "").strip() if "customer_phone" in r.keys() else "",
                ),
            )
            cid = int(cur.lastrowid)
            line1 = (r["shipping_line1"] or "").strip()
            if line1:
                db.execute(
                    """
                    INSERT INTO customer_addresses (
                        customer_id, type, first_name, last_name, address_line1, address_line2,
                        city, region, postcode, country, is_default
                    ) VALUES (?, 'shipping', ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        cid,
                        (r["customer_first"] or "").strip(),
                        (r["customer_last"] or "").strip(),
                        line1,
                        (r["shipping_line2"] or "").strip(),
                        (r["shipping_city"] or "").strip(),
                        (r["shipping_region"] or "").strip(),
                        (r["shipping_postal"] or "").strip(),
                        (r["shipping_country"] or "").strip(),
                    ),
                )
        db.execute("UPDATE orders SET customer_id = ? WHERE id = ?", (cid, int(r["id"])))


def backfill_payments_from_orders(db: sqlite3.Connection) -> None:
    rows = db.execute(
        """
        SELECT o.id, o.total_cents, o.payment_method, o.stripe_checkout_session_id, o.created_at
        FROM orders o
        WHERE NOT EXISTS (SELECT 1 FROM payments p WHERE p.order_id = o.id)
        """
    ).fetchall()
    for r in rows:
        session_id = (r["stripe_checkout_session_id"] or "").strip() or None
        db.execute(
            """
            INSERT INTO payments (
                order_id, provider, provider_session_id, amount_cents, currency,
                status, payment_method, paid_at
            ) VALUES (?, 'stripe', ?, ?, 'NZD', 'succeeded', ?, ?)
            """,
            (
                int(r["id"]),
                session_id,
                int(r["total_cents"] or 0),
                (r["payment_method"] or "").strip(),
                r["created_at"],
            ),
        )


def upsert_shipment_for_order(
    db: sqlite3.Connection,
    order_id: int,
    *,
    status: str,
    tracking_number: Optional[str] = None,
) -> None:
    """Create or update the shipment row for an order. Does not insert a second shipment."""
    allowed = {"pending", "ready", "shipped", "delivered", "cancelled"}
    st = (status or "pending").strip().lower()
    if st not in allowed:
        raise ValueError("invalid_shipment_status")
    row = db.execute(
        "SELECT id, tracking_number, shipped_at FROM shipments WHERE order_id = ? ORDER BY id LIMIT 1",
        (int(order_id),),
    ).fetchone()
    tracking = "" if tracking_number is None else str(tracking_number).strip()
    if row is None:
        db.execute(
            """
            INSERT INTO shipments (order_id, tracking_number, status, shipped_at, delivered_at)
            VALUES (
                ?, ?, ?,
                CASE WHEN ? = 'shipped' THEN datetime('now') ELSE NULL END,
                CASE WHEN ? = 'delivered' THEN datetime('now') ELSE NULL END
            )
            """,
            (int(order_id), tracking, st, st, st),
        )
        return
    keep_tracking = tracking if tracking_number is not None else (row["tracking_number"] or "")
    shipped_at_sql = "datetime('now')" if st == "shipped" and not row["shipped_at"] else (
        "shipped_at" if st in ("shipped", "delivered") else "NULL"
    )
    db.execute(
        f"""
        UPDATE shipments
        SET tracking_number = ?,
            status = ?,
            shipped_at = {shipped_at_sql},
            delivered_at = CASE WHEN ? = 'delivered' THEN COALESCE(delivered_at, datetime('now')) ELSE delivered_at END,
            updated_at = datetime('now')
        WHERE id = ?
        """,
        (keep_tracking, st, st, int(row["id"])),
    )


def backfill_shipments_from_orders(db: sqlite3.Connection) -> None:
    rows = db.execute(
        """
        SELECT id, shipping_tracking, fulfillment_status, status
        FROM orders
        WHERE NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = orders.id)
        """
    ).fetchall()
    for r in rows:
        tracking = (r["shipping_tracking"] or "").strip()
        ff = (r["fulfillment_status"] or r["status"] or "").strip().lower()
        if not tracking and ff not in ("shipped", "delivered"):
            continue
        st = "shipped" if ff == "shipped" or tracking else "pending"
        db.execute(
            """
            INSERT INTO shipments (order_id, tracking_number, status, shipped_at)
            VALUES (?, ?, ?, CASE WHEN ? = 'shipped' THEN datetime('now') ELSE NULL END)
            """,
            (int(r["id"]), tracking, st, st),
        )


def insert_audit_log(
    db: sqlite3.Connection,
    *,
    action: str,
    entity_type: str,
    entity_id: Optional[int] = None,
    user_id: Optional[int] = None,
    before: Any = None,
    after: Any = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    db.execute(
        """
        INSERT INTO audit_logs (user_id, action, entity_type, entity_id, before_json, after_json, metadata_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            action,
            entity_type,
            entity_id,
            json.dumps(before) if before is not None else None,
            json.dumps(after) if after is not None else None,
            json.dumps(metadata) if metadata is not None else None,
        ),
    )


def insert_business_event(
    db: sqlite3.Connection,
    *,
    event_type: str,
    entity_type: str,
    entity_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    db.execute(
        """
        INSERT INTO business_events (type, entity_type, entity_id, payload_json)
        VALUES (?, ?, ?, ?)
        """,
        (event_type, entity_type, entity_id, json.dumps(payload or {})),
    )


def get_or_create_customer(
    db: sqlite3.Connection,
    *,
    email: str,
    first_name: str,
    last_name: str,
    phone: str = "",
) -> int:
    norm = normalize_email(email)
    row = db.execute("SELECT id FROM customers WHERE email_normalized = ?", (norm,)).fetchone()
    if row:
        return int(row["id"])
    cur = db.execute(
        """
        INSERT INTO customers (email, email_normalized, first_name, last_name, phone)
        VALUES (?, ?, ?, ?, ?)
        """,
        (email.strip(), norm, first_name.strip(), last_name.strip(), (phone or "").strip()),
    )
    return int(cur.lastrowid)


def insert_order_item_with_snapshot(
    db: sqlite3.Connection,
    *,
    order_id: int,
    product: sqlite3.Row,
    quantity: int,
    unit_price_cents: int,
    variant: Optional[sqlite3.Row] = None,
) -> None:
    if int(quantity) < 1:
        raise ValueError("order_item_quantity_must_be_positive")
    if int(unit_price_cents) < 0:
        raise ValueError("order_item_unit_price_cannot_be_negative")
    if variant is None:
        variant = default_variant_for_product(db, int(product["id"]))
    vid = int(variant["id"]) if variant else None
    subtotal = int(quantity) * int(unit_price_cents)
    db.execute(
        """
        INSERT INTO order_items (
            order_id, product_id, quantity, unit_price_cents,
            variant_id, product_name_snapshot, sku_snapshot,
            discount_cents, tax_cents, line_subtotal_cents, line_total_cents
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?, ?)
        """,
        (
            order_id,
            int(product["id"]),
            int(quantity),
            int(unit_price_cents),
            vid,
            str(product["name"] or ""),
            str(product["sku"] or "") if "sku" in product.keys() else "",
            subtotal,
            subtotal,
        ),
    )


def fill_order_items_if_empty(
    db: sqlite3.Connection,
    order_id: int,
    lines: List[Tuple[sqlite3.Row, int, int]],
) -> bool:
    """Insert snapshots only when the order has no items. Returns True if this call filled them.

    Does not invent products or prices; callers must pass authoritative (product, qty, unit) lines.
    """
    n = int(
        db.execute(
            "SELECT COUNT(*) AS c FROM order_items WHERE order_id = ?",
            (int(order_id),),
        ).fetchone()["c"]
    )
    if n > 0:
        return False
    if not lines:
        raise ValueError("incomplete_order_missing_authoritative_lines")
    for product, qty, unit in lines:
        insert_order_item_with_snapshot(
            db,
            order_id=int(order_id),
            product=product,
            quantity=int(qty),
            unit_price_cents=int(unit),
        )
    return True


def record_stripe_event_processed(
    db: sqlite3.Connection, event_id: str, event_type: str = "", session_id: str = ""
) -> bool:
    """Return True if this Stripe event id is newly recorded; False if already seen."""
    eid = (event_id or "").strip()
    if not eid:
        return True
    try:
        db.execute(
            """
            INSERT INTO stripe_processed_events (event_id, event_type, session_id)
            VALUES (?, ?, ?)
            """,
            (eid, event_type or "", session_id or ""),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def bom_cost_cents(db: sqlite3.Connection, bom_id: int) -> int:
    """Sum of component qty × unit cost in integer cents (qty rounded after multiply)."""
    rows = db.execute(
        """
        SELECT i.quantity, c.cost_cents
        FROM bill_of_material_items i
        INNER JOIN components c ON c.id = i.component_id
        WHERE i.bom_id = ?
        """,
        (int(bom_id),),
    ).fetchall()
    total = 0
    for r in rows:
        qty = float(r["quantity"] or 0)
        cost = int(r["cost_cents"] or 0)
        if cost < 0:
            continue
        total += int(round(qty * cost))
    return total


def integrity_report(db: sqlite3.Connection) -> Dict[str, Any]:
    """Read-only orphan / duplicate / invalid-value scan. Does not modify data."""

    def _count(sql: str) -> int:
        try:
            return int(db.execute(sql).fetchone()[0])
        except sqlite3.Error:
            return -1

    fk_violations = []
    try:
        for row in db.execute("PRAGMA foreign_key_check"):
            fk_violations.append(
                {"table": row[0], "rowid": row[1], "parent": row[2], "fkid": row[3]}
            )
    except sqlite3.Error:
        pass

    orphans = {
        "order_items_without_order": _count(
            "SELECT COUNT(*) FROM order_items oi LEFT JOIN orders o ON o.id = oi.order_id WHERE o.id IS NULL"
        ),
        "order_items_missing_product": _count(
            "SELECT COUNT(*) FROM order_items oi LEFT JOIN products p ON p.id = oi.product_id WHERE p.id IS NULL"
        ),
        "payments_without_order": _count(
            "SELECT COUNT(*) FROM payments p LEFT JOIN orders o ON o.id = p.order_id WHERE o.id IS NULL"
        ),
        "shipments_without_order": _count(
            "SELECT COUNT(*) FROM shipments s LEFT JOIN orders o ON o.id = s.order_id WHERE o.id IS NULL"
        ),
        "variants_without_product": _count(
            "SELECT COUNT(*) FROM product_variants v LEFT JOIN products p ON p.id = v.product_id WHERE p.id IS NULL"
        ),
        "inventory_without_variant_or_component": _count(
            """
            SELECT COUNT(*) FROM inventory_items i
            WHERE (i.variant_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM product_variants v WHERE v.id = i.variant_id))
               OR (i.component_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM components c WHERE c.id = i.component_id))
            """
        ),
        "movements_without_item": _count(
            "SELECT COUNT(*) FROM inventory_movements m LEFT JOIN inventory_items i ON i.id = m.inventory_item_id WHERE i.id IS NULL"
        ),
        "bom_items_without_bom": _count(
            "SELECT COUNT(*) FROM bill_of_material_items i LEFT JOIN bills_of_materials b ON b.id = i.bom_id WHERE b.id IS NULL"
        ),
        "bom_items_without_component": _count(
            "SELECT COUNT(*) FROM bill_of_material_items i LEFT JOIN components c ON c.id = i.component_id WHERE c.id IS NULL"
        ),
        "production_materials_without_order": _count(
            "SELECT COUNT(*) FROM production_materials m LEFT JOIN production_orders p ON p.id = m.production_order_id WHERE p.id IS NULL"
        ),
        "commissions_without_affiliate": _count(
            "SELECT COUNT(*) FROM commissions c LEFT JOIN users u ON u.id = c.affiliate_user_id WHERE u.id IS NULL"
        ),
        "orders_without_items": _count(
            "SELECT COUNT(*) FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id)"
        ),
        "shipping_rates_without_method": _count(
            """
            SELECT COUNT(*) FROM shipping_rates r
            LEFT JOIN shipping_methods m ON m.id = r.shipping_method_id
            WHERE m.id IS NULL
            """
        ),
        "shipping_rates_without_zone": _count(
            """
            SELECT COUNT(*) FROM shipping_rates r
            LEFT JOIN shipping_zones z ON z.id = r.shipping_zone_id
            WHERE z.id IS NULL
            """
        ),
    }
    duplicates = {
        "duplicate_product_slugs": _count(
            "SELECT COUNT(*) FROM (SELECT slug FROM products GROUP BY slug HAVING COUNT(*) > 1)"
        ),
        "duplicate_order_numbers": _count(
            "SELECT COUNT(*) FROM (SELECT order_number FROM orders GROUP BY order_number HAVING COUNT(*) > 1)"
        ),
        "duplicate_customer_emails": _count(
            "SELECT COUNT(*) FROM (SELECT email_normalized FROM customers GROUP BY email_normalized HAVING COUNT(*) > 1)"
        ),
        "duplicate_stripe_sessions": _count(
            """
            SELECT COUNT(*) FROM (
                SELECT stripe_checkout_session_id FROM orders
                WHERE stripe_checkout_session_id IS NOT NULL AND trim(stripe_checkout_session_id) != ''
                GROUP BY stripe_checkout_session_id HAVING COUNT(*) > 1
            )
            """
        ),
        "duplicate_inventory_sales": _count(
            """
            SELECT COUNT(*) FROM (
                SELECT inventory_item_id, reference_id FROM inventory_movements
                WHERE movement_type = 'SALE' AND reference_type = 'order' AND reference_id IS NOT NULL
                GROUP BY inventory_item_id, reference_id HAVING COUNT(*) > 1
            )
            """
        ),
    }
    invalid_financial = {
        "products_negative_price": _count("SELECT COUNT(*) FROM products WHERE price_cents < 0"),
        "order_items_nonpositive_qty": _count("SELECT COUNT(*) FROM order_items WHERE quantity < 1"),
        "order_items_negative_unit": _count("SELECT COUNT(*) FROM order_items WHERE unit_price_cents < 0"),
        "orders_negative_total": _count("SELECT COUNT(*) FROM orders WHERE total_cents < 0"),
        "payments_negative_amount": _count("SELECT COUNT(*) FROM payments WHERE amount_cents < 0"),
        "commissions_negative": _count("SELECT COUNT(*) FROM commissions WHERE commission_cents < 0"),
        "variants_negative_price": _count("SELECT COUNT(*) FROM product_variants WHERE price_cents < 0"),
        "shipping_rates_negative_price": _count(
            "SELECT COUNT(*) FROM shipping_rates WHERE price_cents < 0"
        ),
        "shipping_rates_invalid_currency": _count(
            "SELECT COUNT(*) FROM shipping_rates WHERE UPPER(COALESCE(currency, '')) != 'NZD'"
        ),
        "shipping_rates_invalid_order_range": _count(
            """
            SELECT COUNT(*) FROM shipping_rates
            WHERE min_order_value_cents IS NOT NULL
              AND max_order_value_cents IS NOT NULL
              AND max_order_value_cents < min_order_value_cents
            """
        ),
        "shipping_rates_invalid_weight_range": _count(
            """
            SELECT COUNT(*) FROM shipping_rates
            WHERE min_weight_grams IS NOT NULL
              AND max_weight_grams IS NOT NULL
              AND max_weight_grams < min_weight_grams
            """
        ),
    }
    invalid_statuses = {
        "orders_unknown_status": _count(
            """
            SELECT COUNT(*) FROM orders
            WHERE LOWER(COALESCE(status, '')) NOT IN (
                '', 'completed', 'paid', 'shipped', 'delivered', 'cancelled', 'canceled', 'pending'
            )
            """
        ),
        "payments_unknown_status": _count(
            """
            SELECT COUNT(*) FROM payments
            WHERE LOWER(COALESCE(status, '')) NOT IN ('pending', 'succeeded', 'failed', 'refunded')
            """
        ),
        "shipped_cancelled_orders": _count(
            """
            SELECT COUNT(*) FROM orders
            WHERE LOWER(COALESCE(status, '')) IN ('cancelled', 'canceled')
              AND LOWER(COALESCE(fulfillment_status, '')) = 'shipped'
            """
        ),
    }
    shipping = {}
    try:
        from shipping import shipping_integrity

        shipping = shipping_integrity(db)
    except Exception:
        shipping = {"available": False}

    return {
        "foreign_key_violations": fk_violations[:50],
        "foreign_key_violation_count": len(fk_violations),
        "orphans": orphans,
        "duplicates": duplicates,
        "invalid_financial": invalid_financial,
        "invalid_statuses": invalid_statuses,
        "shipping": shipping,
    }


def expected_tables() -> Tuple[str, ...]:
    return CORE_TABLES + (
        "product_variants",
        "customers",
        "payments",
        "shipments",
        "inventory_locations",
        "inventory_items",
        "inventory_movements",
        "schema_migrations",
        "audit_logs",
        "business_events",
        "stripe_processed_events",
        "shipping_methods",
        "shipping_zones",
        "shipping_zone_countries",
        "shipping_rates",
        "shipping_settings",
    )


def database_health_report(db: sqlite3.Connection) -> Dict[str, Any]:
    from migrations.runner import MIGRATIONS, applied_versions, current_version

    path = get_database_path()
    exists = path.is_file()
    size = path.stat().st_size if exists else 0
    readable = writable = False
    if exists:
        readable = os.access(path, os.R_OK)
        writable = os.access(path, os.W_OK)
    journal = db.execute("PRAGMA journal_mode").fetchone()[0]
    fk = db.execute("PRAGMA foreign_keys").fetchone()[0]
    busy = db.execute("PRAGMA busy_timeout").fetchone()[0]
    present = {
        r[0]
        for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    missing = [t for t in expected_tables() if t not in present]
    counts: Dict[str, int] = {}
    for table in (
        "products",
        "customers",
        "orders",
        "payments",
        "users",
        "commissions",
        "inventory_items",
        "order_items",
        "product_variants",
        "shipments",
        "audit_logs",
        "business_events",
        "analytics_events",
        "affiliates",
        "shipping_rates",
        "shipping_methods",
        "shipping_zones",
    ):
        if table == "affiliates":
            try:
                counts["affiliates"] = int(
                    db.execute("SELECT COUNT(*) AS c FROM users WHERE role = 'affiliate'").fetchone()["c"]
                )
            except sqlite3.Error:
                counts["affiliates"] = -1
            continue
        try:
            counts[table] = int(db.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"])
        except sqlite3.Error:
            counts[table] = -1
    last_write = None
    try:
        row = db.execute("SELECT MAX(created_at) AS t FROM orders").fetchone()
        last_write = row["t"] if row else None
    except sqlite3.Error:
        pass
    applied = applied_versions(db)
    integrity = integrity_report(db)
    from inventory import inventory_enforced, ledger_discrepancies

    enforce = inventory_enforced()
    ledger = ledger_discrepancies(db)
    try:
        neg_qty = int(
            db.execute(
                """
                SELECT COUNT(*) AS c FROM inventory_items
                WHERE quantity_on_hand < 0 OR quantity_reserved < 0
                """
            ).fetchone()["c"]
        )
    except sqlite3.Error:
        neg_qty = -1
    volume_root = expected_volume_root()
    configured_root = configured_volume_root()
    mount = railway_volume_mount()
    inside_volume = None
    if volume_root is not None:
        inside_volume = path_is_inside(path, volume_root)
    env_name = detect_environment()
    boot_state = last_database_state()
    if env_name == "production" and inside_volume:
        persistence_status = "VERIFIED"
    elif env_name == "production":
        persistence_status = "unverified"
    else:
        persistence_status = "local"
    inventory_block = {
        "enforcement": "ENABLED" if enforce else "DISABLED",
        "records": counts.get("inventory_items", -1),
        "negative_quantities": neg_qty,
        "ledger_discrepancy_count": len(ledger),
        "ledger_discrepancies": ledger,
        "unmanaged_sellable_skus": _unmanaged_sellable_sku_count(db),
    }
    integrity_ok = _sqlite_integrity_ok(db)
    fk_check_ok = _sqlite_fk_ok(db)
    return {
        "database": {
            "path": str(path),
            "exists": exists,
            "size_bytes": size,
            "readable": readable,
            "writable": writable,
            "sqlite_header_ok": sqlite_header_ok(path),
        },
        "persistence": {
            "environment": env_name,
            "status": persistence_status,
            "database_state": boot_state,
            "volume_root": volume_root.as_posix() if volume_root is not None else None,
            "volume_root_configured": configured_root is not None,
            "railway_volume_detected": mount is not None,
            "railway_volume_mount_path": mount.as_posix() if mount is not None else None,
            "resolved_path": str(path),
            "inside_volume": inside_volume,
        },
        "connection": {
            "path": str(path),
            "exists": exists,
            "size_bytes": size,
            "readable": readable,
            "writable": writable,
        },
        "sqlite": {
            "version": sqlite3.sqlite_version,
            "journal_mode": journal,
            "wal": str(journal).lower() == "wal",
            "foreign_keys": bool(fk),
            "busy_timeout_ms": busy,
            "integrity_check_ok": integrity_ok,
            "foreign_key_check_ok": fk_check_ok,
        },
        "schema": {
            "migration_version": current_version(db),
            "migration_count": len(applied),
            "migration_expected": len(MIGRATIONS),
            "missing_expected_tables": missing,
            "core_table_counts": {
                "products": counts.get("products", -1),
                "users": counts.get("users", -1),
                "orders": counts.get("orders", -1),
                "order_items": counts.get("order_items", -1),
                "shipping_rates": counts.get("shipping_rates", -1),
            },
        },
        "integrity": integrity,
        "integrity_status": "OK"
        if integrity_ok and fk_check_ok and not integrity.get("foreign_key_violation_count")
        else "CHECK",
        "business": {
            "products": counts.get("products", -1),
            "users": counts.get("users", -1),
            "customers": counts.get("customers", -1),
            "orders": counts.get("orders", -1),
            "payments": counts.get("payments", -1),
            "affiliates": counts.get("affiliates", -1),
            "commissions": counts.get("commissions", -1),
            "shipping_rates": counts.get("shipping_rates", -1),
        },
        "inventory": inventory_block,
        "row_counts": counts,
        "last_order_created_at": last_write,
        "environment": env_name,
        "railway": railway_diagnostics(),
    }
