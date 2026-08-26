"""Authoritative SQLite path and environment detection for Liquorice Locker.

Production never silently falls back to a local file and never creates a blank
database. Development may create ``data/licorice-dev.db`` (or an existing
``data/licorice.db`` so local work is not stranded).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

log = logging.getLogger("licorice.database")

PROJECT_ROOT = Path(__file__).resolve().parent
DEV_DB_NAME = "licorice-dev.db"
LEGACY_DEV_DB_NAME = "licorice.db"
CORE_TABLES = (
    "users",
    "products",
    "orders",
    "order_items",
    "affiliate_pages",
    "commissions",
    "product_images",
)


class ProductionDatabaseError(RuntimeError):
    """Production refused to start because the persistent database is unsafe or missing."""


def detect_environment() -> str:
    """Return ``production``, ``test``, or ``development``."""
    explicit = (os.environ.get("LICORICE_ENV") or os.environ.get("APP_ENV") or "").strip().lower()
    if explicit in ("production", "prod"):
        return "production"
    if explicit in ("test", "testing"):
        return "test"
    if (os.environ.get("PYTEST_CURRENT_TEST") or "").strip():
        return "test"
    flask_env = (os.environ.get("FLASK_ENV") or "").strip().lower()
    if flask_env == "production":
        return "production"
    if (os.environ.get("RAILWAY_ENVIRONMENT") or "").strip():
        return "production"
    if (os.environ.get("RENDER") or "").strip():
        return "production"
    return "development"


def is_production() -> bool:
    return detect_environment() == "production"


def is_test() -> bool:
    return detect_environment() == "test"


def _expand(raw: str) -> Path:
    return Path(raw).expanduser()


def _reject_dangerous_test_path(path: Path) -> None:
    """Tests must never point at the developer shop database."""
    forbidden = {
        (PROJECT_ROOT / "data" / LEGACY_DEV_DB_NAME).resolve(),
        (PROJECT_ROOT / "data" / DEV_DB_NAME).resolve(),
    }
    try:
        resolved = path.expanduser().resolve()
    except OSError:
        return
    if resolved in forbidden:
        raise ProductionDatabaseError(
            "TEST DATABASE SAFETY ERROR\n"
            f"Refusing to use {resolved} for tests.\n"
            "Automated tests must use an isolated temporary database."
        )


def railway_volume_mount() -> Optional[Path]:
    """Railway injects this when a volume is attached. Do not invent a mount path."""
    raw = (os.environ.get("RAILWAY_VOLUME_MOUNT_PATH") or "").strip()
    if not raw:
        return None
    return _expand(raw)


def _existing_db_on_volume(mount: Path) -> Optional[Path]:
    """Prefer a real shop file already on the volume. Never create one here."""
    for candidate in (
        mount / "licorice.db",
        mount / "licorice-dev.db",
        mount / "data" / "licorice.db",
        mount / "data" / "licorice-dev.db",
    ):
        try:
            if candidate.is_file() and candidate.stat().st_size >= 100:
                return candidate
        except OSError:
            continue
    return None


def get_database_path() -> Path:
    """Single resolver for the SQLite file. Do not construct ``data/licorice.db`` elsewhere."""
    env = detect_environment()
    raw = (os.environ.get("DATABASE_PATH") or "").strip()

    if env == "production":
        if raw:
            return _expand(raw).resolve()
        mount = railway_volume_mount()
        if mount is not None:
            existing = _existing_db_on_volume(mount)
            if existing is not None:
                log.warning(
                    "DATABASE_PATH is unset; using existing Railway volume file %s. "
                    "Set DATABASE_PATH explicitly to this file.",
                    existing,
                )
                return existing.resolve()
            default = (mount / "licorice.db").resolve()
            raise ProductionDatabaseError(
                "CRITICAL DATABASE SAFETY ERROR\n"
                "A Railway volume is mounted, but no Liquorice Locker database file was found on it.\n"
                f"  volume mount: {mount.resolve()}\n"
                f"  looked for: {default}\n"
                "Refusing to create a blank production shop.\n"
                "Restore the live .db onto the volume, then set DATABASE_PATH to that file "
                f"(example: {default})."
            )
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production requires a persistent SQLite file.\n"
            "DATABASE_PATH is not set, and no Railway volume is attached "
            "(RAILWAY_VOLUME_MOUNT_PATH is empty).\n"
            "Refusing to fall back to a local SQLite file that would be wiped on the next deploy.\n\n"
            "In Railway:\n"
            "  1. Attach a Volume to this service and note the mount path.\n"
            "  2. Put the live database on that volume.\n"
            "  3. Set DATABASE_PATH to that file (example: /data/licorice.db).\n"
            "Optional: set DATABASE_VOLUME_ROOT to the same mount."
        )

    if raw:
        resolved = _expand(raw)
        if env == "test":
            _reject_dangerous_test_path(resolved)
        return resolved

    if env == "test":
        raise ProductionDatabaseError(
            "TEST DATABASE_PATH is not set. Tests must use an isolated temporary database."
        )

    legacy = PROJECT_ROOT / "data" / LEGACY_DEV_DB_NAME
    dev = PROJECT_ROOT / "data" / DEV_DB_NAME
    if legacy.is_file() and not dev.is_file():
        log.warning(
            "Using existing local database %s. New installs default to %s; "
            "set DATABASE_PATH to choose explicitly.",
            legacy,
            dev,
        )
        return legacy
    return dev


def expected_volume_root() -> Optional[Path]:
    raw = (os.environ.get("DATABASE_VOLUME_ROOT") or "").strip()
    if raw:
        return _expand(raw).resolve()
    mount = railway_volume_mount()
    if mount is not None:
        try:
            return mount.resolve()
        except OSError:
            return mount
    return None


def assert_production_path_on_volume(path: Path) -> None:
    """If DATABASE_VOLUME_ROOT is set, DATABASE_PATH must live under it.

    The repository does not record Railway's volume mount. Do not guess it.
    """
    root = expected_volume_root()
    if root is None:
        return
    try:
        path.resolve().relative_to(root)
    except ValueError as exc:
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            f"DATABASE_PATH={path} is not inside DATABASE_VOLUME_ROOT={root}.\n"
            "Refusing to start so production cannot write outside the persistent volume."
        ) from exc


def log_database_config(*, exists: bool, created_new: bool = False) -> None:
    path = get_database_path()
    log.info(
        "Database:\n  environment: %s\n  path: %s\n  exists: %s\n  created_new: %s",
        detect_environment(),
        path,
        exists,
        created_new,
    )
