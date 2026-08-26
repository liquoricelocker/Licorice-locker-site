"""Authoritative SQLite path and environment detection for Liquorice Locker.

Production never silently falls back to a local file and never creates a blank
database. Development may create ``data/licorice-dev.db`` (or an existing
``data/licorice.db`` so local work is not stranded).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger("licorice.database")

PROJECT_ROOT = Path(__file__).resolve().parent
DEV_DB_NAME = "licorice-dev.db"
LEGACY_DEV_DB_NAME = "licorice.db"
SQLITE_HEADER = b"SQLite format 3\x00"
PRODUCTION_DB_FILENAME = "licorice.db"
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


def _env_raw(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def path_is_inside(path: Path, root: Path) -> bool:
    """True if ``path`` is ``root`` or a descendant. Follows symlinks. Not a string prefix."""
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except (ValueError, OSError):
        return False


def sqlite_uri(path: Path, *, mode: str) -> str:
    """SQLite URI. ``mode=rw`` fails if the file is missing instead of creating it."""
    posix = Path(path).expanduser().resolve().as_posix()
    return f"file:{posix}?mode={mode}"


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
    raw = _env_raw("RAILWAY_VOLUME_MOUNT_PATH")
    if not raw:
        return None
    return _expand(raw)


def configured_volume_root() -> Optional[Path]:
    """Explicit ``DATABASE_VOLUME_ROOT`` only. Used for fail-loud containment checks."""
    raw = _env_raw("DATABASE_VOLUME_ROOT")
    if not raw:
        return None
    try:
        return _expand(raw).resolve()
    except OSError:
        return _expand(raw)


def expected_volume_root() -> Optional[Path]:
    """Health/reporting root: explicit ``DATABASE_VOLUME_ROOT``, else Railway mount."""
    configured = configured_volume_root()
    if configured is not None:
        return configured
    mount = railway_volume_mount()
    if mount is not None:
        try:
            return mount.resolve()
        except OSError:
            return mount
    return None


def display_database_path(path: Path) -> str:
    """Log operational volume paths as-is; avoid dumping unexpected private prefixes."""
    try:
        resolved = path.expanduser().resolve()
    except OSError:
        resolved = path
    posix = resolved.as_posix()
    if posix == "/data" or posix.startswith("/data/"):
        return posix
    mount = railway_volume_mount()
    if mount is not None and path_is_inside(resolved, mount):
        return posix
    root = configured_volume_root()
    if root is not None and path_is_inside(resolved, root):
        return posix
    raw = _env_raw("DATABASE_PATH")
    if raw:
        return f"DATABASE_PATH={raw}"
    return resolved.name


def _require_absolute_production_path(path: Path, *, source: str) -> Path:
    if not path.is_absolute():
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"{source} must be an absolute path (example: /data/licorice.db).\n"
            "A relative path can resolve onto ephemeral container disk.\n"
            "No production database will be created automatically."
        )
    try:
        return path.resolve()
    except OSError:
        return path


def get_database_path() -> Path:
    """Single resolver for the SQLite file. Do not construct ``data/licorice.db`` elsewhere.

    Production priority:
      1. ``DATABASE_PATH`` if set
      2. ``<RAILWAY_VOLUME_MOUNT_PATH>/licorice.db`` if a volume is attached
      3. fail loud — never ``data/licorice.db``
    Existence is checked later. This function only resolves the path.
    """
    env = detect_environment()
    raw = _env_raw("DATABASE_PATH")

    if env == "production":
        if raw:
            return _require_absolute_production_path(_expand(raw), source="DATABASE_PATH")
        mount = railway_volume_mount()
        if mount is not None:
            derived = mount / PRODUCTION_DB_FILENAME
            return _require_absolute_production_path(
                derived, source="RAILWAY_VOLUME_MOUNT_PATH"
            )
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production requires a persistent SQLite file.\n"
            "DATABASE_PATH is not set, and no Railway volume is attached "
            "(RAILWAY_VOLUME_MOUNT_PATH is empty).\n"
            "Refusing to fall back to a local SQLite file that would be wiped on the next deploy.\n\n"
            "In Railway:\n"
            "  1. Attach a Volume to this service and note the mount path.\n"
            "  2. Put the live database on that volume (the app will not copy it for you).\n"
            "  3. Set DATABASE_PATH to that file (example: /data/licorice.db).\n"
            "  4. Set DATABASE_VOLUME_ROOT to the same mount (example: /data)."
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


def assert_production_path_on_volume(path: Path) -> None:
    """If DATABASE_VOLUME_ROOT is set, DATABASE_PATH must live under it.

    Uses Path.relative_to after resolve(), not a string prefix. The repository
    does not record Railway's volume mount. Do not guess it.
    """
    root = configured_volume_root()
    if root is None:
        return
    if path_is_inside(path, root):
        return
    shown = display_database_path(path)
    raise ProductionDatabaseError(
        "CRITICAL DATABASE SAFETY ERROR\n"
        "Production database could not be verified.\n"
        f"DATABASE_PATH={shown}\n"
        f"DATABASE_VOLUME_ROOT={root.as_posix()}\n"
        "The resolved database path is not inside DATABASE_VOLUME_ROOT.\n"
        "Refusing to start so production cannot write outside the persistent volume."
    )


def assert_production_database_file(path: Path) -> None:
    """Filesystem checks before sqlite3.connect. Never creates the file."""
    parent = path.parent
    if not parent.exists() or not parent.is_dir():
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The parent directory does not exist. The Railway volume may not be mounted.\n"
            "No production database will be created automatically."
        )
    if not path.exists():
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The file does not exist.\n"
            "Refusing to create a new production database.\n"
            "No production database will be created automatically."
        )
    if not path.is_file():
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The path exists but is not a regular file.\n"
            "No production database will be created automatically."
        )
    if not os.access(path, os.R_OK):
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The file is not readable."
        )
    if not os.access(path, os.W_OK):
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The file is not writable."
        )
    try:
        size = path.stat().st_size
    except OSError as exc:
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            f"Could not stat the file: {exc}"
        ) from exc
    if size < 100:
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The file is too small to be a real store database.\n"
            "Refusing to start."
        )
    try:
        with open(path, "rb") as fh:
            header = fh.read(16)
    except OSError as exc:
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            f"Could not read the SQLite header: {exc}"
        ) from exc
    if header != SQLITE_HEADER:
        shown = display_database_path(path)
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "The file is not a valid SQLite database.\n"
            "No production database will be created automatically."
        )


def sqlite_header_ok(path: Path) -> bool:
    try:
        if not path.is_file():
            return False
        with open(path, "rb") as fh:
            return fh.read(16) == SQLITE_HEADER
    except OSError:
        return False


def railway_diagnostics() -> Dict[str, Any]:
    """Safe operator report. No secrets, Stripe keys, or credentials."""
    mount = railway_volume_mount()
    raw_path = _env_raw("DATABASE_PATH") or None
    raw_root = _env_raw("DATABASE_VOLUME_ROOT") or None
    resolved: Optional[Path] = None
    resolve_error: Optional[str] = None
    try:
        resolved = get_database_path()
    except ProductionDatabaseError as exc:
        resolve_error = str(exc)
    exists = bool(resolved and resolved.is_file())
    root = expected_volume_root()
    inside: Optional[bool] = None
    if resolved is not None and root is not None:
        inside = path_is_inside(resolved, root)
    persistent = bool(mount) and exists and (inside is not False)
    return {
        "environment": detect_environment(),
        "railway_volume_detected": bool(mount),
        "RAILWAY_VOLUME_MOUNT_PATH": mount.as_posix() if mount is not None else None,
        "DATABASE_PATH": raw_path,
        "DATABASE_VOLUME_ROOT": raw_root,
        "resolved_database": resolved.as_posix() if resolved is not None else None,
        "database_exists": exists,
        "database_persistent": persistent,
        "inside_volume": inside,
        "resolve_error": resolve_error,
    }


def format_railway_diagnostics() -> str:
    info = railway_diagnostics()
    lines = [
        f"Environment: {info['environment']}",
        f"Railway Volume detected: {'yes' if info['railway_volume_detected'] else 'no'}",
        f"RAILWAY_VOLUME_MOUNT_PATH: {info['RAILWAY_VOLUME_MOUNT_PATH'] or '(empty)'}",
        f"DATABASE_PATH: {info['DATABASE_PATH'] or '(empty)'}",
        f"DATABASE_VOLUME_ROOT: {info['DATABASE_VOLUME_ROOT'] or '(empty)'}",
        f"Resolved database: {info['resolved_database'] or '(not resolved)'}",
        f"Database exists: {'yes' if info['database_exists'] else 'no'}",
        f"Database persistent: {'yes' if info['database_persistent'] else 'no'}",
    ]
    if info.get("resolve_error"):
        lines.append("")
        lines.append(str(info["resolve_error"]))
    return "\n".join(lines)


def log_database_config(*, exists: bool, created_new: bool = False) -> None:
    path = get_database_path()
    log.info(
        "Database:\n  environment: %s\n  path: %s\n  exists: %s\n  created_new: %s",
        detect_environment(),
        display_database_path(path),
        exists,
        created_new,
    )


def log_startup_success(
    *,
    path: Path,
    journal_mode: str,
    foreign_keys: bool,
    migration: str,
    integrity_ok: bool,
) -> None:
    persistent = "verified" if is_production() else "local"
    log.info(
        "Liquorice Locker database\n"
        "Environment: %s\n"
        "Database: %s\n"
        "Persistent storage: %s\n"
        "SQLite: OK\n"
        "WAL: %s\n"
        "Foreign keys: %s\n"
        "Migration: %s\n"
        "Integrity: %s",
        detect_environment(),
        display_database_path(path),
        persistent,
        "enabled" if str(journal_mode).lower() == "wal" else journal_mode,
        "enabled" if foreign_keys else "disabled",
        migration,
        "OK" if integrity_ok else "FAILED",
    )


if __name__ == "__main__":
    print(format_railway_diagnostics())
