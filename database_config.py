"""Authoritative SQLite path and environment detection for Liquorice Locker.

Production never silently falls back to a local file and never creates a blank
database. Development may create ``data/licorice-dev.db`` (or an existing
``data/licorice.db`` so local work is not stranded).
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

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


DATABASE_STATE_EXISTING = "EXISTING"
DATABASE_STATE_EMPTY_VOLUME_BOOTSTRAP = "EMPTY_VOLUME_BOOTSTRAP"
DATABASE_STATE_INVALID = "INVALID"
DATABASE_STATE_MISSING_VOLUME = "MISSING_VOLUME"
DATABASE_STATE_DEVELOPMENT = "DEVELOPMENT"
DATABASE_STATE_TEST = "TEST"

_last_database_state = DATABASE_STATE_DEVELOPMENT


def last_database_state() -> str:
    return _last_database_state


def set_last_database_state(state: str) -> None:
    global _last_database_state
    _last_database_state = state


def ephemeral_production_roots() -> Tuple[Path, ...]:
    return (PROJECT_ROOT / "data", Path("/app/data"))


def path_is_ephemeral_production_location(path: Path) -> bool:
    """True for container/app data dirs that are wiped on deploy. Not string-prefix based."""
    try:
        resolved = path.expanduser().resolve()
    except OSError:
        resolved = path
    for root in ephemeral_production_roots():
        try:
            root_resolved = root.resolve()
        except OSError:
            root_resolved = root
        if path_is_inside(resolved, root_resolved):
            return True
    return False


def classify_production_database_state(path: Optional[Path] = None) -> str:
    """EXISTING | EMPTY_VOLUME_BOOTSTRAP | INVALID | MISSING_VOLUME.

    Creation is allowed only on a Railway-injected volume mount that exists and
    contains the resolved path. Missing volume never creates a database.
    """
    env = detect_environment()
    if env == "test":
        return DATABASE_STATE_TEST
    if env != "production":
        return DATABASE_STATE_DEVELOPMENT
    try:
        resolved = path if path is not None else get_database_path()
    except ProductionDatabaseError:
        return DATABASE_STATE_MISSING_VOLUME

    if path_is_ephemeral_production_location(resolved):
        return DATABASE_STATE_INVALID

    mount = railway_volume_mount()
    configured = configured_volume_root()
    volume = configured
    if volume is None and mount is not None:
        try:
            volume = mount.resolve()
        except OSError:
            volume = mount
    if volume is None:
        return DATABASE_STATE_MISSING_VOLUME
    try:
        volume_ok = volume.is_dir()
    except OSError:
        volume_ok = False
    if not volume_ok:
        return DATABASE_STATE_MISSING_VOLUME
    if not path_is_inside(resolved, volume):
        return DATABASE_STATE_INVALID
    if mount is not None:
        try:
            mount_resolved = mount.resolve()
        except OSError:
            mount_resolved = mount
        if mount_resolved.is_dir() and not path_is_inside(resolved, mount_resolved):
            return DATABASE_STATE_INVALID

    if resolved.is_file():
        return DATABASE_STATE_EXISTING

    if mount is None:
        return DATABASE_STATE_MISSING_VOLUME
    try:
        mount_dir = mount.is_dir()
        mount_writable = os.access(mount, os.W_OK)
    except OSError:
        return DATABASE_STATE_MISSING_VOLUME
    if not mount_dir or not mount_writable:
        return DATABASE_STATE_MISSING_VOLUME
    if not path_is_inside(resolved, mount):
        return DATABASE_STATE_INVALID
    parent = resolved.parent
    try:
        if parent.is_dir():
            if not os.access(parent, os.W_OK):
                return DATABASE_STATE_INVALID
        else:
            if not (path_is_inside(parent, mount) or parent.resolve() == mount.resolve()):
                return DATABASE_STATE_INVALID
    except OSError:
        return DATABASE_STATE_INVALID
    return DATABASE_STATE_EMPTY_VOLUME_BOOTSTRAP


def raise_for_production_state(state: str, path: Optional[Path] = None) -> None:
    """Turn INVALID / MISSING_VOLUME into a startup error. Bootstrap states pass through."""
    if state in (DATABASE_STATE_EXISTING, DATABASE_STATE_EMPTY_VOLUME_BOOTSTRAP):
        return
    shown = display_database_path(path) if path is not None else "(unresolved)"
    if state == DATABASE_STATE_MISSING_VOLUME:
        raise ProductionDatabaseError(
            "CRITICAL DATABASE SAFETY ERROR\n"
            "Production database could not be verified.\n"
            f"DATABASE_PATH={shown}\n"
            "No verified Railway Volume is mounted.\n"
            "Refusing to create a database on ephemeral container disk.\n"
            "Attach web-volume at /data and set DATABASE_PATH=/data/licorice.db "
            "and DATABASE_VOLUME_ROOT=/data."
        )
    raise ProductionDatabaseError(
        "CRITICAL DATABASE SAFETY ERROR\n"
        "Production database could not be verified.\n"
        f"DATABASE_PATH={shown}\n"
        "Database state: INVALID.\n"
        "The path is outside the persistent Volume or on ephemeral storage "
        "(/app/data, data/licorice.db). Refusing to start."
    )


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


def _safe_stat_size(path: Path) -> Optional[int]:
    try:
        if path.is_file():
            return int(path.stat().st_size)
    except OSError:
        return None
    return None


def _count_table_readonly(conn: Any, table: str) -> Optional[int]:
    import sqlite3

    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            (table,),
        ).fetchone()
        if not row:
            return None
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    except sqlite3.Error:
        return None


def inspect_sqlite_file_readonly(path: Path) -> Dict[str, Any]:
    """Read-only probe. Never creates the file. No row contents, PII, or secrets."""
    import sqlite3

    report: Dict[str, Any] = {
        "path": str(path),
        "exists": False,
        "is_file": False,
        "size_bytes": None,
        "wal_size_bytes": None,
        "shm_size_bytes": None,
        "sqlite_valid": False,
        "migration_version": None,
        "migration_count": None,
        "products": None,
        "orders": None,
        "users": None,
        "open_error": None,
    }
    try:
        exists = path.exists()
    except OSError:
        report["open_error"] = "stat_failed"
        return report
    report["exists"] = bool(exists)
    if not exists:
        return report
    try:
        is_file = path.is_file()
    except OSError:
        report["open_error"] = "stat_failed"
        return report
    report["is_file"] = bool(is_file)
    if not is_file:
        return report
    report["size_bytes"] = _safe_stat_size(path)
    report["wal_size_bytes"] = _safe_stat_size(Path(str(path) + "-wal"))
    report["shm_size_bytes"] = _safe_stat_size(Path(str(path) + "-shm"))
    report["sqlite_valid"] = sqlite_header_ok(path)
    if not report["sqlite_valid"]:
        return report
    try:
        conn = sqlite3.connect(sqlite_uri(path, mode="ro"), uri=True)
    except sqlite3.Error:
        report["open_error"] = "sqlite_open_failed"
        return report
    try:
        try:
            conn.execute("PRAGMA query_only = ON")
        except sqlite3.Error:
            pass
        try:
            row = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations' LIMIT 1"
            ).fetchone()
            if row:
                versions = [
                    str(r[0])
                    for r in conn.execute("SELECT version FROM schema_migrations ORDER BY version")
                ]
                report["migration_count"] = len(versions)
                report["migration_version"] = versions[-1] if versions else "(none)"
        except sqlite3.Error:
            report["open_error"] = "sqlite_schema_read_failed"
        report["products"] = _count_table_readonly(conn, "products")
        report["orders"] = _count_table_readonly(conn, "orders")
        report["users"] = _count_table_readonly(conn, "users")
    finally:
        conn.close()
    return report


def inspect_directory_readonly(path: Path) -> Dict[str, Any]:
    """Directory presence and SQLite-looking filenames only. Never creates the directory."""
    report: Dict[str, Any] = {
        "path": str(path),
        "exists": False,
        "is_directory": False,
        "sqlite_filenames": [],
        "error": None,
    }
    try:
        exists = path.exists()
    except OSError:
        report["error"] = "stat_failed"
        return report
    report["exists"] = bool(exists)
    if not exists:
        return report
    try:
        is_dir = path.is_dir()
    except OSError:
        report["error"] = "stat_failed"
        return report
    report["is_directory"] = bool(is_dir)
    if not is_dir:
        return report
    names: List[str] = []
    try:
        for child in path.iterdir():
            name = child.name
            lower = name.lower()
            if lower.endswith(".db") or lower.endswith(".db-wal") or lower.endswith(".db-shm"):
                names.append(name)
    except OSError:
        report["error"] = "list_failed"
        return report
    report["sqlite_filenames"] = sorted(names)
    return report


def cutover_inspect_targets() -> List[Tuple[str, Path]]:
    """Candidate locations. Does not create or copy any of them."""
    targets: List[Tuple[str, Path]] = [
        ("volume_mount_dir", Path("/data")),
        ("volume_database", Path("/data") / PRODUCTION_DB_FILENAME),
        ("ephemeral_data_licorice_db", PROJECT_ROOT / "data" / LEGACY_DEV_DB_NAME),
    ]
    mount = railway_volume_mount()
    if mount is not None:
        targets.append(("railway_volume_mount_dir", mount))
        targets.append(("railway_volume_database", mount / PRODUCTION_DB_FILENAME))
    raw = _env_raw("DATABASE_PATH")
    if raw:
        targets.append(("configured_database_path", _expand(raw)))
    return targets


def inspect_production_cutover(*, targets: Optional[Sequence[Tuple[str, Path]]] = None) -> Dict[str, Any]:
    """Read-only Railway cutover probe. Does not bootstrap Flask or create databases."""
    chosen = list(targets) if targets is not None else cutover_inspect_targets()
    files: Dict[str, Any] = {}
    directories: Dict[str, Any] = {}
    file_cache: Dict[str, Dict[str, Any]] = {}
    dir_cache: Dict[str, Dict[str, Any]] = {}
    for label, path in chosen:
        try:
            is_dir = path.is_dir()
        except OSError:
            is_dir = False
        if is_dir or label.endswith("_dir"):
            key = str(path)
            if key not in dir_cache:
                dir_cache[key] = inspect_directory_readonly(path)
            directories[label] = dir_cache[key]
            continue
        key = str(path)
        if key not in file_cache:
            file_cache[key] = inspect_sqlite_file_readonly(path)
        files[label] = file_cache[key]

    volume_dir = directories.get("volume_mount_dir") or directories.get("railway_volume_mount_dir")
    volume_db = files.get("volume_database") or files.get("railway_volume_database")
    ephemeral = files.get("ephemeral_data_licorice_db")
    volume_empty = bool(
        volume_dir
        and volume_dir.get("exists")
        and volume_dir.get("is_directory")
        and not (volume_db and volume_db.get("exists"))
        and not (volume_dir.get("sqlite_filenames") or [])
    )
    return {
        "environment": detect_environment(),
        "DATABASE_PATH": _env_raw("DATABASE_PATH") or None,
        "RAILWAY_VOLUME_MOUNT_PATH": _env_raw("RAILWAY_VOLUME_MOUNT_PATH") or None,
        "DATABASE_VOLUME_ROOT": _env_raw("DATABASE_VOLUME_ROOT") or None,
        "cwd": str(Path.cwd()),
        "project_root": str(PROJECT_ROOT),
        "directories": directories,
        "files": files,
        "conclusions": {
            "volume_directory_exists": bool(volume_dir and volume_dir.get("exists")),
            "volume_database_exists": bool(volume_db and volume_db.get("is_file")),
            "ephemeral_data_licorice_db_exists": bool(ephemeral and ephemeral.get("is_file")),
            "volume_looks_empty_of_sqlite": volume_empty,
            "existing_production_database_accessible": bool(
                (volume_db and volume_db.get("sqlite_valid"))
                or (ephemeral and ephemeral.get("sqlite_valid"))
            ),
        },
        "created_or_copied_anything": False,
    }


def format_cutover_diagnostics(*, targets: Optional[Sequence[Tuple[str, Path]]] = None) -> str:
    info = inspect_production_cutover(targets=targets)
    files = info["files"]
    dirs = info["directories"]
    volume_db = files.get("volume_database") or files.get("railway_volume_database") or {}
    ephemeral = files.get("ephemeral_data_licorice_db") or {}
    volume_dir = dirs.get("volume_mount_dir") or dirs.get("railway_volume_mount_dir") or {}

    def _yn(val: Any) -> str:
        return "yes" if val else "no"

    def _file_block(title: str, probe: Dict[str, Any]) -> List[str]:
        if not probe:
            return [f"{title}: (not inspected)"]
        lines = [
            f"{title}: {probe.get('path')}",
            f"  exists: {_yn(probe.get('is_file'))}",
            f"  size_bytes: {probe.get('size_bytes')}",
            f"  wal_size_bytes: {probe.get('wal_size_bytes')}",
            f"  shm_size_bytes: {probe.get('shm_size_bytes')}",
            f"  sqlite_valid: {_yn(probe.get('sqlite_valid'))}",
            f"  migration_version: {probe.get('migration_version')}",
            f"  products: {probe.get('products')}",
            f"  orders: {probe.get('orders')}",
            f"  users: {probe.get('users')}",
        ]
        if probe.get("open_error"):
            lines.append(f"  open_error: {probe.get('open_error')}")
        return lines

    lines = [
        "Liquorice Locker read-only database location diagnostic",
        "This command does not start Gunicorn, does not create files, and does not copy data.",
        "",
        f"Environment: {info['environment']}",
        f"DATABASE_PATH: {info['DATABASE_PATH'] or '(empty)'}",
        f"RAILWAY_VOLUME_MOUNT_PATH: {info['RAILWAY_VOLUME_MOUNT_PATH'] or '(empty)'}",
        f"DATABASE_VOLUME_ROOT: {info['DATABASE_VOLUME_ROOT'] or '(empty)'}",
        "",
        f"/data exists: {_yn(volume_dir.get('exists'))}",
        f"/data is directory: {_yn(volume_dir.get('is_directory'))}",
        f"/data sqlite filenames: {', '.join(volume_dir.get('sqlite_filenames') or []) or '(none)'}",
        "",
        *_file_block("/data/licorice.db", volume_db),
        "",
        *_file_block("data/licorice.db (container/app tree)", ephemeral),
        "",
        "Conclusions:",
        f"  A existing shop DB accessible: {_yn(info['conclusions']['existing_production_database_accessible'])}",
        f"  B ephemeral data/licorice.db present: {_yn(info['conclusions']['ephemeral_data_licorice_db_exists'])}",
        f"  C /data volume has no SQLite files: {_yn(info['conclusions']['volume_looks_empty_of_sqlite'])}",
        f"  D destination file (do not create automatically): /data/licorice.db",
        "",
        "If ephemeral data/licorice.db exists and /data/licorice.db does not,",
        "the operator must copy the live file onto the volume later. This command did not copy it.",
    ]
    return "\n".join(lines)


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
    database_state: str = "",
) -> None:
    persistent = "VERIFIED" if is_production() else "local"
    state = database_state or last_database_state()
    if state == DATABASE_STATE_EMPTY_VOLUME_BOOTSTRAP:
        state_label = "EMPTY VOLUME BOOTSTRAP"
    elif state == DATABASE_STATE_EXISTING:
        state_label = "EXISTING DATABASE"
    else:
        state_label = state or detect_environment()
    log.info(
        "LIQUORICE LOCKER DATABASE\n"
        "Environment: %s\n"
        "Database: %s\n"
        "Database state: %s\n"
        "Persistent storage: %s\n"
        "SQLite: OK\n"
        "WAL: %s\n"
        "Foreign keys: %s\n"
        "Migration: %s\n"
        "Integrity: %s",
        detect_environment(),
        display_database_path(path),
        state_label,
        persistent,
        "ENABLED" if str(journal_mode).lower() == "wal" else journal_mode,
        "ENABLED" if foreign_keys else "disabled",
        migration,
        "PASS" if integrity_ok else "FAILED",
    )


if __name__ == "__main__":
    print(format_cutover_diagnostics())
