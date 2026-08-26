"""Railway production path resolution. Isolated temp databases only."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class RailwayDatabaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "licorice-test.db")
        os.environ["LICORICE_ENV"] = "test"
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ.pop("RAILWAY_ENVIRONMENT", None)
        os.environ.pop("RAILWAY_VOLUME_MOUNT_PATH", None)
        os.environ.pop("RENDER", None)
        os.environ.pop("DATABASE_VOLUME_ROOT", None)
        os.environ.pop("INVENTORY_ENFORCE", None)
        os.environ["SECRET_KEY"] = "test-secret-licorice"

        import database_config
        import db as database

        self.database = database
        self.database_config = database_config

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _boot_isolated_shop(self) -> None:
        os.environ["LICORICE_ENV"] = "test"
        os.environ["DATABASE_PATH"] = self.db_path
        self.database.bootstrap()

    def test_explicit_database_path_wins(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        target = Path(self._tmp.name) / "volume" / "custom.db"
        target.parent.mkdir(parents=True, exist_ok=True)
        os.environ["DATABASE_PATH"] = str(target)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(Path(self._tmp.name) / "volume")
        path = self.database_config.get_database_path()
        self.assertEqual(path, target.resolve())

    def test_railway_volume_derives_licorice_db(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        mount = Path(self._tmp.name) / "data-mount"
        mount.mkdir(parents=True, exist_ok=True)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(mount)
        path = self.database_config.get_database_path()
        self.assertEqual(path, (mount / "licorice.db").resolve())

    def test_both_configured_database_path_takes_precedence(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        mount = Path(self._tmp.name) / "data"
        mount.mkdir(parents=True, exist_ok=True)
        chosen = mount / "live.db"
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(mount)
        os.environ["DATABASE_PATH"] = str(chosen)
        path = self.database_config.get_database_path()
        self.assertEqual(path, chosen.resolve())
        self.assertNotEqual(path.name, "licorice.db")

    def test_neither_configured_fails(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("RAILWAY_VOLUME_MOUNT_PATH", None)
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database_config.get_database_path()
        self.assertIn("RAILWAY_VOLUME_MOUNT_PATH is empty", str(ctx.exception))
        self.assertIn("Refusing to fall back to a local SQLite file", str(ctx.exception))

    def test_path_outside_volume_root_fails(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        outside = Path(self._tmp.name) / "ephemeral" / "licorice.db"
        outside.parent.mkdir(parents=True, exist_ok=True)
        volume = Path(self._tmp.name) / "data"
        volume.mkdir(parents=True, exist_ok=True)
        os.environ["DATABASE_PATH"] = str(outside)
        os.environ["DATABASE_VOLUME_ROOT"] = str(volume)
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database_config.assert_production_path_on_volume(
                self.database_config.get_database_path()
            )
        self.assertIn("not inside DATABASE_VOLUME_ROOT", str(ctx.exception))

    def test_path_inside_volume_root_is_valid(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        volume = Path(self._tmp.name) / "data"
        volume.mkdir(parents=True, exist_ok=True)
        dbfile = volume / "licorice.db"
        os.environ["DATABASE_PATH"] = str(dbfile)
        os.environ["DATABASE_VOLUME_ROOT"] = str(volume)
        path = self.database_config.get_database_path()
        self.database_config.assert_production_path_on_volume(path)
        self.assertTrue(self.database_config.path_is_inside(path, volume.resolve()))

    def test_volume_root_not_required_when_unset(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = str(Path(self._tmp.name) / "anywhere.db")
        os.environ.pop("DATABASE_VOLUME_ROOT", None)
        path = self.database_config.get_database_path()
        self.database_config.assert_production_path_on_volume(path)

    def test_missing_database_file_does_not_create(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        missing = Path(self._tmp.name) / "data" / "licorice.db"
        missing.parent.mkdir(parents=True, exist_ok=True)
        os.environ["DATABASE_PATH"] = str(missing)
        os.environ["DATABASE_VOLUME_ROOT"] = str(missing.parent)
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database.bootstrap()
        self.assertIn("Volume", str(ctx.exception))
        self.assertFalse(missing.exists())
        with self.assertRaises(self.database_config.ProductionDatabaseError):
            self.database.get_connection()
        self.assertFalse(missing.exists())

    def test_parent_directory_missing_fails(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        missing = Path(self._tmp.name) / "not-mounted" / "licorice.db"
        os.environ["DATABASE_PATH"] = str(missing)
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database.bootstrap()
        self.assertIn("Volume", str(ctx.exception))
        self.assertFalse(missing.exists())

    def test_invalid_sqlite_header_fails(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        junk = Path(self._tmp.name) / "data" / "licorice.db"
        junk.parent.mkdir(parents=True, exist_ok=True)
        junk.write_bytes(b"x" * 200)
        os.environ["DATABASE_PATH"] = str(junk)
        os.environ["DATABASE_VOLUME_ROOT"] = str(junk.parent)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(junk.parent)
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database.bootstrap()
        self.assertIn("not a valid SQLite database", str(ctx.exception))

    def test_valid_production_database_opens(self) -> None:
        self._boot_isolated_shop()
        volume = Path(self.db_path).parent
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["DATABASE_VOLUME_ROOT"] = str(volume)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(volume)
        self.database.bootstrap()
        with self.database.get_db(commit=False) as db:
            n = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
        self.assertGreaterEqual(n, 1)

    def test_production_rejects_relative_local_fallback(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = "data/licorice.db"
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database_config.get_database_path()
        self.assertIn("absolute path", str(ctx.exception))

    def test_development_fallback_allows_local_file(self) -> None:
        os.environ["LICORICE_ENV"] = "development"
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("RAILWAY_VOLUME_MOUNT_PATH", None)
        path = self.database_config.get_database_path()
        self.assertIn(path.name, ("licorice.db", "licorice-dev.db"))
        self.assertEqual(path.parent, PROJECT_ROOT / "data")

    def test_tests_refuse_known_shop_paths(self) -> None:
        os.environ["LICORICE_ENV"] = "test"
        os.environ["DATABASE_PATH"] = str(PROJECT_ROOT / "data" / "licorice-dev.db")
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database_config.get_database_path()
        self.assertIn("isolated temporary database", str(ctx.exception))

    def test_diagnostics_have_no_secrets(self) -> None:
        os.environ["STRIPE_SECRET_KEY"] = "sk_live_fake"
        os.environ["SECRET_KEY"] = "super-secret"
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        text = self.database_config.format_railway_diagnostics()
        self.assertIn("Railway Volume detected:", text)
        self.assertNotIn("sk_live_fake", text)
        self.assertNotIn("super-secret", text)
        info = self.database_config.railway_diagnostics()
        blob = str(info)
        self.assertNotIn("sk_live_fake", blob)
        self.assertNotIn("super-secret", blob)

    def test_sqlite_rw_uri_does_not_create(self) -> None:
        missing = Path(self._tmp.name) / "nope.db"
        uri = self.database_config.sqlite_uri(missing, mode="rw")
        with self.assertRaises(sqlite3.OperationalError):
            sqlite3.connect(uri, uri=True)
        self.assertFalse(missing.exists())

    def test_health_reports_railway_fields(self) -> None:
        self._boot_isolated_shop()
        with self.database.get_db(commit=False) as db:
            report = self.database.database_health_report(db)
        self.assertEqual(report["environment"], "test")
        self.assertIn("status", report["persistence"])
        self.assertIn("inside_volume", report["persistence"])
        self.assertTrue(report["sqlite"]["foreign_keys"])
        self.assertEqual(report["sqlite"]["busy_timeout_ms"], 5000)
        self.assertIn("013_canonical_shipping_rates", report["schema"]["migration_version"])
        self.assertGreaterEqual(report["schema"]["core_table_counts"]["products"], 1)
        self.assertNotIn("sk_live", str(report).lower())

    def test_cutover_inspect_does_not_create_files(self) -> None:
        missing_dir = Path(self._tmp.name) / "no-data"
        missing_db = missing_dir / "licorice.db"
        ephemeral_missing = Path(self._tmp.name) / "app-data" / "licorice.db"
        text = self.database_config.format_cutover_diagnostics(
            targets=(
                ("volume_mount_dir", missing_dir),
                ("volume_database", missing_db),
                ("ephemeral_data_licorice_db", ephemeral_missing),
            )
        )
        self.assertFalse(missing_dir.exists())
        self.assertFalse(missing_db.exists())
        self.assertFalse(ephemeral_missing.exists())
        self.assertIn("does not create files", text)
        self.assertIn("exists: no", text)
        self.assertNotIn("sk_live", text)

    def test_cutover_inspect_reads_existing_sqlite_only(self) -> None:
        self._boot_isolated_shop()
        before = Path(self.db_path).stat().st_mtime
        volume = Path(self._tmp.name) / "empty-volume"
        volume.mkdir()
        info = self.database_config.inspect_production_cutover(
            targets=(
                ("volume_mount_dir", volume),
                ("volume_database", volume / "licorice.db"),
                ("ephemeral_data_licorice_db", Path(self.db_path)),
            )
        )
        after = Path(self.db_path).stat().st_mtime
        self.assertEqual(before, after)
        self.assertFalse((volume / "licorice.db").exists())
        self.assertTrue(info["conclusions"]["ephemeral_data_licorice_db_exists"])
        self.assertFalse(info["conclusions"]["volume_database_exists"])
        self.assertTrue(info["conclusions"]["volume_looks_empty_of_sqlite"])
        eph = info["files"]["ephemeral_data_licorice_db"]
        self.assertTrue(eph["sqlite_valid"])
        self.assertGreaterEqual(eph["products"], 1)
        self.assertIsInstance(eph["orders"], int)
        self.assertIsInstance(eph["users"], int)
        self.assertIn("013_canonical_shipping_rates", eph["migration_version"])
        blob = str(info)
        self.assertNotIn("password", blob.lower())
        self.assertNotIn("@", blob)


if __name__ == "__main__":
    unittest.main()
