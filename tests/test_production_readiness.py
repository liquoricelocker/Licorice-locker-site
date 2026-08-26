"""Production-readiness tests. Isolated temp databases only — never data/licorice.db."""

from __future__ import annotations

import os
import re
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path

from werkzeug.security import generate_password_hash

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class IsolatedDbTest(unittest.TestCase):
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
        import inventory
        import commerce_state
        from migrations import runner as mig

        self.database = database
        self.database_config = database_config
        self.inventory = inventory
        self.commerce_state = commerce_state
        self.mig = mig

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _product(self, db: sqlite3.Connection, slug: str = "riff"):
        return db.execute("SELECT * FROM products WHERE slug = ?", (slug,)).fetchone()


class ProductionSafetyTests(IsolatedDbTest):
    def test_production_never_falls_back_to_local_file(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("RAILWAY_VOLUME_MOUNT_PATH", None)
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database_config.get_database_path()
        self.assertIn("DATABASE_PATH", str(ctx.exception))

    def test_production_uses_existing_file_on_railway_volume(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        mount = Path(self._tmp.name) / "volume"
        mount.mkdir(parents=True, exist_ok=True)
        dbfile = mount / "licorice.db"
        dbfile.write_bytes(b"x" * 200)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(mount)
        path = self.database_config.get_database_path()
        self.assertEqual(path, dbfile.resolve())

    def test_production_empty_railway_volume_refuses_to_create(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        mount = Path(self._tmp.name) / "empty-volume"
        mount.mkdir(parents=True, exist_ok=True)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(mount)
        path = self.database_config.get_database_path()
        self.assertEqual(path, (mount / "licorice.db").resolve())
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database.bootstrap()
        self.assertIn("does not exist", str(ctx.exception))
        self.assertFalse(path.exists())

    def test_production_volume_root_mismatch_fails(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = str(Path(self._tmp.name) / "outside.db")
        os.environ["DATABASE_VOLUME_ROOT"] = str(Path(self._tmp.name) / "volume")
        Path(os.environ["DATABASE_VOLUME_ROOT"]).mkdir(parents=True, exist_ok=True)
        with self.assertRaises(self.database_config.ProductionDatabaseError):
            self.database_config.assert_production_path_on_volume(
                self.database_config.get_database_path()
            )

    def test_tests_cannot_use_developer_database(self) -> None:
        os.environ["LICORICE_ENV"] = "test"
        os.environ["DATABASE_PATH"] = str(PROJECT_ROOT / "data" / "licorice.db")
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database_config.get_database_path()
        self.assertIn("isolated", str(ctx.exception).lower())

    def test_catalogue_price_constants_cannot_overwrite(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            with self.assertRaises(RuntimeError):
                self.database._sync_product_catalog_prices(db)


class MoneyAndStateTests(IsolatedDbTest):
    def test_exact_cents_persist(self) -> None:
        self.database.bootstrap()
        from commissions import commission_cents_for_nth_sale

        self.assertEqual(commission_cents_for_nth_sale(1, 12900), 2580)
        self.assertEqual(commission_cents_for_nth_sale(1, 9999), 2000)
        self.assertEqual(commission_cents_for_nth_sale(1, 42900), 8580)
        with self.database.get_db() as db:
            for slug, cents in (("melody", 9999), ("sound-wave", 42900), ("riff", 9999)):
                p = self._product(db, slug)
                cur = db.execute(
                    """
                    INSERT INTO orders (
                        order_number, order_type, customer_first, customer_last, customer_email,
                        shipping_line1, shipping_city, shipping_postal, total_cents
                    ) VALUES (?, 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', ?)
                    """,
                    (f"LL-MONEY-{slug}", cents),
                )
                oid = int(cur.lastrowid)
                self.database.insert_order_item_with_snapshot(
                    db, order_id=oid, product=p, quantity=1, unit_price_cents=cents
                )
        with self.database.get_db() as db:
            for slug, cents in (("melody", 9999), ("sound-wave", 42900), ("riff", 9999)):
                row = db.execute(
                    """
                    SELECT oi.unit_price_cents FROM order_items oi
                    JOIN orders o ON o.id = oi.order_id
                    WHERE o.order_number = ?
                    """,
                    (f"LL-MONEY-{slug}",),
                ).fetchone()
                self.assertEqual(int(row["unit_price_cents"]), cents)

    def test_invalid_financial_values_rejected(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            p = self._product(db)
            cur = db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-BAD-MONEY', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 0)
                """
            )
            oid = int(cur.lastrowid)
            with self.assertRaises(ValueError):
                self.database.insert_order_item_with_snapshot(
                    db, order_id=oid, product=p, quantity=1, unit_price_cents=-1
                )
            with self.assertRaises(ValueError):
                self.database.attach_commerce_records_for_new_order(
                    db,
                    order_id=oid,
                    csid="cs_neg",
                    email="a@b.c",
                    first="A",
                    last="B",
                    phone="",
                    total_cents=-5,
                    payment_method="stripe",
                )

    def test_order_and_payment_transitions(self) -> None:
        cs = self.commerce_state
        self.assertTrue(cs.order_transition_allowed("completed", "paid", True))
        self.assertFalse(cs.order_transition_allowed("cancelled", "cancelled", True))
        self.assertFalse(cs.order_transition_allowed("delivered", "delivered", False))
        self.assertTrue(cs.order_transition_allowed("shipped", "shipped", False))
        self.assertTrue(cs.payment_transition_allowed("pending", "succeeded"))
        self.assertTrue(cs.payment_transition_allowed("succeeded", "succeeded"))
        self.assertFalse(cs.payment_transition_allowed("succeeded", "failed"))
        self.assertFalse(cs.payment_transition_allowed("refunded", "pending"))


class InventoryLedgerTests(IsolatedDbTest):
    def test_ledger_reconstruction_and_discrepancy_report(self) -> None:
        self.database.bootstrap()
        with self.database.get_db(immediate=True) as db:
            item = db.execute("SELECT id FROM inventory_items ORDER BY id LIMIT 1").fetchone()
            iid = int(item["id"])
            db.execute(
                "UPDATE inventory_items SET quantity_on_hand = 0, quantity_reserved = 0 WHERE id = ?",
                (iid,),
            )
            self.inventory.receive_inventory(db, iid, 10, reason="open", reference_type="po", reference_id=1)
            self.inventory.consume_inventory_sale(db, iid, 3, reference_type="order", reference_id=11)
            self.inventory.return_inventory(db, iid, 1, reference_type="order", reference_id=11)
            self.inventory.adjust_inventory(db, iid, -1, reason="damage write-off")
            stored = int(db.execute("SELECT quantity_on_hand FROM inventory_items WHERE id = ?", (iid,)).fetchone()[0])
            recon = self.inventory.reconstructed_on_hand(db, iid)
            self.assertEqual(stored, recon)
            self.assertEqual(recon, 7)
            db.execute("UPDATE inventory_items SET quantity_on_hand = 99 WHERE id = ?", (iid,))
            disc = self.inventory.ledger_discrepancies(db)
            self.assertTrue(any(d["inventory_item_id"] == iid and d["stored_on_hand"] == 99 for d in disc))

    def test_duplicate_receipt_and_return_skipped(self) -> None:
        self.database.bootstrap()
        with self.database.get_db(immediate=True) as db:
            iid = int(db.execute("SELECT id FROM inventory_items ORDER BY id LIMIT 1").fetchone()["id"])
            db.execute(
                "UPDATE inventory_items SET quantity_on_hand = 0, quantity_reserved = 0 WHERE id = ?",
                (iid,),
            )
            self.inventory.receive_inventory(db, iid, 4, reference_type="po", reference_id=77)
            self.inventory.receive_inventory(db, iid, 4, reference_type="po", reference_id=77)
            self.assertEqual(
                int(db.execute("SELECT quantity_on_hand FROM inventory_items WHERE id = ?", (iid,)).fetchone()[0]),
                4,
            )

    def test_ten_concurrent_reservations_never_exceed_stock(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            iid = int(db.execute("SELECT id FROM inventory_items ORDER BY id LIMIT 1").fetchone()["id"])
            db.execute(
                "UPDATE inventory_items SET quantity_on_hand = 5, quantity_reserved = 0 WHERE id = ?",
                (iid,),
            )
        ok = {"n": 0}
        lock = threading.Lock()

        def attempt() -> None:
            try:
                with self.database.get_db(immediate=True) as db:
                    self.inventory.reserve_inventory(db, iid, 1, reason="load")
                with lock:
                    ok["n"] += 1
            except self.inventory.InsufficientStock:
                pass
            except sqlite3.OperationalError:
                pass

        threads = [threading.Thread(target=attempt) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=15)
        self.assertEqual(ok["n"], 5)
        with self.database.get_db() as db:
            reserved = int(
                db.execute("SELECT quantity_reserved FROM inventory_items WHERE id = ?", (iid,)).fetchone()[0]
            )
            self.assertEqual(reserved, 5)


class StripeRepairBackupTests(IsolatedDbTest):
    def test_incomplete_order_repair_once(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            p = self._product(db, "harmony")
            db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents,
                    stripe_checkout_session_id
                ) VALUES ('LL-INCOMPLETE', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 9999, 'cs_repair')
                """
            )
            oid = int(db.execute("SELECT id FROM orders WHERE order_number = 'LL-INCOMPLETE'").fetchone()["id"])
            first = self.database.fill_order_items_if_empty(db, oid, [(p, 1, int(p["price_cents"]))])
            second = self.database.fill_order_items_if_empty(db, oid, [(p, 1, int(p["price_cents"]))])
            self.assertTrue(first)
            self.assertFalse(second)
            self.assertEqual(
                int(db.execute("SELECT COUNT(*) FROM order_items WHERE order_id = ?", (oid,)).fetchone()[0]),
                1,
            )

    def test_fill_without_lines_does_not_fabricate(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-NOLINES', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 1)
                """
            )
            oid = int(db.execute("SELECT id FROM orders WHERE order_number = 'LL-NOLINES'").fetchone()["id"])
            with self.assertRaises(ValueError):
                self.database.fill_order_items_if_empty(db, oid, [])

    def test_backup_restore_roundtrip(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            db.execute("UPDATE products SET price_cents = 12900 WHERE slug = 'sound-wave'")
        backup = Path(self._tmp.name) / "backup.db"
        restored = Path(self._tmp.name) / "restored.db"
        self.database.restore_sqlite_backup(backup_path=Path(self.db_path), destination=backup)
        with self.database.get_db() as db:
            db.execute("UPDATE products SET price_cents = 1 WHERE slug = 'sound-wave'")
        self.database.restore_sqlite_backup(backup_path=backup, destination=restored)
        conn = sqlite3.connect(str(restored))
        conn.row_factory = sqlite3.Row
        try:
            cents = int(conn.execute("SELECT price_cents FROM products WHERE slug = 'sound-wave'").fetchone()[0])
            self.assertEqual(cents, 12900)
        finally:
            conn.close()

    def test_failed_migration_is_not_stamped(self) -> None:
        self.database.bootstrap()

        def boom(_db: sqlite3.Connection) -> None:
            _db.execute("CREATE TABLE IF NOT EXISTS mig_probe_fail (id INTEGER PRIMARY KEY)")
            raise RuntimeError("intentional_migration_failure")

        original = list(self.mig.MIGRATIONS)
        self.mig.MIGRATIONS = original + [("999_fail", boom)]
        try:
            with self.assertRaises(RuntimeError):
                with self.database.get_db() as db:
                    self.mig.run_migrations(db)
            with self.database.get_db() as db:
                versions = [r[0] for r in db.execute("SELECT version FROM schema_migrations")]
                self.assertNotIn("999_fail", versions)
        finally:
            self.mig.MIGRATIONS = original


class CsrfAndHealthTests(IsolatedDbTest):
    def test_csrf_admin_post(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            db.execute(
                """
                INSERT INTO users (email, password_hash, role, full_name)
                VALUES (?, ?, 'admin', 'Staff')
                """,
                (
                    "joshuafrenchdesign@gmail.com",
                    generate_password_hash("correct-horse", method="pbkdf2:sha256"),
                ),
            )
        import importlib
        import app as app_module

        importlib.reload(app_module)
        client = app_module.app.test_client()
        login = client.post(
            "/login",
            data={"email": "joshuafrenchdesign@gmail.com", "password": "correct-horse"},
            follow_redirects=False,
        )
        self.assertIn(login.status_code, (302, 303))
        dash = client.get("/dashboard/admin")
        self.assertEqual(dash.status_code, 200)
        m = re.search(rb'name="csrf-token" content="([^"]+)"', dash.data)
        if not m:
            m = re.search(rb'name="csrf_token" value="([^"]+)"', dash.data)
        self.assertIsNotNone(m)
        token = m.group(1).decode("ascii")
        missing = client.post("/dashboard/admin/mark-affiliate-paid", data={"affiliate_user_id": "1"})
        self.assertEqual(missing.status_code, 403)
        bad = client.post(
            "/dashboard/admin/mark-affiliate-paid",
            data={"csrf_token": "not-the-session-token", "affiliate_user_id": "1", "year_month": "2026-08"},
        )
        self.assertEqual(bad.status_code, 403)
        valid = client.post(
            "/dashboard/admin/mark-affiliate-paid",
            data={
                "csrf_token": token,
                "affiliate_user_id": "1",
                "year_month": "2026-08",
            },
            follow_redirects=False,
        )
        self.assertNotEqual(valid.status_code, 403)
        self.assertIn(valid.status_code, (302, 303))
        other = app_module.app.test_client()
        other.get("/shop")
        mismatched = other.post(
            "/dashboard/admin/mark-affiliate-paid",
            data={"csrf_token": token, "affiliate_user_id": "1", "year_month": "2026-08"},
        )
        self.assertIn(mismatched.status_code, (302, 303, 403, 401))
        if mismatched.status_code not in (401, 403):
            # Unauthenticated client should not mutate; login_required redirects.
            self.assertIn(mismatched.status_code, (302, 303))

    def test_health_reports_inventory_enforcement_without_pii(self) -> None:
        self.database.bootstrap()
        with self.database.get_db(commit=False) as db:
            report = self.database.database_health_report(db)
        self.assertEqual(report["inventory"]["enforcement"], "DISABLED")
        blob = str(report)
        self.assertNotIn("sk_live", blob)
        self.assertNotIn("password", blob.lower())
        self.assertIn("integrity", report)
        self.assertIn("business", report)
        self.assertIn("environment", report["persistence"])
        self.assertIn("status", report["persistence"])
        self.assertIn("resolved_path", report["persistence"])
        self.assertIn("wal", report["sqlite"])
        self.assertIn("busy_timeout_ms", report["sqlite"])
        self.assertIn("core_table_counts", report["schema"])
        self.assertIn("integrity_status", report)

    def test_repeated_shop_requests_close_connections(self) -> None:
        self.database.bootstrap()
        import importlib
        import app as app_module

        importlib.reload(app_module)
        client = app_module.app.test_client()
        for _ in range(25):
            r = client.get("/shop")
            self.assertEqual(r.status_code, 200)
        with self.database.get_db() as db:
            n = int(db.execute("SELECT COUNT(*) FROM products").fetchone()[0])
        self.assertGreaterEqual(n, 5)

    def test_migrations_include_011(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            versions = [r[0] for r in db.execute("SELECT version FROM schema_migrations ORDER BY version")]
        self.assertIn("011_inventory_refs", versions)
        self.assertEqual(versions, [v for v, _ in self.mig.MIGRATIONS])


if __name__ == "__main__":
    unittest.main()
