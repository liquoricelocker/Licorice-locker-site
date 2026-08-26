"""Verified Railway Volume first-boot vs existing-database protection."""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class VolumeBootstrapTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.volume = Path(self._tmp.name) / "data"
        self.volume.mkdir(parents=True, exist_ok=True)
        self.db_path = str(self.volume / "licorice.db")
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
        import shipping as shipping_mod

        self.database = database
        self.database_config = database_config
        self.shipping = shipping_mod

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _production_volume(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ["DATABASE_VOLUME_ROOT"] = str(self.volume)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(self.volume)

    def test_ephemeral_app_data_path_is_invalid(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = "/app/data/licorice.db"
        os.environ["DATABASE_VOLUME_ROOT"] = str(self.volume)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(self.volume)
        state = self.database_config.classify_production_database_state()
        self.assertEqual(state, self.database_config.DATABASE_STATE_INVALID)
        with self.assertRaises(self.database_config.ProductionDatabaseError):
            self.database.bootstrap()
        self.assertFalse(Path("/app/data/licorice.db").exists())
        self.assertFalse(Path(self.db_path).exists())

    def test_no_path_no_volume_fails(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("RAILWAY_VOLUME_MOUNT_PATH", None)
        os.environ.pop("DATABASE_VOLUME_ROOT", None)
        with self.assertRaises(self.database_config.ProductionDatabaseError):
            self.database.bootstrap()
        self.assertFalse(Path(self.db_path).exists())

    def test_path_outside_volume_fails(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        outside = Path(self._tmp.name) / "data-evil" / "licorice.db"
        outside.parent.mkdir(parents=True, exist_ok=True)
        os.environ["DATABASE_PATH"] = str(outside)
        os.environ["DATABASE_VOLUME_ROOT"] = str(self.volume)
        os.environ["RAILWAY_VOLUME_MOUNT_PATH"] = str(self.volume)
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database.bootstrap()
        self.assertIn("INVALID", str(ctx.exception))
        self.assertFalse(outside.exists())
        self.assertFalse(Path(self.db_path).exists())

    def test_empty_verified_volume_bootstraps(self) -> None:
        self._production_volume()
        self.database.bootstrap()
        self.assertTrue(Path(self.db_path).is_file())
        self.assertEqual(
            self.database_config.last_database_state(),
            self.database_config.DATABASE_STATE_EMPTY_VOLUME_BOOTSTRAP,
        )
        with self.database.get_db(commit=False) as db:
            products = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
            users = int(db.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"])
            orders = int(db.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"])
            rates = int(db.execute("SELECT COUNT(*) AS c FROM shipping_rates").fetchone()["c"])
            versions = [r[0] for r in db.execute("SELECT version FROM schema_migrations ORDER BY version")]
            sw = db.execute("SELECT price_cents FROM products WHERE slug = 'sound-wave'").fetchone()
            q = self.shipping.calculate_shipping(
                db,
                [{"product_id": int(db.execute("SELECT id FROM products WHERE slug='riff'").fetchone()["id"]), "quantity": 1}],
                "NZ",
                "NZ_STANDARD",
            )
            pickup = self.shipping.calculate_shipping(
                db,
                [{"product_id": int(db.execute("SELECT id FROM products WHERE slug='riff'").fetchone()["id"]), "quantity": 1}],
                "NZ",
                "LOCAL_PICKUP",
            )
        self.assertGreaterEqual(products, 5)
        self.assertEqual(users, 0)
        self.assertEqual(orders, 0)
        self.assertGreaterEqual(rates, 5)
        self.assertEqual(int(sw["price_cents"]), 42900)
        self.assertIn("013_canonical_shipping_rates", versions)
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 1200)
        self.assertTrue(pickup.ok)
        self.assertEqual(pickup.price_cents, 0)
        report = None
        with self.database.get_db(commit=False) as db:
            report = self.database.database_health_report(db)
        self.assertEqual(report["persistence"]["status"], "VERIFIED")
        self.assertTrue(report["sqlite"]["wal"])
        self.assertTrue(report["sqlite"]["foreign_keys"])
        self.assertTrue(report["sqlite"]["integrity_check_ok"])
        self.assertTrue(report["sqlite"]["foreign_key_check_ok"])

    def test_second_boot_does_not_reset(self) -> None:
        self._production_volume()
        self.database.bootstrap()
        with self.database.get_db() as db:
            db.execute("UPDATE products SET price_cents = 42900 WHERE slug = 'sound-wave'")
            db.execute("UPDATE shipping_rates SET price_cents = 1500 WHERE price_cents = 1200")
            db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-KEEP', 'direct', 'A', 'B', 'keep@example.test', '1 St', 'CHCH', '8011', 44400)
                """
            )
        for _ in range(5):
            self.database.bootstrap()
        self.assertEqual(
            self.database_config.last_database_state(),
            self.database_config.DATABASE_STATE_EXISTING,
        )
        with self.database.get_db(commit=False) as db:
            price = int(db.execute("SELECT price_cents FROM products WHERE slug = 'sound-wave'").fetchone()[0])
            rate = db.execute("SELECT 1 FROM shipping_rates WHERE price_cents = 1500").fetchone()
            restored = db.execute("SELECT 1 FROM shipping_rates WHERE price_cents = 1200").fetchone()
            orders = int(db.execute("SELECT COUNT(*) AS c FROM orders").fetchone()["c"])
            products = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
            versions = [r[0] for r in db.execute("SELECT version FROM schema_migrations ORDER BY version")]
        self.assertEqual(price, 42900)
        self.assertIsNotNone(rate)
        self.assertIsNone(restored)
        self.assertEqual(orders, 1)
        self.assertGreaterEqual(products, 5)
        self.assertEqual(len(versions), len(set(versions)))

    def test_persistence_across_restart(self) -> None:
        self._production_volume()
        self.database.bootstrap()
        with self.database.get_db() as db:
            pid = int(db.execute("SELECT id FROM products WHERE slug = 'riff'").fetchone()["id"])
            db.execute("UPDATE products SET price_cents = 8888 WHERE slug = 'riff'")
            db.execute(
                """
                INSERT INTO customers (email, email_normalized, first_name, last_name)
                VALUES ('persist@example.test', 'persist@example.test', 'Pat', 'Ron')
                """
            )
            db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-PERSIST', 'direct', 'Pat', 'Ron', 'persist@example.test', '1 St', 'CHCH', '8011', 8888)
                """
            )
            oid = int(db.execute("SELECT id FROM orders WHERE order_number = 'LL-PERSIST'").fetchone()["id"])
            db.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
                VALUES (?, ?, 1, 8888)
                """,
                (oid, pid),
            )
        self.database.bootstrap()
        with self.database.get_db(commit=False) as db:
            price = int(db.execute("SELECT price_cents FROM products WHERE slug = 'riff'").fetchone()[0])
            customers = int(db.execute("SELECT COUNT(*) AS c FROM customers WHERE email = 'persist@example.test'").fetchone()["c"])
            orders = int(db.execute("SELECT COUNT(*) AS c FROM orders WHERE order_number = 'LL-PERSIST'").fetchone()["c"])
            items = int(db.execute("SELECT COUNT(*) AS c FROM order_items WHERE unit_price_cents = 8888").fetchone()["c"])
            riff_count = int(db.execute("SELECT COUNT(*) AS c FROM products WHERE slug = 'riff'").fetchone()["c"])
        self.assertEqual(price, 8888)
        self.assertEqual(customers, 1)
        self.assertEqual(orders, 1)
        self.assertEqual(items, 1)
        self.assertEqual(riff_count, 1)

    def test_shipping_quotes_after_production_bootstrap(self) -> None:
        self._production_volume()
        self.database.bootstrap()
        with self.database.get_db(commit=False) as db:
            pid = int(db.execute("SELECT id FROM products WHERE slug = 'riff'").fetchone()["id"])

            def cart(cents: int):
                db.execute("UPDATE products SET price_cents = ? WHERE id = ?", (cents, pid))
                db.execute("UPDATE product_variants SET price_cents = ? WHERE product_id = ?", (cents, pid))
                return [{"product_id": pid, "quantity": 1}]

            nz_low = self.shipping.calculate_shipping(db, cart(10000), "NZ", "NZ_STANDARD")
            nz_mid = self.shipping.calculate_shipping(db, cart(20000), "NZ", "NZ_STANDARD")
            nz_free = self.shipping.calculate_shipping(db, cart(30000), "NZ", "NZ_STANDARD")
            au_low = self.shipping.calculate_shipping(db, cart(10000), "AU", "INTERNATIONAL_STANDARD")
            au_high = self.shipping.calculate_shipping(db, cart(20000), "AU", "INTERNATIONAL_STANDARD")
            row = self.shipping.calculate_shipping(db, cart(10000), "US", "INTERNATIONAL_STANDARD")
        self.assertEqual(nz_low.price_cents, 1200)
        self.assertEqual(nz_mid.price_cents, 1800)
        self.assertEqual(nz_free.price_cents, 0)
        self.assertEqual(au_low.price_cents, 3500)
        self.assertEqual(au_high.price_cents, 5000)
        self.assertFalse(row.ok)

    def test_flask_storefront_after_volume_bootstrap(self) -> None:
        self._production_volume()
        self.database.bootstrap()
        import importlib
        import app as app_module

        importlib.reload(app_module)
        client = app_module.app.test_client()
        home = client.get("/")
        self.assertIn(home.status_code, (200, 302))
        self.assertEqual(client.get("/shop").status_code, 200)
        self.assertEqual(client.get("/product/sound-wave").status_code, 200)
        self.assertEqual(client.get("/product/riff").status_code, 200)
        self.assertEqual(client.get("/cart").status_code, 200)
        checkout = client.get("/checkout")
        self.assertIn(checkout.status_code, (200, 302))
        health = client.get("/dashboard/admin/database-health")
        self.assertIn(health.status_code, (302, 401, 403))

    def test_gunicorn_preload_boots_on_volume(self) -> None:
        self._production_volume()
        port = _free_port()
        env = os.environ.copy()
        env["PORT"] = str(port)
        env["LICORICE_ENV"] = "production"
        env["DATABASE_PATH"] = self.db_path
        env["DATABASE_VOLUME_ROOT"] = str(self.volume)
        env["RAILWAY_VOLUME_MOUNT_PATH"] = str(self.volume)
        env["SECRET_KEY"] = "test-secret-licorice"
        env["PYTHONUNBUFFERED"] = "1"
        cmd = [
            sys.executable,
            "-m",
            "gunicorn",
            "--workers",
            "1",
            "--bind",
            f"127.0.0.1:{port}",
            "app:app",
        ]
        if sys.platform != "darwin":
            cmd[3:3] = ["--preload"]
        log_path = Path(self._tmp.name) / "gunicorn.log"
        log_f = open(log_path, "wb")
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=log_f,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.time() + 20
            while time.time() < deadline:
                if proc.poll() is not None:
                    log_f.flush()
                    text = log_path.read_text(errors="replace")
                    self.fail(f"gunicorn exited {proc.returncode}:\n{text}")
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{port}/shop", timeout=2) as resp:
                        self.assertEqual(resp.status, 200)
                        break
                except (urllib.error.URLError, TimeoutError, ConnectionError):
                    time.sleep(0.2)
            else:
                log_f.flush()
                text = log_path.read_text(errors="replace")
                self.fail(f"gunicorn did not become ready:\n{text}")
            self.assertTrue(Path(self.db_path).is_file())
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            log_f.close()


def _free_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = int(sock.getsockname()[1])
    sock.close()
    return port


if __name__ == "__main__":
    unittest.main()
