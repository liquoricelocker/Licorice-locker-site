"""Hardening tests: transactions, Stripe idempotency, inventory, manufacturing, safety."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path


class DatabaseHardeningTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "licorice-test.db")
        os.environ["LICORICE_ENV"] = "development"
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ.pop("RAILWAY_ENVIRONMENT", None)
        os.environ.pop("RENDER", None)
        os.environ.pop("DATABASE_VOLUME_ROOT", None)
        os.environ.pop("INVENTORY_ENFORCE", None)

        import database_config
        import db as database
        import inventory
        from migrations import runner as mig

        self.database = database
        self.database_config = database_config
        self.inventory = inventory
        self.mig = mig

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _product(self, db: sqlite3.Connection, slug: str = "riff"):
        return db.execute("SELECT * FROM products WHERE slug = ?", (slug,)).fetchone()

    def _managed_item(self, db: sqlite3.Connection, on_hand: int = 5) -> int:
        item = db.execute("SELECT id FROM inventory_items ORDER BY id LIMIT 1").fetchone()
        self.assertIsNotNone(item)
        iid = int(item["id"])
        db.execute(
            "UPDATE inventory_items SET quantity_on_hand = ?, quantity_reserved = 0 WHERE id = ?",
            (on_hand, iid),
        )
        return iid

    def test_transaction_rollback_on_failure(self) -> None:
        self.database.bootstrap()
        with self.assertRaises(ValueError):
            with self.database.get_db() as db:
                p = self._product(db)
                cur = db.execute(
                    """
                    INSERT INTO orders (
                        order_number, order_type, customer_first, customer_last, customer_email,
                        shipping_line1, shipping_city, shipping_postal, total_cents
                    ) VALUES ('LL-ROLL-1', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 100)
                    """
                )
                oid = int(cur.lastrowid)
                self.database.insert_order_item_with_snapshot(
                    db, order_id=oid, product=p, quantity=1, unit_price_cents=int(p["price_cents"])
                )
                raise ValueError("simulated_failure")
        with self.database.get_db() as db:
            n = db.execute("SELECT COUNT(*) AS c FROM orders WHERE order_number = 'LL-ROLL-1'").fetchone()["c"]
            self.assertEqual(n, 0)

    def test_transaction_commit_persists(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            p = self._product(db)
            cur = db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-COMMIT-1', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 100)
                """
            )
            oid = int(cur.lastrowid)
            self.database.insert_order_item_with_snapshot(
                db, order_id=oid, product=p, quantity=1, unit_price_cents=int(p["price_cents"])
            )
        with self.database.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) AS c FROM orders WHERE order_number = 'LL-COMMIT-1'").fetchone()["c"],
                1,
            )

    def test_invalid_order_quantity_rejected(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            p = self._product(db)
            cur = db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-QTY-0', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 0)
                """
            )
            oid = int(cur.lastrowid)
            with self.assertRaises(ValueError):
                self.database.insert_order_item_with_snapshot(
                    db, order_id=oid, product=p, quantity=0, unit_price_cents=100
                )
            with self.assertRaises(ValueError):
                self.database.insert_order_item_with_snapshot(
                    db, order_id=oid, product=p, quantity=-1, unit_price_cents=100
                )

    def test_historical_price_preserved_when_product_changes(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            p = self._product(db)
            original = int(p["price_cents"])
            cur = db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-PRICE-1', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', ?)
                """,
                (original,),
            )
            oid = int(cur.lastrowid)
            self.database.insert_order_item_with_snapshot(
                db, order_id=oid, product=p, quantity=1, unit_price_cents=original
            )
            db.execute("UPDATE products SET price_cents = ? WHERE id = ?", (original + 10000, int(p["id"])))
        with self.database.get_db() as db:
            item = db.execute(
                "SELECT unit_price_cents, line_total_cents FROM order_items WHERE order_id = ?",
                (oid,),
            ).fetchone()
            p2 = self._product(db)
            self.assertEqual(int(item["unit_price_cents"]), original)
            self.assertEqual(int(item["line_total_cents"]), original)
            self.assertEqual(int(p2["price_cents"]), original + 10000)

    def test_stripe_event_and_session_idempotent(self) -> None:
        self.database.bootstrap()
        csid = "cs_test_triple"
        event_id = "evt_test_triple"
        with self.database.get_db() as db:
            p = self._product(db, "harmony")
            db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents,
                    stripe_checkout_session_id
                ) VALUES ('LL-STRIPE-A', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 9999, ?)
                """,
                (csid,),
            )
            oid = int(db.execute("SELECT id FROM orders WHERE stripe_checkout_session_id = ?", (csid,)).fetchone()["id"])
            self.database.insert_order_item_with_snapshot(
                db, order_id=oid, product=p, quantity=1, unit_price_cents=int(p["price_cents"])
            )
            self.database.attach_commerce_records_for_new_order(
                db,
                order_id=oid,
                csid=csid,
                email="a@b.c",
                first="A",
                last="B",
                phone="",
                total_cents=9999,
                payment_method="stripe",
            )
            first = self.database.record_stripe_event_processed(db, event_id, "checkout.session.completed", csid)
            second = self.database.record_stripe_event_processed(db, event_id, "checkout.session.completed", csid)
            third = self.database.record_stripe_event_processed(db, event_id, "checkout.session.completed", csid)
            self.assertTrue(first)
            self.assertFalse(second)
            self.assertFalse(third)
            self.database.attach_commerce_records_for_new_order(
                db,
                order_id=oid,
                csid=csid,
                email="a@b.c",
                first="A",
                last="B",
                phone="",
                total_cents=9999,
                payment_method="stripe",
            )
        with self.database.get_db() as db:
            self.assertEqual(
                db.execute("SELECT COUNT(*) AS c FROM orders WHERE stripe_checkout_session_id = ?", (csid,)).fetchone()["c"],
                1,
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) AS c FROM payments WHERE provider_session_id = ?", (csid,)).fetchone()["c"],
                1,
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) AS c FROM stripe_processed_events WHERE event_id = ?", (event_id,)).fetchone()["c"],
                1,
            )
            self.assertEqual(
                db.execute("SELECT COUNT(*) AS c FROM order_items WHERE order_id = ?", (oid,)).fetchone()["c"],
                1,
            )

    def test_customer_email_normalized_unique(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            a = self.database.get_or_create_customer(
                db, email="Jane.Doe+tag@gmail.com", first_name="Jane", last_name="Doe"
            )
            b = self.database.get_or_create_customer(
                db, email="janedoe@gmail.com", first_name="Jane", last_name="Doe"
            )
            self.assertEqual(a, b)
            n = db.execute("SELECT COUNT(*) AS c FROM customers").fetchone()["c"]
            self.assertEqual(n, 1)

    def test_inventory_reserve_release_sale_return_adjust_transfer(self) -> None:
        self.database.bootstrap()
        with self.database.get_db(immediate=True) as db:
            db.execute(
                "INSERT INTO inventory_locations (name, type) VALUES ('Test Bench', 'workshop')"
            )
            dest_loc = int(db.execute("SELECT id FROM inventory_locations WHERE name = 'Test Bench'").fetchone()["id"])
            src = self._managed_item(db, 10)
            variant_id = int(db.execute("SELECT variant_id FROM inventory_items WHERE id = ?", (src,)).fetchone()["variant_id"])
            db.execute(
                """
                INSERT INTO inventory_items (variant_id, location_id, quantity_on_hand, quantity_reserved)
                VALUES (?, ?, 0, 0)
                """,
                (variant_id, dest_loc),
            )
            dest = int(
                db.execute(
                    "SELECT id FROM inventory_items WHERE location_id = ?",
                    (dest_loc,),
                ).fetchone()["id"]
            )
            self.inventory.reserve_inventory(db, src, 3, reason="hold")
            row = db.execute("SELECT quantity_reserved FROM inventory_items WHERE id = ?", (src,)).fetchone()
            self.assertEqual(int(row["quantity_reserved"]), 3)
            with self.assertRaises(self.inventory.InsufficientStock):
                self.inventory.reserve_inventory(db, src, 8, reason="too much")
            self.inventory.release_inventory(db, src, 1, reason="unhold")
            self.inventory.consume_inventory_sale(
                db, src, 2, reference_type="order", reference_id=9001, reason="sale"
            )
            self.inventory.consume_inventory_sale(
                db, src, 2, reference_type="order", reference_id=9001, reason="sale-retry"
            )
            self.inventory.return_inventory(db, src, 1, reason="customer_return")
            self.inventory.adjust_inventory(db, src, -1, reason="cycle count")
            with self.assertRaises(self.inventory.InventoryError):
                self.inventory.adjust_inventory(db, src, 1, reason="")
            self.inventory.transfer_inventory(db, src, dest, 2, reason="to bench")
            src_row = db.execute(
                "SELECT quantity_reserved FROM inventory_items WHERE id = ?",
                (src,),
            ).fetchone()
            dest_row = db.execute(
                "SELECT quantity_on_hand FROM inventory_items WHERE id = ?",
                (dest,),
            ).fetchone()
            self.assertEqual(int(src_row["quantity_reserved"]), 0)
            self.assertEqual(int(dest_row["quantity_on_hand"]), 2)
            self.assertGreaterEqual(
                int(db.execute("SELECT COUNT(*) AS c FROM inventory_movements WHERE inventory_item_id = ?", (src,)).fetchone()["c"]),
                5,
            )
            sales = db.execute(
                """
                SELECT COUNT(*) AS c FROM inventory_movements
                WHERE inventory_item_id = ? AND movement_type = 'SALE' AND reference_id = 9001
                """,
                (src,),
            ).fetchone()["c"]
            self.assertEqual(int(sales), 1)

    def test_inventory_concurrency_last_unit(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            iid = self._managed_item(db, 1)
        errors: list[str] = []
        success = {"n": 0}
        lock = threading.Lock()

        def attempt() -> None:
            try:
                with self.database.get_db(immediate=True) as db:
                    self.inventory.reserve_inventory(db, iid, 1, reason="race")
                with lock:
                    success["n"] += 1
            except self.inventory.InsufficientStock:
                with lock:
                    errors.append("insufficient")
            except sqlite3.OperationalError:
                with lock:
                    errors.append("locked")

        t1 = threading.Thread(target=attempt)
        t2 = threading.Thread(target=attempt)
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
        self.assertEqual(success["n"], 1)
        self.assertGreaterEqual(len(errors), 1)
        with self.database.get_db() as db:
            reserved = int(
                db.execute("SELECT quantity_reserved FROM inventory_items WHERE id = ?", (iid,)).fetchone()["quantity_reserved"]
            )
            self.assertEqual(reserved, 1)

    def test_bom_cost_integer_cents(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            pid = int(self._product(db)["id"])
            db.execute(
                "INSERT INTO product_versions (product_id, version, status) VALUES (?, 'v1', 'active')",
                (pid,),
            )
            vid = int(db.execute("SELECT id FROM product_versions WHERE product_id = ?", (pid,)).fetchone()["id"])
            db.execute(
                "INSERT INTO components (name, sku, cost_cents) VALUES ('Acrylic sheet', 'CMP-ACR', 1250)"
            )
            db.execute(
                "INSERT INTO components (name, sku, cost_cents) VALUES ('Feet pack', 'CMP-FEET', 400)"
            )
            c1 = int(db.execute("SELECT id FROM components WHERE sku = 'CMP-ACR'").fetchone()["id"])
            c2 = int(db.execute("SELECT id FROM components WHERE sku = 'CMP-FEET'").fetchone()["id"])
            db.execute(
                "INSERT INTO bills_of_materials (product_version_id, name, status) VALUES (?, 'Riff BOM', 'active')",
                (vid,),
            )
            bom_id = int(db.execute("SELECT id FROM bills_of_materials ORDER BY id DESC LIMIT 1").fetchone()["id"])
            db.execute(
                "INSERT INTO bill_of_material_items (bom_id, component_id, quantity) VALUES (?, ?, 2)",
                (bom_id, c1),
            )
            db.execute(
                "INSERT INTO bill_of_material_items (bom_id, component_id, quantity) VALUES (?, ?, 1)",
                (bom_id, c2),
            )
            self.assertEqual(self.database.bom_cost_cents(db, bom_id), 1250 * 2 + 400)

    def test_empty_production_database_refused(self) -> None:
        empty = str(Path(self._tmp.name) / "empty-prod.db")
        conn = sqlite3.connect(empty)
        try:
            conn.executescript(
                """
                CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, password_hash TEXT, role TEXT);
                CREATE TABLE products (id INTEGER PRIMARY KEY, slug TEXT, name TEXT, price_cents INTEGER, description TEXT);
                CREATE TABLE orders (id INTEGER PRIMARY KEY, order_number TEXT, total_cents INTEGER);
                CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price_cents INTEGER);
                CREATE TABLE affiliate_pages (user_id INTEGER);
                CREATE TABLE commissions (id INTEGER PRIMARY KEY);
                CREATE TABLE product_images (id INTEGER PRIMARY KEY);
                """
            )
            conn.commit()
        finally:
            conn.close()
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = empty
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database.bootstrap()
        self.assertIn("unexpectedly empty", str(ctx.exception))

    def test_migrations_rerun_and_010_present(self) -> None:
        self.database.bootstrap()
        self.database.bootstrap()
        with self.database.get_db() as db:
            applied = [r[0] for r in db.execute("SELECT version FROM schema_migrations ORDER BY version")]
            self.assertEqual(applied, [v for v, _ in self.mig.MIGRATIONS])
            row = db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stripe_processed_events'"
            ).fetchone()
            self.assertIsNotNone(row)

    def test_migration_from_older_state(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL,
                    affiliate_slug TEXT UNIQUE,
                    full_name TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                CREATE TABLE affiliate_pages (
                    user_id INTEGER PRIMARY KEY REFERENCES users(id),
                    headline TEXT NOT NULL DEFAULT 'Welcome',
                    tagline TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    monthly_sales_target INTEGER NOT NULL DEFAULT 25
                );
                CREATE TABLE products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slug TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    price_cents INTEGER NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    is_main INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_number TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    status TEXT NOT NULL DEFAULT 'completed',
                    order_type TEXT NOT NULL,
                    affiliate_user_id INTEGER,
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
                CREATE TABLE order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES orders(id),
                    product_id INTEGER NOT NULL REFERENCES products(id),
                    quantity INTEGER NOT NULL,
                    unit_price_cents INTEGER NOT NULL
                );
                CREATE TABLE commissions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    affiliate_user_id INTEGER NOT NULL,
                    year_month TEXT NOT NULL,
                    sales_count INTEGER NOT NULL,
                    commission_rate REAL NOT NULL,
                    total_sales_cents INTEGER NOT NULL,
                    commission_cents INTEGER NOT NULL,
                    payout_status TEXT NOT NULL DEFAULT 'pending',
                    UNIQUE (affiliate_user_id, year_month)
                );
                CREATE TABLE product_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL REFERENCES products(id),
                    path TEXT NOT NULL,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    role TEXT NOT NULL DEFAULT 'gallery'
                );
                INSERT INTO products (slug, name, price_cents, description) VALUES ('riff', 'Riff', 9999, 'test');
                """
            )
            conn.commit()
        finally:
            conn.close()
        self.database.bootstrap()
        with self.database.get_db() as db:
            self.assertEqual(
                int(db.execute("SELECT price_cents FROM products WHERE slug = 'riff'").fetchone()["price_cents"]),
                9999,
            )
            applied = [r[0] for r in db.execute("SELECT version FROM schema_migrations")]
            self.assertIn("010_hardening", applied)
            self.assertGreaterEqual(int(db.execute("SELECT COUNT(*) FROM product_variants").fetchone()[0]), 1)

    def test_health_endpoint_is_read_only(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            before = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
        with self.database.get_db(commit=False) as db:
            report = self.database.database_health_report(db)
            db.execute("UPDATE products SET name = 'SHOULD_NOT_STICK' WHERE slug = 'riff'")
        with self.database.get_db() as db:
            after = int(db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"])
            name = db.execute("SELECT name FROM products WHERE slug = 'riff'").fetchone()["name"]
        self.assertEqual(before, after)
        self.assertNotEqual(name, "SHOULD_NOT_STICK")
        self.assertIn("connection", report)
        self.assertIn("integrity", report)
        self.assertIn("sqlite", report)

    def test_integrity_report_detects_orphan_order_item(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            db.execute("PRAGMA foreign_keys = OFF")
            db.execute(
                """
                INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
                VALUES (999999, 1, 1, 100)
                """
            )
            db.execute("PRAGMA foreign_keys = ON")
            report = self.database.integrity_report(db)
        self.assertGreaterEqual(report["orphans"]["order_items_without_order"], 1)

    def test_shop_cart_flow_against_temp_db(self) -> None:
        self.database.bootstrap()
        import importlib
        import app as app_module

        importlib.reload(app_module)
        client = app_module.app.test_client()
        shop = client.get("/shop", follow_redirects=True)
        self.assertEqual(shop.status_code, 200)
        product = client.get("/product/riff")
        self.assertEqual(product.status_code, 200)
        with self.database.get_db() as db:
            pid = int(self._product(db)["id"])
        added = client.post("/cart/add", data={"product_id": str(pid), "quantity": "1"}, follow_redirects=True)
        self.assertEqual(added.status_code, 200)
        cart = client.get("/cart")
        self.assertEqual(cart.status_code, 200)
        self.assertIn(b"Riff", cart.data)


if __name__ == "__main__":
    unittest.main()
