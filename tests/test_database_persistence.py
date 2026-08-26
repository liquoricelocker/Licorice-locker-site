"""Persistence, migration, and production-safety tests. Isolated temp databases only."""

from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path


class DatabasePersistenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self._tmp.name) / "licorice-test.db")
        os.environ["LICORICE_ENV"] = "development"
        os.environ["DATABASE_PATH"] = self.db_path
        os.environ.pop("RAILWAY_ENVIRONMENT", None)
        os.environ.pop("RAILWAY_VOLUME_MOUNT_PATH", None)
        os.environ.pop("RENDER", None)
        os.environ.pop("DATABASE_VOLUME_ROOT", None)

        import database_config
        import db as database
        from migrations import runner as mig

        self.database = database
        self.database_config = database_config
        self.mig = mig

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_catalogue_not_overwritten_on_restart(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            db.execute("UPDATE products SET price_cents = 12345 WHERE slug = 'riff'")
            db.execute("UPDATE products SET name = 'Riff Custom' WHERE slug = 'riff'")
        self.database.bootstrap()
        with self.database.get_db() as db:
            row = db.execute("SELECT name, price_cents FROM products WHERE slug = 'riff'").fetchone()
            self.assertEqual(row["price_cents"], 12345)
            self.assertEqual(row["name"], "Riff Custom")

    def test_order_survives_restart(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            p = db.execute("SELECT id, name, sku, price_cents FROM products WHERE slug = 'melody'").fetchone()
            cur = db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents
                ) VALUES ('LL-TEST-1', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 9999)
                """
            )
            oid = int(cur.lastrowid)
            self.database.insert_order_item_with_snapshot(
                db, order_id=oid, product=p, quantity=1, unit_price_cents=int(p["price_cents"])
            )
        self.database.bootstrap()
        with self.database.get_db() as db:
            n = db.execute("SELECT COUNT(*) AS c FROM orders WHERE order_number = 'LL-TEST-1'").fetchone()["c"]
            items = db.execute("SELECT COUNT(*) AS c FROM order_items WHERE order_id = ?", (oid,)).fetchone()["c"]
            self.assertEqual(n, 1)
            self.assertEqual(items, 1)

    def test_migrations_are_idempotent(self) -> None:
        self.database.bootstrap()
        self.database.bootstrap()
        with self.database.get_db() as db:
            rows = db.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
            versions = [r["version"] for r in rows]
            self.assertEqual(versions, [v for v, _ in self.mig.MIGRATIONS])
            self.assertEqual(len(versions), len(set(versions)))

    def test_product_ids_preserved(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            before = {
                r["slug"]: r["id"]
                for r in db.execute("SELECT id, slug FROM products")
            }
        self.database.bootstrap()
        with self.database.get_db() as db:
            after = {
                r["slug"]: r["id"]
                for r in db.execute("SELECT id, slug FROM products")
            }
        self.assertEqual(before, after)
        self.assertIn("sound-wave", before)

    def test_stripe_session_idempotent(self) -> None:
        self.database.bootstrap()
        csid = "cs_test_duplicate_session"
        with self.database.get_db() as db:
            p = db.execute("SELECT * FROM products WHERE slug = 'harmony'").fetchone()
            db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents,
                    stripe_checkout_session_id
                ) VALUES ('LL-TEST-STRIPE', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 9999, ?)
                """,
                (csid,),
            )
            oid = int(db.execute("SELECT id FROM orders WHERE stripe_checkout_session_id = ?", (csid,)).fetchone()["id"])
            self.database.insert_order_item_with_snapshot(
                db, order_id=oid, product=p, quantity=1, unit_price_cents=int(p["price_cents"])
            )
        with self.database.get_db() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute(
                    """
                    INSERT INTO orders (
                        order_number, order_type, customer_first, customer_last, customer_email,
                        shipping_line1, shipping_city, shipping_postal, total_cents,
                        stripe_checkout_session_id
                    ) VALUES ('LL-TEST-STRIPE-2', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 9999, ?)
                    """,
                    (csid,),
                )
        with self.database.get_db() as db:
            n = db.execute(
                "SELECT COUNT(*) AS c FROM orders WHERE stripe_checkout_session_id = ?",
                (csid,),
            ).fetchone()["c"]
            self.assertEqual(n, 1)

    def test_inventory_survives_restart(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            item = db.execute("SELECT id, quantity_on_hand FROM inventory_items LIMIT 1").fetchone()
            self.assertIsNotNone(item)
            db.execute(
                "UPDATE inventory_items SET quantity_on_hand = 7, quantity_reserved = 2 WHERE id = ?",
                (int(item["id"]),),
            )
            db.execute(
                """
                INSERT INTO inventory_movements (
                    inventory_item_id, movement_type, quantity, reason
                ) VALUES (?, 'RESERVATION', 2, 'test')
                """,
                (int(item["id"]),),
            )
        self.database.bootstrap()
        with self.database.get_db() as db:
            row = db.execute(
                "SELECT quantity_on_hand, quantity_reserved FROM inventory_items WHERE id = ?",
                (int(item["id"]),),
            ).fetchone()
            self.assertEqual(row["quantity_on_hand"], 7)
            self.assertEqual(row["quantity_reserved"], 2)
            moves = db.execute("SELECT COUNT(*) AS c FROM inventory_movements").fetchone()["c"]
            self.assertGreaterEqual(moves, 1)

    def test_missing_production_database_refuses_to_create(self) -> None:
        missing = str(Path(self._tmp.name) / "does-not-exist.db")
        os.environ["LICORICE_ENV"] = "production"
        os.environ["DATABASE_PATH"] = missing
        with self.assertRaises(self.database_config.ProductionDatabaseError) as ctx:
            self.database.bootstrap()
        self.assertIn("Volume", str(ctx.exception))
        self.assertFalse(Path(missing).exists())

    def test_production_requires_database_path(self) -> None:
        os.environ["LICORICE_ENV"] = "production"
        os.environ.pop("DATABASE_PATH", None)
        os.environ.pop("RAILWAY_VOLUME_MOUNT_PATH", None)
        with self.assertRaises(self.database_config.ProductionDatabaseError):
            self.database_config.get_database_path()

    def test_default_variants_and_customers_tables(self) -> None:
        self.database.bootstrap()
        with self.database.get_db() as db:
            variants = db.execute("SELECT COUNT(*) AS c FROM product_variants").fetchone()["c"]
            products = db.execute("SELECT COUNT(*) AS c FROM products").fetchone()["c"]
            self.assertEqual(variants, products)
            self.assertGreaterEqual(products, 5)


if __name__ == "__main__":
    unittest.main()
