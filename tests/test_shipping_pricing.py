"""Shipping pricing engine, checkout quoting, admin security, historical snapshots."""

from __future__ import annotations

import os
import re
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from werkzeug.security import generate_password_hash

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ADMIN_EMAIL = "joshuafrenchdesign@gmail.com"


class ShippingPricingTest(unittest.TestCase):
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
        os.environ["CHECKOUT_SHIPPING_CENTS_DEFAULT"] = "99999"

        import database_config
        import db as database
        import shipping as shipping_mod
        from migrations import runner as mig

        self.database = database
        self.database_config = database_config
        self.shipping = shipping_mod
        self.mig = mig
        self.database.bootstrap()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _db(self):
        return self.database.get_db()

    def _product(self, db: sqlite3.Connection, slug: str = "riff"):
        return db.execute("SELECT * FROM products WHERE slug = ?", (slug,)).fetchone()

    def _set_price(self, db: sqlite3.Connection, slug: str, cents: int) -> int:
        db.execute("UPDATE products SET price_cents = ? WHERE slug = ?", (cents, slug))
        db.execute(
            "UPDATE product_variants SET price_cents = ? WHERE product_id = (SELECT id FROM products WHERE slug = ?)",
            (cents, slug),
        )
        return int(self._product(db, slug)["id"])

    def _set_weight(self, db: sqlite3.Connection, slug: str, grams):
        db.execute(
            """
            UPDATE product_variants SET weight_grams = ?
            WHERE product_id = (SELECT id FROM products WHERE slug = ?)
            """,
            (grams, slug),
        )

    def _ids(self, db: sqlite3.Connection, method: str, zone: str) -> tuple[int, int]:
        m = db.execute("SELECT id FROM shipping_methods WHERE code = ?", (method,)).fetchone()
        z = db.execute("SELECT id FROM shipping_zones WHERE code = ?", (zone,)).fetchone()
        self.assertIsNotNone(m)
        self.assertIsNotNone(z)
        return int(m["id"]), int(z["id"])

    def _cart(self, db: sqlite3.Connection, slug: str = "riff", qty: int = 1):
        p = self._product(db, slug)
        return [{"product_id": int(p["id"]), "quantity": qty}]


class CalculatorNzTests(ShippingPricingTest):
    def test_nz_standard_under_150(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 14999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 1200)
        self.assertEqual(q.currency, "NZD")
        self.assertEqual(q.shipping_method, "NZ_STANDARD")
        self.assertIsNotNone(q.rule_id)

    def test_nz_standard_mid_band(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 15000)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 1800)

    def test_nz_standard_one_cent_below_150(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 14999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertEqual(q.price_cents, 1200)

    def test_nz_standard_one_cent_below_free(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 29999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertEqual(q.price_cents, 1800)

    def test_nz_standard_free_at_300(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 30000)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 0)

    def test_sound_wave_free_shipping(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, self._cart(db, "sound-wave"), "NZ", "NZ_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 0)

    def test_express_not_offered(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_EXPRESS")
        self.assertFalse(q.ok)
        self.assertEqual(q.error, self.shipping.ERROR_NO_RATE)

    def test_env_shipping_default_is_ignored(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 14999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertEqual(q.price_cents, 1200)
        self.assertNotEqual(q.price_cents, 99999)


class CalculatorAustraliaTests(ShippingPricingTest):
    def test_australia_under_200(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 19999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "AU", "INTERNATIONAL_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 3500)

    def test_australia_at_200(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 20000)
            q = self.shipping.calculate_shipping(db, self._cart(db), "AU", "INTERNATIONAL_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 5000)

    def test_australia_no_free_shipping(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, self._cart(db, "sound-wave"), "AU", "INTERNATIONAL_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 5000)

    def test_australia_not_weight_based(self) -> None:
        with self._db() as db:
            self._set_weight(db, "riff", 5001)
            self._set_price(db, "riff", 9999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "AU", "INTERNATIONAL_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 3500)


class CalculatorInternationalTests(ShippingPricingTest):
    def test_rest_of_world_not_automated(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, self._cart(db), "US", "INTERNATIONAL_STANDARD")
        self.assertFalse(q.ok)
        self.assertEqual(q.error, self.shipping.ERROR_NO_RATE)

    def test_germany_not_automated(self) -> None:
        with self._db() as db:
            quotes, err, _, _ = self.shipping.list_quotes_for_destination(db, self._cart(db), "DE")
        self.assertEqual(quotes, [])

    def test_unsupported_when_catch_all_inactive(self) -> None:
        with self._db() as db:
            db.execute("UPDATE shipping_zones SET active = 0 WHERE code = 'REST_OF_WORLD'")
            q = self.shipping.calculate_shipping(db, self._cart(db), "US", "INTERNATIONAL_STANDARD")
        self.assertFalse(q.ok)
        self.assertEqual(q.error, self.shipping.ERROR_NO_ZONE)

    def test_invalid_country(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZZ", "NZ_STANDARD")
        self.assertFalse(q.ok)
        self.assertEqual(q.error, self.shipping.ERROR_INVALID_COUNTRY)


class CalculatorPickupAndEdges(ShippingPricingTest):
    def test_local_pickup_not_offered_by_default(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "LOCAL_PICKUP")
        self.assertFalse(q.ok)
        self.assertEqual(q.error, self.shipping.ERROR_NO_RATE)

    def test_local_pickup_zero_when_configured(self) -> None:
        with self._db() as db:
            mid, zid = self._ids(db, "LOCAL_PICKUP", "NZ")
            db.execute(
                """
                INSERT INTO shipping_rates (
                    shipping_method_id, shipping_zone_id, price_cents, currency, active, priority
                ) VALUES (?, ?, 0, 'NZD', 1, 20)
                """,
                (mid, zid),
            )
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "LOCAL_PICKUP")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 0)

    def test_empty_cart(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, [], "NZ", "NZ_STANDARD")
        self.assertFalse(q.ok)
        self.assertEqual(q.error, self.shipping.ERROR_EMPTY_CART)

    def test_quantity_zero_is_empty(self) -> None:
        with self._db() as db:
            p = self._product(db)
            q = self.shipping.calculate_shipping(
                db, [{"product_id": int(p["id"]), "quantity": 0}], "NZ", "NZ_STANDARD"
            )
        self.assertEqual(q.error, self.shipping.ERROR_EMPTY_CART)

    def test_negative_quantity(self) -> None:
        with self._db() as db:
            p = self._product(db)
            q = self.shipping.calculate_shipping(
                db, [{"product_id": int(p["id"]), "quantity": -1}], "NZ", "NZ_STANDARD"
            )
        self.assertEqual(q.error, self.shipping.ERROR_INVALID_QUANTITY)

    def test_unknown_product(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(
                db, [{"product_id": 999999, "quantity": 1}], "NZ", "NZ_STANDARD"
            )
        self.assertEqual(q.error, self.shipping.ERROR_UNKNOWN_PRODUCT)

    def test_missing_weight_allows_order_value_rate(self) -> None:
        with self._db() as db:
            self._set_weight(db, "riff", None)
            self._set_price(db, "riff", 14999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 1200)
        self.assertTrue(q.missing_weight)

    def test_missing_weight_still_quotes_australia(self) -> None:
        with self._db() as db:
            self._set_weight(db, "riff", None)
            self._set_price(db, "riff", 9999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "AU", "INTERNATIONAL_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 3500)
        self.assertTrue(q.missing_weight)

    def test_inactive_method(self) -> None:
        with self._db() as db:
            db.execute("UPDATE shipping_methods SET active = 0 WHERE code = 'NZ_EXPRESS'")
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_EXPRESS")
        self.assertEqual(q.error, self.shipping.ERROR_METHOD_INACTIVE)

    def test_inactive_rate(self) -> None:
        with self._db() as db:
            db.execute("UPDATE shipping_rates SET active = 0")
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertEqual(q.error, self.shipping.ERROR_NO_RATE)

    def test_overlapping_same_priority_fails_safely(self) -> None:
        with self._db() as db:
            mid, zid = self._ids(db, "NZ_STANDARD", "NZ")
            db.execute("DELETE FROM shipping_rates WHERE shipping_method_id = ? AND shipping_zone_id = ?", (mid, zid))
            db.execute(
                """
                INSERT INTO shipping_rates (
                    shipping_method_id, shipping_zone_id, price_cents, currency, active, priority
                ) VALUES (?, ?, 1500, 'NZD', 1, 5), (?, ?, 2000, 'NZD', 1, 5)
                """,
                (mid, zid, mid, zid),
            )
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertFalse(q.ok)
        self.assertEqual(q.error, self.shipping.ERROR_AMBIGUOUS_RATE)

    def test_higher_priority_wins(self) -> None:
        with self._db() as db:
            mid, zid = self._ids(db, "NZ_STANDARD", "NZ")
            db.execute("DELETE FROM shipping_rates WHERE shipping_method_id = ? AND shipping_zone_id = ?", (mid, zid))
            db.execute(
                """
                INSERT INTO shipping_rates (
                    shipping_method_id, shipping_zone_id, price_cents, currency, active, priority
                ) VALUES (?, ?, 1500, 'NZD', 1, 1), (?, ?, 0, 'NZD', 1, 9)
                """,
                (mid, zid, mid, zid),
            )
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 0)

    def test_no_matching_rate(self) -> None:
        with self._db() as db:
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "INTERNATIONAL_EXPRESS")
        self.assertEqual(q.error, self.shipping.ERROR_NO_RATE)

    def test_huge_order_still_quotes(self) -> None:
        with self._db() as db:
            self._set_price(db, "riff", 9_999_999)
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.price_cents, 0)

    def test_packaging_weight_added(self) -> None:
        with self._db() as db:
            self._set_weight(db, "riff", 1000)
            self.shipping.set_setting(db, self.shipping.SETTING_PACKAGING_WEIGHT, "250")
            q = self.shipping.calculate_shipping(db, self._cart(db), "AU", "INTERNATIONAL_STANDARD")
        self.assertTrue(q.ok)
        self.assertEqual(q.estimated_weight_grams, 1250)

    def test_never_silently_zero_without_rate(self) -> None:
        with self._db() as db:
            db.execute("DELETE FROM shipping_rates")
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertFalse(q.ok)
        self.assertIsNone(q.price_cents)

    def test_apply_canonical_does_not_overwrite_existing_rates(self) -> None:
        with self._db() as db:
            db.execute("UPDATE shipping_rates SET price_cents = 4242 WHERE price_cents = 1200")
            self.shipping.apply_canonical_shipping_rates(db)
            row = db.execute(
                "SELECT price_cents FROM shipping_rates WHERE price_cents = 4242"
            ).fetchone()
            self.assertIsNotNone(row)
            restored = db.execute(
                "SELECT 1 FROM shipping_rates WHERE price_cents = 1200"
            ).fetchone()
            self.assertIsNone(restored)

    def test_available_methods_omit_unmatched(self) -> None:
        with self._db() as db:
            quotes, err, _, _ = self.shipping.list_quotes_for_destination(db, self._cart(db), "NZ")
        self.assertIsNone(err)
        codes = [q.shipping_method for q in quotes]
        self.assertIn("NZ_STANDARD", codes)
        self.assertNotIn("LOCAL_PICKUP", codes)
        self.assertNotIn("INTERNATIONAL_STANDARD", codes)
        self.assertNotIn("NZ_EXPRESS", codes)

    def test_stripe_total_cents(self) -> None:
        self.assertEqual(self.shipping.stripe_payable_total_cents(29900, 1800), 31700)

    def test_format_free(self) -> None:
        self.assertEqual(self.shipping.format_shipping_price(0), "FREE")
        self.assertNotEqual(self.shipping.format_shipping_price(0), "$0.00")


class HistoricalAndIntegrityTests(ShippingPricingTest):
    def test_historical_order_keeps_old_shipping(self) -> None:
        with self._db() as db:
            p = self._product(db)
            cur = db.execute(
                """
                INSERT INTO orders (
                    order_number, order_type, customer_first, customer_last, customer_email,
                    shipping_line1, shipping_city, shipping_postal, total_cents, shipping_cents, subtotal_cents
                ) VALUES ('LL-SHIP-HIST', 'direct', 'A', 'B', 'a@b.c', '1 St', 'CHCH', '8011', 31400, 1500, 29900)
                """
            )
            oid = int(cur.lastrowid)
            db.execute("UPDATE shipping_rates SET price_cents = 2000 WHERE price_cents = 1200")
            row = db.execute("SELECT shipping_cents FROM orders WHERE id = ?", (oid,)).fetchone()
            q = self.shipping.calculate_shipping(db, self._cart(db), "NZ", "NZ_STANDARD")
        self.assertEqual(int(row["shipping_cents"]), 1500)
        self.assertEqual(q.price_cents, 2000)

    def test_snapshot_not_recalculated_from_current_table(self) -> None:
        snap = self.shipping.snapshot_from_checkout_metadata(
            {"shipping_cents": "1500", "shipping_method": "NZ_STANDARD", "shipping_price": "0"}
        )
        self.assertEqual(snap["shipping_cents"], 1500)

    def test_integrity_reports_overlapping_rules(self) -> None:
        with self._db() as db:
            mid, zid = self._ids(db, "NZ_EXPRESS", "NZ")
            db.execute(
                """
                INSERT INTO shipping_rates (
                    shipping_method_id, shipping_zone_id, price_cents, currency, active, priority
                ) VALUES (?, ?, 1111, 'NZD', 1, 10), (?, ?, 2222, 'NZD', 1, 10)
                """,
                (mid, zid, mid, zid),
            )
            report = self.database.integrity_report(db)
        self.assertGreaterEqual(report["shipping"]["duplicate_active_overlapping_rules"], 1)

    def test_integrity_invalid_thresholds(self) -> None:
        with self._db() as db:
            mid, zid = self._ids(db, "NZ_EXPRESS", "NZ")
            db.execute(
                """
                INSERT INTO shipping_rates (
                    shipping_method_id, shipping_zone_id, price_cents, currency,
                    min_order_value_cents, max_order_value_cents, active, priority
                ) VALUES (?, ?, 100, 'NZD', 5000, 1000, 0, 0)
                """,
                (mid, zid),
            )
            report = self.database.integrity_report(db)
        self.assertGreaterEqual(report["invalid_financial"]["shipping_rates_invalid_order_range"], 1)

    def test_validation_rejects_negatives_and_bad_ranges(self) -> None:
        parsed, errors = self.shipping.validate_rate_fields(
            price_cents=-1,
            currency="USD",
            min_order_value_cents=5000,
            max_order_value_cents=1000,
            min_weight_grams=500,
            max_weight_grams=100,
            priority=0,
            estimated_min_days=5,
            estimated_max_days=1,
        )
        blob = " ".join(errors).lower()
        self.assertIn("negative", blob)
        self.assertIn("currency", blob)
        self.assertIn("order", blob)
        self.assertIn("weight", blob)
        self.assertIn("days", blob)
        self.assertEqual(parsed["currency"], "NZD")

    def test_migration_012_and_013_applied(self) -> None:
        with self._db() as db:
            versions = [r[0] for r in db.execute("SELECT version FROM schema_migrations")]
        self.assertIn("012_shipping_pricing", versions)
        self.assertIn("013_canonical_shipping_rates", versions)
        self.assertEqual(versions, [v for v, _ in self.mig.MIGRATIONS])


class CheckoutSecurityTests(ShippingPricingTest):
    def _client(self):
        import importlib
        import app as app_module

        importlib.reload(app_module)
        return app_module, app_module.app.test_client()

    def _login_admin(self, client) -> str:
        with self._db() as db:
            db.execute(
                """
                INSERT INTO users (email, password_hash, role, full_name)
                VALUES (?, ?, 'admin', 'Staff')
                """,
                (ADMIN_EMAIL, generate_password_hash("correct-horse", method="pbkdf2:sha256")),
            )
        login = client.post(
            "/login",
            data={"email": ADMIN_EMAIL, "password": "correct-horse"},
            follow_redirects=False,
        )
        self.assertIn(login.status_code, (302, 303))
        page = client.get("/dashboard/admin/shipping")
        self.assertEqual(page.status_code, 200)
        m = re.search(rb'name="csrf-token" content="([^"]+)"', page.data)
        if not m:
            m = re.search(rb'name="csrf_token" value="([^"]+)"', page.data)
        self.assertIsNotNone(m)
        return m.group(1).decode("ascii")

    def test_unauthenticated_cannot_open_admin_shipping(self) -> None:
        _, client = self._client()
        r = client.get("/dashboard/admin/shipping", follow_redirects=False)
        self.assertIn(r.status_code, (302, 303))

    def test_unauthenticated_post_rejected(self) -> None:
        _, client = self._client()
        r = client.post("/dashboard/admin/shipping/rates/1/toggle", data={})
        self.assertIn(r.status_code, (302, 303, 403, 401))

    def test_missing_csrf_fails(self) -> None:
        _, client = self._client()
        self._login_admin(client)
        r = client.post("/dashboard/admin/shipping/settings", data={"packaging_weight_grams": "10"})
        self.assertEqual(r.status_code, 403)

    def test_invalid_csrf_fails(self) -> None:
        _, client = self._client()
        self._login_admin(client)
        r = client.post(
            "/dashboard/admin/shipping/settings",
            data={"csrf_token": "not-the-session-token", "packaging_weight_grams": "10"},
        )
        self.assertEqual(r.status_code, 403)

    def test_valid_csrf_updates_packaging(self) -> None:
        _, client = self._client()
        token = self._login_admin(client)
        r = client.post(
            "/dashboard/admin/shipping/settings",
            data={"csrf_token": token, "packaging_weight_grams": "80"},
            follow_redirects=False,
        )
        self.assertIn(r.status_code, (302, 303))
        with self._db() as db:
            self.assertEqual(self.shipping.packaging_weight_grams(db), 80)
            n = int(
                db.execute(
                    "SELECT COUNT(*) FROM audit_logs WHERE action = 'SHIPPING_SETTINGS_UPDATED'"
                ).fetchone()[0]
            )
        self.assertGreaterEqual(n, 1)

    def test_checkout_ignores_client_shipping_price(self) -> None:
        os.environ["STRIPE_SECRET_KEY"] = "sk_test_shipping_dummy"
        app_module, client = self._client()
        with self._db() as db:
            pid = self._set_price(db, "riff", 29900)
        with client.session_transaction() as sess:
            sess["cart"] = [{"product_id": pid, "quantity": 1}]
        fake = MagicMock()
        fake.url = "https://checkout.stripe.test/pay"
        with patch.object(app_module.stripe.checkout.Session, "create", return_value=fake) as create:
            r = client.post(
                "/checkout",
                data={
                    "country": "NZ",
                    "shipping_method": "NZ_STANDARD",
                    "shipping_cents": "0",
                    "shipping_price": "0",
                    "notes": "",
                },
                follow_redirects=False,
            )
        self.assertEqual(r.status_code, 303)
        kwargs = create.call_args.kwargs
        meta = kwargs["metadata"]
        self.assertEqual(meta["shipping_cents"], "1800")
        amounts = [li["price_data"]["unit_amount"] for li in kwargs["line_items"]]
        self.assertIn(29900, amounts)
        self.assertIn(1800, amounts)
        self.assertEqual(sum(a * li["quantity"] for a, li in zip(amounts, kwargs["line_items"])), 31700)
        self.assertEqual(kwargs["shipping_address_collection"]["allowed_countries"], ["NZ"])

    def test_checkout_requires_method(self) -> None:
        os.environ["STRIPE_SECRET_KEY"] = "sk_test_shipping_dummy"
        app_module, client = self._client()
        with self._db() as db:
            pid = int(self._product(db)["id"])
        with client.session_transaction() as sess:
            sess["cart"] = [{"product_id": pid, "quantity": 1}]
        with patch.object(app_module.stripe.checkout.Session, "create") as create:
            r = client.post("/checkout", data={"country": "NZ", "notes": ""}, follow_redirects=True)
        create.assert_not_called()
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"shipping method", r.data.lower())

    def test_checkout_international_contact_us(self) -> None:
        _, client = self._client()
        with self._db() as db:
            pid = int(self._product(db)["id"])
        with client.session_transaction() as sess:
            sess["cart"] = [{"product_id": pid, "quantity": 1}]
        page = client.get("/checkout?country=DE")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"International shipping", page.data)
        self.assertIn(b"Contact us", page.data)

    def test_checkout_shows_free_not_zero(self) -> None:
        _, client = self._client()
        with self._db() as db:
            pid = self._set_price(db, "riff", 30000)
        with client.session_transaction() as sess:
            sess["cart"] = [{"product_id": pid, "quantity": 1}]
        page = client.get("/checkout?country=NZ&method=NZ_STANDARD")
        self.assertEqual(page.status_code, 200)
        self.assertIn(b"FREE", page.data)
        self.assertNotIn(b"$0.00", page.data)

    def test_admin_create_rate_rejects_overlap(self) -> None:
        _, client = self._client()
        token = self._login_admin(client)
        with self._db() as db:
            mid, zid = self._ids(db, "NZ_STANDARD", "NZ")
        r = client.post(
            "/dashboard/admin/shipping/rates/new",
            data={
                "csrf_token": token,
                "shipping_method_id": str(mid),
                "shipping_zone_id": str(zid),
                "price": "12.00",
                "max_order_value": "149.99",
                "priority": "10",
                "active": "1",
                "currency": "NZD",
            },
            follow_redirects=True,
        )
        self.assertEqual(r.status_code, 200)
        self.assertIn(b"overlaps", r.data.lower())


if __name__ == "__main__":
    unittest.main()
