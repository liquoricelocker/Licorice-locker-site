"""Ordered SQLite migrations. Each version runs at most once and is recorded in schema_migrations."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Callable, List, Tuple

log = logging.getLogger("licorice.database")

MigrateFn = Callable[[sqlite3.Connection], None]


def _table_exists(db: sqlite3.Connection, name: str) -> bool:
    row = db.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
        (name,),
    ).fetchone()
    return row is not None


def _ensure_migrations_table(db: sqlite3.Connection) -> None:
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TEXT NOT NULL
        )
        """
    )


def applied_versions(db: sqlite3.Connection) -> List[str]:
    if not _table_exists(db, "schema_migrations"):
        return []
    rows = db.execute("SELECT version FROM schema_migrations ORDER BY version").fetchall()
    return [str(r[0]) for r in rows]


def current_version(db: sqlite3.Connection) -> str:
    vers = applied_versions(db)
    return vers[-1] if vers else "(none)"


def _stamp(db: sqlite3.Connection, version: str) -> None:
    db.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        (version, datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")),
    )


def migrate_001_baseline(db: sqlite3.Connection) -> None:
    import db as database

    database.apply_baseline_schema(db)


def migrate_002_indexes(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        CREATE INDEX IF NOT EXISTS idx_orders_fulfillment ON orders(fulfillment_status);
        CREATE INDEX IF NOT EXISTS idx_orders_customer_email ON orders(customer_email);
        CREATE INDEX IF NOT EXISTS idx_order_items_product ON order_items(product_id);
        CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
        CREATE INDEX IF NOT EXISTS idx_products_slug ON products(slug);
        """
    )


def migrate_003_product_variants(db: sqlite3.Connection) -> None:
    import db as database

    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS product_variants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
            name TEXT NOT NULL DEFAULT 'Default',
            sku TEXT,
            barcode TEXT,
            status TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'archived', 'draft')),
            price_cents INTEGER NOT NULL,
            compare_at_price_cents INTEGER,
            cost_cents INTEGER,
            currency TEXT NOT NULL DEFAULT 'NZD',
            weight_grams INTEGER,
            width_cm REAL,
            height_cm REAL,
            depth_cm REAL,
            is_default INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            deleted_at TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_product_variants_sku
            ON product_variants(sku)
            WHERE sku IS NOT NULL AND length(trim(sku)) > 0;
        CREATE INDEX IF NOT EXISTS idx_product_variants_product ON product_variants(product_id);
        """
    )
    database.ensure_default_variants(db)
    cols = {row[1] for row in db.execute("PRAGMA table_info(order_items)").fetchall()}
    if "variant_id" not in cols:
        db.execute(
            "ALTER TABLE order_items ADD COLUMN variant_id INTEGER REFERENCES product_variants(id)"
        )
    if "product_name_snapshot" not in cols:
        db.execute("ALTER TABLE order_items ADD COLUMN product_name_snapshot TEXT")
    if "sku_snapshot" not in cols:
        db.execute("ALTER TABLE order_items ADD COLUMN sku_snapshot TEXT")
    if "discount_cents" not in cols:
        db.execute("ALTER TABLE order_items ADD COLUMN discount_cents INTEGER NOT NULL DEFAULT 0")
    if "tax_cents" not in cols:
        db.execute("ALTER TABLE order_items ADD COLUMN tax_cents INTEGER NOT NULL DEFAULT 0")
    if "line_subtotal_cents" not in cols:
        db.execute("ALTER TABLE order_items ADD COLUMN line_subtotal_cents INTEGER")
    if "line_total_cents" not in cols:
        db.execute("ALTER TABLE order_items ADD COLUMN line_total_cents INTEGER")
    db.execute(
        """
        UPDATE order_items
        SET line_subtotal_cents = quantity * unit_price_cents
        WHERE line_subtotal_cents IS NULL
        """
    )
    db.execute(
        """
        UPDATE order_items
        SET line_total_cents = IFNULL(line_subtotal_cents, quantity * unit_price_cents)
            - IFNULL(discount_cents, 0) + IFNULL(tax_cents, 0)
        WHERE line_total_cents IS NULL
        """
    )
    db.execute(
        """
        UPDATE order_items
        SET product_name_snapshot = (
            SELECT name FROM products WHERE products.id = order_items.product_id
        )
        WHERE product_name_snapshot IS NULL OR trim(product_name_snapshot) = ''
        """
    )
    db.execute(
        """
        UPDATE order_items
        SET sku_snapshot = (
            SELECT sku FROM products WHERE products.id = order_items.product_id
        )
        WHERE sku_snapshot IS NULL OR trim(sku_snapshot) = ''
        """
    )
    db.execute(
        """
        UPDATE order_items
        SET variant_id = (
            SELECT v.id FROM product_variants v
            WHERE v.product_id = order_items.product_id AND v.is_default = 1
            ORDER BY v.id LIMIT 1
        )
        WHERE variant_id IS NULL
        """
    )


def migrate_004_customers(db: sqlite3.Connection) -> None:
    import db as database

    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            email_normalized TEXT NOT NULL,
            first_name TEXT NOT NULL DEFAULT '',
            last_name TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            company_name TEXT NOT NULL DEFAULT '',
            customer_type TEXT NOT NULL DEFAULT 'retail',
            marketing_consent INTEGER NOT NULL DEFAULT 0,
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            deleted_at TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_customers_email_normalized
            ON customers(email_normalized);
        CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);

        CREATE TABLE IF NOT EXISTS customer_addresses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
            type TEXT NOT NULL DEFAULT 'shipping'
                CHECK (type IN ('shipping', 'billing', 'other')),
            first_name TEXT NOT NULL DEFAULT '',
            last_name TEXT NOT NULL DEFAULT '',
            company TEXT NOT NULL DEFAULT '',
            address_line1 TEXT NOT NULL DEFAULT '',
            address_line2 TEXT NOT NULL DEFAULT '',
            city TEXT NOT NULL DEFAULT '',
            region TEXT NOT NULL DEFAULT '',
            postcode TEXT NOT NULL DEFAULT '',
            country TEXT NOT NULL DEFAULT '',
            is_default INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_customer_addresses_customer
            ON customer_addresses(customer_id);
        """
    )
    ocols = {row[1] for row in db.execute("PRAGMA table_info(orders)").fetchall()}
    if "customer_id" not in ocols:
        db.execute("ALTER TABLE orders ADD COLUMN customer_id INTEGER REFERENCES customers(id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)")
    database.backfill_customers_from_orders(db)


def migrate_005_payments(db: sqlite3.Connection) -> None:
    import db as database

    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
            provider TEXT NOT NULL DEFAULT 'stripe',
            provider_payment_id TEXT,
            provider_session_id TEXT,
            amount_cents INTEGER NOT NULL,
            currency TEXT NOT NULL DEFAULT 'NZD',
            status TEXT NOT NULL DEFAULT 'succeeded'
                CHECK (status IN ('pending', 'succeeded', 'failed', 'refunded')),
            payment_method TEXT NOT NULL DEFAULT '',
            paid_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_payments_order ON payments(order_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_provider_session
            ON payments(provider_session_id)
            WHERE provider_session_id IS NOT NULL AND length(trim(provider_session_id)) > 0;
        """
    )
    database.backfill_payments_from_orders(db)


def migrate_006_shipments(db: sqlite3.Connection) -> None:
    import db as database

    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS shipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL REFERENCES orders(id) ON DELETE RESTRICT,
            carrier TEXT NOT NULL DEFAULT '',
            service TEXT NOT NULL DEFAULT '',
            tracking_number TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'ready', 'shipped', 'delivered', 'cancelled')),
            shipped_at TEXT,
            delivered_at TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_shipments_order ON shipments(order_id);
        """
    )
    database.backfill_shipments_from_orders(db)


def migrate_007_inventory(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS inventory_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'warehouse'
                CHECK (type IN ('workshop', 'warehouse', 'showroom', 'supplier', 'home', 'other')),
            address TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_locations_name ON inventory_locations(name);

        CREATE TABLE IF NOT EXISTS inventory_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            variant_id INTEGER REFERENCES product_variants(id) ON DELETE RESTRICT,
            component_id INTEGER,
            location_id INTEGER NOT NULL REFERENCES inventory_locations(id) ON DELETE RESTRICT,
            quantity_on_hand INTEGER NOT NULL DEFAULT 0,
            quantity_reserved INTEGER NOT NULL DEFAULT 0,
            reorder_point INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            CHECK (quantity_on_hand >= 0),
            CHECK (quantity_reserved >= 0),
            CHECK (
                (variant_id IS NOT NULL AND component_id IS NULL)
                OR (variant_id IS NULL AND component_id IS NOT NULL)
            )
        );
        CREATE INDEX IF NOT EXISTS idx_inventory_items_location ON inventory_items(location_id);
        CREATE INDEX IF NOT EXISTS idx_inventory_items_variant ON inventory_items(variant_id);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_items_variant_loc
            ON inventory_items(variant_id, location_id)
            WHERE variant_id IS NOT NULL;

        CREATE TABLE IF NOT EXISTS inventory_movements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inventory_item_id INTEGER NOT NULL REFERENCES inventory_items(id) ON DELETE RESTRICT,
            movement_type TEXT NOT NULL
                CHECK (movement_type IN (
                    'RECEIPT', 'SALE', 'RETURN', 'DAMAGE', 'ADJUSTMENT', 'TRANSFER',
                    'RESERVATION', 'RELEASE', 'PRODUCTION_CONSUMPTION', 'PRODUCTION_OUTPUT'
                )),
            quantity INTEGER NOT NULL,
            reference_type TEXT NOT NULL DEFAULT '',
            reference_id INTEGER,
            reason TEXT NOT NULL DEFAULT '',
            created_by_id INTEGER REFERENCES users(id),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_inventory_movements_item ON inventory_movements(inventory_item_id);
        CREATE INDEX IF NOT EXISTS idx_inventory_movements_created ON inventory_movements(created_at);
        CREATE INDEX IF NOT EXISTS idx_inventory_movements_ref ON inventory_movements(reference_type, reference_id);
        """
    )
    db.execute(
        """
        INSERT INTO inventory_locations (name, type, status)
        SELECT 'Finished Goods Storage', 'warehouse', 'active'
        WHERE NOT EXISTS (SELECT 1 FROM inventory_locations LIMIT 1)
        """
    )
    loc = db.execute("SELECT id FROM inventory_locations ORDER BY id LIMIT 1").fetchone()
    if loc:
        lid = int(loc["id"] if isinstance(loc, sqlite3.Row) else loc[0])
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


def migrate_008_manufacturing(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS components (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sku TEXT,
            description TEXT NOT NULL DEFAULT '',
            unit TEXT NOT NULL DEFAULT 'each',
            cost_cents INTEGER NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'NZD',
            reorder_point INTEGER NOT NULL DEFAULT 0,
            reorder_quantity INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_components_sku
            ON components(sku) WHERE sku IS NOT NULL AND length(trim(sku)) > 0;

        CREATE TABLE IF NOT EXISTS product_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE RESTRICT,
            version TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'draft'
                CHECK (status IN ('draft', 'active', 'archived')),
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE (product_id, version)
        );

        CREATE TABLE IF NOT EXISTS bills_of_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_version_id INTEGER NOT NULL REFERENCES product_versions(id) ON DELETE RESTRICT,
            name TEXT NOT NULL,
            version TEXT NOT NULL DEFAULT '1',
            status TEXT NOT NULL DEFAULT 'draft'
                CHECK (status IN ('draft', 'active', 'archived')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS bill_of_material_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bom_id INTEGER NOT NULL REFERENCES bills_of_materials(id) ON DELETE CASCADE,
            component_id INTEGER NOT NULL REFERENCES components(id) ON DELETE RESTRICT,
            quantity REAL NOT NULL,
            unit TEXT NOT NULL DEFAULT 'each',
            notes TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_bom_items_bom ON bill_of_material_items(bom_id);

        CREATE TABLE IF NOT EXISTS suppliers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            contact_name TEXT NOT NULL DEFAULT '',
            email TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            website TEXT NOT NULL DEFAULT '',
            notes TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'inactive')),
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_suppliers_name ON suppliers(name);

        CREATE TABLE IF NOT EXISTS supplier_components (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_id INTEGER NOT NULL REFERENCES suppliers(id) ON DELETE RESTRICT,
            component_id INTEGER NOT NULL REFERENCES components(id) ON DELETE RESTRICT,
            supplier_sku TEXT NOT NULL DEFAULT '',
            unit_cost_cents INTEGER NOT NULL DEFAULT 0,
            currency TEXT NOT NULL DEFAULT 'NZD',
            minimum_order_quantity INTEGER NOT NULL DEFAULT 1,
            lead_time_days INTEGER NOT NULL DEFAULT 0,
            is_preferred INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_supplier_components_component ON supplier_components(component_id);
        CREATE INDEX IF NOT EXISTS idx_supplier_components_supplier_sku ON supplier_components(supplier_sku);

        CREATE TABLE IF NOT EXISTS purchase_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_order_number TEXT NOT NULL UNIQUE,
            supplier_id INTEGER NOT NULL REFERENCES suppliers(id) ON DELETE RESTRICT,
            status TEXT NOT NULL DEFAULT 'draft'
                CHECK (status IN ('draft', 'sent', 'partial', 'received', 'cancelled')),
            currency TEXT NOT NULL DEFAULT 'NZD',
            subtotal_cents INTEGER NOT NULL DEFAULT 0,
            tax_cents INTEGER NOT NULL DEFAULT 0,
            total_cents INTEGER NOT NULL DEFAULT 0,
            expected_delivery_at TEXT,
            received_at TEXT,
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS purchase_order_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_order_id INTEGER NOT NULL REFERENCES purchase_orders(id) ON DELETE CASCADE,
            component_id INTEGER NOT NULL REFERENCES components(id) ON DELETE RESTRICT,
            quantity_ordered INTEGER NOT NULL,
            quantity_received INTEGER NOT NULL DEFAULT 0,
            unit_cost_cents INTEGER NOT NULL,
            total_cents INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS production_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_variant_id INTEGER NOT NULL REFERENCES product_variants(id) ON DELETE RESTRICT,
            product_version_id INTEGER REFERENCES product_versions(id),
            quantity_planned INTEGER NOT NULL,
            quantity_produced INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'PLANNED'
                CHECK (status IN (
                    'PLANNED', 'MATERIALS_REQUIRED', 'READY', 'IN_PRODUCTION',
                    'QUALITY_CONTROL', 'COMPLETED', 'CANCELLED'
                )),
            planned_start_at TEXT,
            planned_completion_at TEXT,
            actual_start_at TEXT,
            actual_completion_at TEXT,
            notes TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_production_orders_status ON production_orders(status);

        CREATE TABLE IF NOT EXISTS production_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            production_order_id INTEGER NOT NULL REFERENCES production_orders(id) ON DELETE CASCADE,
            component_id INTEGER NOT NULL REFERENCES components(id) ON DELETE RESTRICT,
            quantity_planned REAL NOT NULL,
            quantity_used REAL NOT NULL DEFAULT 0,
            unit_cost_cents INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS quality_control_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            production_order_id INTEGER NOT NULL REFERENCES production_orders(id) ON DELETE RESTRICT,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending', 'passed', 'failed')),
            inspected_by_id INTEGER REFERENCES users(id),
            notes TEXT NOT NULL DEFAULT '',
            inspected_at TEXT
        );
        """
    )


def migrate_009_audit_events(db: sqlite3.Connection) -> None:
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER REFERENCES users(id),
            action TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id INTEGER,
            before_json TEXT,
            after_json TEXT,
            metadata_json TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_audit_logs_entity ON audit_logs(entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_audit_logs_created ON audit_logs(created_at);
        CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);

        CREATE TABLE IF NOT EXISTS business_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id INTEGER,
            payload_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            processed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_business_events_entity ON business_events(entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_business_events_type ON business_events(type);
        CREATE INDEX IF NOT EXISTS idx_business_events_created ON business_events(created_at);
        """
    )


def migrate_010_hardening(db: sqlite3.Connection) -> None:
    """Additive indexes and idempotency keys. Does not rebuild existing tables."""
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS stripe_processed_events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL DEFAULT '',
            session_id TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_stripe_processed_events_session
            ON stripe_processed_events(session_id);

        CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_sale_once
            ON inventory_movements(inventory_item_id, reference_type, reference_id)
            WHERE movement_type = 'SALE'
              AND reference_type = 'order'
              AND reference_id IS NOT NULL;

        CREATE UNIQUE INDEX IF NOT EXISTS idx_supplier_component_pair
            ON supplier_components(supplier_id, component_id);

        CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
        CREATE INDEX IF NOT EXISTS idx_commissions_payout ON commissions(payout_status);
        """
    )


def migrate_011_inventory_refs(db: sqlite3.Connection) -> None:
    """Additive unique refs for receipts/returns/production. Does not rebuild tables."""
    db.executescript(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_receipt_once
            ON inventory_movements(inventory_item_id, reference_type, reference_id)
            WHERE movement_type = 'RECEIPT'
              AND reference_type != ''
              AND reference_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_return_once
            ON inventory_movements(inventory_item_id, reference_type, reference_id)
            WHERE movement_type = 'RETURN'
              AND reference_type != ''
              AND reference_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_inventory_production_output_once
            ON inventory_movements(inventory_item_id, reference_type, reference_id)
            WHERE movement_type = 'PRODUCTION_OUTPUT'
              AND reference_type != ''
              AND reference_id IS NOT NULL;
        """
    )


def migrate_012_shipping_pricing(db: sqlite3.Connection) -> None:
    """Additive shipping methods, zones, rates, and order snapshots. Does not rebuild tables."""
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS shipping_methods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL DEFAULT '',
            currency TEXT NOT NULL DEFAULT 'NZD',
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            packaging_weight_grams INTEGER NOT NULL DEFAULT 0,
            rate_source TEXT NOT NULL DEFAULT 'database',
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_shipping_methods_active ON shipping_methods(active, sort_order);

        CREATE TABLE IF NOT EXISTS shipping_zones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL DEFAULT '',
            active INTEGER NOT NULL DEFAULT 1,
            catch_all INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_shipping_zones_active ON shipping_zones(active);

        CREATE TABLE IF NOT EXISTS shipping_zone_countries (
            shipping_zone_id INTEGER NOT NULL REFERENCES shipping_zones(id) ON DELETE CASCADE,
            country_code TEXT NOT NULL,
            PRIMARY KEY (shipping_zone_id, country_code)
        );
        CREATE INDEX IF NOT EXISTS idx_shipping_zone_countries_code ON shipping_zone_countries(country_code);

        CREATE TABLE IF NOT EXISTS shipping_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            shipping_method_id INTEGER NOT NULL REFERENCES shipping_methods(id) ON DELETE RESTRICT,
            shipping_zone_id INTEGER NOT NULL REFERENCES shipping_zones(id) ON DELETE RESTRICT,
            price_cents INTEGER NOT NULL CHECK (price_cents >= 0),
            currency TEXT NOT NULL DEFAULT 'NZD',
            min_order_value_cents INTEGER,
            max_order_value_cents INTEGER,
            min_weight_grams INTEGER,
            max_weight_grams INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 0,
            estimated_min_days INTEGER,
            estimated_max_days INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_shipping_rates_lookup
            ON shipping_rates(shipping_method_id, shipping_zone_id, active, priority);

        CREATE TABLE IF NOT EXISTS shipping_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        """
    )
    cols = {row[1] for row in db.execute("PRAGMA table_info(orders)").fetchall()}
    additive = [
        ("shipping_method_code", "TEXT"),
        ("shipping_method_name", "TEXT"),
        ("shipping_rate_id", "INTEGER"),
        ("shipping_currency", "TEXT"),
        ("shipping_weight_grams", "INTEGER"),
        ("shipping_zone_code", "TEXT"),
    ]
    for name, typ in additive:
        if name not in cols:
            db.execute(f"ALTER TABLE orders ADD COLUMN {name} {typ}")
    import shipping as shipping_mod

    shipping_mod.seed_reference_data(db)


def migrate_013_canonical_shipping_rates(db: sqlite3.Connection) -> None:
    """Store shipping policy as database rates. Does not change the calculator."""
    import shipping as shipping_mod

    shipping_mod.apply_canonical_shipping_rates(db)


MIGRATIONS: List[Tuple[str, MigrateFn]] = [
    ("001_baseline", migrate_001_baseline),
    ("002_indexes", migrate_002_indexes),
    ("003_product_variants", migrate_003_product_variants),
    ("004_customers", migrate_004_customers),
    ("005_payments", migrate_005_payments),
    ("006_shipments", migrate_006_shipments),
    ("007_inventory", migrate_007_inventory),
    ("008_manufacturing", migrate_008_manufacturing),
    ("009_audit_events", migrate_009_audit_events),
    ("010_hardening", migrate_010_hardening),
    ("011_inventory_refs", migrate_011_inventory_refs),
    ("012_shipping_pricing", migrate_012_shipping_pricing),
    ("013_canonical_shipping_rates", migrate_013_canonical_shipping_rates),
]


def pending_migrations(db: sqlite3.Connection) -> List[Tuple[str, MigrateFn]]:
    _ensure_migrations_table(db)
    done = set(applied_versions(db))
    return [(v, fn) for v, fn in MIGRATIONS if v not in done]


def has_pending_migrations_readonly(path) -> bool:
    """True if the file needs migration work. Does not write."""
    from pathlib import Path

    p = Path(path)
    if not p.is_file():
        return True
    conn = sqlite3.connect(f"file:{p.resolve()}?mode=ro", uri=True)
    try:
        row = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name = 'schema_migrations' LIMIT 1"
        ).fetchone()
        if not row:
            return True
        done = {r[0] for r in conn.execute("SELECT version FROM schema_migrations")}
        return any(v not in done for v, _fn in MIGRATIONS)
    except sqlite3.Error:
        return True
    finally:
        conn.close()


def run_migrations(db: sqlite3.Connection) -> List[str]:
    """Apply pending migrations in order. Caller owns the transaction."""
    _ensure_migrations_table(db)
    applied: List[str] = []
    for version, fn in pending_migrations(db):
        log.info("Applying database migration %s", version)
        try:
            fn(db)
            _stamp(db, version)
        except Exception:
            log.exception(
                "migration_failed version=%s — version was not recorded. "
                "SQLite executescript() issues COMMIT, so DDL from this version may "
                "already be applied. Restore from the pre-migration backup if the schema looks wrong.",
                version,
            )
            raise
        applied.append(version)
        log.info("Applied database migration %s", version)
    return applied
