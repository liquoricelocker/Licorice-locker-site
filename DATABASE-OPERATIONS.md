# Liquorice Locker — database operations

The business database is SQLite. The application must never silently create an empty **production** file, and must never overwrite catalogue rows on startup.

## Locations

| Environment | Path |
|-------------|------|
| Production | **`DATABASE_PATH` (required)** on the Railway persistent volume |
| Development | `DATABASE_PATH` if set; otherwise existing `data/licorice.db` if present, else `data/licorice-dev.db` |
| Tests | Isolated temp file via `DATABASE_PATH` + `LICORICE_ENV=test` or `development` |

Do not commit `.db` files. Pre-migration backups are written next to the live file as `licorice-pre-migration-YYYYMMDD-HHMMSS.db`. Those files must never be served as static assets.

## Environment variables

| Variable | Purpose |
|----------|---------|
| `DATABASE_PATH` | Absolute path to the SQLite file. Required in production unless a Railway volume already contains `licorice.db`. |
| `DATABASE_VOLUME_ROOT` | Optional. If set, `DATABASE_PATH` must be inside this directory. |
| `RAILWAY_VOLUME_MOUNT_PATH` | Set by Railway when a volume is attached. If `DATABASE_PATH` is unset, the app will use an existing `licorice.db` on that mount. It will **not** create a new empty file. |
| `LICORICE_ENV` | `production` / `development` / `test`. Production is also inferred from `RAILWAY_ENVIRONMENT`. |
| `INVENTORY_ENFORCE` | If `1`/`true`, paid orders fail when stock is insufficient. **Default is off.** Enabling this before inventory is populated can block checkout. The app will not invent stock. Health reports `ENABLED` / `DISABLED`. |

## Railway volume — manual verification required

If `DATABASE_PATH` is unset and Railway has attached a volume (`RAILWAY_VOLUME_MOUNT_PATH`), the app will use an existing `licorice.db` (or `licorice-dev.db`) on that volume. It still **will not create** a blank production database.

**RAILWAY CONFIGURATION REQUIRES MANUAL VERIFICATION**

In Railway:

1. Open the service → **Volumes**. If none is attached, attach one and note the **mount path**.
2. Put the live database on that mount as `licorice.db`.
3. Set `DATABASE_PATH` to that file, e.g. `<mount>/licorice.db` (recommended even if auto-detect works).
4. Redeploy. If the file is missing, the app **will refuse to start** rather than create a blank shop.

If the live site was previously using a database inside the container filesystem (lost on deploy), restore from backup onto the volume first.

## Startup safety

1. Resolve `DATABASE_PATH`.
2. Production: fail if the path is missing, the file is missing, the file is tiny, core tables are missing, or the file has zero products **and** zero users **and** zero orders.
3. Backup with the SQLite backup API if migrations are pending.
4. Run unused migrations once; record them in `schema_migrations`.
5. **Do not** rewrite product names, prices, descriptions, or galleries from Python.
6. Development only: seed catalogue if `products` is empty.

Production never creates a new database file. That rule is permanent.

## Connection strategy

All application code must use `db.get_db()` / `db.get_connection()`.

- `foreign_keys = ON`
- `journal_mode = WAL`
- `busy_timeout = 5000`
- `synchronous = NORMAL`
- connect timeout `30s`
- commit on success, rollback on exception, always close
- `get_db(immediate=True)` takes `BEGIN IMMEDIATE` for inventory and Stripe order writes
- `get_db(commit=False)` is for read-only health/integrity scans (rolls back)

Do not add a second database helper.

## Migrations

Code: `migrations/runner.py`, versions `001`–`011`.

```
flask --app app init-db
```

runs `bootstrap()` (safe migrations). `seed_if_empty` is a no-op in production.

Re-running migrations is safe; applied versions are skipped.

- `010_hardening` — `stripe_processed_events`, unique SALE-per-order, supplier-component uniqueness.
- `011_inventory_refs` — unique RECEIPT/RETURN/PRODUCTION_OUTPUT per reference.

**Migration failure:** a version is stamped only after the migration function returns. Python `executescript()` issues `COMMIT`, so DDL from a failed version may already exist even though `schema_migrations` does not list it. Recovery: restore the timestamped pre-migration backup, fix the migration, restart. Do not stamp by hand.

## CSRF (admin and affiliate dashboard)

State-changing `POST`/`PUT`/`PATCH`/`DELETE` to `/dashboard/*` and `/api/admin*` require a session-tied CSRF token (`csrf_token` form field or `X-CSRF-Token` header). Invalid or missing tokens return **403** with no internals. Stripe `/webhook` and `/api/analytics/*` are excluded.

## Refunds

Stripe refunds are **not** implemented. Do not invent a refund pipeline. When adding one, hook:

- `payments.status` `succeeded` → `refunded` (`commerce_state.py`)
- order cancellation policy (commissions currently stay on the monthly snapshot unless you change payout rules on purpose)
- inventory `return_inventory(..., reference_type='order', reference_id=order_id)`
- `insert_audit_log` / `insert_business_event`

## Tests

```bash
python3 -m unittest tests.test_database_persistence tests.test_database_hardening tests.test_production_readiness -v
```

Tests refuse `data/licorice.db` and `data/licorice-dev.db` when `LICORICE_ENV=test`.

## Backup and restore

**Backup (automatic):** before pending migrations, a timestamped copy is written beside the database using the SQLite backup API (`Connection.backup`). This is not a raw copy of a live WAL file.

**Backup (manual):** from a shell that can see the volume:

```bash
python3 -c "import sqlite3,sys; s=sqlite3.connect(sys.argv[1]); d=sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()" \
  "$DATABASE_PATH" "/tmp/licorice-manual-backup.db"
```

Never overwrite `$DATABASE_PATH` with the backup command’s destination. Never expose backup files on a public URL.

**Restore (disaster recovery):**

1. Stop the app (Railway deploy pause / scale to 0).
2. Confirm the volume mount path and `DATABASE_PATH`.
3. Copy the chosen backup onto `DATABASE_PATH` (replace the file; do not merge).
4. Start the app. Confirm `/dashboard/admin/database-health` (staff login): same path, migration count, product/order counts.
5. Do not `DROP` tables to “fix” production. Repair with an additive migration or application-level fix.

## Health check

Staff only (admin login + allowlist):

`GET /dashboard/admin/database-health`

Returns connection path/size/readable/writable, SQLite version/journal/foreign_keys/busy_timeout, migration version, missing tables, integrity (orphans, duplicates, invalid financial values), and row counts. No customer PII, card data, or Stripe secrets. The endpoint does not write.

Integrity is also available in-process via `database.integrity_report(conn)` (read-only).

## Stripe idempotency

Duplicate Checkout Sessions cannot create a second order: unique index on `orders.stripe_checkout_session_id`. Payments use a unique index on `payments.provider_session_id`. Webhook event ids are stored in `stripe_processed_events` (primary key `event_id`).

Finalize uses `BEGIN IMMEDIATE`. If an order row exists but has no items (crash mid-write), a later webhook/return completes the items instead of returning success with an empty order.

Commission snapshots are monthly (`UNIQUE(affiliate_user_id, year_month)`). A complete duplicate session skips commission refresh so a Stripe retry does not rewrite history as a new sale. Incomplete-order repair does apply commission once items exist.

## Inventory operations

Use `inventory.py` only. Do not `UPDATE inventory_items` quantity columns from routes.

| Function | Effect |
|----------|--------|
| `reserve_inventory` | Increase reserved if available ≥ qty (`BEGIN IMMEDIATE` + `WHERE` on available) |
| `release_inventory` | Decrease reserved; refuses negative reserved |
| `receive_inventory` | Increase on-hand + RECEIPT movement |
| `consume_inventory_sale` | SALE movement first (unique per item+order), then deduct on-hand |
| `return_inventory` | Increase on-hand + RETURN movement |
| `adjust_inventory` | Requires a reason; refuses negative on-hand |
| `transfer_inventory` | Two movements in one transaction |

Unmanaged SKUs (on-hand 0, reserved 0, no movements) skip deduction on paid orders unless `INVENTORY_ENFORCE=1`.

`inventory.ledger_discrepancies()` reports on-hand vs reconstructed ledger. It never auto-corrects.

## Historical prices

`order_items.unit_price_cents` is a snapshot. Changing `products.price_cents` must not change existing orders.
