# Database architecture audit — implemented fixes

Phase 1 found a Flask + SQLite store (not Prisma). This document records what was actually changed to make the database the source of truth without rewriting the app.

## Root cause of disappearing data

Two mechanisms:

1. **`init_db()` on every process start** called `_sync_product_catalog_prices()` and `_backfill_*_marketing()`, which `UPDATE`d product prices/copy and `DELETE`d/`INSERT`ed gallery rows from Python constants. Any in-database catalogue edit was reverted on deploy/restart.
2. **`sqlite3.connect()` creates a file if missing.** If `DATABASE_PATH` was unset on Railway, or pointed at ephemeral disk, a **new empty** `data/licorice.db` could appear after deploy. Orders, users, and affiliates then looked “wiped.”

## What was preserved

Existing 14 tables remain. No ID, slug, or order-number rewrites. Local verification after migration: product ids 1–5, prices, 9 users, 1 order unchanged.

## Fixes

- Single path resolver: `database_config.get_database_path()`.
- Production refuses to start if `DATABASE_PATH` is unset or the file is missing/empty.
- Startup no longer overwrites catalogue data.
- Lightweight `schema_migrations` runner (`001`–`009`).
- WAL + foreign_keys + busy_timeout on each connection.
- Pre-migration SQLite backup beside the live file.
- Stripe unique session index plus `IntegrityError` recovery on insert.
- Admin health JSON at `/dashboard/admin/database-health`.

## Additive systems (empty or backfilled, storefront unchanged)

Default `product_variants` (one per product, same price/SKU), `customers` / addresses backfilled from orders, `payments` from Stripe session ids, `shipments` where tracking/fulfillment existed, inventory locations/items at **quantity 0** (no fabricated stock), manufacturing tables, `audit_logs`, `business_events`.

## Railway

Mount path is **not** in this repo. Set `DATABASE_PATH` on the volume after checking Railway → Volumes. See `DATABASE-OPERATIONS.md`.
