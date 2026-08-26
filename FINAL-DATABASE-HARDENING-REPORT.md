# Final database hardening report

**Verdict: READY WITH MANUAL ACTIONS**

Railway production persistence was **not** verified in this pass. Local tests, the isolated production-like schema, and the local `data/licorice.db` copy from the previous pass are not a substitute for checking the live volume.

## Executive summary

This pass added session-tied CSRF on dashboard mutations, explicit order/payment transitions, inventory enforcement visibility, integer-cent commission math, ledger discrepancy reporting, backup/restore tests, test isolation from `data/licorice.db`, and migration `011_inventory_refs`. Flask + SQLite were not replaced. No production tables were rebuilt or dropped.

**42 tests passed** (`tests.test_database_persistence`, `tests.test_database_hardening`, `tests.test_production_readiness`).

## Issues found

- Admin and affiliate dashboard POSTs had no CSRF tokens.
- `_sync_product_catalog_prices` still existed and would overwrite catalogue prices if called.
- `INVENTORY_ENFORCE` was easy to turn on accidentally with empty stock.
- Health report did not show inventory enforcement or ledger discrepancies.
- Display/list-price math used `99.99 * 100` / `429 * 0.30` floats.
- Receipt/return movements were not uniquely keyed by business reference.
- Tests could theoretically be pointed at `data/licorice.db`.
- Stripe refunds are not implemented (confirmed).
- SQLite `executescript()` commits, so a failed migration cannot always roll back DDL.

## Issues fixed

- Lightweight CSRF (`csrf.py`): session token, form field + `X-CSRF-Token`, 403 with no internals. Applied to `/dashboard/*` and `/api/admin*` state-changing methods.
- Admin/affiliate forms include the token; Listening Room terms AJAX sends the header.
- Token rotated after login.
- Order fulfillment uses `commerce_state` (cancelled → shipped blocked; shipped → paid unfulfill remains **explicitly** supported).
- Payments: succeeded → failed blocked; succeeded → succeeded idempotent.
- Incomplete orders fill items only from authoritative lines; empty line lists refuse to fabricate.
- Inventory ledger reconstruction + read-only discrepancy report.
- Unique RECEIPT/RETURN/PRODUCTION_OUTPUT indexes (migration 011).
- Health JSON: persistence/volume flags, business counts, `inventory.enforcement` ENABLED/DISABLED, ledger discrepancies.
- Startup warning if `INVENTORY_ENFORCE` is on.
- `_sync_product_catalog_prices` now raises instead of updating rows.
- Test mode refuses the developer shop database files.
- `restore_sqlite_backup()` refuses to overwrite the live production path.
- Audit logs on commission marked paid, tracking updates, and unfulfill.
- Commission amounts use integer percents.

## Issues intentionally not fixed

- **Railway volume** — cannot be verified from this repo. Operator must confirm mount + `DATABASE_PATH`.
- **SQLite CHECK rebuilds** on existing money columns — too risky; application validation remains.
- **Full Stripe refund product** — not present; hooks documented, not invented.
- **Storefront cart CSRF** — public cart POSTs are not behind the dashboard CSRF gate (would need every shop form). Remaining site-wide CSRF gap.
- **Login/signup CSRF** — `/login` and `/auth/*` are outside `/dashboard/`.
- **GET `/logout`** — still a GET (session CSRF not applied).
- **Product admin editor** — still none; catalogue is DB-owned.
- **Monthly commission snapshot recalculation** on a genuine new sale — existing payout behaviour, documented.
- **Perfect migration DDL rollback** — SQLite `executescript` limitation, documented.

## Database integrity

Read-only `integrity_report()` / health `integrity` key. Does not auto-repair. Previous local scan: no FK violations, no orphans, historical Sound Wave order still $129 vs catalogue $429.

## Transaction safety

`get_db()` still commits or rolls back and always closes. Stripe order writes and inventory use `BEGIN IMMEDIATE`. Analytics remain fail-soft.

## Stripe safety

Uniques unchanged: checkout session, payment session, event id, SALE-per-order. Incomplete repair does not invent prices. No live charge was made in this pass.

## Inventory safety

Ledger ops unchanged in spirit; receipts/returns with references are idempotent. Ten concurrent reservations against stock 5 yielded 5. Enforcement default **DISABLED**. Health makes that explicit.

## Affiliate safety

Monthly unique snapshot unchanged. Duplicate Stripe complete replay still skips refresh. CSRF on “mark paid”. Integer commission cents for $129 / $99.99 / $429.

## Security

| Route | Method | Auth | Admin allowlist | CSRF | Mutates DB |
|-------|--------|------|-----------------|------|------------|
| `/dashboard/admin` | GET | login | yes | n/a | no |
| `/dashboard/admin/database-health` | GET | login | yes | n/a | no (rollback) |
| `/dashboard/admin/mark-affiliate-paid` | POST | login | yes | yes | commissions |
| `/dashboard/admin/order/<id>/fulfillment` | POST | login | yes | yes | orders, shipments, audit |
| `/dashboard/admin/order/<id>` tracking | POST | login | yes | yes | orders, shipments, audit |
| `/dashboard/admin/creative-library` | POST | login | yes | yes | creative_assets |
| `/dashboard/admin/creative-library/<id>/delete` | POST | login | yes | yes | creative_assets |
| `/api/admin/summary` | GET | login | yes | n/a | no |
| `/api/admin/analytics` | GET | login | yes | n/a | no |
| `/dashboard/affiliate/*` POST | POST | login | affiliate (not admin) | yes | users / pages |
| `/webhook` | POST | Stripe signature | n/a | excluded | orders/payments |

Health remains staff-only. Path is included for operators; it is not shown to customers. 500s stay generic.

## Backup and recovery

SQLite backup API. Restore helper tested on disposable files. Application code will not restore over the live production path. Operator restore: stop app, replace file at `DATABASE_PATH`, start, check health.

## Migration status

`001`–`011`. Failed migrations are not stamped. DDL from `executescript` may remain; restore from pre-migration backup.

## Test results

42 passed, including CSRF (missing/invalid 403, valid not 403), production no local fallback, volume root mismatch, test-path isolation, backup/restore, failed migration not stamped, 10-way inventory reservation, ledger discrepancy report, exact cents, incomplete-order fill-once, shop request loop.

## Remaining risks

- Railway not verified.
- Storefront and auth CSRF not complete.
- `INVENTORY_ENFORCE=1` with empty stock can stop sales.
- No refund automation.
- No CSRF on GET logout.

## Required Railway actions

Follow `PRODUCTION-DEPLOYMENT-CHECKLIST.md`. Confirm volume mount, set `DATABASE_PATH` on the volume, optionally `DATABASE_VOLUME_ROOT`, backup, deploy, compare health counts, leave inventory enforcement disabled until stock is loaded.

## Production readiness

**READY WITH MANUAL ACTIONS**

The application is not “production ready” solely because tests pass. It is ready to deploy **after** the operator confirms persistent storage and post-deploy health counts. It is **not** claimed that Railway is already correct.
