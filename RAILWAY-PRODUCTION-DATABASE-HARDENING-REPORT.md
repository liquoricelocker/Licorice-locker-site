# Liquorice Locker — Railway production database hardening

## Root cause

Gunicorn imported `app.py`, which always calls `database.bootstrap()`. Railway sets production. The previous rule was: missing SQLite file → crash, even on an empty persistent Volume. With **web-volume** mounted at `/data` and no shop file yet, that crash was expected — and it blocked a legitimate first boot.

There was no production business data to preserve. First-boot create is now allowed **only** on a verified Railway Volume.

## Railway configuration

| Item | Value |
|------|--------|
| Service | web |
| Volume | web-volume |
| Mount | `/data` |
| `DATABASE_PATH` | `/data/licorice.db` |
| `DATABASE_VOLUME_ROOT` | `/data` |
| `RAILWAY_VOLUME_MOUNT_PATH` | injected by Railway (expected `/data`) |
| Start | `gunicorn --preload --bind 0.0.0.0:$PORT app:app` |

## Database path resolution

Single authority: `database_config.get_database_path()`.

Production:

1. `DATABASE_PATH` if set (must be absolute)
2. else `<RAILWAY_VOLUME_MOUNT_PATH>/licorice.db`
3. else crash

Never `data/licorice.db`, `./licorice.db`, or `/app/data/licorice.db` in production.

## Volume validation

`classify_production_database_state()`:

| State | Meaning |
|-------|---------|
| `EXISTING` | File exists inside the verified Volume. Open, migrate, never recreate. |
| `EMPTY_VOLUME_BOOTSTRAP` | File missing, `RAILWAY_VOLUME_MOUNT_PATH` is a writable directory, path is inside it (`Path.relative_to`). Create once. |
| `MISSING_VOLUME` | No mount / mount directory missing. Crash. Do not create. |
| `INVALID` | Path on ephemeral storage or outside the Volume (`/data-evil` is not inside `/data`). Crash. |

`/data/licorice.db` under `/data` is valid. `/data-evil/licorice.db` is not.

## First-bootstrap logic

On `EMPTY_VOLUME_BOOTSTRAP`:

1. Create a temp file next to the destination (not `licorice.db` itself)
2. WAL, foreign keys, busy_timeout 5000, synchronous NORMAL
3. Migrations `001`–`013`
4. Official catalogue (Sound Wave, Mini Series) if `products` is empty
5. Canonical NZ/AU shipping rates if `shipping_rates` is empty; LOCAL_PICKUP $0 if missing
6. No fake users, customers, orders, payments, or Stripe rows
7. Inventory rows at on_hand 0 if the inventory tables exist
8. `PRAGMA integrity_check` and `PRAGMA foreign_key_check`
9. Atomic `os.replace` onto `/data/licorice.db`

If any step fails, the temp file is deleted. The live path is not left half-created.

## Migration safety

`migrations/runner.py` stamps a version only after the migration function returns. Failures are not marked complete. Startup stops. A file lock serializes schema work if more than one process runs bootstrap. `--preload` runs bootstrap in the Gunicorn master on Linux before workers serve traffic.

Existing catalogue and shipping rates are not deleted by migrations. `apply_canonical_shipping_rates()` inserts only when the rates table is empty.

## Database integrity

After boot: `PRAGMA integrity_check` must be `ok`, `PRAGMA foreign_key_check` must be empty, core tables present, production shop not unexpectedly empty (after first-boot seed). Failure → non-zero exit, no traffic.

## SQLite configuration

Every `get_db()` connection: foreign_keys ON, WAL, busy_timeout 5000, rollback on exception, close in `finally`. Production opens existing files with URI `mode=rw` so SQLite cannot create a missing file on a normal request.

## Gunicorn configuration

`Dockerfile`, `Procfile`, `railway.json`: `gunicorn --preload --bind 0.0.0.0:$PORT app:app`.

On macOS, `--preload` after SQLite use can SIGSEGV forked workers (CPython/SQLite). Railway is Linux. Local gunicorn tests omit `--preload` on Darwin only. Production keeps `--preload`.

## Shipping configuration

Database-owned. Calculator: `calculate_shipping()` only.

First boot (empty rates table): NZ $12 / $18 / FREE, AU $35 / $50, rest of world no automated rate, local pickup $0 NZ. Later boots do not overwrite rates.

## Stripe configuration

Unchanged. Bootstrap does not create payments or sessions. Checkout remains transactional, integer cents, server-side shipping, idempotent sessions/payments/events.

## Inventory configuration

No invented stock. First boot may insert inventory_items at quantity 0. `INVENTORY_ENFORCE` remains off unless set. Health reports ENABLED/DISABLED.

## Test results

```
Ran 124 tests in 8.482s
OK
```

Includes previous 113 plus Volume first-boot, ephemeral refusal, outside-volume refusal, five-times restart without reset, persistence of product/customer/order, shipping quotes, Flask storefront, gunicorn boot.

## Remaining risks

- Operator must attach **web-volume** at `/data` and set the two `DATABASE_*` variables. The repo cannot do that.
- macOS `gunicorn --preload` + SQLite can SIGSEGV workers; Linux/Railway is the production target.
- First boot seeds the official catalogue. After that, the database is source of truth; Python will not overwrite prices.
- If someone points `DATABASE_PATH` at ephemeral disk, boot fails. If they later attach a Volume with a **different** empty disk, first-boot will create a **new** shop on that Volume. That is intended when the Volume is empty. Do not attach a second empty Volume if `/data/licorice.db` already holds live data.

## Exact Railway actions required

1. Confirm **web-volume** is attached to **web**, mount **`/data`**.
2. Set `DATABASE_PATH=/data/licorice.db`.
3. Set `DATABASE_VOLUME_ROOT=/data`.
4. Deploy this code.
5. First logs: `EMPTY VOLUME BOOTSTRAP`, `Persistent storage: VERIFIED`.
6. Health + storefront + test-mode checkout.
7. Restart: `EXISTING DATABASE`, same counts.

No other manual database copy is required for this empty-Volume first shop.
