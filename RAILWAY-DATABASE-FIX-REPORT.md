# Liquorice Locker — Railway database fix report

## Root cause

Gunicorn crashed while importing `app.py`, which always calls `database.bootstrap()`.

Railway sets `RAILWAY_ENVIRONMENT`, so the process is treated as **production**. Production refuses to use ephemeral `data/licorice.db` (that file is wiped on every deploy). The previous resolver therefore required either:

- an explicit `DATABASE_PATH`, or
- a Railway volume **and** an existing shop file already found by scanning the mount

The live logs were:

```
DATABASE_PATH is not set, and no Railway volume is attached (RAILWAY_VOLUME_MOUNT_PATH is empty).
Refusing to fall back to a local SQLite file that would be wiped on the next deploy.
CRITICAL DATABASE SAFETY ERROR
Production requires a persistent SQLite file.
```

That failure is correct safety behaviour. The deployment architecture was incomplete: no Volume (or Railway did not inject `RAILWAY_VOLUME_MOUNT_PATH`), and `DATABASE_PATH` was not set to a file on persistent storage.

This change does **not** make production boot against a local SQLite file. It makes the intended Volume configuration work, and it still fails loud if the persistent file is missing.

## Code changes

| File | What changed |
|------|----------------|
| `database_config.py` | Single path authority. Production priority: `DATABASE_PATH` → `<RAILWAY_VOLUME_MOUNT_PATH>/licorice.db` → fail. Absolute paths only. `DATABASE_VOLUME_ROOT` containment via `Path.relative_to`. Pre-connect file checks. Path masking for logs. `python -m database_config` diagnostics. |
| `db.py` | Production `sqlite3.connect` uses URI `mode=rw` (missing file cannot be created). Bootstrap validates parent/file/SQLite header/core tables/emptiness/integrity. Startup lock around migrations. Clear success/failure logs. Staff health report expanded. Backup still uses the SQLite backup API. |
| `app.py` | Bootstrap failures exit with status 1. `flask --app app railway-db-status` diagnostic (no secrets). |
| `shipping.py` | `apply_canonical_shipping_rates()` inserts only when `shipping_rates` is empty. No `DELETE`. Does not overwrite production prices. |
| `Dockerfile` | Gunicorn `--preload` so schema work runs once in the master process. |
| `Procfile` | Same `--preload`. |
| `railway.json` | Same `--preload`. |
| `.env.example` | Documents Volume variables. No secrets. |
| `PRODUCTION-DEPLOYMENT-CHECKLIST.md` | Exact Railway steps, cutover warning, health checks. |
| `DATABASE-OPERATIONS.md` | Path priority, env ownership, backup/restore, migrations through 013. |
| `tests/test_railway_database.py` | Path matrix, missing DB, invalid SQLite, volume root, diagnostics, isolation. |
| `tests/test_production_readiness.py` | Empty-volume resolve-then-fail; health fields. |
| `tests/test_shipping_pricing.py` | Canonical apply does not overwrite existing rates. |
| `RAILWAY-DATABASE-FIX-REPORT.md` | This file. |

No catalogue rows, product IDs, slugs, orders, users, or commissions are rewritten on startup.

## Database safety

Production startup is:

1. Resolve path (never `data/licorice.db`).
2. If `DATABASE_VOLUME_ROOT` is set, require the file inside that directory.
3. Confirm parent directory exists (volume mounted).
4. Confirm the file exists, is readable/writable, is large enough, and has a SQLite header.
5. Open with `mode=rw` so SQLite cannot create a missing file.
6. Confirm core tables and that the shop is not empty.
7. Backup with the SQLite backup API if migrations are pending.
8. Run unused migrations 001–013 once.
9. Confirm `schema_migrations` and `PRAGMA integrity_check`.

Missing production database:

```
CRITICAL DATABASE SAFETY ERROR
Production database could not be verified.
DATABASE_PATH=/data/licorice.db
The file does not exist.
Refusing to create a new production database.
No production database will be created automatically.
```

The app never copies a database from ephemeral disk onto the Volume.

## Railway configuration

Set these on the **service** (Variables). Do not commit them.

| Variable | Set by | Production |
|----------|--------|------------|
| `RAILWAY_VOLUME_MOUNT_PATH` | Railway, when a Volume is attached | Automatic. Example: `/data` |
| `DATABASE_PATH` | You | **Set this.** Example: `/data/licorice.db` |
| `DATABASE_VOLUME_ROOT` | You | **Set this.** Example: `/data` |
| `RAILWAY_ENVIRONMENT` | Railway | Treats the process as production |

`DATABASE_PATH` always wins when set.

If `DATABASE_PATH` is empty and `RAILWAY_VOLUME_MOUNT_PATH=/data`, the resolved file is `/data/licorice.db`. If that file is missing, startup still fails.

If both are missing, startup fails. There is no local fallback.

## Volume configuration

The operator must:

1. Attach a Volume to the Liquorice Locker service.
2. Read the **actual** mount path from Railway (do not assume `/data` unless Railway shows it).
3. Place the **live** `licorice.db` on that Volume. The app will not copy it.
4. Set `DATABASE_PATH=<mount>/licorice.db`.
5. Set `DATABASE_VOLUME_ROOT=<mount>`.
6. Deploy and read logs.

If the live database is still on ephemeral container disk, copy it to the Volume **before** relying on a deploy. Startup will not migrate files.

## Migration status

Current chain: `001_baseline` … `013_canonical_shipping_rates`.

`012_shipping_pricing` is schema + methods/zones. `013_canonical_shipping_rates` inserts rates **only if the table is empty**. Existing production shipping prices are left alone. Checkout still refuses a silent `$0` when no active rate matches.

## Test results

```
Ran 113 tests in 6.994s
OK
```

Includes database hardening, persistence, production-readiness, shipping, Stripe idempotency fixtures, and the new Railway path matrix. Tests use isolated temporary SQLite files only. They never open the live Railway database and never charge Stripe.

## Manual actions (Railway only)

These cannot be done from this repository:

1. Attach the Volume.
2. Confirm the mount path.
3. Put the live `licorice.db` on the Volume (SQLite backup API or a stopped-writer copy of the `.db` plus WAL if needed).
4. Set `DATABASE_PATH` and `DATABASE_VOLUME_ROOT`.
5. Deploy.
6. Confirm logs: `Persistent storage: verified` and `Migration: 013_canonical_shipping_rates`.
7. Staff: `/dashboard/admin/database-health`.
8. Storefront + Stripe **test-mode** checkout. Confirm shipping quote.
9. Restart once; counts must match.

Optional diagnostic (does not create a database):

```bash
python3 -m database_config
```

## Rollback

Roll back the **code** deploy (previous image/commit) in Railway.

Do **not** delete the Volume. Do **not** replace `licorice.db` unless you are restoring a known-good backup.

Code rollback does not delete the database. Database rollback is a separate operator restore onto `DATABASE_PATH` after stopping writers (see `DATABASE-OPERATIONS.md`).
