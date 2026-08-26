# Production deployment checklist

This repository does **not** record Railway’s volume mount path. Do not guess it.
The application will **not** copy a database from ephemeral disk onto a Volume.

Use this list on every production deploy. Check boxes only after you have actually looked.

## Railway (required)

1. Attach a Volume to the Liquorice Locker service.
2. Confirm the **actual** mount path in Railway (often `/data`, but only Railway knows).
3. Ensure the live production database **already exists** on that Volume as `licorice.db`.
   The app will not create it, seed it, or copy `data/licorice.db` for you.
4. Set `DATABASE_PATH` to that file (example: `/data/licorice.db`).
5. Set `DATABASE_VOLUME_ROOT` to the same mount (example: `/data`).
6. Deploy.
7. Check logs for `Liquorice Locker database` / `Persistent storage: verified`.
   If you see `CRITICAL DATABASE SAFETY ERROR`, fix the volume/path. Do not create a blank DB.
8. Open `/dashboard/admin/database-health` (staff login).
9. Verify product / user / order / affiliate / commission counts.
10. Test the storefront (home, shop, Sound Wave, Riff, Listening Room, cart).
11. Test checkout **without charging a real card** (Stripe test mode).
12. Confirm an active shipping rate quotes correctly (never silent `$0`).
13. Confirm the database survives a Railway restart (same path, same counts).

## Persistence

- [ ] Railway **Volume** is attached to the service
- [ ] Volume **mount path** is noted from the Railway UI (not from this repo)
- [ ] Production `licorice.db` is already on that Volume (operator placed it)
- [ ] `DATABASE_PATH` is set to that file (example shape only: `<mount>/licorice.db`)
- [ ] `DATABASE_VOLUME_ROOT` is the same mount; if set, `DATABASE_PATH` must live under it or the app **will not start**
- [ ] `RAILWAY_VOLUME_MOUNT_PATH` is injected by Railway when a volume is attached (do not invent it)
- [ ] Confirm production will **not** create a new database if the file is missing (app fails loud)

## Environment variables

| Variable | Who sets it | Production |
|----------|-------------|------------|
| `RAILWAY_VOLUME_MOUNT_PATH` | Railway, automatically when a Volume is attached | Used if `DATABASE_PATH` is empty: `<mount>/licorice.db` |
| `DATABASE_PATH` | You (recommended) | Required unless the volume fallback above is enough. Always set it. |
| `DATABASE_VOLUME_ROOT` | You | Optional but recommended. If set, the database must be inside this directory. |
| `RAILWAY_ENVIRONMENT` | Railway | Makes the app treat the process as production |
| `LICORICE_ENV` | You (optional) | `production` / `development` / `test` |

If `DATABASE_PATH` is unset **and** `RAILWAY_VOLUME_MOUNT_PATH` is unset, startup **fails**. There is no `data/licorice.db` fallback in production.

## Before deploy

- [ ] Timestamped backup of the live SQLite file (SQLite backup API, not a copy of an active WAL file if you can avoid it)
- [ ] Record current counts: products, users, orders, affiliates, commissions
- [ ] Record current `schema_migrations` if you can query the live file
- [ ] Record current shipping rate count / a known NZ quote

## After deploy

- [ ] App started (if it refused to start, restore/fix `DATABASE_PATH` — do not create a blank DB)
- [ ] Staff login → `GET /dashboard/admin/database-health`
- [ ] Path is on the volume (`inside_volume: true`)
- [ ] `environment` is production
- [ ] WAL + foreign keys on
- [ ] Migration version includes `013_canonical_shipping_rates` (or at least `012_shipping_pricing`)
- [ ] Product / user / order / affiliate / commission counts match the pre-deploy record
- [ ] Shipping rates were **not** rewritten (admin-owned). Checkout still quotes or fails safe if no rate exists.
- [ ] `inventory.enforcement` is **DISABLED** unless you have loaded stock and intentionally set `INVENTORY_ENFORCE=1`
- [ ] Integrity: FK violations 0, no unexpected orphans
- [ ] Stripe keys: secret, publishable, and webhook secret still match the Checkout mode (test vs live)
- [ ] Webhook endpoint still points at `/webhook`
- [ ] Place a **test-mode** checkout if the environment is test; do not use a live card to “see if it works” unless that is an agreed live smoke test
- [ ] Application logs: no `CRITICAL DATABASE SAFETY ERROR`, no unexpected migration failures
- [ ] Restart the service once and confirm health counts are unchanged

## Diagnostics

From a shell that can see the service filesystem (does not create a database):

```bash
python3 -m database_config
```

If the app is already running (staff):

```bash
flask --app app railway-db-status
```

## Cutover from ephemeral disk

If the live database is still on container disk (lost on deploy), **do not** expect startup to copy it.

1. Stop deploys that would wipe the container if the file is still there.
2. Copy the live `.db` (and `-wal` / `-shm` if present, or use the SQLite backup API) onto the Volume yourself.
3. Set `DATABASE_PATH` / `DATABASE_VOLUME_ROOT`.
4. Deploy.

## Inventory switch (separate decision)

- [ ] Do **not** set `INVENTORY_ENFORCE=1` until on-hand quantities and ledger movements exist for sellable SKUs
- [ ] Enabling enforcement with zero stock can block sales
- [ ] The application will **not** invent stock for you

## If something looks wrong

- [ ] Do not DROP tables
- [ ] Do not restore over a running writer without stopping the app
- [ ] Restore the pre-deploy backup onto `DATABASE_PATH` after stopping the service
- [ ] Roll back the **code** deploy if needed; that does not delete the Volume database
- [ ] See `DATABASE-OPERATIONS.md`
