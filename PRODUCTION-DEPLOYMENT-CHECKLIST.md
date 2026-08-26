# Production deployment checklist

This repository does **not** record Railway’s volume mount path. Do not guess it.

Use this list on every production deploy. Check boxes only after you have actually looked.

## Persistence

- [ ] Railway **Volume** is attached to the service
- [ ] Volume **mount path** is noted from the Railway UI (not from this repo)
- [ ] `DATABASE_PATH` is set to a file **on that mount** (example shape only: `<mount>/licorice.db`)
- [ ] Optional: `DATABASE_VOLUME_ROOT` is the same mount; if set, `DATABASE_PATH` must live under it or the app **will not start**
- [ ] Confirm production will **not** create a new database if the file is missing (app fails loud)

## Before deploy

- [ ] Timestamped backup of the live SQLite file (SQLite backup API, not a copy of an active WAL file if you can avoid it)
- [ ] Record current counts: products, users, orders, affiliates, commissions
- [ ] Record current `schema_migrations` if you can query the live file

## After deploy

- [ ] App started (if it refused to start, restore/fix `DATABASE_PATH` — do not create a blank DB)
- [ ] Staff login → `GET /dashboard/admin/database-health`
- [ ] Path is on the volume
- [ ] `environment` is production
- [ ] WAL + foreign keys on
- [ ] Migration version includes `011_inventory_refs` (or at least `010_hardening` if this deploy predates 011)
- [ ] Product / user / order / affiliate / commission counts match the pre-deploy record
- [ ] `inventory.enforcement` is **DISABLED** unless you have loaded stock and intentionally set `INVENTORY_ENFORCE=1`
- [ ] Integrity: FK violations 0, no unexpected orphans
- [ ] Stripe keys: secret, publishable, and webhook secret still match the Checkout mode (test vs live)
- [ ] Webhook endpoint still points at `/webhook`
- [ ] Place a **test-mode** checkout if the environment is test; do not use a live card to “see if it works” unless that is an agreed live smoke test
- [ ] Application logs: no `CRITICAL DATABASE SAFETY ERROR`, no unexpected migration failures

## Inventory switch (separate decision)

- [ ] Do **not** set `INVENTORY_ENFORCE=1` until on-hand quantities and ledger movements exist for sellable SKUs
- [ ] Enabling enforcement with zero stock can block sales
- [ ] The application will **not** invent stock for you

## If something looks wrong

- [ ] Do not DROP tables
- [ ] Do not restore over a running writer without stopping the app
- [ ] Restore the pre-deploy backup onto `DATABASE_PATH` after stopping the service
- [ ] See `DATABASE-OPERATIONS.md`
