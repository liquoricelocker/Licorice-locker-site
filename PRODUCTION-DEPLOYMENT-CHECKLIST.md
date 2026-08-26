# Production deployment checklist

Intended Railway configuration (confirm in the Railway UI):

- Service: **web**
- Volume: **web-volume**
- Mount: **`/data`**
- `DATABASE_PATH=/data/licorice.db`
- `DATABASE_VOLUME_ROOT=/data`

`RAILWAY_VOLUME_MOUNT_PATH` is injected by Railway when the Volume is attached. Do not invent it.

## First production deployment (empty Volume)

There is no production data to preserve. The app **may create** `/data/licorice.db` **only** when it has verified:

- environment is production
- Railway Volume mount exists and is writable
- resolved path is inside that Volume (`Path.relative_to`)
- the file does not already exist

It will **not** create SQLite on `/app`, `/app/data`, `data/licorice.db`, or any path outside the Volume.

1. Attach **web-volume** to the web service. Mount path **`/data`**.
2. Set `DATABASE_PATH=/data/licorice.db`.
3. Set `DATABASE_VOLUME_ROOT=/data`.
4. Deploy.
5. Logs should show `Database state: EMPTY VOLUME BOOTSTRAP` and `Persistent storage: VERIFIED`.
6. Staff: `/dashboard/admin/database-health` — path `/data/licorice.db`, catalogue present, shipping rates present, no fake customers/orders.
7. Storefront + Stripe **test-mode** checkout. Confirm NZ/AU quotes.
8. Restart once. Logs should show `Database state: EXISTING DATABASE`. Counts unchanged.

## Every deployment after that

The file on the Volume is production data.

1. Do **not** delete `/data/licorice.db`.
2. Deploy. The app opens the existing file, runs unused migrations only, and **does not** reseed catalogue or shipping rates.
3. Compare health counts with the previous deploy.
4. If the Volume is unmounted or the path is outside `/data`, the app **will not start** and will **not** create a replacement database.

## Persistence

- [ ] Volume **web-volume** attached, mount **`/data`**
- [ ] `DATABASE_PATH=/data/licorice.db`
- [ ] `DATABASE_VOLUME_ROOT=/data`
- [ ] First boot: empty Volume created the database (or you restored a known file)
- [ ] Later boots: existing database is never recreated

## Environment variables

| Variable | Who sets it | Production |
|----------|-------------|------------|
| `RAILWAY_VOLUME_MOUNT_PATH` | Railway | Proof the Volume is attached. Required to **create** a first database. |
| `DATABASE_PATH` | You | `/data/licorice.db` |
| `DATABASE_VOLUME_ROOT` | You | `/data` |
| `RAILWAY_ENVIRONMENT` | Railway | Treated as production |

If `DATABASE_PATH` and `RAILWAY_VOLUME_MOUNT_PATH` are both missing, startup **fails**. There is no `data/licorice.db` fallback.

## After every deploy

- [ ] Logs: `Persistent storage: VERIFIED` (not UNKNOWN)
- [ ] Health: WAL, foreign keys, busy timeout, integrity PASS
- [ ] Migration includes `013_canonical_shipping_rates`
- [ ] Catalogue prices unchanged vs previous deploy (after first boot)
- [ ] Shipping rates unchanged vs previous deploy (after first boot)
- [ ] `inventory.enforcement` DISABLED unless you set `INVENTORY_ENFORCE=1`
- [ ] Stripe test-mode checkout; no live card unless agreed
- [ ] Restart once; same path and counts

## Diagnostics (read-only, does not create a database)

```bash
python3 -m database_config
```

## If the Volume is missing

Do not create `data/licorice.db` in the container. Re-attach **web-volume** at `/data` and redeploy.

## Rollback

Roll back the **code** image. That does not delete the Volume database. Restore a SQLite backup onto `/data/licorice.db` only after stopping writers. See `DATABASE-OPERATIONS.md`.
