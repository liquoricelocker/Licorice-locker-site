# Shipping pricing — implementation report

Liquorice Locker now charges shipping from **database rates**, not from `CHECKOUT_SHIPPING_CENTS_DEFAULT` or template constants. Staff can change a rate and the next checkout uses it. Historical orders keep the cents they paid.

## What was implemented

* Additive migration `012_shipping_pricing`
* `shipping.py` — `calculate_shipping()` as the single server-side engine
* Checkout destination + method selection, with server-side requote
* Stripe Checkout amounts = product subtotal + server quote
* Order snapshots for method, rate id, currency, zone, weight
* Admin UI at `/dashboard/admin/shipping`
* Integrity checks, audit/business events, tests, and operator docs

## Database changes

New tables: `shipping_methods`, `shipping_zones`, `shipping_zone_countries`, `shipping_rates`, `shipping_settings`.

Additive columns on `orders` (existing `shipping_cents` unchanged):

* `shipping_method_code`
* `shipping_method_name`
* `shipping_rate_id`
* `shipping_currency`
* `shipping_weight_grams`
* `shipping_zone_code`

Existing order IDs, shipment IDs, and historical `shipping_cents` values are preserved. No tables were rebuilt.

## Shipping zones

Seeded: New Zealand (`NZ`), Australia (`AUSTRALIA`), Rest of world (`REST_OF_WORLD`, catch-all). Country membership is ISO codes (`NZ`, `AU`). More zones can be added later.

## Shipping methods

Seeded codes: `NZ_STANDARD`, `NZ_EXPRESS`, `INTERNATIONAL_STANDARD`, `INTERNATIONAL_EXPRESS`, `LOCAL_PICKUP`. `rate_source` defaults to `database` for a later carrier adapter. Local pickup is a $0 method, not a hard-coded checkout path.

## Rate rules

Rates combine method + zone + optional order-value range + optional weight range + priority. `price_cents = 0` is free shipping. Canonical store rates (NZ $12 / $18 / FREE, Australia $35 / $50, rest of world contact-us) are applied by migration `013_canonical_shipping_rates`.

## Checkout integration

`/checkout` lists server-quoted methods for the selected country. Place order recalculates. Client fields `shipping_cents` / `shipping_price` are ignored. No matching rate → error, never silent $0. Free shipping displays as **FREE**, not `$0.00`. Estimates (min/max business days) are labelled as estimates.

Cart quantity, product, variant, destination, and method changes requote because the price is not cached beyond the current request.

## Stripe integration

Stripe session line items use the server quote. Address collection is restricted to the quoted country. Metadata stores the snapshot (cents, method, rate id, destination). On return/webhook the order uses that snapshot and the paid Stripe total; it does not re-run today’s rate table. Tests assert $299.00 + $15.00 = $314.00 in cents (`29900 + 1500 = 31400`) without a live charge.

## Historical pricing protection

A test creates an order at $15 shipping, changes the live rate to $20, and asserts the order remains $15.

## Admin interface

Allowlisted admins can view zones/methods/rates and create, edit, activate, or deactivate rates. Packaging grams are optional. Sellable variants missing `weight_grams` are listed and never given an invented weight.

## Security

Admin routes require login, effective-admin allowlist, and CSRF. Customers cannot set the shipping amount. Overlapping active rates at the same priority are rejected. Invalid prices, ranges, currencies, and country codes are rejected server-side. `price_cents = 0` is allowed.

## Tax

Existing behaviour is unchanged: no Stripe Tax / GST line. Shipping is added as NZD cents. It is not separately taxed and is not double-taxed.

## Tests

`python3 -m unittest tests.test_database_persistence tests.test_database_hardening tests.test_production_readiness tests.test_shipping_pricing`

* Existing 42 production-readiness / persistence / hardening tests: **PASS**
* New shipping pricing tests (calculator, edges, NZ/AU/ROW/pickup, CSRF, historical snapshot, Stripe cents, integrity): **PASS**
* Full combined run: **87 tests, OK**

## Remaining limitations

* Rest of world is not automated — checkout asks the customer to contact us.
* No live NZ Post / DHL / FedEx / UPS APIs. `rate_source` is reserved.
* No FX; charges stay NZD.
* Stripe can only collect addresses for its supported country list.
* Current rates are order-value based. Weight tiers are unused.
* Region-inside-country rules are not modelled yet (zone + country only).
