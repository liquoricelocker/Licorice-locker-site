# Liquorice Locker — shipping pricing

Shipping prices live in the database. Checkout never hard-codes a dollar amount. Change a rate in **Admin → Shipping pricing** (or directly in SQLite) and the next checkout uses the new price. Existing orders keep the shipping amount they originally paid.

## How rates work

`calculate_shipping()` is the only calculator. It answers:

> Given this cart and this destination, what should Liquorice Locker charge for shipping?

Inputs:

* cart (product ids and quantities)
* destination country (ISO 3166-1 alpha-2, for example `NZ`)
* shipping method code (for example `NZ_STANDARD`)

It does **not** accept a client-submitted price. A browser may send `shipping_method=NZ_STANDARD`; it must not send a trusted `shipping_price`.

Matching, in order:

1. The method exists and is active.
2. The destination is in an active zone (specific country membership first; otherwise an active catch-all zone such as Rest of World).
3. The rate is active.
4. Order value (integer NZD cents) is inside `min_order_value_cents`–`max_order_value_cents` (blank = no limit). Ranges are inclusive.
5. Billable weight in grams is inside `min_weight_grams`–`max_weight_grams` (blank = no limit). If the rate has weight bounds and a product has no weight, the rate does not match.
6. Among matching rates, **higher `priority` wins**.
7. If two matching rates share the same priority, calculation **fails safely** — checkout will not pick one at random and will not charge $0.

If nothing matches, checkout shows an error. It does not silently charge $0.

## Zones

| Code | Meaning |
|------|---------|
| `NZ` | New Zealand (`country_code` `NZ`) |
| `AUSTRALIA` | Australia (`AU`) |
| `REST_OF_WORLD` | Catch-all for other destinations |

You can add more zones and attach ISO country codes in `shipping_zone_countries` without changing application code.

## Methods

Seeded reference methods (edit/deactivate in the database as needed):

* `NZ_STANDARD`
* `NZ_EXPRESS`
* `INTERNATIONAL_STANDARD`
* `INTERNATIONAL_EXPRESS`
* `LOCAL_PICKUP`

Local pickup is a normal method with `price_cents = 0`, not a special checkout branch.

`shipping_methods.rate_source` defaults to `database`. Future NZ Post / DHL / FedEx / UPS integrations can use another source without replacing this engine. Live carrier APIs are **not** wired yet.

## Thresholds and free shipping

Free shipping is a rate with `price_cents = 0`, optionally limited by `min_order_value_cents`.

Example (development seed):

* NZ Standard, orders ≤ $199.99 → $15 (`price_cents = 1500`, `max_order_value_cents = 19999`)
* NZ Standard, orders ≥ $200 → FREE (`price_cents = 0`, `min_order_value_cents = 20000`)

There is no separate hard-coded free-shipping flag.

## Weight

Weights are **grams** (integers). Checkout uses:

```
billable_weight = sum(quantity × variant.weight_grams) + packaging_weight_grams
```

`product_variants.weight_grams` is authoritative. Dimensions are not converted into weight.

If a variant has no weight, the app does **not** invent one. Unbounded rates can still match. Weight-tiered rates will not. Admin → Shipping pricing lists sellable variants that are missing a weight.

Optional packaging grams live in `shipping_settings` (`packaging_weight_grams`) and/or on the method. Default is 0.

## Checkout calculation

1. Customer chooses destination country on `/checkout`.
2. The server lists methods that have a matching rate.
3. Customer selects a method.
4. On Place order, the server calls `calculate_shipping()` again.
5. Stripe Checkout is created for `product subtotal + shipping_total_cents` (NZD integer cents).
6. Stripe address collection is locked to the quoted country so the customer cannot switch to a cheaper zone after quoting.
7. Order rows store the quoted cents plus method/rate snapshots.

If no method is available, checkout shows a clear error and payment is not started.

## Currency

The store currency is **NZD**. Rate rows must use `NZD`. There is no currency conversion. International customers are still charged in NZD (the existing Stripe Checkout behaviour). Display currency on the site remains an estimate only.

## Tax

Liquorice Locker does not currently apply a separate Stripe Tax or GST line. Product prices and shipping are charged as integer NZD cents. Shipping is a Checkout line item with no extra tax multiplier, so shipping is not double-taxed. Do not invent GST behaviour in this engine.

## Historical order pricing

Orders store:

* `shipping_cents` — the amount actually charged (source of truth for history)
* `shipping_method_code`, `shipping_method_name`
* `shipping_rate_id` (may later be inactive or missing)
* `shipping_currency`, `shipping_zone_code`, `shipping_weight_grams`

Changing today’s NZ Standard from $15 to $20 does **not** rewrite old orders. Webhooks and success URLs must not recalculate an old order from the current rate table. They reuse the shipping cents written into the Stripe session metadata when that checkout was created.

## Admin

`/dashboard/admin/shipping` (allowlisted staff only, CSRF on every POST):

* View zones, methods, and rates
* Create / edit / activate / deactivate rates
* Set packaging grams

Prices in the form are NZD dollars; storage is integer cents. Overlapping active rules at the same priority are rejected.

Audit actions: `SHIPPING_RATE_CREATED`, `SHIPPING_RATE_UPDATED`, `SHIPPING_RATE_ACTIVATED`, `SHIPPING_RATE_DEACTIVATED`. Business event: `SHIPPING_RATE_CHANGED`.

## Current store rates

These live in `shipping_rates` (migration `013_canonical_shipping_rates`). Change them in Admin without changing code.

**New Zealand (Standard)**

| Order | Shipping |
|-------|----------|
| $0–$149.99 | $12 |
| $150–$299.99 | $18 |
| $300+ | FREE |

**Australia (Standard)**

| Order | Shipping |
|-------|----------|
| $0–$199.99 | $35 |
| $200+ | $50 |

No free Australia shipping.

**Rest of world**

Not offered at automated checkout. Customers see **International shipping — Contact us** and can email for a freight quote.

Rates are order-value based. Weight tiers are not used for this catalogue.

## Future carrier integrations

Keep using `calculate_shipping()` as the single entry point. A later carrier adapter can return the same quote shape (`price_cents`, `currency`, `rule_id`, estimates). Until then, database rates are authoritative.
