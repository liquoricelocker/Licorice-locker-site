# Database integrity report

Read-only scan of the local application database at the time of the hardening pass.
No rows were corrected automatically. Migration `010_hardening` is additive (indexes + `stripe_processed_events`).

**Database path:** `/Users/joshuafrench/Documents/Licorice Locker Cursor/data/licorice.db`

## SQLite configuration (application connection)

- version: 3.51.0
- journal_mode: wal
- foreign_keys: 1 (ON for app connections)
- busy_timeout_ms: 5000
- synchronous: 1 (1 = NORMAL, 2 = FULL)

## Migration state

- `001_baseline`
- `002_indexes`
- `003_product_variants`
- `004_customers`
- `005_payments`
- `006_shipments`
- `007_inventory`
- `008_manufacturing`
- `009_audit_events`
- `010_hardening`

## Row counts

| Table | Rows |
|-------|------|
| `affiliate_account_deletions` | 0 |
| `affiliate_invite_tokens` | 0 |
| `affiliate_pages` | 9 |
| `affiliate_visits` | 15 |
| `analytics_events` | 167 |
| `analytics_sessions` | 1 |
| `audit_logs` | 0 |
| `bill_of_material_items` | 0 |
| `bills_of_materials` | 0 |
| `business_events` | 0 |
| `commissions` | 1 |
| `components` | 0 |
| `creative_assets` | 0 |
| `customer_addresses` | 1 |
| `customers` | 1 |
| `inventory_items` | 5 |
| `inventory_locations` | 1 |
| `inventory_movements` | 0 |
| `order_items` | 1 |
| `orders` | 1 |
| `payments` | 1 |
| `product_images` | 50 |
| `product_tags` | 0 |
| `product_variants` | 5 |
| `product_versions` | 0 |
| `production_materials` | 0 |
| `production_orders` | 0 |
| `products` | 5 |
| `purchase_order_items` | 0 |
| `purchase_orders` | 0 |
| `quality_control_records` | 0 |
| `schema_migrations` | 10 |
| `shipments` | 0 |
| `stripe_processed_events` | 0 |
| `supplier_components` | 0 |
| `suppliers` | 0 |
| `users` | 9 |

## Foreign key violations

None reported by `PRAGMA foreign_key_check`.

## Orphan / missing-relationship counts

| Check | Count |
|-------|-------|
| `order_items_without_order` | 0 |
| `order_items_missing_product` | 0 |
| `payments_without_order` | 0 |
| `shipments_without_order` | 0 |
| `variants_without_product` | 0 |
| `inventory_without_variant_or_component` | 0 |
| `movements_without_item` | 0 |
| `bom_items_without_bom` | 0 |
| `bom_items_without_component` | 0 |
| `production_materials_without_order` | 0 |
| `commissions_without_affiliate` | 0 |
| `orders_without_items` | 0 |

## Duplicates

| Check | Count |
|-------|-------|
| `duplicate_product_slugs` | 0 |
| `duplicate_order_numbers` | 0 |
| `duplicate_customer_emails` | 0 |
| `duplicate_stripe_sessions` | 0 |

## Invalid financial values

| Check | Count |
|-------|-------|
| `products_negative_price` | 0 |
| `order_items_nonpositive_qty` | 0 |
| `order_items_negative_unit` | 0 |
| `orders_negative_total` | 0 |
| `payments_negative_amount` | 0 |
| `commissions_negative` | 0 |
| `variants_negative_price` | 0 |

## Invalid statuses

| Check | Count |
|-------|-------|
| `orders_unknown_status` | 0 |
| `payments_unknown_status` | 0 |
| `shipped_cancelled_orders` | 0 |

## Product catalogue (IDs must not change)

| id | slug | name | sku | price_cents |
|----|------|------|-----|-------------|
| 1 | `sound-wave` | Sound Wave | `LL-SOUND-WAVE` | 42900 |
| 2 | `allegro` | Allegra | `LL-ALLEGRO` | 9999 |
| 3 | `melody` | Melody | `LL-MELODY` | 9999 |
| 4 | `harmony` | Harmony | `LL-HARMONY` | 9999 |
| 5 | `riff` | Riff | `LL-RIFF` | 9999 |

Verified unchanged across applying `010_hardening` and a second bootstrap (restart simulation).

## Existing orders (historical price)

- order `LL-20260329-70FAAF` id=1 total_cents=12900 status=completed
  - item product_id=1 qty=1 unit_price_cents=12900

## Table inventory (schema)

### `affiliate_account_deletions` (0 rows)

Columns:
- `id` INTEGER PK
- `deleted_at` TEXT NOT NULL default=datetime('now')
- `former_user_id` INTEGER NOT NULL
- `email_hash` TEXT NOT NULL default=''

### `affiliate_invite_tokens` (0 rows)

Columns:
- `id` INTEGER PK
- `token` TEXT NOT NULL
- `email` TEXT
- `expires_at` TEXT NOT NULL
- `used_at` TEXT
- `created_at` TEXT NOT NULL default=datetime('now')

Indexes:
- `idx_affiliate_invite_tokens_token`
- `sqlite_autoindex_affiliate_invite_tokens_1` UNIQUE

Nullable: `email`, `used_at`

### `affiliate_pages` (9 rows)

Columns:
- `user_id` INTEGER PK
- `headline` TEXT NOT NULL default='Welcome'
- `tagline` TEXT NOT NULL default=''
- `description` TEXT NOT NULL default=''
- `instagram_url` TEXT NOT NULL default=''
- `tiktok_url` TEXT NOT NULL default=''
- `banner_image_url` TEXT NOT NULL default=''
- `monthly_sales_target` INTEGER NOT NULL default=16
- `page_updated_at` TEXT

Foreign keys:
- `user_id` → `users(id)` ON DELETE CASCADE

Nullable: `page_updated_at`

### `affiliate_visits` (15 rows)

Columns:
- `id` INTEGER PK
- `affiliate_user_id` INTEGER NOT NULL
- `visitor_id` TEXT NOT NULL
- `created_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `affiliate_user_id` → `users(id)` ON DELETE CASCADE

Indexes:
- `idx_visits_affiliate`

### `analytics_events` (167 rows)

Columns:
- `id` INTEGER PK
- `session_id` TEXT NOT NULL
- `event` TEXT NOT NULL
- `page` TEXT NOT NULL
- `meta_json` TEXT NOT NULL default='{}'
- `created_at` TEXT NOT NULL default=datetime('now')

Indexes:
- `idx_analytics_events_session`

### `analytics_sessions` (1 rows)

Columns:
- `id` INTEGER PK
- `session_id` TEXT NOT NULL
- `country` TEXT NOT NULL default=''
- `device_class` TEXT NOT NULL default=''
- `user_agent` TEXT NOT NULL default=''
- `affiliate_code` TEXT
- `started_at` TEXT NOT NULL
- `last_active_at` TEXT NOT NULL
- `converted` INTEGER NOT NULL default=0
- `dropped_at` TEXT
- `city` TEXT NOT NULL default=''
- `ip_hash` TEXT NOT NULL default=''

Indexes:
- `idx_analytics_sessions_started`
- `idx_analytics_sessions_conv`
- `idx_analytics_sessions_last`
- `sqlite_autoindex_analytics_sessions_1` UNIQUE

Nullable: `affiliate_code`, `dropped_at`

### `audit_logs` (0 rows)

Columns:
- `id` INTEGER PK
- `user_id` INTEGER
- `action` TEXT NOT NULL
- `entity_type` TEXT NOT NULL
- `entity_id` INTEGER
- `before_json` TEXT
- `after_json` TEXT
- `metadata_json` TEXT
- `created_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `user_id` → `users(id)` ON DELETE NO ACTION

Indexes:
- `idx_audit_logs_action`
- `idx_audit_logs_created`
- `idx_audit_logs_entity`

Nullable: `user_id`, `entity_id`, `before_json`, `after_json`, `metadata_json`

### `bill_of_material_items` (0 rows)

Columns:
- `id` INTEGER PK
- `bom_id` INTEGER NOT NULL
- `component_id` INTEGER NOT NULL
- `quantity` REAL NOT NULL
- `unit` TEXT NOT NULL default='each'
- `notes` TEXT NOT NULL default=''

Foreign keys:
- `component_id` → `components(id)` ON DELETE RESTRICT
- `bom_id` → `bills_of_materials(id)` ON DELETE CASCADE

Indexes:
- `idx_bom_items_bom`

### `bills_of_materials` (0 rows)

Columns:
- `id` INTEGER PK
- `product_version_id` INTEGER NOT NULL
- `name` TEXT NOT NULL
- `version` TEXT NOT NULL default='1'
- `status` TEXT NOT NULL default='draft'
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `product_version_id` → `product_versions(id)` ON DELETE RESTRICT

### `business_events` (0 rows)

Columns:
- `id` INTEGER PK
- `type` TEXT NOT NULL
- `entity_type` TEXT NOT NULL
- `entity_id` INTEGER
- `payload_json` TEXT NOT NULL default='{}'
- `created_at` TEXT NOT NULL default=datetime('now')
- `processed_at` TEXT

Indexes:
- `idx_business_events_created`
- `idx_business_events_type`
- `idx_business_events_entity`

Nullable: `entity_id`, `processed_at`

### `commissions` (1 rows)

Columns:
- `id` INTEGER PK
- `affiliate_user_id` INTEGER NOT NULL
- `year_month` TEXT NOT NULL
- `sales_count` INTEGER NOT NULL
- `commission_rate` REAL NOT NULL
- `total_sales_cents` INTEGER NOT NULL
- `commission_cents` INTEGER NOT NULL
- `payout_status` TEXT NOT NULL default='pending'
- `bonus_cents` INTEGER NOT NULL default=0

Foreign keys:
- `affiliate_user_id` → `users(id)` ON DELETE CASCADE

Indexes:
- `idx_commissions_payout`
- `sqlite_autoindex_commissions_1` UNIQUE

### `components` (0 rows)

Columns:
- `id` INTEGER PK
- `name` TEXT NOT NULL
- `sku` TEXT
- `description` TEXT NOT NULL default=''
- `unit` TEXT NOT NULL default='each'
- `cost_cents` INTEGER NOT NULL default=0
- `currency` TEXT NOT NULL default='NZD'
- `reorder_point` INTEGER NOT NULL default=0
- `reorder_quantity` INTEGER NOT NULL default=0
- `status` TEXT NOT NULL default='active'
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Indexes:
- `idx_components_sku` UNIQUE

Nullable: `sku`

### `creative_assets` (0 rows)

Columns:
- `id` INTEGER PK
- `title` TEXT NOT NULL default=''
- `file_path` TEXT NOT NULL
- `thumbnail_path` TEXT
- `asset_type` TEXT NOT NULL
- `tags` TEXT NOT NULL default=''
- `created_at` TEXT NOT NULL default=datetime('now')

Indexes:
- `idx_creative_assets_created`

Nullable: `thumbnail_path`

### `customer_addresses` (1 rows)

Columns:
- `id` INTEGER PK
- `customer_id` INTEGER NOT NULL
- `type` TEXT NOT NULL default='shipping'
- `first_name` TEXT NOT NULL default=''
- `last_name` TEXT NOT NULL default=''
- `company` TEXT NOT NULL default=''
- `address_line1` TEXT NOT NULL default=''
- `address_line2` TEXT NOT NULL default=''
- `city` TEXT NOT NULL default=''
- `region` TEXT NOT NULL default=''
- `postcode` TEXT NOT NULL default=''
- `country` TEXT NOT NULL default=''
- `is_default` INTEGER NOT NULL default=0
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `customer_id` → `customers(id)` ON DELETE CASCADE

Indexes:
- `idx_customer_addresses_customer`

### `customers` (1 rows)

Columns:
- `id` INTEGER PK
- `email` TEXT NOT NULL
- `email_normalized` TEXT NOT NULL
- `first_name` TEXT NOT NULL default=''
- `last_name` TEXT NOT NULL default=''
- `phone` TEXT NOT NULL default=''
- `company_name` TEXT NOT NULL default=''
- `customer_type` TEXT NOT NULL default='retail'
- `marketing_consent` INTEGER NOT NULL default=0
- `notes` TEXT NOT NULL default=''
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')
- `deleted_at` TEXT

Indexes:
- `idx_customers_email`
- `idx_customers_email_normalized` UNIQUE

Nullable: `deleted_at`

### `inventory_items` (5 rows)

Columns:
- `id` INTEGER PK
- `variant_id` INTEGER
- `component_id` INTEGER
- `location_id` INTEGER NOT NULL
- `quantity_on_hand` INTEGER NOT NULL default=0
- `quantity_reserved` INTEGER NOT NULL default=0
- `reorder_point` INTEGER NOT NULL default=0
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `location_id` → `inventory_locations(id)` ON DELETE RESTRICT
- `variant_id` → `product_variants(id)` ON DELETE RESTRICT

Indexes:
- `idx_inventory_items_variant_loc` UNIQUE
- `idx_inventory_items_variant`
- `idx_inventory_items_location`

Nullable: `variant_id`, `component_id`

### `inventory_locations` (1 rows)

Columns:
- `id` INTEGER PK
- `name` TEXT NOT NULL
- `type` TEXT NOT NULL default='warehouse'
- `address` TEXT NOT NULL default=''
- `status` TEXT NOT NULL default='active'
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Indexes:
- `idx_inventory_locations_name` UNIQUE

### `inventory_movements` (0 rows)

Columns:
- `id` INTEGER PK
- `inventory_item_id` INTEGER NOT NULL
- `movement_type` TEXT NOT NULL
- `quantity` INTEGER NOT NULL
- `reference_type` TEXT NOT NULL default=''
- `reference_id` INTEGER
- `reason` TEXT NOT NULL default=''
- `created_by_id` INTEGER
- `created_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `created_by_id` → `users(id)` ON DELETE NO ACTION
- `inventory_item_id` → `inventory_items(id)` ON DELETE RESTRICT

Indexes:
- `idx_inventory_sale_once` UNIQUE
- `idx_inventory_movements_ref`
- `idx_inventory_movements_created`
- `idx_inventory_movements_item`

Nullable: `reference_id`, `created_by_id`

### `order_items` (1 rows)

Columns:
- `id` INTEGER PK
- `order_id` INTEGER NOT NULL
- `product_id` INTEGER NOT NULL
- `quantity` INTEGER NOT NULL
- `unit_price_cents` INTEGER NOT NULL
- `variant_id` INTEGER
- `product_name_snapshot` TEXT
- `sku_snapshot` TEXT
- `discount_cents` INTEGER NOT NULL default=0
- `tax_cents` INTEGER NOT NULL default=0
- `line_subtotal_cents` INTEGER
- `line_total_cents` INTEGER

Foreign keys:
- `variant_id` → `product_variants(id)` ON DELETE NO ACTION
- `product_id` → `products(id)` ON DELETE NO ACTION
- `order_id` → `orders(id)` ON DELETE CASCADE

Indexes:
- `idx_order_items_order`
- `idx_order_items_product`

Nullable: `variant_id`, `product_name_snapshot`, `sku_snapshot`, `line_subtotal_cents`, `line_total_cents`

### `orders` (1 rows)

Columns:
- `id` INTEGER PK
- `order_number` TEXT NOT NULL
- `created_at` TEXT NOT NULL default=datetime('now')
- `status` TEXT NOT NULL default='completed'
- `order_type` TEXT NOT NULL
- `affiliate_user_id` INTEGER
- `customer_first` TEXT NOT NULL
- `customer_last` TEXT NOT NULL
- `customer_email` TEXT NOT NULL
- `shipping_line1` TEXT NOT NULL
- `shipping_line2` TEXT NOT NULL default=''
- `shipping_city` TEXT NOT NULL
- `shipping_region` TEXT NOT NULL default=''
- `shipping_postal` TEXT NOT NULL
- `shipping_country` TEXT NOT NULL default=''
- `shipping_tracking` TEXT NOT NULL default=''
- `total_cents` INTEGER NOT NULL
- `subtotal_cents` INTEGER
- `shipping_cents` INTEGER NOT NULL default=0
- `affiliate_code` TEXT
- `affiliate_counted` INTEGER NOT NULL default=0
- `guest_session_id` TEXT
- `receipt_sent` INTEGER NOT NULL default=0
- `receipt_sent_at` TEXT
- `shipping_name` TEXT
- `payment_method` TEXT NOT NULL default=''
- `customer_notes` TEXT NOT NULL default=''
- `fulfillment_status` TEXT NOT NULL default='paid'
- `affiliate_commission_cents` INTEGER
- `affiliate_commission_rate` REAL
- `customer_phone` TEXT NOT NULL default=''
- `geo_country` TEXT
- `geo_city` TEXT
- `stripe_checkout_session_id` TEXT
- `customer_id` INTEGER

Foreign keys:
- `customer_id` → `customers(id)` ON DELETE NO ACTION
- `affiliate_user_id` → `users(id)` ON DELETE NO ACTION

Indexes:
- `idx_orders_customer_id`
- `idx_orders_customer_email`
- `idx_orders_fulfillment`
- `idx_orders_status`
- `idx_orders_stripe_checkout_session` UNIQUE
- `idx_orders_created`
- `idx_orders_affiliate`
- `sqlite_autoindex_orders_1` UNIQUE

Nullable: `affiliate_user_id`, `subtotal_cents`, `affiliate_code`, `guest_session_id`, `receipt_sent_at`, `shipping_name`, `affiliate_commission_cents`, `affiliate_commission_rate`, `geo_country`, `geo_city`, `stripe_checkout_session_id`, `customer_id`

### `payments` (1 rows)

Columns:
- `id` INTEGER PK
- `order_id` INTEGER NOT NULL
- `provider` TEXT NOT NULL default='stripe'
- `provider_payment_id` TEXT
- `provider_session_id` TEXT
- `amount_cents` INTEGER NOT NULL
- `currency` TEXT NOT NULL default='NZD'
- `status` TEXT NOT NULL default='succeeded'
- `payment_method` TEXT NOT NULL default=''
- `paid_at` TEXT
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `order_id` → `orders(id)` ON DELETE RESTRICT

Indexes:
- `idx_payments_provider_session` UNIQUE
- `idx_payments_order`

Nullable: `provider_payment_id`, `provider_session_id`, `paid_at`

### `product_images` (50 rows)

Columns:
- `id` INTEGER PK
- `product_id` INTEGER NOT NULL
- `path` TEXT NOT NULL
- `sort_order` INTEGER NOT NULL default=0
- `role` TEXT NOT NULL default='gallery'

Foreign keys:
- `product_id` → `products(id)` ON DELETE CASCADE

Indexes:
- `idx_product_images_product`

### `product_tags` (0 rows)

Columns:
- `product_id` INTEGER PK NOT NULL
- `tag` TEXT PK NOT NULL

Foreign keys:
- `product_id` → `products(id)` ON DELETE CASCADE

Indexes:
- `idx_product_tags_tag`
- `sqlite_autoindex_product_tags_1` UNIQUE

### `product_variants` (5 rows)

Columns:
- `id` INTEGER PK
- `product_id` INTEGER NOT NULL
- `name` TEXT NOT NULL default='Default'
- `sku` TEXT
- `barcode` TEXT
- `status` TEXT NOT NULL default='active'
- `price_cents` INTEGER NOT NULL
- `compare_at_price_cents` INTEGER
- `cost_cents` INTEGER
- `currency` TEXT NOT NULL default='NZD'
- `weight_grams` INTEGER
- `width_cm` REAL
- `height_cm` REAL
- `depth_cm` REAL
- `is_default` INTEGER NOT NULL default=1
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')
- `deleted_at` TEXT

Foreign keys:
- `product_id` → `products(id)` ON DELETE RESTRICT

Indexes:
- `idx_product_variants_product`
- `idx_product_variants_sku` UNIQUE

Nullable: `sku`, `barcode`, `compare_at_price_cents`, `cost_cents`, `weight_grams`, `width_cm`, `height_cm`, `depth_cm`, `deleted_at`

### `product_versions` (0 rows)

Columns:
- `id` INTEGER PK
- `product_id` INTEGER NOT NULL
- `version` TEXT NOT NULL
- `status` TEXT NOT NULL default='draft'
- `notes` TEXT NOT NULL default=''
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `product_id` → `products(id)` ON DELETE RESTRICT

Indexes:
- `sqlite_autoindex_product_versions_1` UNIQUE

### `production_materials` (0 rows)

Columns:
- `id` INTEGER PK
- `production_order_id` INTEGER NOT NULL
- `component_id` INTEGER NOT NULL
- `quantity_planned` REAL NOT NULL
- `quantity_used` REAL NOT NULL default=0
- `unit_cost_cents` INTEGER NOT NULL default=0
- `created_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `component_id` → `components(id)` ON DELETE RESTRICT
- `production_order_id` → `production_orders(id)` ON DELETE CASCADE

### `production_orders` (0 rows)

Columns:
- `id` INTEGER PK
- `product_variant_id` INTEGER NOT NULL
- `product_version_id` INTEGER
- `quantity_planned` INTEGER NOT NULL
- `quantity_produced` INTEGER NOT NULL default=0
- `status` TEXT NOT NULL default='PLANNED'
- `planned_start_at` TEXT
- `planned_completion_at` TEXT
- `actual_start_at` TEXT
- `actual_completion_at` TEXT
- `notes` TEXT NOT NULL default=''
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `product_version_id` → `product_versions(id)` ON DELETE NO ACTION
- `product_variant_id` → `product_variants(id)` ON DELETE RESTRICT

Indexes:
- `idx_production_orders_status`

Nullable: `product_version_id`, `planned_start_at`, `planned_completion_at`, `actual_start_at`, `actual_completion_at`

### `products` (5 rows)

Columns:
- `id` INTEGER PK
- `slug` TEXT NOT NULL
- `name` TEXT NOT NULL
- `price_cents` INTEGER NOT NULL
- `description` TEXT NOT NULL default=''
- `sort_order` INTEGER NOT NULL default=0
- `is_main` INTEGER NOT NULL default=0
- `dimensions` TEXT NOT NULL default=''
- `materials` TEXT NOT NULL default=''
- `capacity` TEXT NOT NULL default=''
- `sku` TEXT
- `created_at` TEXT
- `width_cm` REAL
- `height_cm` REAL
- `depth_cm` REAL
- `capacity_records` INTEGER
- `add_to_cart_enabled` INTEGER NOT NULL default=1
- `featured` INTEGER NOT NULL default=0
- `collection` TEXT NOT NULL default=''

Indexes:
- `idx_products_slug`
- `idx_products_sku` UNIQUE
- `idx_products_featured`
- `idx_products_collection`
- `sqlite_autoindex_products_1` UNIQUE

Nullable: `sku`, `created_at`, `width_cm`, `height_cm`, `depth_cm`, `capacity_records`

### `purchase_order_items` (0 rows)

Columns:
- `id` INTEGER PK
- `purchase_order_id` INTEGER NOT NULL
- `component_id` INTEGER NOT NULL
- `quantity_ordered` INTEGER NOT NULL
- `quantity_received` INTEGER NOT NULL default=0
- `unit_cost_cents` INTEGER NOT NULL
- `total_cents` INTEGER NOT NULL

Foreign keys:
- `component_id` → `components(id)` ON DELETE RESTRICT
- `purchase_order_id` → `purchase_orders(id)` ON DELETE CASCADE

### `purchase_orders` (0 rows)

Columns:
- `id` INTEGER PK
- `purchase_order_number` TEXT NOT NULL
- `supplier_id` INTEGER NOT NULL
- `status` TEXT NOT NULL default='draft'
- `currency` TEXT NOT NULL default='NZD'
- `subtotal_cents` INTEGER NOT NULL default=0
- `tax_cents` INTEGER NOT NULL default=0
- `total_cents` INTEGER NOT NULL default=0
- `expected_delivery_at` TEXT
- `received_at` TEXT
- `notes` TEXT NOT NULL default=''
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `supplier_id` → `suppliers(id)` ON DELETE RESTRICT

Indexes:
- `sqlite_autoindex_purchase_orders_1` UNIQUE

Nullable: `expected_delivery_at`, `received_at`

### `quality_control_records` (0 rows)

Columns:
- `id` INTEGER PK
- `production_order_id` INTEGER NOT NULL
- `status` TEXT NOT NULL default='pending'
- `inspected_by_id` INTEGER
- `notes` TEXT NOT NULL default=''
- `inspected_at` TEXT

Foreign keys:
- `inspected_by_id` → `users(id)` ON DELETE NO ACTION
- `production_order_id` → `production_orders(id)` ON DELETE RESTRICT

Nullable: `inspected_by_id`, `inspected_at`

### `schema_migrations` (10 rows)

Columns:
- `version` TEXT PK
- `applied_at` TEXT NOT NULL

Indexes:
- `sqlite_autoindex_schema_migrations_1` UNIQUE

### `shipments` (0 rows)

Columns:
- `id` INTEGER PK
- `order_id` INTEGER NOT NULL
- `carrier` TEXT NOT NULL default=''
- `service` TEXT NOT NULL default=''
- `tracking_number` TEXT NOT NULL default=''
- `status` TEXT NOT NULL default='pending'
- `shipped_at` TEXT
- `delivered_at` TEXT
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `order_id` → `orders(id)` ON DELETE RESTRICT

Indexes:
- `idx_shipments_order`

Nullable: `shipped_at`, `delivered_at`

### `stripe_processed_events` (0 rows)

Columns:
- `event_id` TEXT PK
- `event_type` TEXT NOT NULL default=''
- `session_id` TEXT NOT NULL default=''
- `created_at` TEXT NOT NULL default=datetime('now')

Indexes:
- `idx_stripe_processed_events_session`
- `sqlite_autoindex_stripe_processed_events_1` UNIQUE

### `supplier_components` (0 rows)

Columns:
- `id` INTEGER PK
- `supplier_id` INTEGER NOT NULL
- `component_id` INTEGER NOT NULL
- `supplier_sku` TEXT NOT NULL default=''
- `unit_cost_cents` INTEGER NOT NULL default=0
- `currency` TEXT NOT NULL default='NZD'
- `minimum_order_quantity` INTEGER NOT NULL default=1
- `lead_time_days` INTEGER NOT NULL default=0
- `is_preferred` INTEGER NOT NULL default=0
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Foreign keys:
- `component_id` → `components(id)` ON DELETE RESTRICT
- `supplier_id` → `suppliers(id)` ON DELETE RESTRICT

Indexes:
- `idx_supplier_component_pair` UNIQUE
- `idx_supplier_components_supplier_sku`
- `idx_supplier_components_component`

### `suppliers` (0 rows)

Columns:
- `id` INTEGER PK
- `name` TEXT NOT NULL
- `contact_name` TEXT NOT NULL default=''
- `email` TEXT NOT NULL default=''
- `phone` TEXT NOT NULL default=''
- `website` TEXT NOT NULL default=''
- `notes` TEXT NOT NULL default=''
- `status` TEXT NOT NULL default='active'
- `created_at` TEXT NOT NULL default=datetime('now')
- `updated_at` TEXT NOT NULL default=datetime('now')

Indexes:
- `idx_suppliers_name`

### `users` (9 rows)

Columns:
- `id` INTEGER PK
- `email` TEXT NOT NULL
- `password_hash` TEXT NOT NULL
- `role` TEXT NOT NULL
- `affiliate_slug` TEXT
- `full_name` TEXT
- `created_at` TEXT NOT NULL default=datetime('now')
- `totp_secret` TEXT
- `totp_confirmed` INTEGER NOT NULL default=0
- `password_reset_token` TEXT
- `password_reset_expires` TEXT
- `affiliate_code` TEXT
- `affiliate_active` INTEGER NOT NULL default=1
- `terms_accepted_at` TEXT
- `display_picture_url` TEXT
- `admin_notes` TEXT
- `signup_first_name` TEXT
- `signup_last_name` TEXT
- `terms_accepted` INTEGER NOT NULL default=0
- `affiliate_country` TEXT NOT NULL default=''
- `affiliate_bank_details` TEXT NOT NULL default=''
- `affiliate_bank_details_updated_at` TEXT

Indexes:
- `idx_users_role`
- `idx_users_affiliate_code` UNIQUE
- `sqlite_autoindex_users_2` UNIQUE
- `sqlite_autoindex_users_1` UNIQUE

Nullable: `affiliate_slug`, `full_name`, `totp_secret`, `password_reset_token`, `password_reset_expires`, `affiliate_code`, `terms_accepted_at`, `display_picture_url`, `admin_notes`, `signup_first_name`, `signup_last_name`, `affiliate_bank_details_updated_at`

## Findings (do not auto-fix)

- No orphan or foreign-key problems detected on this local copy.
- No duplicate slugs, order numbers, customer emails, or Stripe sessions detected.
- No negative financial values detected.
- Historical price already holds: order `LL-20260329-70FAAF` stores `unit_price_cents = 12900` while product `sound-wave` is now `42900`. Do not “correct” the order item to the current catalogue price.
- That same order has `stripe_checkout_session_id` NULL (placed before session uniqueness). Leave it; the unique index is partial (`WHERE session_id IS NOT NULL`).
- Inventory movements are empty and on-hand quantities are unused (catalogue SKUs are unmanaged until stock is received). Paid orders will not deduct stock unless `INVENTORY_ENFORCE=1` or movements exist. This is intentional so live sales are not blocked.
- audit_logs is empty on this copy (new table / no admin mutations since 009). Future orders write ORDER_CREATED.
- Orders exist without shipment rows. Backfill only creates shipments when tracking or shipped/delivered status is present. Safe; fulfillment now upserts a shipment.

## Application code map (high level)

| Area | Tables | Code |
|------|--------|------|
| Catalogue | products, product_images, product_tags, product_variants | `db.py`, shop routes in `app.py` |
| Checkout / orders | orders, order_items, customers, payments | `_stripe_finalize_checkout_session` |
| Affiliates | users, affiliate_pages, commissions | `app.py` commission helpers |
| Inventory | inventory_locations, inventory_items, inventory_movements | `inventory.py` |
| Manufacturing | components, product_versions, bills_of_materials, bill_of_material_items, suppliers, purchase_orders, production_orders | schema in migration 008; `bom_cost_cents` |
| Analytics | analytics_sessions, analytics_events | `/api/analytics/*` (fail-soft) |
| Audit | audit_logs, business_events | `insert_audit_log`, `insert_business_event` |
| Stripe idempotency | orders.stripe_checkout_session_id, payments.provider_session_id, stripe_processed_events | webhook + unique indexes |

