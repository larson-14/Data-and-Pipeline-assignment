# NammaMart source data profile

Raw exports are copied byte-for-byte into `include/data/nammamart/`; no source values were edited. This profile is reproducible from unchanged source exports.

| File | Rows | Unique keys | Duplicate excess | Issue entries | Affected rows |
|---|---:|---:|---:|---:|---:|
| customers.csv | 405 | 400 | 5 | 60 | 51 |
| orders.csv | 2412 | 2400 | 12 | 164 | 153 |
| orders_2026-10-01_increment.csv | 160 | 160 | 0 | 0 | 0 |
| payments.csv | 2228 | 2224 | 4 | 48 | 44 |
| products.csv | 152 | 150 | 2 | 13 | 12 |
| stores.csv | 15 | 15 | 0 | 2 | 2 |

Total: 5,372 source rows; 287 issue entries on 262 source rows. One row may have multiple issues. Duplicate groups flag every participating row; duplicate excess counts only copies beyond the first.

## Findings by check

| File | Problem | Severity | Entries |
|---|---|---|---:|
| customers.csv | Duplicate key across 2 rows (conflicting records) | ERROR | 10 |
| customers.csv | Malformed email (value redacted) | ERROR | 12 |
| customers.csv | Missing value | ERROR | 6 |
| customers.csv | Noncanonical tier spelling/case; inferred catalogue | REVIEW | 8 |
| customers.csv | Not a canonical 10-digit Indian mobile number (value redacted) | REVIEW | 7 |
| customers.csv | Not a six-digit Indian pincode | ERROR | 5 |
| customers.csv | Pincode differs from store master for same area | REVIEW | 5 |
| customers.csv | Shared email across distinct customer IDs (redacted); review identity | REVIEW | 4 |
| customers.csv | Signup after September snapshot | REVIEW | 3 |
| orders.csv | Delivered order has no payment export record | REVIEW | 27 |
| orders.csv | Discount outside 0–100 | ERROR | 3 |
| orders.csv | Duplicate key across 2 rows (identical records) | ERROR | 24 |
| orders.csv | Invalid ISO date/timestamp | ERROR | 5 |
| orders.csv | Leading/trailing whitespace | REVIEW | 2 |
| orders.csv | Missing value | ERROR | 27 |
| orders.csv | Must be a positive integer | ERROR | 10 |
| orders.csv | Negative value | ERROR | 6 |
| orders.csv | No matching dimension key | ERROR | 14 |
| orders.csv | Not in client order-status allowlist | ERROR | 5 |
| orders.csv | Not in client payment-mode allowlist | ERROR | 10 |
| orders.csv | Outside September reporting window | ERROR | 7 |
| orders.csv | Outside normal 8–35 minute range | REVIEW | 6 |
| payments.csv | Duplicate key across 2 rows (identical records) | ERROR | 8 |
| payments.csv | Leading/trailing whitespace | REVIEW | 2 |
| payments.csv | Negative value | ERROR | 6 |
| payments.csv | No matching base order | ERROR | 8 |
| payments.csv | Not in client payment-mode allowlist | ERROR | 10 |
| payments.csv | SUCCESS payment does not equal base order amount | ERROR | 14 |
| products.csv | Cost price exceeds MRP | REVIEW | 5 |
| products.csv | Duplicate key across 2 rows (conflicting records) | ERROR | 4 |
| products.csv | Missing value | ERROR | 3 |
| products.csv | Negative value | ERROR | 1 |
| stores.csv | Missing value | ERROR | 1 |
| stores.csv | Must be a positive integer | ERROR | 1 |
| orders.csv | Order amount formula mismatch | ERROR | 18 |

## Increment behavior

10 existing order IDs are corrections, and 150 IDs are new. An overlap across feeds is expected and is not a uniqueness defect. Apply an upsert by order_id after resolving duplicates within the base feed. Payment reconciliation here uses the September base snapshot; a later returned status does not retroactively invalidate a September payment.

## Scope and interpretation

- Source row is the CSV record number with the header counted as row 1. Keys are retained; names, emails and phone values are excluded from the logs.
- ERROR denotes an explicit client rule or structural/format defect. REVIEW denotes an inferred convention, anomaly, or business question; do not automatically reject these rows.
- Delivery outside 8–35 minutes is an anomaly, not proof of error. Missing delivery values are required only for delivered orders. Blank gateway references on Cash on Delivery are expected.
- Money uses Decimal and half-up rounding to two decimals. The client should confirm its rounding convention.
- Customer contact formats and tier spelling, positive store capacity, cost above MRP, shared emails, and area/pincode mappings need confirmation. Store area mappings are internal reference evidence, not authoritative postal validation.
- No authoritative catalogue was supplied for channel, loyalty tiers, payment status, category, unit or zones; observed values cannot prove correctness. Factual accuracy and freshness require source-owner verification.
- Resolve conflicting key versions with the source owner. Normalize approved aliases in a derived layer, quarantine invalid foreign keys/numeric values, retain rejected records and reconcile revenue only for canonical DELIVERED orders.
- Raw customer data contains personal information; restrict access and use masked values in downstream reports. The full source bundle preserves the original personal data.

## Reproduce

Run `python scripts/profile_nammamart.py` from this project. Output is deterministic for unchanged inputs. SHA-256 hashes and per-column blank counts are in the JSON profile.

## Complete column statistics

[Machine-readable statistics](DATA_PROFILE.json). Blank means an empty/whitespace CSV field. Invalid numeric/date strings are separately listed in the issues log.

### customers.csv

| Column | Blank count |
| --- | ---: |
| customer_id | 0 |
| full_name | 0 |
| email | 0 |
| phone | 0 |
| city | 6 |
| area | 0 |
| pincode | 0 |
| signup_date | 0 |
| loyalty_tier | 0 |
| is_active | 0 |

| Numeric/date column | Minimum | Maximum |
| --- | --- | --- |
| signup_date | 2024-06-01T00:00:00 | 2027-01-05T00:00:00 |

| Category | Distinct source values |
| --- | --- |
| loyalty_tier | 'BRONZE', 'Bronze', 'Gold', 'Platinum', 'SILVER', 'Silver', 'bronze' |
| is_active | 'N', 'Y' |

### orders.csv

| Column | Blank count |
| --- | ---: |
| order_id | 0 |
| order_ts | 1 |
| customer_id | 18 |
| store_id | 0 |
| product_id | 0 |
| quantity | 0 |
| unit_price | 0 |
| discount_pct | 0 |
| order_amount | 0 |
| payment_mode | 0 |
| order_status | 0 |
| channel | 0 |
| delivery_minutes | 164 |

| Numeric/date column | Minimum | Maximum |
| --- | --- | --- |
| quantity | -3 | 5 |
| unit_price | 10.0 | 883.0 |
| discount_pct | 0 | 250 |
| order_amount | -1593.0 | 9576.0 |
| delivery_minutes | 8 | 1440 |
| order_ts | 2026-09-01T06:30:15 | 2027-01-01T09:00:00 |

| Category | Distinct source values |
| --- | --- |
| payment_mode | 'CARD', 'Card', 'Cash on Delivery', 'Cashh', 'UPI', 'UPI ', 'Wallet', 'cod', 'upi' |
| order_status | 'CANCELLED', 'DELIVERED', 'Pending??', 'RETURNED', 'delivered' |
| channel | 'App', 'Web' |

### orders_2026-10-01_increment.csv

| Column | Blank count |
| --- | ---: |
| order_id | 0 |
| order_ts | 0 |
| customer_id | 0 |
| store_id | 0 |
| product_id | 0 |
| quantity | 0 |
| unit_price | 0 |
| discount_pct | 0 |
| order_amount | 0 |
| payment_mode | 0 |
| order_status | 0 |
| channel | 0 |
| delivery_minutes | 0 |

| Numeric/date column | Minimum | Maximum |
| --- | --- | --- |
| quantity | 1 | 4 |
| unit_price | 10.0 | 883.0 |
| discount_pct | 0 | 20 |
| order_amount | 11.0 | 2052.0 |
| delivery_minutes | 8 | 34 |
| order_ts | 2026-09-01T09:38:13 | 2026-10-01T22:33:00 |

| Category | Distinct source values |
| --- | --- |
| payment_mode | 'Card', 'Cash on Delivery', 'UPI', 'Wallet' |
| order_status | 'DELIVERED', 'RETURNED' |
| channel | 'App' |

### payments.csv

| Column | Blank count |
| --- | ---: |
| payment_id | 0 |
| order_id | 0 |
| payment_ts | 0 |
| payment_mode | 0 |
| amount_paid | 0 |
| payment_status | 0 |
| gateway_ref | 301 |

| Numeric/date column | Minimum | Maximum |
| --- | --- | --- |
| amount_paid | -1593.0 | 9576.0 |
| payment_ts | 2026-09-01T06:30:24 | 2027-01-01T09:01:46 |

| Category | Distinct source values |
| --- | --- |
| payment_mode | 'CARD', 'Card', 'Cash on Delivery', 'Cashh', 'UPI', 'UPI ', 'Wallet', 'cod', 'upi' |
| payment_status | 'FAILED', 'REFUNDED', 'SUCCESS' |

### products.csv

| Column | Blank count |
| --- | ---: |
| product_id | 0 |
| product_name | 0 |
| category | 3 |
| sub_category | 0 |
| brand | 0 |
| mrp | 0 |
| cost_price | 0 |
| unit | 0 |
| is_perishable | 0 |
| launch_date | 0 |

| Numeric/date column | Minimum | Maximum |
| --- | --- | --- |
| mrp | -203.0 | 883.0 |
| cost_price | 6.79 | 631.79 |
| launch_date | 2023-01-03T00:00:00 | 2025-09-20T00:00:00 |

| Category | Distinct source values |
| --- | --- |
| category | '', 'Dairy & Bakery', 'Fruits & Vegetables', 'Household', 'Personal Care', 'Snacks & Beverages', 'Staples' |
| unit | 'kg', 'pack', 'unit' |
| is_perishable | 'False', 'True' |

### stores.csv

| Column | Blank count |
| --- | ---: |
| store_id | 0 |
| store_name | 0 |
| zone | 1 |
| area | 0 |
| pincode | 0 |
| open_date | 0 |
| manager_name | 0 |
| capacity_orders_per_day | 0 |

| Numeric/date column | Minimum | Maximum |
| --- | --- | --- |
| capacity_orders_per_day | 0 | 600 |
| open_date | 2024-02-14T00:00:00 | 2025-07-28T00:00:00 |

| Category | Distinct source values |
| --- | --- |
| zone | '', 'East', 'North', 'South', 'West' |
