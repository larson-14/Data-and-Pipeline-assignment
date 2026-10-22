# Part E appendix: dictionary, lineage and audit

Generated using current ETL functions and ELT SQL in temporary databases. Types below are the actual DuckDB schema produced by this dataset, not assumed types.

The fact table grain is one accepted order_id. Dimensions use customer_id, product_id and store_id as natural keys. PII includes indirect identification: a customer-linked transaction can be personal even without a name.

## etl_fact_orders

| Column | Type | Meaning | Source | Expected values / current enforcement | PII |
| --- | --- | --- | --- | --- | --- |
| order_id | VARCHAR | Order key; one row per accepted order | orders.csv | ORDâ€¦; unique primary key | Yes, linked transaction |
| order_ts | TIMESTAMP | Order creation timestamp | orders.csv | Valid business-local date/timestamp | Yes, linked transaction |
| customer_id | VARCHAR | Customer key; links orders to CRM | orders.csv | Câ€¦; primary / foreign key | Yes, linked transaction |
| store_id | VARCHAR | Store key; links orders to store master | orders.csv | Sâ€¦; primary / foreign key | Yes, linked transaction |
| product_id | VARCHAR | Product key; links orders to catalogue | orders.csv | Pâ€¦; primary / foreign key | Yes, linked transaction |
| quantity | BIGINT | Units ordered | orders.csv | > 0 | Yes, linked transaction |
| unit_price | DECIMAL(18,2) | Selling price per unit in INR | orders.csv | Finite, nonnegative INR amount | Yes, linked transaction |
| discount_pct | DOUBLE | Discount applied to the order | orders.csv | Business contract: 0â€“100; no dedicated ETL range check | Yes, linked transaction |
| order_amount | DECIMAL(18,2) | Source billed amount in INR | orders.csv | Equals half-up rounded formula, two decimals | Yes, linked transaction |
| payment_mode | VARCHAR | Standardised payment method | orders.csv | UPI, CARD, CASH ON DELIVERY, WALLET | Yes, linked transaction |
| order_status | VARCHAR | Standardised order state | orders.csv | Finance fact: DELIVERED; operational layer also CANCELLED/RETURNED | Yes, linked transaction |
| channel | VARCHAR | Order entry channel | orders.csv | Business text | Yes, linked transaction |
| delivery_minutes | DOUBLE | Delivery duration in minutes | orders.csv | 0?<120; duration required for DELIVERED in both pipelines | Yes, linked transaction |
| _source_file | VARCHAR | Original export filename | orders.csv | Business text | Yes, linked transaction |
| _source_row | BIGINT | One-based data row in source file | orders.csv | Source numeric value | Yes, linked transaction |
| _run_id | VARCHAR | Airflow run identifier | orders.csv | Business text | Yes, linked transaction |
| order_date | DATE | Calendar date derived from order_ts | Derived from orders.csv | Valid business-local date/timestamp | Yes, linked transaction |
| net_revenue | DECIMAL(18,2) | quantity Ã— unit_price Ã— (1 âˆ’ discount_pct / 100) | Derived from orders.csv | Derived INR; business revenue counts DELIVERED only | Yes, linked transaction |
| gross_margin | DECIMAL(18,2) | net_revenue âˆ’ quantity Ã— product cost_price | Derived from orders.csv + products.csv | Derived INR, may be negative | Yes, linked transaction |
| within_15_min | BOOLEAN | Whether delivery_minutes â‰¤ 15 | Derived from orders.csv | true / false / null | Yes, linked transaction |

## etl_dim_customer

| Column | Type | Meaning | Source | Expected values / current enforcement | PII |
| --- | --- | --- | --- | --- | --- |
| customer_id | VARCHAR | Customer key; links orders to CRM | customers.csv | Câ€¦; primary / foreign key | Yes |
| full_name | VARCHAR | Customer name, trimmed and title-cased; unmasked | customers.csv | Business text | Yes |
| email | VARCHAR | Masked email; first two local characters and domain retained | customers.csv | Masked string / missing | Yes (masked, still linkable) |
| phone | VARCHAR | Masked phone; first two and last two characters retained | customers.csv | Masked string / missing | Yes (masked, still linkable) |
| city | VARCHAR | Customer city | customers.csv | Business text | Yes |
| area | VARCHAR | Customer or store locality | customers.csv | Business text | Yes |
| pincode | VARCHAR | Postal code; ETL type is inferred from the input column | customers.csv | 6 digits; format problems produce warnings | Yes |
| signup_date | VARCHAR | CRM account creation date | customers.csv | ISO date/timestamp text; no future signup | Yes |
| loyalty_tier | VARCHAR | Standardised loyalty level | customers.csv | BRONZE, SILVER, GOLD, PLATINUM | Yes |
| is_active | VARCHAR | CRM active-account indicator | customers.csv | Y / N | Yes |

## etl_dim_product

| Column | Type | Meaning | Source | Expected values / current enforcement | PII |
| --- | --- | --- | --- | --- | --- |
| product_id | VARCHAR | Product key; links orders to catalogue | products.csv | Pâ€¦; primary / foreign key | No |
| product_name | VARCHAR | Catalogue item name | products.csv | Business text | No |
| category | VARCHAR | Product category | products.csv | Fruits & Vegetables, Dairy & Bakery, Staples, Snacks & Beverages, Personal Care, Household | No |
| sub_category | VARCHAR | Product subcategory | products.csv | Business text | No |
| brand | VARCHAR | Product brand | products.csv | Business text | No |
| mrp | DECIMAL(18,2) | Maximum retail price per unit in INR | products.csv | Finite and > 0 | No |
| cost_price | DECIMAL(18,2) | Product acquisition cost per unit in INR | products.csv | â‰¤ mrp | No |
| unit | VARCHAR | Selling unit | products.csv | kg, pack, unit | No |
| is_perishable | VARCHAR | Whether the product is perishable | products.csv | Source True / False; inferred ETL string or boolean type shown below | No |
| launch_date | VARCHAR | Product launch date | products.csv | Source YYYY-MM-DD text; not type-enforced | No |

## etl_dim_store

| Column | Type | Meaning | Source | Expected values / current enforcement | PII |
| --- | --- | --- | --- | --- | --- |
| store_id | VARCHAR | Store key; links orders to store master | stores.csv | Sâ€¦; primary / foreign key | No |
| store_name | VARCHAR | Dark-store name | stores.csv | Business text | No |
| zone | VARCHAR | Store operating zone | stores.csv | North, South, East, West | No |
| area | VARCHAR | Customer or store locality | stores.csv | Business text | No |
| pincode | VARCHAR | Postal code; ETL type is inferred from the input column | stores.csv | 6 digits; format problems produce warnings | No |
| open_date | VARCHAR | Store opening date | stores.csv | Source YYYY-MM-DD text; not type-enforced | No |
| manager_name | VARCHAR | Store manager name; unmasked employee personal data | stores.csv | Business text | Yes |
| capacity_orders_per_day | BIGINT | Planned daily order capacity | stores.csv | Source numeric value | No |

## Worked lineage number

`mart_daily_store_kpis.net_revenue` = **3674.50 INR** for `2026-09-03`, `Nammamart Hsr Layout` (`S11`), zone `East`, payment mode `UPI`. The value sums 6 accepted, delivered, reconciled orders.

Exact order rows and corresponding master/payment CSV rows are listed in [LINEAGE_EXAMPLE.csv](LINEAGE_EXAMPLE.csv). Data rows are counted from 1 after the header; these supplied files have one physical line per data row, so physical line = data row + 1.

| Order | Orders CSV data row | Revenue contribution |
| --- | ---: | ---: |
| ORD000173 | 580 | 158.10 |
| ORD000192 | 2232 | 230.00 |
| ORD000197 | 1827 | 285.60 |
| ORD000212 | 1921 | 1998.00 |
| ORD000213 | 112 | 754.80 |
| ORD000224 | 1175 | 248.00 |

Formula for each contribution: `quantity Ã— unit_price Ã— (1 âˆ’ discount_pct / 100)`. Each contributing stg_orders row retains `_source_file`, `_source_row`, `_run_id`; the immutable raw snapshot and load ledger identify the input version. ETL facts now retain source file/row/run metadata; extraction archives exact CSV bytes and SHA-256 under include/data/nammamart/private/archive/<run>/.

## Worked rejected order

`ORD000469`, `orders.csv` data row 126, was rejected for `order_quantity` in ETL and `order_quantity` in ELT.

Exact source fields and the SHA-256 of the original input are in [AUDIT_EXAMPLE.json](AUDIT_EXAMPLE.json). ETL quarantine contains its source row, severity and reasons; ELT `elt_quarantine.raw_record` exactly matches the original CSV fields. Neither query below contributes an order to revenue:

```sql
-- ETL warehouse: expected 0
SELECT COUNT(*) FROM etl_fact_orders WHERE order_id = 'ORD000469';

-- ELT warehouse: exact raw source record and broken rule
SELECT source_file, row_key, check_name, raw_record FROM elt_quarantine WHERE row_key = 'ORD000469';

-- ELT revenue input: expected 0
SELECT COUNT(*) FROM elt_reconciled_orders WHERE order_id = 'ORD000469';
```

The evidence JSON records both counts as 0. ETL extraction preserves original fields and archives exact CSV bytes with SHA-256 per run. ELT retains all original fields in raw tables with file/run metadata.

## Personal-data inventory

| Source | Personal columns / linkage |
| --- | --- |
| customers.csv | customer_id, full_name, email, phone, city, area, pincode, signup_date, loyalty_tier, is_active |
| orders.csv and increment | order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes: personal as a linked customer transaction |
| payments.csv | payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref: personal when linked to the customer order |
| stores.csv | manager_name is direct employee personal data; other columns describe the business store |
| products.csv | No direct personal columns in this dataset; purchased product IDs become personal within linked order records |

Only customer email and phone are masked today. Names, customer keys, localities, account attributes, transaction history and manager_name remain linkable or directly identifying; masking two fields does not anonymise the dataset. Raw tables, input CSVs, XCom payloads and restricted source quarantine can contain unmasked information.
