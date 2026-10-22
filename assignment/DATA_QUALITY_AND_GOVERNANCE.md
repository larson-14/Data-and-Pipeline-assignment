# Part D: Data quality dimensions

ETL uses the existing pandas checks in `include/validation_checks.py`; ELT uses `include/sql/stg_masters.sql` and `stg_orders.sql`.
Both engines implement the same rule contract, record passes and failures for base and increment phases, and add named schema, volume, freshness and price-distribution checks. The captured scorecards show the actual check counts for each phase.

| Dimension | ETL Python example | ELT SQL example | Severity |
| --- | --- | --- | --- |
| Completeness | Null/blank order customer_id; blank product category | order_customer_required; product_category_required | Critical |
| Uniqueness | Duplicate order_id and payment_id, before dedupe | order_duplicate; payment_duplicate | Critical |
| Validity | Allowed payment modes; phone and email formats | order_payment_mode; customer_phone; customer_email | Critical mode; Warning contacts |
| Accuracy | Positive quantity; amount formula; cost_price ≤ mrp; delivery < 120 | order_quantity; order_amount; product_cost_mrp; order_delivery_limit | Critical |
| Consistency | Master foreign keys; amount_paid matches order_amount; loyalty casing | order_*_key; order_payment_amount; customer_loyalty_casing | Critical keys/amount; Warning casing |
| Timeliness | No future order_ts or signup_date | order_future_timestamp; customer_future_signup | Critical |

**Severity handling:** Critical ETL orders go to `include/quarantine/sudatta/<file>_failed_<phase>_<run_id>.csv` with source row, severity and reasons. Critical payments, customers, products and stores also get source-specific files, including master rows with no related orders. Warning-only orders remain in `passed`, load into the fact table and are logged; a row that is both Critical and Warning remains quarantined.

ELT keeps every source row in append-only raw tables. `elt_quarantine` exposes Critical source records and rule names, including the unmodified raw field values. Critical orders are retained with `dq_status` and reasons in staging and excluded from revenue marts; Warning rows remain eligible and their check results are logged. Duplicate customer/product/store masters are deduped with Warning severity, matching the existing ETL first-record policy; payment/order duplicates are Critical.

**Quality history:** `dq_results` exists in each pipeline's DuckDB warehouse, with exactly `run_id`, `dag_id`, `check_name`, `dimension`, `severity`, `rows_checked`, `rows_failed`, `status`, `checked_at`. Each new run retains its results; retries replace only the same run/check entry. Both engines prefix check names with `base.` or `increment.` so both checks of the order feed remain distinct.

Counts describe the table where the check runs: customer format checks count customer rows, product checks count product rows, and order checks count orders. The explicit schema requires every supplied source column; product-category completeness runs on products, and related orders inherit Critical master failures. Status is `Passed`, `Failed` for a failed Critical check, or `Warning` for a failed Warning check.

```sql
-- Run separately against each warehouse; the names and column layout match.
SELECT run_id, dag_id, dimension, severity, check_name, rows_checked, rows_failed, status
FROM dq_results ORDER BY checked_at, check_name;

-- ELT: exact rejected source records, with one evidence row per broken rule.
SELECT run_id, source_file, row_key, check_name, raw_record FROM elt_quarantine;
```

Validation commands: `python -W ignore -m unittest tests.test_dq_scorecard tests.test_elt_pipeline -v`.
Reproduce the worked evidence: `python -W ignore -m include.governance_examples`.

# Part E: Data governance

The short answers below describe this project; proposed organisational controls are identified as proposals rather than existing enforcement.

## 1. Ownership

Order Operations owns orders; Finance/Payments owns payments; CRM owns customers; Merchandising owns products; Store Operations owns stores.
Sudatta/Data Engineering is the technical steward of the DuckDB layers, while Finance owns definitions of revenue and margin in the marts.
Store Operations proposes a change to the 120-minute rule; Finance and the data-governance owner approve the impact before a reviewed, tested change to Python and SQL rules is deployed.

## 2. Data dictionary

The fact grain is one accepted order_id, with customer_id, product_id and store_id linking to the three natural-key dimensions.
[GOVERNANCE_APPENDIX.md](GOVERNANCE_APPENDIX.md) lists every actual fact/dimension column, DuckDB type, meaning, source, expected values and PII classification.
Types were inspected from a fresh warehouse built by the current ETL functions, so the dictionary reflects the code rather than claiming that all dates or pincodes have ideal types.

## 3. Lineage

The 3 September `S11`/UPI revenue of **3,674.50 INR** in `mart_daily_store_kpis` comes from six specific orders; [LINEAGE_EXAMPLE.csv](LINEAGE_EXAMPLE.csv) lists their original order, customer, product, store and payment CSV row numbers and contributions.
ELT staging retains `_source_file`, `_source_row`, `_run_id`; raw history and `elt_raw_loads.file_hash` identify the exact input version.
ETL now archives exact CSV bytes with per-run SHA-256 manifests and stores source file/row/run references on accepted orders. Both support historical proof; ELT is easier to investigate entirely with SQL because the raw versions are in the warehouse.

## 4. Personal data

All ten CRM columns describe a customer; manager_name identifies an employee, and order/payment fields are personal when joined to customer_id. The full column inventory is in the appendix.
ETL and ELT mask email as `pr***@example.com` and phone as `98******21`; IDs remain for joins, reconciliation and permitted analytics, while unmasked source evidence stays restricted. Names and location attributes are not masked today, so these outputs are not anonymous.
The **Digital Personal Data Protection Act, 2023**, with the 2025 Rules, is the relevant framework: lawful purpose/consent or another permitted basis, clear notice, limited processing, security, correction/erasure and breach response are expected as applicable provisions commence. [Official Act](https://www.indiacode.nic.in/bitstream/123456789/22037/1/a2023-22.pdf)
As of **9 October 2026**, the core processing obligations are in the 18-month commencement group after the November 2025 notification, rather than all already being in force; applicable transition obligations also need review. [Official commencement notification](https://www.meity.gov.in/static/uploads/2025/11/c56ceae6c383460ca69577428d36828b.pdf)

## 5. Access control

Proposed access: Data Engineering and authorised source owners read raw/quarantine for remediation; vetted analysts read masked staging; Finance and leadership read approved marts.
Unmasked raw customer contacts should be visible only to authorised CRM/privacy personnel and tightly controlled engineering service accounts with a specific need; Finance should not receive them for revenue reporting.
This local DuckDB project currently relies on filesystem/service access, not enforced per-role table permissions; deploy separate restricted storage or controlled query services, read-only consumer access and access logs before production use.

## 6. Retention

Propose a restricted **one-year raw/audit archive**, **30 days of operational quarantine** with necessary evidence retained in that archive, and customer-linked analytic data only while its purpose remains valid, with a review at 12 months; longer statutory holds must be documented.
These are proposed policies, not deletion automation. When effective, Rule 8(3) requires minimum one-year retention of relevant processing data/logs; operational cleanup must not remove required evidence. [Official 2025 Rules](https://www.meity.gov.in/static/uploads/2025/11/53450e6e5dc0bfa85ebd78686cadad39.pdf)
After the approved period/hold expires, securely erase or irreversibly anonymise expired records and age out backups; masking alone does not remove personal-data status. The current pipeline never deletes raw rows, so compliant archival/erasure needs a separate governed lifecycle process.

## 7. Secrets

The optional failure callback reads `SLACK_WEBHOOK_URL` from an Airflow Variable. Delivery is opt-in (`send_notifications=true`) and disabled during observability tests. Assignment processing requires no API key or paid AI service; the trainer example is retained unchanged.
Use encrypted Airflow Variables or a secrets backend, restrict access and rotate credentials. Never commit values to DAGs, exports or GitHub; deleting a commit does not revoke a leaked credential. Actual remote delivery was not exercised.

## 8. Audit

`ORD000469` at `orders.csv` data row 126 has quantity `0`; [AUDIT_EXAMPLE.json](AUDIT_EXAMPLE.json) records its exact source fields, input SHA-256, ETL reasons and the ELT rule `order_quantity`.
ETL quarantine preserves original text and source-row metadata; its per-run byte archive and checksum manifest prove the received file version. `etl_fact_orders` contains **0** rows for this ID, so it cannot contribute to ETL reports.
ELT `elt_quarantine.raw_record` preserves those same original field values, and `elt_reconciled_orders` contains **0** rows for the ID, proving exclusion from the CFO revenue input; runnable evidence queries are in the appendix.
Both engines retain reasons for rejected source records even if no accepted order references them. Private archives and raw_record values are excluded from the public evidence package.

## 9. Change history

Both engines apply the increment as an upsert. A DELIVERED order changed to RETURNED is removed from current September finance revenue; a newly invalid correction also removes a formerly valid fact, so stale revenue cannot survive.
For the supplied files, both change from **882,163.60** to **878,583.90 INR** (1,635 to 1,628 finance rows). All 150 new IDs and ten corrections are retained in current order state; new orders without payment evidence stay out of finance. [Exact comparison](ELT_VERIFICATION.md)
`etl_finance_history` and `elt_finance_history` retain run-versioned published snapshots; ELT also keeps base/final phases and append-only raw versions. Finance can inspect an as-reported run alongside the current restated view. A return is not proof of a refund: Finance must approve accounting treatment and retention of those snapshots.
