-- Preserve September before the next-day corrections for a like-for-like check.
CREATE OR REPLACE TABLE elt_base_orders AS SELECT * FROM stg_orders;
CREATE OR REPLACE TABLE elt_base_daily_store_kpis AS SELECT * FROM mart_daily_store_kpis;
CREATE OR REPLACE TABLE elt_base_category_margin AS SELECT * FROM mart_category_margin;
CREATE OR REPLACE TABLE elt_base_report_orders AS SELECT * FROM elt_reconciled_orders;
