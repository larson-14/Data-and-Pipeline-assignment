DELETE FROM elt_quality_rows WHERE run_id=(SELECT run_id FROM pipeline_context) AND phase=(SELECT phase FROM pipeline_context) AND source_file LIKE 'orders%';

CREATE TEMP TABLE orders_checks AS
SELECT *, [dq_check('order_key_required','Completeness','Critical',trim(order_id) = ''),
dq_check('order_duplicate','Uniqueness','Critical',count(*) OVER (PARTITION BY upper(trim(order_id))) > 1),
dq_check('order_customer_required','Completeness','Critical',trim(customer_id) = ''),
dq_check('order_payment_mode','Validity','Critical',upper(trim(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET')),
dq_check('order_status','Validity','Critical',upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED')),
dq_check('order_timestamp','Validity','Critical',try_cast(order_ts AS TIMESTAMPTZ) IS NULL),
dq_check('order_future_timestamp','Timeliness','Critical',try_cast(order_ts AS TIMESTAMPTZ) > (SELECT checked_at FROM pipeline_context)),
dq_check('order_quantity','Accuracy','Critical',NOT coalesce(isfinite(try_cast(quantity AS DOUBLE)) AND try_cast(quantity AS DOUBLE)>0 AND try_cast(quantity AS DOUBLE)%1=0,false)),
dq_check('order_unit_price','Accuracy','Critical',NOT coalesce(isfinite(try_cast(unit_price AS DOUBLE)) AND try_cast(unit_price AS DOUBLE)>=0,false)),
dq_check('order_discount','Validity','Critical',NOT coalesce(isfinite(try_cast(discount_pct AS DOUBLE)) AND try_cast(discount_pct AS DOUBLE) BETWEEN 0 AND 100,false)),
dq_check('order_amount','Accuracy','Critical',NOT coalesce(try_cast(order_amount AS DECIMAL(18,2)) = round(try_cast(try_cast(quantity AS DOUBLE)*try_cast(unit_price AS DOUBLE)*(100-try_cast(discount_pct AS DOUBLE))/100 AS DECIMAL(30,8)),2)::DECIMAL(18,2),false)),
dq_check('order_delivery_required','Completeness','Critical',upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE) IS NULL),
dq_check('order_delivery_limit','Accuracy','Critical',try_cast(delivery_minutes AS DOUBLE) IS NOT NULL AND (NOT isfinite(try_cast(delivery_minutes AS DOUBLE)) OR try_cast(delivery_minutes AS DOUBLE)<0 OR try_cast(delivery_minutes AS DOUBLE)>=120)),
dq_check('order_customer_key','Consistency','Critical',NOT EXISTS (SELECT 1 FROM stg_customers m WHERE m.customer_id=upper(trim(source.customer_id)))),
dq_check('order_customer_quality','Consistency','Critical',EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=source._run_id AND q.source_file='customers.csv' AND upper(trim(q.row_key))=upper(trim(source.customer_id)) AND q.failed AND q.severity='Critical')),
dq_check('order_customer_warning','Consistency','Warning',EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=source._run_id AND q.source_file='customers.csv' AND upper(trim(q.row_key))=upper(trim(source.customer_id)) AND q.failed AND q.severity='Warning')),
dq_check('order_product_key','Consistency','Critical',NOT EXISTS (SELECT 1 FROM stg_products m WHERE m.product_id=upper(trim(source.product_id)))),
dq_check('order_product_quality','Consistency','Critical',EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=source._run_id AND q.source_file='products.csv' AND upper(trim(q.row_key))=upper(trim(source.product_id)) AND q.failed AND q.severity='Critical')),
dq_check('order_product_warning','Consistency','Warning',EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=source._run_id AND q.source_file='products.csv' AND upper(trim(q.row_key))=upper(trim(source.product_id)) AND q.failed AND q.severity='Warning')),
dq_check('order_store_key','Consistency','Critical',NOT EXISTS (SELECT 1 FROM stg_stores m WHERE m.store_id=upper(trim(source.store_id)))),
dq_check('order_store_quality','Consistency','Critical',EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=source._run_id AND q.source_file='stores.csv' AND upper(trim(q.row_key))=upper(trim(source.store_id)) AND q.failed AND q.severity='Critical')),
dq_check('order_store_warning','Consistency','Warning',EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=source._run_id AND q.source_file='stores.csv' AND upper(trim(q.row_key))=upper(trim(source.store_id)) AND q.failed AND q.severity='Warning')),
dq_check('order_payment_quality','Consistency','Critical',EXISTS (SELECT 1 FROM stg_payments p WHERE p._run_id=source._run_id AND p.order_id=upper(trim(source.order_id)) AND p.dq_status='Critical')),
dq_check('order_payment_amount','Consistency','Critical',EXISTS (SELECT 1 FROM raw_payments p WHERE p._run_id=source._run_id AND upper(trim(p.order_id))=upper(trim(source.order_id)) AND upper(trim(p.payment_status))='SUCCESS' AND NOT coalesce(try_cast(p.amount_paid AS DECIMAL(18,2))=try_cast(source.order_amount AS DECIMAL(18,2)),false)))] AS checks FROM
(SELECT rowid AS source_row,row_number() OVER (ORDER BY rowid) AS source_csv_row,* FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context) AND _source_file=CASE WHEN (SELECT phase FROM pipeline_context)='base' THEN 'orders.csv' ELSE 'orders_2026-10-01_increment.csv' END) source;
INSERT INTO elt_quality_rows
SELECT _run_id,(SELECT phase FROM pipeline_context),_source_file,source_row,order_id,
x.check_name,x.dimension,x.severity,x.failed,(SELECT checked_at FROM pipeline_context)
FROM orders_checks,unnest(checks) t(x);
CREATE TEMP TABLE incoming_orders AS
WITH flagged AS (SELECT *,list_transform(list_filter(checks,x->x.failed AND x.severity='Critical'),x->x.check_name) AS critical,
list_transform(list_filter(checks,x->x.failed AND x.severity='Warning'),x->x.check_name) AS warnings FROM orders_checks)
SELECT upper(trim(order_id)) AS order_id,timezone('Asia/Kolkata',try_cast(order_ts AS TIMESTAMPTZ)) AS order_ts,
cast(timezone('Asia/Kolkata',try_cast(order_ts AS TIMESTAMPTZ)) AS DATE) AS order_date,
upper(trim(customer_id)) AS customer_id,upper(trim(product_id)) AS product_id,upper(trim(store_id)) AS store_id,
try_cast(quantity AS BIGINT) AS quantity,try_cast(unit_price AS DECIMAL(18,2)) AS unit_price,try_cast(discount_pct AS DOUBLE) AS discount_pct,
try_cast(order_amount AS DECIMAL(18,2)) AS order_amount,upper(trim(payment_mode)) AS payment_mode,upper(trim(order_status)) AS order_status,
title_case(channel) AS channel,try_cast(delivery_minutes AS DOUBLE) AS delivery_minutes,
CASE WHEN upper(trim(order_status))='DELIVERED' THEN round(try_cast(try_cast(quantity AS DOUBLE)*try_cast(unit_price AS DOUBLE)*(100-try_cast(discount_pct AS DOUBLE))/100 AS DECIMAL(30,8)),2)::DECIMAL(18,2) ELSE 0::DECIMAL(18,2) END AS net_revenue,
upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE)<=15 AS within_15_min,
CASE WHEN len(critical)>0 THEN 'Critical' WHEN len(warnings)>0 THEN 'Warning' ELSE 'Passed' END AS dq_status,
array_to_string(critical,'; ') AS failure_reason,array_to_string(warnings,'; ') AS warning_reason,
source_csv_row AS _source_row,_source_file,_loaded_at,_run_id
FROM flagged QUALIFY row_number() OVER (PARTITION BY upper(trim(order_id)) ORDER BY source_row)=1;

CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM incoming_orders WHERE false;
DELETE FROM stg_orders WHERE (SELECT phase FROM pipeline_context)='base';
MERGE INTO stg_orders t USING incoming_orders s ON t.order_id=s.order_id
WHEN MATCHED THEN UPDATE BY NAME WHEN NOT MATCHED THEN INSERT BY NAME;
