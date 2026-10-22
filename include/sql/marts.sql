-- Reconcile every order and payment, including rejected orders and orphan payments.
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH totals AS (
 SELECT order_id,count(*) payment_count,count(*) FILTER(WHERE payment_status='SUCCESS') successful_count,
 sum(amount_paid) FILTER(WHERE payment_status='SUCCESS') amount_paid,
 bool_or(dq_status='Critical') quality_failure,bool_or(contains(failure_reason,'payment_duplicate')) duplicate_payment
 FROM stg_payments GROUP BY order_id
), comparisons AS (
 SELECT coalesce(o.order_id,p.order_id) order_id,o.order_status,o.payment_mode,
 CASE WHEN o.order_id IS NULL OR o.order_status='CANCELLED' THEN 0::DECIMAL(18,2) ELSE o.order_amount END order_amount,p.amount_paid,
 coalesce(CASE WHEN o.order_id IS NULL OR o.order_status='CANCELLED' THEN 0 ELSE o.order_amount END,0)-coalesce(p.amount_paid,0) amount_difference,
 concat_ws('; ',
 CASE WHEN o.order_id IS NULL THEN 'orphan payment' END,
 CASE WHEN o.order_status='DELIVERED' AND p.payment_count IS NULL THEN 'missing payment' END,
 CASE WHEN o.order_status='DELIVERED' AND p.payment_count>0 AND p.successful_count=0 THEN 'no successful payment' END,
 CASE WHEN o.order_id IS NOT NULL AND p.successful_count>0 AND
  (o.order_amount IS NULL OR p.amount_paid IS NULL OR p.amount_paid<>CASE WHEN o.order_status='CANCELLED' THEN 0 ELSE o.order_amount END) THEN 'payment amount mismatch' END,
 CASE WHEN p.duplicate_payment THEN 'duplicate payment_id' END,
 CASE WHEN p.quality_failure THEN 'payment quality failure' END) reason
 FROM stg_orders o FULL JOIN totals p USING(order_id)
)
SELECT * FROM comparisons WHERE reason<>'';

-- Delivery operations are independent of payment collection problems.
CREATE OR REPLACE VIEW elt_operational_orders AS
SELECT o.*,p.category,p.cost_price,c.full_name,c.loyalty_tier,s.store_name,s.zone,
 CASE WHEN o.order_status='DELIVERED' THEN (o.net_revenue-o.quantity*p.cost_price)::DECIMAL(18,2) ELSE 0::DECIMAL(18,2) END gross_margin
FROM stg_orders o JOIN stg_customers c USING(customer_id) JOIN stg_products p USING(product_id) JOIN stg_stores s USING(store_id)
WHERE NOT EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=o._run_id AND q.source_file=o._source_file
 AND upper(trim(q.row_key))=o.order_id AND q.failed AND q.severity='Critical' AND q.check_name NOT IN ('order_payment_quality','order_payment_amount'))
 AND c.dq_status<>'Critical' AND p.dq_status<>'Critical' AND s.dq_status<>'Critical';
CREATE OR REPLACE VIEW elt_reconciled_orders AS
SELECT * FROM elt_operational_orders o WHERE o.dq_status<>'Critical' AND o.order_status='DELIVERED'
 AND NOT EXISTS (SELECT 1 FROM mart_payment_reconciliation r WHERE r.order_id=o.order_id);
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT order_date,zone,store_id,store_name,payment_mode,count(*) order_count,sum(quantity) units_sold,sum(net_revenue) net_revenue,sum(gross_margin) gross_margin
FROM elt_reconciled_orders GROUP BY ALL;
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT order_date,category,count(*) order_count,sum(net_revenue) net_revenue,sum(gross_margin) gross_margin FROM elt_reconciled_orders GROUP BY ALL;
CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT order_date,zone,store_id,store_name,count(*) delivered_orders,avg(delivery_minutes) average_delivery_minutes,
100.0*count(*) FILTER(WHERE within_15_min)/count(*) within_15_min_pct
FROM elt_operational_orders WHERE order_status='DELIVERED' GROUP BY ALL;
CREATE OR REPLACE TABLE mart_top_customers AS
SELECT customer_id,loyalty_tier,count(*) order_count,sum(net_revenue) total_spend,
row_number() OVER(ORDER BY sum(net_revenue) DESC,customer_id) spend_rank FROM elt_reconciled_orders
GROUP BY customer_id,loyalty_tier QUALIFY spend_rank<=10;
-- One occurrence per source row/reason/run, across every run on the IST date.
CREATE OR REPLACE VIEW elt_rejected_rows AS
SELECT run_id,source_file,check_name failure_reason,cast(timezone('Asia/Kolkata',checked_at) AS DATE) checked_date,
count(DISTINCT source_row) rejected_rows FROM elt_quality_rows WHERE failed AND severity='Critical' GROUP BY ALL;
CREATE TABLE IF NOT EXISTS elt_finance_history AS SELECT ''::VARCHAR snapshot_run_id,* FROM elt_reconciled_orders WHERE false;
DELETE FROM elt_finance_history WHERE snapshot_run_id=(SELECT run_id||':'||phase FROM pipeline_context);
INSERT INTO elt_finance_history SELECT (SELECT run_id||':'||phase FROM pipeline_context),* FROM elt_reconciled_orders;
