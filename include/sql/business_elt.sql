-- Q1: Daily delivered, reconciled revenue by zone and store (separate grains).
SELECT CASE WHEN grouping(store_id)=1 THEN 'ZONE' ELSE 'STORE' END grain,order_date,zone,store_id,store_name,count(*) orders,sum(net_revenue) net_revenue
FROM elt_reconciled_orders GROUP BY GROUPING SETS((order_date,zone),(order_date,zone,store_id,store_name)) ORDER BY ALL;
-- Q2: Category revenue and gross margin.
SELECT category,count(*) orders,sum(net_revenue) net_revenue,sum(gross_margin) gross_margin FROM elt_reconciled_orders GROUP BY category ORDER BY net_revenue DESC;
-- Q3: All operationally valid delivered orders, independent of payment collection.
SELECT store_id,store_name,count(*) delivered_orders,round(avg(delivery_minutes),2) average_delivery_minutes,
round(100.0*count(*) FILTER(WHERE within_15_min)/count(*),2) within_15_min_pct
FROM elt_operational_orders WHERE order_status='DELIVERED' GROUP BY store_id,store_name ORDER BY store_id;
-- Q4: Finance revenue and every reconciliation exception, including rejected/orphan records.
WITH r AS (SELECT payment_mode,sum(net_revenue) revenue FROM elt_reconciled_orders GROUP BY payment_mode),
m AS (SELECT coalesce(payment_mode,'UNKNOWN') payment_mode,sum(abs(amount_difference)) mismatch,count(*) mismatch_orders FROM mart_payment_reconciliation GROUP BY 1),
x AS (SELECT coalesce(r.payment_mode,m.payment_mode) payment_mode,coalesce(revenue,0) revenue,coalesce(mismatch,0) mismatch,coalesce(mismatch_orders,0) mismatch_orders FROM r FULL JOIN m USING(payment_mode))
SELECT coalesce(payment_mode,'ALL') payment_mode,sum(revenue) net_revenue,sum(mismatch) absolute_unreconciled_amount,sum(mismatch_orders) mismatch_orders FROM x GROUP BY GROUPING SETS((payment_mode),()) ORDER BY payment_mode;
-- Q5: Top ten customers by reconciled spend, with loyalty tier and no contacts.
SELECT customer_id,loyalty_tier,count(*) orders,sum(net_revenue) total_spend FROM elt_reconciled_orders GROUP BY customer_id,loyalty_tier ORDER BY total_spend DESC,customer_id LIMIT 10;
-- Q6: Rejection occurrences across all runs today in IST, grouped by source and reason.
SELECT source_file,failure_reason,sum(rejected_rows) rejected_rows FROM elt_rejected_rows WHERE checked_date=cast(timezone('Asia/Kolkata',current_timestamp) AS DATE) GROUP BY source_file,failure_reason ORDER BY ALL;
