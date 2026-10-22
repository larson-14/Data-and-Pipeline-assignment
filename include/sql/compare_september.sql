-- Compare like-for-like September snapshots after the configured increment.
-- etl_reference is attached read-only by the comparison task.
CREATE OR REPLACE TABLE elt_september_comparison AS
WITH etl_orders AS (
    SELECT o.order_id, o.order_status, o.net_revenue,
           o.gross_margin, p.category, s.zone
    FROM etl_reference.etl_fact_orders o
    JOIN etl_reference.etl_dim_product p USING (product_id)
    JOIN etl_reference.etl_dim_store s USING (store_id)
    WHERE o.order_date >= DATE '2026-09-01' AND o.order_date < DATE '2026-10-01'
      AND NOT EXISTS (SELECT 1 FROM etl_reference.etl_payment_reconciliation r
                      WHERE r.order_id = o.order_id)
), elt_orders AS (
    SELECT order_id, order_status, net_revenue, gross_margin, category, zone
    FROM elt_reconciled_orders
    WHERE order_date >= DATE '2026-09-01' AND order_date < DATE '2026-10-01'
)
SELECT coalesce(e.order_id, l.order_id) AS order_id, e.order_status AS etl_status,
       l.order_status AS elt_status, coalesce(e.net_revenue, 0) AS etl_net_revenue,
       coalesce(l.net_revenue, 0) AS elt_net_revenue,
       coalesce(l.net_revenue, 0) - coalesce(e.net_revenue, 0) AS revenue_difference,
       coalesce(e.gross_margin, 0) AS etl_gross_margin,
       coalesce(l.gross_margin, 0) AS elt_gross_margin,
       coalesce(e.category, l.category) AS category, coalesce(e.zone, l.zone) AS zone,
       CASE WHEN e.order_id IS NULL THEN 'ELT only: review ETL validation or warehouse freshness'
            WHEN l.order_id IS NULL AND e.order_status <> 'DELIVERED' THEN 'ETL report includes a non-delivered order'
            WHEN l.order_id IS NULL THEN coalesce(nullif(s.failure_reason, ''), 'ELT master or reconciliation exclusion')
            ELSE 'Different transformed revenue or margin' END AS reason
FROM etl_orders e FULL OUTER JOIN elt_orders l USING (order_id)
LEFT JOIN stg_orders s ON s.order_id = coalesce(e.order_id, l.order_id)
WHERE e.order_id IS NULL OR l.order_id IS NULL
   OR abs(e.net_revenue - l.net_revenue) > 0.000001
   OR abs(e.gross_margin - l.gross_margin) > 0.000001;
