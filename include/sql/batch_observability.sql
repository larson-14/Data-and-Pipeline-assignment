-- ── BATCH WARNINGS: volume and freshness do not quarantine rows ────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ,
    rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR,
    PRIMARY KEY (run_id, dag_id)
);
CREATE TABLE IF NOT EXISTS pipeline_batches (
    run_id VARCHAR, dag_id VARCHAR, order_count BIGINT, checked_at TIMESTAMPTZ,
    PRIMARY KEY (run_id, dag_id)
);
DELETE FROM dq_results WHERE run_id=(SELECT run_id FROM pipeline_context)
    AND dag_id=(SELECT dag_id FROM pipeline_context)
    AND check_name IN ('order_volume', 'order_freshness');
INSERT INTO dq_results
WITH current_batch AS (
    SELECT count(*) AS rows, max(try_cast(order_ts AS TIMESTAMPTZ)) FILTER (WHERE try_cast(order_ts AS TIMESTAMPTZ)<=(SELECT checked_at FROM pipeline_context) AND NOT EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=raw_orders._run_id AND q.source_file=raw_orders._source_file AND q.source_row=raw_orders.rowid AND q.failed AND q.severity='Critical' AND q.check_name NOT IN ('order_payment_quality','order_payment_amount'))) AS latest
    FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context) AND _source_file='orders.csv'
), previous_batch AS (
    SELECT b.order_count FROM pipeline_batches b JOIN pipeline_runs r USING (run_id, dag_id)
    WHERE b.dag_id=(SELECT dag_id FROM pipeline_context)
      AND b.run_id<>(SELECT run_id FROM pipeline_context) AND r.status='success'
    ORDER BY r.end_time DESC LIMIT 1
), checks AS (
    SELECT 'order_volume' AS name, 'Completeness' AS dimension,
           coalesce(rows < (SELECT order_count FROM previous_batch)*0.1, false) AS failed
    FROM current_batch
    UNION ALL
    SELECT 'order_freshness', 'Timeliness', latest IS NULL OR latest < c.checked_at-INTERVAL '1 day'
    FROM current_batch CROSS JOIN pipeline_context c
)
SELECT c.run_id, c.dag_id, checks.name, checks.dimension, 'Warning', 1,
       failed::BIGINT, CASE WHEN failed THEN 'Warning' ELSE 'Passed' END, c.checked_at
FROM checks CROSS JOIN pipeline_context c;
INSERT OR REPLACE INTO pipeline_batches
SELECT c.run_id, c.dag_id, count(r.order_id), c.checked_at
FROM pipeline_context c LEFT JOIN raw_orders r ON r._run_id=c.run_id AND r._source_file='orders.csv'
GROUP BY c.run_id, c.dag_id, c.checked_at;

-- Store-level median price drift: detect coordinated price/amount changes too.
CREATE TABLE IF NOT EXISTS pipeline_store_prices(run_id VARCHAR,dag_id VARCHAR,store_id VARCHAR,median_price DOUBLE,PRIMARY KEY(run_id,dag_id,store_id));
DELETE FROM dq_results WHERE run_id=(SELECT run_id FROM pipeline_context) AND check_name='order_price_distribution';
CREATE TEMP TABLE current_prices AS SELECT upper(trim(store_id)) store_id,median(try_cast(unit_price AS DOUBLE)) median_price
FROM raw_orders WHERE _run_id=(SELECT run_id FROM pipeline_context) AND _source_file='orders.csv'
AND isfinite(try_cast(unit_price AS DOUBLE)) AND try_cast(unit_price AS DOUBLE)>=0 GROUP BY 1;
INSERT INTO dq_results
WITH comparison AS (SELECT p.store_id,p.median_price,old.median_price previous_price FROM current_prices p LEFT JOIN pipeline_store_prices old
ON old.store_id=p.store_id AND old.run_id=(SELECT run_id FROM pipeline_runs WHERE dag_id=(SELECT dag_id FROM pipeline_context)
AND run_id<>(SELECT run_id FROM pipeline_context) AND status='success' ORDER BY end_time DESC LIMIT 1)),
counts AS (SELECT count(*) checked,count(*) FILTER(WHERE previous_price>0 AND (median_price>previous_price*3 OR median_price<previous_price/3)) failed FROM comparison)
SELECT c.run_id,c.dag_id,'order_price_distribution','Accuracy','Warning',checked,failed,CASE WHEN failed>0 THEN 'Warning' ELSE 'Passed' END,c.checked_at
FROM counts CROSS JOIN pipeline_context c;
DELETE FROM pipeline_store_prices WHERE run_id=(SELECT run_id FROM pipeline_context);
INSERT INTO pipeline_store_prices SELECT c.run_id,c.dag_id,p.store_id,p.median_price FROM current_prices p CROSS JOIN pipeline_context c;
