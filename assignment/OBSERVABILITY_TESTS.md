# Part F -> measured Airflow experiments

Twenty experiments ran in the Astro scheduler against isolated CSV copies and warehouses. Original hashes and restored copies were verified. Missing-file/schema experiments are expected failures; Recovery uses Airflow Clear. External notifications were disabled.

Audit rows_extracted counts loaded/extracted source records; rows_loaded now means finance-eligible current-run rows in BOTH pipelines; rows_quarantined counts distinct Critical source rows. One run_id/dag_id row is updated on retry/Clear.

Baseline/re-run use only the five September exports. Experiment 3 adds the 160-row increment. Each subsequent mutation starts from a baseline copy. Source data are never mutated.

## 1. Happy path

**Expected:** Five extracts run in parallel; valid finance rows publish and rejected rows retain reasons.

**ETL -> `part_f_20261021_162218_01_etl`**

Observed: success in 18.23s; five extracts overlap=True. Extracted=5212, finance=1635, quarantined=576; revenue=882163.60 INR.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_01_elt`**

Observed: success in 18.42s; five extracts overlap=True. Extracted=5212, finance=1635, quarantined=576; revenue=882163.60 INR.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Keep an always-on scheduler for the deadline; scale testing remains separate.

## 2. Re-run (idempotency)

**Expected:** Current order and finance counts remain unchanged; a distinct ELT run appends raw history.

**ETL -> `part_f_20261021_162218_02_etl`**

Observed: Orders [2400, 2400] -> [2400, 2400]; finance=1635; raw orders ETL archive -> new run archive. Revenue=882163.60 INR.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_02_elt`**

Observed: Orders [2400, 2400] -> [2400, 2400]; finance=1635; raw orders 2412 -> 4824. Revenue=882163.60 INR.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Retain governed raw/archive history; monitor storage growth.

## 3. Incremental load

**Expected:** 150 source IDs are inserted; ten old statuses change without duplicates; finance applies quality and payment gates.

**ETL -> `part_f_20261021_162218_03_etl`**

Observed: Inserted/matched/status-changed=[150, 10, 10]; orders/distinct IDs=[2550, 2550]; finance=1628; corrected revenue=878583.90 INR.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_03_elt`**

Observed: Inserted/matched/status-changed=[150, 10, 10]; orders/distinct IDs=[2550, 2550]; finance=1628; corrected revenue=878583.90 INR.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Preserve finance history and agree return-versus-refund accounting with Finance.

## 4. Missing source file

**Expected:** Missing products.csv retries twice, then the run and audit fail; a named source/schema result is stored.

**ETL -> `part_f_20261021_162218_04_etl`**

Observed: failed: extract_products (3 attempts); up_for_retry observed=True; audit=failed, loaded=0; schema.products.csv: Failed. Failure/recovery versions remain in attempt logs.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_04_elt`**

Observed: failed: extract_products (3 attempts); up_for_retry observed=True; audit=failed, loaded=0; schema.products.csv: Failed. Failure/recovery versions remain in attempt logs.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Optional external failure callback is implemented; test runs use console alerts only.

## 5. New bad data

**Expected:** All five injected errors are retained in quarantine with named rules; none enters finance.

**ETL -> `part_f_20261021_162218_05_etl`**

Observed: success; 7 injected-rule evidence rows; 29 failing Critical checks. Finance remains 1635 rows / 882163.60 INR; all five injected IDs are excluded from finance.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_05_elt`**

Observed: success; 7 injected-rule evidence rows; 29 failing Critical checks. Finance remains 1635 rows / 882163.60 INR; all five injected IDs are excluded from finance.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Maintain regression tests for the five client cases in both engines.

## 6. Schema change

**Expected:** Renamed order_amount fails loudly and produces schema.orders.csv in dq_results.

**ETL -> `part_f_20261021_162218_06_etl`**

Observed: failed: extract_orders (3 attempts); up_for_retry observed=True; audit=failed, loaded=0; schema.orders.csv: Failed. Failure/recovery versions remain in attempt logs.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_06_elt`**

Observed: failed: extract_orders (3 attempts); up_for_retry observed=True; audit=failed, loaded=0; schema.orders.csv: Failed. Failure/recovery versions remain in attempt logs.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Keep the explicit source schema contract and named failure scorecard.

## 7. Volume anomaly

**Expected:** 50 orders produce a >90% volume-drop warning, with a consistent current snapshot.

**ETL -> `part_f_20261021_162218_07_etl`**

Observed: order_volume=Warning; orders=[50, 50]; finance=37; revenue=20297.50 INR. Historical facts are not released by replacing reconciliation.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_07_elt`**

Observed: order_volume=Warning; orders=[50, 50]; finance=37; revenue=20297.50 INR. Historical facts are not released by replacing reconciliation.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Use a business-approved first-run volume baseline in production.

## 8. Distribution drift

**Expected:** 100x prices trigger formula and store-price-distribution checks; record the revenue change.

**ETL -> `part_f_20261021_162218_08_etl`**

Observed: order_price_distribution=Warning; revenue 882163.60 -> 813626.65 INR (-68536.95; -7.77%).

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_08_elt`**

Observed: order_price_distribution=Warning; revenue 882163.60 -> 813626.65 INR (-68536.95; -7.77%).

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: The threefold median-price threshold is a documented prototype policy; tune it with Merchandising.

## 9. Freshness

**Expected:** Latest valid timestamp 30 days old produces a freshness warning.

**ETL -> `part_f_20261021_162218_09_etl`**

Observed: order_freshness=Warning; run=success; warning does not itself reject rows.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_09_elt`**

Observed: order_freshness=Warning; run=success; warning does not itself reject rows.

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Freshness uses valid nonfuture records; treat the September teaching files as historical.

## 10. Recovery

**Expected:** Clear the failed task and downstream tasks after repair; retain successful extracts and update the same audit row.

**ETL -> `part_f_20261021_162218_04_etl`**

Observed: Recovery=success; four successful extracts unchanged=True; audit=success. Used Airflow clear_task_instances (same operation as UI Clear).

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

**ELT -> `part_f_20261021_162218_04_elt`**

Observed: Recovery=success; four successful extracts unchanged=True; audit=success. Used Airflow clear_task_instances (same operation as UI Clear).

Where: Airflow Grid -> recorded run -> task attempts/logs; pipeline_runs and dq_results in that experiment database. Full structured observations and log lines are in [OBSERVABILITY_RESULTS.json](OBSERVABILITY_RESULTS.json).

What I would change: Retain task-attempt logs; pipeline_runs represents the final recovered state.

## Five pillars

| Pillar | Implemented | Remaining limitation |
| --- | --- | --- |
| Freshness | Valid, nonfuture source timestamps; 24-hour warning | Historical class data will legitimately warn |
| Volume | Prior successful batch; >90% drop warning | First run has no historical baseline |
| Schema | Explicit header/field checks; named audit scorecard | Business schema evolution still needs approval |
| Distribution | Store median-price ratio warning; formula checks | One statistic; tune thresholds and add percentiles at scale |
| Lineage | Raw metadata, ETL byte archives, source row/run references, finance history | Archive retention remains a governed operational policy |

Best covered: lineage. Weakest: distribution breadth, although the dedicated median-price check now catches coordinated changes too.

Reproduce inside the scheduler: `python /usr/local/airflow/include/observability_experiments.py`, then `observability_collect.py`. On the host: `python -m include.observability_submission`. DAGs remain unpaused afterward.
