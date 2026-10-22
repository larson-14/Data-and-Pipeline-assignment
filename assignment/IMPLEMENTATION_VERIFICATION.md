# Implementation verification - 22 October 2026

Completed the review fixes and verified both pipelines in the running Astro/Airflow environment.

- Runtime: **45 tests passed, 8 subtests passed** (`python -m pytest tests -q -p no:cacheprovider`). Local targeted regressions: **36 passed**.
- Airflow import-error listing: **[]**.
- Fresh runs: `submission_20261022_035604_etl` and `submission_20261022_035604_elt`, both successful, including final audit and requested warehouse comparison.
- Both normal scheduled runs at 03:30 UTC on 22 October also succeeded. Both DAGs are unpaused; next start is **23 October 2026, 09:00 IST / 03:30 UTC**.
- All six business-query results match exactly. Per-order September comparison has **zero differences** in revenue and margin.
- Twenty recorded experiments: successful normal, repeat, increment, injected-data, volume, drift, freshness and recovery scenarios; expected failures for missing-file and schema-change scenarios. Original/restored-copy hashes checked. Expected historical failures remain visible in the UI.
- Trainer `meridian_pipeline.py` is unchanged (Git checkout CRLF differs from repository LF); all six supplied CSVs are byte-identical to the repository originals.

## Changes and tutor alignment

Both engines consistently enforce status, delivery, schema, amount, key and master-data rules; list reconciliation exceptions; apply valid and invalid corrections; exclude unreconciled/non-delivered rows from finance; and preserve source evidence and finance history. Delivery SLA is independent of payment quality. Retry-safe audits and source-specific quarantine retain reasons, including extraction/schema failures. Volume, freshness and median-price distribution checks are recorded.

The assignment follows the tutor's TaskFlow structure: parallel extracts, Python validation and pass/fail handling for ETL, helpers under include/, pandas transformations, DuckDB loading and explicit task dependencies. ELT adds raw loading and SQL staging/marts as required. Trainer code is unchanged. Unnecessary ML and paid AI tasks were removed from the assignment DAG.

## Finance results

| Snapshot | Finance orders, both | Revenue INR, both | Gross margin INR, both |
| --- | --- | --- | --- |
| September base | 1,635 | 882,163.60 | 217,377.92 |
| With October corrections | 1,628 | 878,583.90 | 216,122.30 |

All 150 new IDs and ten corrections are retained in current order state; finance publication also requires payment/quality checks. Seven previously finance-eligible returned orders remove INR 3,579.70. [Exact comparison](ELT_VERIFICATION.md), [business answers](BUSINESS_ANSWERS.md), [experiments](OBSERVABILITY_TESTS.md).

## Running locally

Airflow is at **http://localhost:16492** (Docker maps this host port to container port 8080). Keep Docker and Airflow services running for the schedule. Both `nammamart_*_sudatta` DAGs include the increment by default. For explicit comparison, run ETL first and trigger ELT with `compare_etl=true` and the matching `etl_warehouse_db` path. Comparison is optional for simultaneous scheduled runs.

No additional package installation was needed in the tested runtime. requirements.txt lists direct dependencies and retains trainer dependencies. Rebuild/restart through Astro when setting up a fresh environment or changing dependencies.

## Verification limits

The 1,000-times workload was not benchmarked. External failure notifications are implemented but were not sent; controlled tests use console alerts. September teaching data legitimately triggers freshness warnings. Filesystem access/retention are prototype controls; production proposals are documented separately. The before-09:00 versus start-at-09:00 contradiction is explained in [Scheduling](SCHEDULING.md).
