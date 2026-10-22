# Part G - Recommendation to Sameer, CTO

Dear Sameer,

I recommend **ELT with validated, reconciled finance marts** for NammaMart. Both corrected implementations now produce identical results: INR 882,163.60 before the increment and INR 878,583.90 after it. The choice is based on auditability and ease of change, rather than a difference in correctness. [Verification](ELT_VERIFICATION.md)

**Auditability.** ELT preserves every original field in raw tables, with file, row, run and load metadata. SQL can trace a reported number to the exact source and rejection reason. ETL now also retains byte-exact per-run archives and source references, but an investigation requires opening those files as well as querying facts. Both retain finance snapshots and run-audit history.

**Data quality.** Both reject all five injected invalid orders, fail loudly on a missing file or renamed required column, and warn on volume, freshness and store-price distribution. Finance receives only delivered, Critical-check-passing, reconciled orders. Delivery SLA remains independent of payment collection. Recovery preserved four successful extract tasks and reran failed/downstream tasks. These results demonstrate the tested contract, not a guarantee against every future defect. [Twenty experiments](OBSERVABILITY_TESTS.md)

**Ease of change.** Both insert 150 new order IDs and apply ten corrections without duplicate current IDs. Returns remove previously reported revenue; invalid corrections cannot leave stale facts. ELT's separate raw/staging/mart SQL files make reprocessing and rule review straightforward. Keep versioned rules and published snapshots, and agree return-versus-refund treatment with Finance.

**Performance at 1,000 times volume.** This has not been load-tested. Measured task/run durations are in the experiment report. At roughly 2.4 million baseline orders, ETL DataFrames and Airflow payloads will impose memory/transfer pressure. ELT avoids transformed order payloads between tasks, but its Python raw loader still materialises CSV rows. Use bulk ingestion, incremental staging/marts and a representative concurrency benchmark before promising the 09:30 deadline.

**Cost.** The assignment pipelines use open-source Airflow, pandas and DuckDB, with no paid AI dependency. ELT's repeated raw snapshots cost storage and replay compute; ETL archives also need protected storage. Software licence cost is not total operating cost: include infrastructure, retention and engineering support. No production cost benchmark was performed.

**Top three risks:**

1. Raw personal-data exposure: restrict raw/archive access, enforce approved retention and audit access. Masked reports do not protect raw storage.
2. Finance interpretation: current restatements and historical snapshots answer different questions; approve recognition and refund policy before financial publication.
3. Operational scale: the 1,000-times workload and remote failure-notification delivery remain unverified. Keep an always-on scheduler, benchmark the deadline and tune distribution thresholds with source owners.
