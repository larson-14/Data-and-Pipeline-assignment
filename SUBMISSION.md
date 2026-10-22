# NammaMart - Sudatta submission

Current local Airflow UI: http://localhost:16492 (container port 8080).

Both TaskFlow DAGs implement the assignment and pass the recorded checks. Start with [implementation verification](assignment/IMPLEMENTATION_VERIFICATION.md). The trainer DAG and six supplied CSVs are unchanged.

## Submission contents

| Part | Evidence |
| --- | --- |
| 0: Discovery | [Profile](assignment/DATA_PROFILE.md), [287-entry issues log](assignment/DATA_ISSUES_LOG.csv) covering 262 source rows |
| A: ETL | [TaskFlow DAG](dags/nammamart_etl_sudatta.py), [Python transforms](include/etl_pipeline.py), [validation](include/validation_checks.py) |
| B: ELT | [TaskFlow DAG](dags/nammamart_elt_sudatta.py), [SQL layers](include/sql/), [exact parity](assignment/ELT_VERIFICATION.md) |
| C: Schedule | [IST schedule and SLA explanation](assignment/SCHEDULING.md) |
| D/E: Quality and governance | [Answers](assignment/DATA_QUALITY_AND_GOVERNANCE.md), [dictionary and worked evidence](assignment/GOVERNANCE_APPENDIX.md) |
| F: Observability | [Twenty experiments](assignment/OBSERVABILITY_TESTS.md), [captured results](assignment/OBSERVABILITY_RESULTS.json), [task durations](assignment/observability_task_durations.csv) |
| G: Recommendation | [Recommendation to Sameer](assignment/CLIENT_RECOMMENDATION.md) |
| Six business questions | [SQL and actual results for both pipelines](assignment/BUSINESS_ANSWERS.md) |

## Discovery questions for the client

1. Should October returns restate September revenue, and when does a refund become an accounting adjustment?
2. Which delivery, contact and catalogue anomalies should quarantine a row versus warn?
3. How should conflicting master versions and missing or late payment exports be resolved?

The increment has 150 new IDs and ten legitimate corrections, with no discovery defects. Store completeness/capacity checks are enforced. Discovery source_row includes the header; pipeline source_row counts the first data record as 1.

## Screenshots

| Evidence | ETL | ELT |
| --- | --- | --- |
| Successful Graph | [Graph](assignment/screenshots/etl_graph.png) | [Graph](assignment/screenshots/elt_graph.png) |
| Schedule and next run | [Schedule](assignment/screenshots/etl_schedule.png) | [Schedule](assignment/screenshots/elt_schedule.png) |
| Quality scorecard | [dq_results](assignment/screenshots/etl_dq_results.png) | [dq_results](assignment/screenshots/elt_dq_results.png) |
| Run audit | [pipeline_runs](assignment/screenshots/etl_pipeline_runs.png) | [pipeline_runs](assignment/screenshots/elt_pipeline_runs.png) |
| Reconciliation | [Payment exceptions](assignment/screenshots/etl_etl_payment_reconciliation.png) | [Payment exceptions](assignment/screenshots/elt_mart_payment_reconciliation.png) |

[Sanitized quarantine screenshot](assignment/screenshots/quarantine.png) and [CSV](assignment/evidence/quarantine_example.csv). Graph/schedule images are from real Airflow. Query/quarantine images render actual captured DuckDB/CSV results in labelled HTML pages.

## Package

[submission_bundle.zip](submission_bundle.zip) contains source, documentation and evidence without Git history or operational private files. [Scoped scan](assignment/SECURITY_SCAN.md) and [manifest](assignment/PACKAGE_MANIFEST.json) document the package. Source exports stay under include/data/; derived public evidence omits names/contacts. IDs remain pseudonymous. No remote push or Git history rewrite was performed.

The assignment has a timing contradiction: a 09:00 start cannot guarantee data ready before 09:00. The implementation follows Part C: start at 09:00 IST Monday-Saturday, targeting the 09:30 review. Keep Docker and the scheduler running. Production scale and external notification delivery are not established by these local tests.
