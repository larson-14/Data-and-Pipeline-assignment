"""
dags/nammamart_elt_sudatta.py
--------------------------
Nammamart ELT Pipeline — raw, staging and mart layers.

Flow: Extract → Load raw → SQL staging → Load increment → SQL upsert → SQL marts.
Raw rows are retained unchanged; Critical staging rows stay out of the marts.
"""

import os
import sys
from datetime import timedelta
from pathlib import Path

from airflow.sdk import dag, task
import duckdb
import pendulum

INCLUDE_DIR = Path(__file__).resolve().parents[1] / "include"
sys.path.insert(0, str(INCLUDE_DIR))
from run_audit import audit_run, runtime_path
from failure_alert import slack_failure_alert

DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
SQL_DIR = INCLUDE_DIR / "sql"
WAREHOUSE_DB = os.environ.get("NAMMAMART_ELT_DB", str(INCLUDE_DIR / "warehouse_elt_sudatta.duckdb"))

default_args = {
    "owner": "sudatta",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=5),
}


@dag(
    dag_id="nammamart_elt_sudatta",
    schedule="0 9 * * 1-6",
    start_date=pendulum.datetime(2026, 7, 1, tz="Asia/Kolkata"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=slack_failure_alert,
    tags=["fde", "nammamart", "elt"],
)
def nammamart_elt_sudatta():

    # ── EXTRACT: locate the five unchanged CSV exports ──────────────────────

    @task
    def extract_file(filename: str):
        path = Path(runtime_path(DATA_DIR, "data_dir")) / filename
        if not path.is_file():
            raise FileNotFoundError(path)
        import csv
        from data_contract import check_schema
        with path.open(encoding='utf-8-sig', newline='') as source:
            check_schema(path.stem, next(csv.reader(source)))
        print(f"[Extract] {filename} ready for raw loading")
        return str(path)

    # ── LOAD RAW: all fields as text, including bad rows ────────────────────

    @task
    def load_raw(files: list, **context):
        from elt_warehouse import load_raw_files
        load_raw_files(runtime_path(WAREHOUSE_DB, "warehouse_db"), files, context["run_id"])

    # ── STAGING: SQL casts, standardisation, masking and quality checks ─────

    @task
    def transform_in_warehouse(**context):
        from elt_warehouse import run_sql
        run_sql(runtime_path(WAREHOUSE_DB, "warehouse_db"), SQL_DIR,
                ["stg_masters.sql", "stg_orders.sql", "dq_results.sql", "batch_observability.sql"],
                context["run_id"], checked_at=context["dag_run"].start_date)

    # ── INCREMENT: append raw rows, then upsert by order_id ──────────────────

    @task
    def snapshot_september(**context):
        from elt_warehouse import run_sql
        run_sql(runtime_path(WAREHOUSE_DB, "warehouse_db"), SQL_DIR, ["marts.sql", "baseline_snapshot.sql"], context["run_id"])

    @task
    def compare_etl_totals(**context):
        if not (context['dag_run'].conf or {}).get('compare_etl', False):
            print('[Compare] Cross-DAG comparison is opt-in after both current snapshots finish; integration tests enforce equality.')
            return
        from elt_warehouse import compare_september
        etl_database = (context['dag_run'].conf or {}).get('etl_warehouse_db', os.environ.get("NAMMAMART_ETL_DB", str(INCLUDE_DIR / "warehouse_sudatta.duckdb")))
        compare_september(runtime_path(WAREHOUSE_DB, "warehouse_db"), SQL_DIR, etl_database, context["run_id"])

    @task
    def load_increment(**context):
        if not (context['dag_run'].conf or {}).get('include_increment', True):
            return False
        from elt_warehouse import load_raw_files
        load_raw_files(runtime_path(WAREHOUSE_DB, "warehouse_db"),
                       [Path(runtime_path(DATA_DIR, "data_dir")) / "orders_2026-10-01_increment.csv"], context["run_id"])
        return True

    @task
    def upsert_increment(**context):
        if not (context['dag_run'].conf or {}).get('include_increment', True):
            return
        from elt_warehouse import run_sql
        run_sql(runtime_path(WAREHOUSE_DB, "warehouse_db"), SQL_DIR, ["stg_orders.sql", "dq_results.sql"],
                context["run_id"], phase="increment", checked_at=context["dag_run"].start_date)

    # ── MARTS + REPORT: business-ready SQL tables ───────────────────────────

    @task
    def build_marts(**context):
        from elt_warehouse import run_sql
        run_sql(runtime_path(WAREHOUSE_DB, "warehouse_db"), SQL_DIR, ["marts.sql"], context["run_id"], phase='final')
        con = duckdb.connect(runtime_path(WAREHOUSE_DB, "warehouse_db"), read_only=True)
        try:
            for table in ("mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
                          "mart_payment_reconciliation", "mart_top_customers"):
                print(f"[Report] {table}")
                result = con.execute(f"SELECT * FROM {table} ORDER BY ALL")
                print([column[0] for column in result.description])
                for row in result.fetchall():
                    print(row)
        finally:
            con.close()

    # ── DAG WIRING ─────────────────────────────────────────────────────────
    # Five extracts → raw → staging → raw increment → upsert → marts/report.

    # ── RUN AUDIT: record successful and failed runs ──────────────────────────
    @task(trigger_rule="all_done", retries=0)
    def run_audit(**context):
        return audit_run(runtime_path(WAREHOUSE_DB, "warehouse_db"), "elt", context)

    orders = extract_file.override(task_id="extract_orders")("orders.csv")
    payments = extract_file.override(task_id="extract_payments")("payments.csv")
    customers = extract_file.override(task_id="extract_customers")("customers.csv")
    products = extract_file.override(task_id="extract_products")("products.csv")
    stores = extract_file.override(task_id="extract_stores")("stores.csv")

    loaded = load_raw([orders, payments, customers, products, stores])
    staged = transform_in_warehouse()
    baseline = snapshot_september()
    compared = compare_etl_totals()
    increment_loaded = load_increment()
    increment_staged = upsert_increment()
    reported = build_marts()
    loaded >> staged >> baseline >> increment_loaded >> increment_staged >> reported >> compared
    audited = run_audit()
    for completed in (orders, payments, customers, products, stores, loaded, staged, baseline,
                      compared, increment_loaded, increment_staged, reported):
        completed >> audited


nammamart_elt_sudatta()
