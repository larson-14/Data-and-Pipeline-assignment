"""Run audit and batch observability for both Nammamart pipelines."""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def runtime_path(default, key):
    """Optional run configuration keeps experiments in separate files/databases."""
    try:
        from airflow.sdk import get_current_context
        context = get_current_context()
    except (ImportError, RuntimeError):
        return str(default)
    return str((context["dag_run"].conf or {}).get(key, default))


def json_rows(value):
    data = json.loads(value or "{}")
    return len(next(iter(data.values()), {}))


def store_run(database, record):
    """Upsert makes task retries and Airflow Clear retain one row per run."""
    with duckdb.connect(str(database)) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id VARCHAR, dag_id VARCHAR, start_time TIMESTAMPTZ, end_time TIMESTAMPTZ,
            rows_extracted BIGINT, rows_loaded BIGINT, rows_quarantined BIGINT, status VARCHAR,
            PRIMARY KEY (run_id, dag_id))""")
        con.execute("""INSERT INTO pipeline_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (run_id, dag_id) DO UPDATE SET
            start_time=excluded.start_time, end_time=excluded.end_time,
            rows_extracted=excluded.rows_extracted, rows_loaded=excluded.rows_loaded,
            rows_quarantined=excluded.rows_quarantined, status=excluded.status""",
            [record[key] for key in ("run_id", "dag_id", "start_time", "end_time",
                                    "rows_extracted", "rows_loaded", "rows_quarantined", "status")])
    print("[Audit]", json.dumps(record, default=str))


def audit_run(database, pipeline, context):
    ti, dag, run = context["ti"], context["dag"], context["dag_run"]
    task_ids = [name for name in dag.task_ids if name != ti.task_id]
    states = ti.get_task_states(dag_id=dag.dag_id, task_ids=task_ids,
                                run_ids=[context["run_id"]])[context["run_id"]]
    failed = [name for name, state in states.items() if state not in ("success", "skipped")]
    extracted = 0
    for name in ("orders", "payments", "customers", "products", "stores"):
        value = ti.xcom_pull(task_ids="extract_" + name)
        if value is not None:
            if pipeline == "etl":
                extracted += json_rows(value)
            else:
                with open(value, encoding="utf-8-sig", newline="") as source:
                    extracted += max(sum(1 for _ in csv.reader(source)) - 1, 0)
    loaded = quarantined = 0
    if pipeline == "etl":
        result = ti.xcom_pull(task_ids="validate_data") or {}
        increment = ti.xcom_pull(task_ids="validate_increment") or {}
        incoming = ti.xcom_pull(task_ids="extract_increment")
        extracted += json_rows(incoming) if incoming else 0
        load_result = ti.xcom_pull(task_ids="load_to_warehouse") or {}
        loaded = int(load_result.get('rows_loaded', 0))
        quarantined = result.get("failed_count", 0) + increment.get('failed_count', 0) + sum(
            json_rows(value) for value in result.get("source_quarantine", {}).values())
    elif Path(database).is_file():
        with duckdb.connect(str(database)) as con:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
            if "elt_raw_loads" in tables:
                extracted = con.execute("SELECT coalesce(sum(rows_loaded),0) FROM elt_raw_loads WHERE run_id=?",
                                        [context["run_id"]]).fetchone()[0] or extracted
            if "elt_reconciled_orders" in tables:
                loaded = con.execute("SELECT count(*) FROM elt_reconciled_orders WHERE _run_id=?",
                                     [context["run_id"]]).fetchone()[0]
            if "elt_quality_rows" in tables:
                quarantined = con.execute("""SELECT count(*) FROM (
                    SELECT DISTINCT source_file, source_row FROM elt_quality_rows
                    WHERE run_id=? AND severity='Critical' AND failed)""", [context["run_id"]]).fetchone()[0]
    if hasattr(run, 'conf'):
        record_schema_checks(database, context)
    record = dict(run_id=context["run_id"], dag_id=dag.dag_id, start_time=run.start_date,
                  end_time=datetime.now(timezone.utc), rows_extracted=extracted,
                  rows_loaded=loaded, rows_quarantined=quarantined,
                  status="failed" if failed else "success")
    store_run(database, record)
    if failed:
        # A successful all_done leaf would otherwise hide the upstream failure.
        from airflow.exceptions import AirflowFailException
        raise AirflowFailException(f"[Audit][FAILED] Tasks: {', '.join(failed)}")
    return record


def record_schema_checks(database, context):
    """The all_done audit records source/schema failures even when validation never ran."""
    try:
        from include.data_contract import SCHEMAS, check_schema
        from include.dq_scorecard import store_dq_results
    except ImportError:
        from data_contract import SCHEMAS, check_schema
        from dq_scorecard import store_dq_results
    conf=context['dag_run'].conf or {}
    directory=Path(conf.get('data_dir',Path(__file__).resolve().parent/'data/nammamart'))
    files=[n+'.csv' for n in SCHEMAS]
    if conf.get('include_increment',True): files.append('orders_2026-10-01_increment.csv')
    results=[]
    for filename in files:
        failed=False
        try:
            with (directory/filename).open(encoding='utf-8-sig',newline='') as f:
                reader=csv.reader(f); columns=next(reader)
                check_schema('orders' if filename.startswith('orders') else filename[:-4],columns)
                failed=any(len(row)!=len(columns) for row in reader)
        except (OSError,ValueError,StopIteration): failed=True
        results.append(dict(run_id=context['run_id'],dag_id=context['dag'].dag_id,check_name='schema.'+filename,
            dimension='Validity',severity='Critical',rows_checked=1,rows_failed=int(failed),status='Failed' if failed else 'Passed',checked_at=datetime.now(timezone.utc).isoformat()))
    store_dq_results(database,results)


def batch_checks(database, run_id, dag_id, row_count, latest_order, checked_at, store_prices=None):
    """Warnings do not reject rows; volume uses the preceding successful batch."""
    now = datetime.fromisoformat(str(checked_at).replace("Z", "+00:00"))
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    latest = None if latest_order is None else datetime.fromisoformat(str(latest_order).replace("Z", "+00:00"))
    if latest is not None and latest.tzinfo is None:
        latest = latest.replace(tzinfo=timezone.utc)
    with duckdb.connect(str(database)) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS pipeline_batches (
            run_id VARCHAR, dag_id VARCHAR, order_count BIGINT, checked_at TIMESTAMPTZ,
            PRIMARY KEY (run_id, dag_id))""")
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        previous = None
        if "pipeline_runs" in tables:
            previous = con.execute("""SELECT b.order_count FROM pipeline_batches b JOIN pipeline_runs r
                USING (run_id, dag_id) WHERE b.dag_id=? AND b.run_id<>? AND r.status='success'
                ORDER BY r.end_time DESC LIMIT 1""", [dag_id, run_id]).fetchone()
        con.execute("INSERT OR REPLACE INTO pipeline_batches VALUES (?,?,?,?)",
                    [run_id, dag_id, row_count, now])
    drift_checked=drift_failed=0
    if store_prices is not None:
        with duckdb.connect(str(database)) as con:
            con.execute("CREATE TABLE IF NOT EXISTS pipeline_store_prices(run_id VARCHAR,dag_id VARCHAR,store_id VARCHAR,median_price DOUBLE,PRIMARY KEY(run_id,dag_id,store_id))")
            prior={}
            if 'pipeline_runs' in tables:
                prior=dict(con.execute("SELECT store_id,median_price FROM pipeline_store_prices WHERE run_id=(SELECT run_id FROM pipeline_runs WHERE dag_id=? AND run_id<>? AND status='success' ORDER BY end_time DESC LIMIT 1)",[dag_id,run_id]).fetchall())
            con.execute('DELETE FROM pipeline_store_prices WHERE run_id=? AND dag_id=?',[run_id,dag_id])
            for store,price in store_prices.items():
                if not pd_isfinite(price): continue
                drift_checked+=1
                previous_price=prior.get(store)
                drift_failed+=int(previous_price is not None and previous_price>0 and (price>previous_price*3 or price<previous_price/3))
                con.execute('INSERT INTO pipeline_store_prices VALUES(?,?,?,?)',[run_id,dag_id,store,float(price)])
    checks = []
    for name, dimension, failed in (
        ("order_volume", "Completeness", bool(previous and previous[0] > 0 and row_count < previous[0] * 0.1)),
        ("order_freshness", "Timeliness", latest is None or (now - latest).total_seconds() > 86400),
    ):
        checks.append(dict(run_id=run_id, dag_id=dag_id, check_name=name, dimension=dimension,
                           severity="Warning", rows_checked=1, rows_failed=int(failed),
                           status="Warning" if failed else "Passed", checked_at=now.isoformat()))
        print(f"[DQ] {name}: {checks[-1]['status']} | rows={row_count} | previous={previous} | latest={latest}")
    if store_prices is not None:
        checks.append(dict(run_id=run_id,dag_id=dag_id,check_name='order_price_distribution',dimension='Accuracy',severity='Warning',rows_checked=drift_checked,rows_failed=drift_failed,status='Warning' if drift_failed else 'Passed',checked_at=now.isoformat()))
    return checks


def pd_isfinite(value):
    import math
    try: return math.isfinite(float(value)) and float(value)>=0
    except (TypeError,ValueError): return False
