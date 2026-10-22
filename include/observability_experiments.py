"""Run Part F against the local Airflow scheduler using isolated CSV copies.

Run inside Astro: python /usr/local/airflow/include/observability_experiments.py
Source exports and the normal warehouse databases are never changed.
"""

import csv
import hashlib
import json
import shutil
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session

ROOT = Path('/usr/local/airflow')
SOURCE = ROOT / 'include/data/nammamart'
WORK = ROOT / 'include/observability'
OUT = WORK / 'results'
STAMP = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')


def command(*args):
    result = subprocess.run(['airflow', *args], capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def write_orders(directory, mutate):
    path = directory / 'orders.csv'
    with path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        header, rows = reader.fieldnames, list(reader)
    header, rows = mutate(header, rows)
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def states(dag_id, run_id):
    with create_session() as session:
        run = session.query(DagRun).filter_by(dag_id=dag_id, run_id=run_id).one()
        tasks = session.query(TaskInstance).filter_by(dag_id=dag_id, run_id=run_id).all()
        return run.state, [dict(task_id=t.task_id, state=t.state, try_number=t.try_number,
                                start_time=t.start_date, end_time=t.end_date,
                                duration=t.duration) for t in tasks]


def wait(dag_id, run_id):
    history, previous = [], None
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        state, tasks = states(dag_id, run_id)
        signature = [(t['task_id'], t['state'], t['try_number']) for t in tasks]
        if signature != previous:
            history.append(dict(at=datetime.now(timezone.utc).isoformat(), dag_state=state, tasks=tasks))
            previous = signature
        if state in ('success', 'failed'):
            return state, tasks, history
        time.sleep(2)
    raise TimeoutError(f'{dag_id} / {run_id}: scheduler did not finish within 15 minutes')


def tables(database, pipeline, run_id):
    if not database.is_file():
        return {}
    with duckdb.connect(str(database), read_only=True) as con:
        names = {row[0] for row in con.execute('SHOW TABLES').fetchall()}
        result = {}
        for table in ('pipeline_runs', 'dq_results'):
            if table in names:
                query = con.execute(f'SELECT * FROM {table} WHERE run_id=? ORDER BY ALL', [run_id])
                result[table] = [dict(zip([c[0] for c in query.description], row)) for row in query.fetchall()]
        fact = 'etl_current_orders' if pipeline == 'etl' else 'stg_orders'
        if fact in names:
            result['orders'] = con.execute(f'SELECT count(*), count(DISTINCT order_id) FROM {fact}').fetchone()
            result['injected'] = con.execute(f"SELECT order_id, order_status FROM {fact} WHERE order_id LIKE 'OBS_%' ORDER BY 1").fetchall()
        if pipeline == 'etl':
            if 'etl_daily_zone_revenue' in names:
                result['report_columns'] = [r[0] for r in con.execute('DESCRIBE etl_daily_zone_revenue').fetchall()]
                result['revenue'] = con.execute('SELECT coalesce(sum(net_revenue),0) FROM etl_daily_zone_revenue').fetchone()[0]
        else:
            if 'raw_orders' in names:
                result['raw_orders'] = con.execute('SELECT count(*) FROM raw_orders').fetchone()[0]
            if 'mart_daily_store_kpis' in names:
                result['revenue'] = con.execute('SELECT coalesce(sum(net_revenue),0) FROM mart_daily_store_kpis').fetchone()[0]
            if 'elt_quarantine' in names:
                result['injected_quarantine'] = con.execute("SELECT * EXCLUDE(raw_record) FROM elt_quarantine WHERE row_key LIKE 'OBS_%' ORDER BY ALL").fetchall()
            if 'elt_base_orders' in names and 'stg_orders' in names:
                result['increment'] = con.execute("""SELECT count(*) FILTER (WHERE b.order_id IS NULL),
                    count(*) FILTER (WHERE b.order_id IS NOT NULL),
                    count(*) FILTER (WHERE b.order_id IS NOT NULL AND b.order_status<>s.order_status)
                    FROM stg_orders s LEFT JOIN elt_base_orders b USING(order_id)
                    WHERE s._source_file='orders_2026-10-01_increment.csv'""").fetchone()
        return result


def run_one(pipeline, number, directory, database):
    dag_id = f'nammamart_{pipeline}_sudatta'
    run_id = f'part_f_{STAMP}_{number:02d}_{pipeline}'
    logical_date = datetime.now(timezone.utc).isoformat()
    conf = dict(data_dir=str(directory), warehouse_db=str(database), observability_test=True, include_increment=(number==3),
                quarantine_dir=str(WORK / pipeline / f'quarantine_{number:02d}'))
    command('dags', 'trigger', dag_id, '--run-id', run_id, '--logical-date', logical_date,
            '--conf', json.dumps(conf))
    state, tasks, history = wait(dag_id, run_id)
    result = dict(pipeline=pipeline, experiment=number, run_id=run_id, logical_date=logical_date,
                  state=state, tasks=tasks, transitions=history, evidence=tables(database, pipeline, run_id))
    path = OUT / f'observability_{pipeline}_{number:02d}.json'
    path.write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
    print(f'[Experiment] {pipeline} #{number}: {state}; {path.name}', flush=True)
    return result


def suite(pipeline):
    work = WORK / pipeline
    work.mkdir(parents=True, exist_ok=True)
    dag_id = f'nammamart_{pipeline}_sudatta'
    command('dags', 'unpause', dag_id)
    # Historical scheduled runs must not use the normal source/warehouse during the test.
    results = []
    baseline = work / f'baseline_{STAMP}.duckdb'
    for number in (1, 2, 3, 4, 5, 6, 7, 8, 9):
        directory = work / f'data_{number:02d}'
        shutil.copytree(SOURCE, directory, dirs_exist_ok=True, ignore=shutil.ignore_patterns('private','original'))
        database = baseline if number in (1, 2) else work / f'experiment_{STAMP}_{number:02d}.duckdb'
        if number not in (1, 2):
            shutil.copy2(baseline, database)
        missing = None
        if number == 4:
            missing = directory / 'products.csv'
            missing.rename(directory / 'products.csv.missing')
        elif number == 5:
            def bad(header, rows):
                base = next(r for r in rows if r['order_status']=='DELIVERED' and r['customer_id']
                            and float(r['quantity'])>0 and r['delivery_minutes'] and float(r['delivery_minutes'])<120)
                for name, column, value in (('NULL','customer_id',''), ('NEGATIVE','quantity','-1'),
                                            ('STORE','store_id','UNKNOWN'), ('STATUS','order_status','SHIPPED'),
                                            ('FUTURE','order_ts','2027-01-01 09:00:00')):
                    row = dict(base, order_id='OBS_'+name)
                    row[column] = value
                    rows.append(row)
                return header, rows
            write_orders(directory, bad)
        elif number == 6:
            def schema(header, rows):
                return ['total' if h=='order_amount' else h for h in header], [
                    {('total' if k=='order_amount' else k): v for k,v in r.items()} for r in rows]
            write_orders(directory, schema)
        elif number == 7:
            write_orders(directory, lambda header, rows: (header, rows[:50]))
        elif number == 8:
            def drift(header, rows):
                for row in rows:
                    if row['store_id']=='S01':
                        row['unit_price'] = str(float(row['unit_price'])*100)
                return header, rows
            write_orders(directory, drift)
        elif number == 9:
            stale = (datetime.now(timezone.utc)-timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
            def freshness(header, rows):
                for row in rows:
                    row['order_ts']=stale
                return header, rows
            write_orders(directory, freshness)
        result = run_one(pipeline, number, directory, database)
        results.append(result)
        if number == 4:
            (directory / 'products.csv.missing').rename(missing)
            before = result['tasks']
            from airflow.models.taskinstance import clear_task_instances
            from airflow.utils.state import DagRunState
            with create_session() as session:
                instances=session.query(TaskInstance).filter_by(dag_id=dag_id,run_id=result['run_id']).filter(
                    TaskInstance.task_id.notin_(['extract_orders','extract_payments','extract_customers','extract_stores'])).all()
                clear_task_instances(instances,session,dag_run_state=DagRunState.QUEUED)
            state, tasks, history = wait(dag_id,result['run_id'])
            recovered = dict(pipeline=pipeline, experiment=10, run_id=result['run_id'], state=state,
                             clear_method='Airflow clear_task_instances (same operation as UI Clear)',
                             before=before,tasks=tasks,transitions=history,
                             evidence=tables(database,pipeline,result['run_id']))
            (OUT / f'observability_{pipeline}_10.json').write_text(json.dumps(recovered,indent=2,default=str),encoding='utf-8')
            print(f'[Experiment] {pipeline} #10: {state}',flush=True)
            results.append(recovered)
        # Restore the copy after every experiment, including renamed headers and mutations.
        shutil.copytree(SOURCE,directory,dirs_exist_ok=True,ignore=shutil.ignore_patterns('private','original'))
    command('dags','unpause',dag_id)
    return results


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    hashes = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in SOURCE.glob('*.csv')}
    def checked_suite(pipeline):
        try:
            return suite(pipeline)
        except Exception:
            import traceback
            message=traceback.format_exc()
            (OUT/f'{pipeline}_runner_error.txt').write_text(message,encoding='utf-8')
            print(message,flush=True)
            raise
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(checked_suite, ('etl','elt')))
    restored = {p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in SOURCE.glob('*.csv')}
    assert restored==hashes, 'Original source files changed'
    (OUT/'OBSERVABILITY_RESULTS.json').write_text(json.dumps(dict(source_hashes=hashes,
        originals_unchanged=True,results=results),indent=2,default=str),encoding='utf-8')


if __name__=='__main__':
    main()
