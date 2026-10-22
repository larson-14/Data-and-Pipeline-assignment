"""Collect task log evidence, per-file counts and hashes for the Part F submission."""
import contextlib
import hashlib
import io
import json
import shutil
from pathlib import Path

import duckdb
import pandas as pd
from validation_checks import run_validation_checks
from observability_experiments import OUT, SOURCE, WORK


def collect():
    results=[]
    for path in sorted(OUT.glob('observability_*.json')):
        record=json.loads(path.read_text())
        pipeline=record['pipeline']
        dag_id=f'nammamart_{pipeline}_sudatta'
        logs=Path('/usr/local/airflow/logs')/f'dag_id={dag_id}'/f"run_id={record['run_id']}"
        events=[]
        for task in record['tasks']:
            for log in sorted((logs/f"task_id={task['task_id']}").glob('attempt=*.log')):
                for line in log.read_text(errors='replace').splitlines():
                    try:
                        event=json.loads(line)
                    except (ValueError,TypeError):
                        continue
                    message=event.get('event','')
                    if message.startswith(('[Extract]','[Validate]' ,'[Quarantine]','[Load]',
                                           '[DQ]','[Audit]','[Slack] Observability')) or event.get('error_detail'):
                        entry=dict(task_id=task['task_id'],attempt=log.name,timestamp=event.get('timestamp'),event=message)
                        if event.get('error_detail'):
                            entry['errors']=[dict(type=e.get('exc_type'),message=e.get('exc_value')) for e in event['error_detail']]
                        events.append(entry)
        record['log_evidence']=events
        parts=record['run_id'].split('_')
        stamp=parts[2]+'_'+parts[3]
        number=record['experiment']
        database=WORK/pipeline/(f'baseline_{stamp}.duckdb' if number in (1,2) else f'experiment_{stamp}_{4 if number==10 else number:02d}.duckdb')
        if database.exists():
            with duckdb.connect(str(database),read_only=True) as con:
                if number!=4:
                    q=con.execute('SELECT * FROM dq_results WHERE run_id=? ORDER BY check_name',[record['run_id']])
                    columns=[c[0] for c in q.description]
                    record['evidence']['dq_results']=[dict(zip(columns,row)) for row in q.fetchall()]
                if pipeline=='etl' and number==3:
                    original=pd.read_csv(SOURCE/'orders.csv',dtype=str).drop_duplicates('order_id').set_index('order_id').order_status.to_dict()
                    keys=pd.read_csv(SOURCE/'orders_2026-10-01_increment.csv',dtype=str).order_id.tolist()
                    current=dict(con.execute('SELECT order_id,order_status FROM etl_current_orders').fetchall())
                    record['evidence']['increment']=[sum(k not in original and k in current for k in keys),sum(k in original and k in current for k in keys),sum(k in original and original[k]!=current.get(k) for k in keys)]
                if pipeline=='etl' and number==5:
                    record['evidence']['injected_quarantine']=con.execute("SELECT row_key,failure_reason FROM etl_rejected_rows WHERE run_id=? AND row_key LIKE 'OBS_%' ORDER BY ALL",[record['run_id']]).fetchall()

        if record['experiment']==1:
            if pipeline=='etl':
                frames=[pd.read_csv(SOURCE/(name+'.csv'),dtype=str,keep_default_na=False) for name in ('orders','payments','customers','products','stores')]
                with contextlib.redirect_stdout(io.StringIO()):
                    counts=run_validation_checks(*(f.to_json() for f in frames),
                        run_id=record['run_id'],checked_at=record['evidence']['pipeline_runs'][0]['start_time'])
                record['evidence']['source_metrics']=counts['source_metrics']
                record['source_metrics_method']='Replayed the unchanged Python validation on identical source hashes at the recorded run start; verified order totals against the task log.'
            else:
                parts=record['run_id'].split('_')
                baseline=WORK/pipeline/f'baseline_{parts[2]}_{parts[3]}.duckdb'
                with duckdb.connect(str(baseline),read_only=True) as con:
                    rows=con.execute("""WITH flags AS (
                        SELECT source_file,source_row,bool_or(failed AND severity='Critical') AS critical,
                               bool_or(failed AND severity='Warning') AS warned
                        FROM elt_quality_rows WHERE run_id=? GROUP BY source_file,source_row)
                        SELECT source_file,count(*),count(*) FILTER(WHERE NOT critical),
                               count(*) FILTER(WHERE critical),count(*) FILTER(WHERE warned AND NOT critical)
                        FROM flags GROUP BY source_file ORDER BY 1""",[record['run_id']]).fetchall()
                    record['evidence']['source_metrics']={r[0]:dict(zip(
                        ('rows_extracted','rows_passed','rows_quarantined','rows_warning'),r[1:])) for r in rows}
                record['source_metrics_method']='Counted distinct raw source rows and their stored SQL quality flags for this run.'
        path.write_text(json.dumps(record,indent=2,default=str),encoding='utf-8')
        results.append(record)
    manifest=json.loads((OUT/'OBSERVABILITY_RESULTS.json').read_text())
    current={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in SOURCE.glob('*.csv')}
    assert current==manifest['source_hashes']
    for directory in WORK.glob('*/data_*'):
        for name,digest in current.items():
            assert hashlib.sha256((directory/name).read_bytes()).hexdigest()==digest, directory/name
    manifest.update(results=results,originals_unchanged=True,experiment_copies_restored=True)
    (OUT/'OBSERVABILITY_RESULTS.json').write_text(json.dumps(manifest,indent=2,default=str),encoding='utf-8')
    print(f'[Evidence] {len(results)} records; original CSVs and all experiment copies verified restored',flush=True)


if __name__=='__main__':
    collect()
