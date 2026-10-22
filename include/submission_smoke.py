"""Execute both final DAG versions in private warehouses and collect query evidence."""
import csv,json,re,shutil
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from observability_experiments import command,wait,SOURCE,WORK

OUT=Path('/usr/local/airflow/include/submission_evidence')
PRIVATE=Path('/usr/local/airflow/include/data/private/submission')
SQL=Path('/usr/local/airflow/include/sql')


def main():
    OUT.mkdir(exist_ok=True);PRIVATE.mkdir(parents=True,exist_ok=True)
    stamp=datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    for pipeline in ('etl','elt'):
        database=PRIVATE/f'{pipeline}_{stamp}.duckdb'
        run_id=f'submission_{stamp}_{pipeline}'
        conf=dict(data_dir=str(SOURCE),warehouse_db=str(database),quarantine_dir=str(PRIVATE/'quarantine'),observability_test=True,include_increment=True,compare_etl=(pipeline=='elt'),etl_warehouse_db=str(PRIVATE/f'etl_{stamp}.duckdb'))
        command('dags','trigger',f'nammamart_{pipeline}_sudatta','--run-id',run_id,'--conf',json.dumps(conf))
        state,tasks,history=wait(f'nammamart_{pipeline}_sudatta',run_id)
        if state!='success':raise RuntimeError(f'{pipeline} final-version smoke run failed')
        document=dict(pipeline=pipeline,run_id=run_id,state=state,tasks=tasks,queries=[],warehouse=str(database))
        scripts=re.split(r'-- Q\d+:', (SQL/f'business_{pipeline}.sql').read_text(encoding='utf-8'))[1:]
        with duckdb.connect(str(database),read_only=True) as con:
            for number,chunk in enumerate(scripts,1):
                title,query=chunk.split('\n',1)
                result=con.execute(query)
                columns=[c[0] for c in result.description];rows=result.fetchall()
                document['queries'].append(dict(number=number,title=title.strip(),sql=query.strip(),columns=columns,rows=rows))
                with (OUT/f'{pipeline}_question_{number}.csv').open('w',newline='') as handle:
                    writer=csv.writer(handle);writer.writerow(columns);writer.writerows(rows)
            document['table_evidence']={}
            reconciliation='etl_payment_reconciliation' if pipeline=='etl' else 'mart_payment_reconciliation'
            for table,query in [('dq_results','SELECT * FROM dq_results WHERE run_id=? ORDER BY dimension,check_name'),
                ('pipeline_runs','SELECT * FROM pipeline_runs ORDER BY end_time DESC LIMIT 3'),
                (reconciliation,f'SELECT order_id,order_amount,amount_paid,reason FROM {reconciliation} ORDER BY order_id LIMIT 12')]:
                result=con.execute(query,[run_id] if table=='dq_results' else [])
                document['table_evidence'][table]=dict(sql=query,columns=[c[0] for c in result.description],rows=result.fetchall())
        (OUT/f'{pipeline}_results.json').write_text(json.dumps(document,indent=2,default=str),encoding='utf-8')
        print(f'[Submission] {pipeline}: success; all six SQL queries executed',flush=True)


if __name__=='__main__':main()
