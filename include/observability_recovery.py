"""Apply Airflow Clear to the two isolated missing-file runs, then verify recovery."""
import inspect
import json
from pathlib import Path

from airflow.models import DagRun, TaskInstance
from airflow.models.taskinstance import clear_task_instances
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from observability_experiments import OUT, WORK, command, wait, tables


def recover(pipeline):
    previous=json.loads((OUT/f'observability_{pipeline}_04.json').read_text())
    dag_id=f'nammamart_{pipeline}_sudatta'
    run_id=previous['run_id']
    unchanged=['extract_orders','extract_payments','extract_customers','extract_stores']
    command('dags','unpause',dag_id)
    with create_session() as session:
        run=session.query(DagRun).filter_by(dag_id=dag_id,run_id=run_id).one()
        database=Path(run.conf['warehouse_db'])
        instances=session.query(TaskInstance).filter_by(dag_id=dag_id,run_id=run_id).filter(
            TaskInstance.task_id.notin_(unchanged)).all()
        selected=[t.task_id for t in instances]
        # This is the same Airflow operation used by Clear; select this run only.
        clear_task_instances(instances,session,dag_run_state=DagRunState.QUEUED)
    state,tasks,history=wait(dag_id,run_id)
    result=dict(pipeline=pipeline,experiment=10,run_id=run_id,state=state,
                clear_method='Airflow clear_task_instances (same operation as UI Clear)',
                selected=selected,before=previous['tasks'],tasks=tasks,transitions=history,
                evidence=tables(database,pipeline,run_id))
    (OUT/f'observability_{pipeline}_10.json').write_text(json.dumps(result,indent=2,default=str),encoding='utf-8')
    command('dags','pause',dag_id)
    print(f'[Recovery] {pipeline}: {state}; selected={selected}',flush=True)


if __name__=='__main__':
    for pipeline in ('etl','elt'):
        recover(pipeline)
