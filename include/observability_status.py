"""Read scheduler state and recent errors for the isolated Part F experiments."""
import json
import sys
from pathlib import Path
from airflow.models import DagRun, TaskInstance, DagModel
from airflow.utils.session import create_session

with create_session() as session:
    if '--schedule' in sys.argv:
        for dag in session.query(DagModel).filter(DagModel.dag_id.in_(
                ['nammamart_etl_sudatta','nammamart_elt_sudatta'])).all():
            print(dag.dag_id,'is_paused=',dag.is_paused)
        raise SystemExit(0)
    runs = session.query(DagRun).filter(DagRun.dag_id.like('nammamart_%_sudatta')).order_by(DagRun.id.desc()).limit(4).all()
    for run in runs:
        tasks = session.query(TaskInstance).filter_by(dag_id=run.dag_id,run_id=run.run_id).all()
        print(run.run_id,run.state,[(t.task_id,t.state,t.try_number) for t in tasks])
        for task in tasks:
            if task.state in ('failed','up_for_retry'):
                folder=Path('/usr/local/airflow/logs')/f'dag_id={run.dag_id}'/f'run_id={run.run_id}'/f'task_id={task.task_id}'
                for path in folder.glob('attempt=*.log'):
                    for line in path.read_text(errors='replace').splitlines():
                        if 'exception' in line.lower() or 'error' in line.lower():
                            print(task.task_id,path.name,line[-2500:])
