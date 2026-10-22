"""Audit history, failure propagation and warning baseline checks."""
import ast
import contextlib
import io
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace, ModuleType
from unittest.mock import patch

import duckdb
from include.run_audit import audit_run, batch_checks, store_run


class RunAuditTests(unittest.TestCase):
    def test_retry_and_clear_update_one_run_keep_other_runs(self):
        with TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            database=Path(directory)/'audit.duckdb'
            record=dict(run_id='first',dag_id='etl',start_time=datetime.now(timezone.utc),
                        end_time=datetime.now(timezone.utc),rows_extracted=5,rows_loaded=0,
                        rows_quarantined=2,status='failed')
            store_run(database,record)
            record.update(status='success',rows_loaded=3)
            store_run(database,record)
            store_run(database,dict(record,run_id='second'))
            with duckdb.connect(str(database)) as con:
                self.assertEqual(con.execute('SELECT count(*) FROM pipeline_runs').fetchone()[0],2)
                self.assertEqual(con.execute("SELECT status,rows_loaded FROM pipeline_runs WHERE run_id='first'").fetchone(),('success',3))

    def test_failed_upstream_writes_audit_then_fails_leaf(self):
        class Failed(Exception): pass
        airflow=ModuleType('airflow');exceptions=ModuleType('airflow.exceptions')
        exceptions.AirflowFailException=Failed
        ti=SimpleNamespace(task_id='run_audit',get_task_states=lambda **kw: {'test':{'extract_orders':'failed'}},
                           xcom_pull=lambda **kw:None)
        context=dict(ti=ti,dag=SimpleNamespace(task_ids=['extract_orders','run_audit'],dag_id='etl'),
                     dag_run=SimpleNamespace(start_date=datetime.now(timezone.utc)),run_id='test')
        with TemporaryDirectory() as directory,contextlib.redirect_stdout(io.StringIO()),patch.dict(sys.modules,{'airflow':airflow,'airflow.exceptions':exceptions}):
            database=Path(directory)/'failed.duckdb'
            with self.assertRaisesRegex(Failed,'extract_orders'):
                audit_run(database,'etl',context)
            with duckdb.connect(str(database)) as con:
                self.assertEqual(con.execute('SELECT status,rows_extracted FROM pipeline_runs').fetchone(),('failed',0))

    def test_volume_only_uses_prior_success_and_staleness_warns(self):
        now=datetime.now(timezone.utc)
        with TemporaryDirectory() as directory,contextlib.redirect_stdout(io.StringIO()):
            database=Path(directory)/'batch.duckdb'
            first=batch_checks(database,'first','etl',2400,now,now)
            self.assertEqual(first[0]['status'],'Passed')
            store_run(database,dict(run_id='first',dag_id='etl',start_time=now,end_time=now,
                                    rows_extracted=2400,rows_loaded=2300,rows_quarantined=100,status='success'))
            checks=batch_checks(database,'second','etl',50,now-timedelta(days=30),now)
            self.assertEqual([r['status'] for r in checks],['Warning','Warning'])
            self.assertTrue(all(r['severity']=='Warning' and r['rows_checked']==1 for r in checks))

    def test_both_final_tasks_use_all_done(self):
        root=Path(__file__).resolve().parents[1]
        for pipeline in ('etl','elt'):
            tree=ast.parse((root/f'dags/nammamart_{pipeline}_sudatta.py').read_text(encoding='utf-8-sig'))
            audit=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='run_audit')
            self.assertTrue(any(isinstance(d,ast.Call) and any(k.arg=='trigger_rule' and k.value.value=='all_done' for k in d.keywords) for d in audit.decorator_list))


if __name__=='__main__':
    unittest.main()
