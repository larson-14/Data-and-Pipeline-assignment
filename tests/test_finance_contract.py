"""Cross-engine regressions for corrections, reconciliation, money and publication."""
import contextlib,io,tempfile,unittest
from pathlib import Path
from datetime import datetime,timezone
import pandas as pd
import duckdb
from tests.dags import test_validation_checks as fixtures
from include.verify_elt import build_etl_reference,build_elt_reference
from include.etl_pipeline import extract_csv
from include.run_audit import batch_checks,store_run
NOW=datetime(2026,10,21,3,30,tzinfo=timezone.utc)

class FinanceContractTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.data=self.root/'data';self.data.mkdir()
        f=fixtures.ValidationChecksTests();f.setUp();self.tables=f.tables
    def write(self,increment=None):
        for n,f in self.tables.items():f.to_csv(self.data/(n+'.csv'),index=False)
        if increment is not None:increment.to_csv(self.data/'orders_2026-10-01_increment.csv',index=False)
    def run_both(self,run='first',increment=False):
        with contextlib.redirect_stdout(io.StringIO()):
            build_etl_reference(self.root/'etl.duckdb',increment,self.data,run,NOW)
            build_elt_reference(self.root/'elt.duckdb',increment,self.data,run,NOW)
    def rows(self,pipeline,sql):
        with duckdb.connect(str(self.root/(pipeline+'.duckdb')),read_only=True) as c:return c.execute(sql).fetchall()
    def test_return_correction_removes_finance_revenue_preserves_history_and_rerun(self):
        self.write();self.run_both()
        correction=self.tables['orders'].copy();correction['order_status']='RETURNED'
        new=self.tables['orders'].copy();new['order_id']='O2'
        self.write(pd.concat([correction,new],ignore_index=True));self.run_both('second',True)
        self.assertEqual(self.rows('etl','SELECT count(*) FROM etl_fact_orders'),[(0,)])
        self.assertEqual(self.rows('elt','SELECT count(*) FROM elt_reconciled_orders'),[(0,)])
        self.assertEqual(self.rows('etl',"SELECT order_status FROM etl_current_orders WHERE order_id='O1'"),[('RETURNED',)])
        self.assertEqual(self.rows('etl',"SELECT count(*) FROM etl_finance_history WHERE snapshot_run_id='first'"),[(1,)])
        self.assertEqual(self.rows('etl','SELECT count(*) FROM etl_current_orders'),[(2,)])
        self.run_both('third',True)
        self.assertEqual(self.rows('etl','SELECT count(*) FROM etl_current_orders'),[(2,)])
    def test_rejected_correction_cannot_leave_stale_finance_row(self):
        self.write();self.run_both()
        self.tables['orders'].loc[0,'order_status']='SHIPPED';self.write();self.run_both('second')
        self.assertEqual(self.rows('etl','SELECT count(*) FROM etl_fact_orders'),[(0,)])
        self.assertEqual(self.rows('elt','SELECT count(*) FROM elt_reconciled_orders'),[(0,)])
    def test_orphan_cancelled_and_bad_amount_reconciliation_preserved(self):
        self.tables['payments'].loc[0,'amount_paid']='17'
        orphan=self.tables['payments'].copy();orphan['payment_id']='PX';orphan['order_id']='UNKNOWN'
        self.tables['payments']=pd.concat([self.tables['payments'],orphan],ignore_index=True)
        cancelled=self.tables['orders'].copy();cancelled['order_id']='CANCEL';cancelled['order_status']='CANCELLED';cancelled['delivery_minutes']=''
        self.tables['orders']=pd.concat([self.tables['orders'],cancelled],ignore_index=True)
        self.write();self.run_both()
        a=self.rows('etl','SELECT order_id,reason FROM etl_payment_reconciliation ORDER BY 1')
        b=self.rows('elt','SELECT order_id,reason FROM mart_payment_reconciliation ORDER BY 1')
        self.assertEqual(a,b);self.assertEqual({r[0] for r in a},{'O1','UNKNOWN'})
        self.assertEqual(self.rows('etl',"SELECT count(*) FROM etl_orders WHERE order_status='DELIVERED'"),[(1,)])
        self.assertEqual(self.rows('elt','SELECT sum(delivered_orders) FROM mart_delivery_sla'),[(1,)])
    def test_source_archive_detects_changed_file_in_same_run(self):
        self.write();extract_csv(self.data,'orders.csv','archive')
        original=(self.data/'orders.csv').read_bytes()
        self.assertEqual((self.data/'private/archive/archive/orders.csv').read_bytes(),original)
        self.tables['orders'].loc[0,'quantity']='3';self.write()
        with self.assertRaisesRegex(ValueError,'changed during this run'):extract_csv(self.data,'orders.csv','archive')
    def test_distribution_warning_for_coordinated_price_change(self):
        db=self.root/'drift.duckdb'
        with contextlib.redirect_stdout(io.StringIO()):
            batch_checks(db,'first','etl',100,NOW,NOW,{'S1':10})
            store_run(db,dict(run_id='first',dag_id='etl',start_time=NOW,end_time=NOW,rows_extracted=100,rows_loaded=100,rows_quarantined=0,status='success'))
            checks=batch_checks(db,'second','etl',100,NOW,NOW,{'S1':1000})
        self.assertEqual(next(r['status'] for r in checks if r['check_name']=='order_price_distribution'),'Warning')
if __name__=='__main__':unittest.main()
