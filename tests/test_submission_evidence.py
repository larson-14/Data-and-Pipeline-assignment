"""Rejection evidence keeps history and publishes no raw customer attributes."""
import io
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
import duckdb
import pandas as pd
from include.pii_masking import mask_customer_evidence
from include.quarantine_results import store_quarantine_results


class SubmissionEvidenceTests(unittest.TestCase):
    def test_public_evidence_redacts_attributes_keeps_audit_reference(self):
        original=pd.DataFrame([dict(customer_id='TEST_CUSTOMER',full_name='Test Person',email='private@example.invalid',
            phone='1010101010',city='Test City',area='Test Area',pincode='101010',signup_date='2027-01-01',
            loyalty_tier='TEST',is_active=True,source_row=4,failure_reasons=['future signup'])])
        public=mask_customer_evidence(original)
        self.assertEqual(public.loc[0,'customer_id'],'TEST_CUSTOMER')
        self.assertEqual(public.loc[0,'source_row'],4)
        self.assertEqual(public.loc[0,'failure_reasons'],['future signup'])
        self.assertTrue(all(public.loc[0,c]=='[REDACTED]' for c in ('full_name','email','phone','city','area','pincode','signup_date','loyalty_tier','is_active')))
        self.assertEqual(original.loc[0,'email'],'private@example.invalid')

    def test_rejection_retry_is_idempotent_and_new_runs_retain_history(self):
        failed=pd.DataFrame([dict(order_id='TEST_ORDER',source_file='orders.csv',source_row=1,
                                  failure_reasons=['quantity invalid','amount mismatch'])])
        result=dict(failed=failed.to_json(),source_quarantine={},checked_at='2026-10-09T00:00:00+00:00')
        with TemporaryDirectory() as directory:
            database=Path(directory)/'evidence.duckdb'
            store_quarantine_results(database,result,'first','etl')
            store_quarantine_results(database,result,'first','etl')
            store_quarantine_results(database,result,'second','etl')
            with duckdb.connect(str(database)) as con:
                self.assertEqual(con.execute('SELECT count(*) FROM etl_rejected_rows').fetchone()[0],4)
                self.assertEqual(con.execute("SELECT count(DISTINCT source_row) FROM etl_rejected_rows WHERE run_id='first'").fetchone()[0],1)
                self.assertEqual({r[0] for r in con.execute('DESCRIBE etl_rejected_rows').fetchall()},
                                 {'run_id','dag_id','source_file','source_row','row_key','failure_reason','checked_at'})


if __name__=='__main__':unittest.main()
