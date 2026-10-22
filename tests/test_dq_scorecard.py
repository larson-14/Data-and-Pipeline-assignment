"""Quality history, retry safety and queryable master rejection evidence."""
import contextlib,io,unittest
from pathlib import Path
from tempfile import TemporaryDirectory
import duckdb
from tests.dags import test_validation_checks as fixtures
from include.dq_scorecard import store_dq_results
from include.etl_pipeline import write_quarantine

class DqScorecardTests(unittest.TestCase):
    def test_dimensions_history_and_retry(self):
        fixture=fixtures.ValidationChecksTests();fixture.setUp();rows=fixture.validate()['dq_results']
        self.assertEqual({r['dimension'] for r in rows},{'Completeness','Uniqueness','Validity','Accuracy','Consistency','Timeliness'})
        self.assertTrue(all(r['status']=='Passed' for r in rows))
        with TemporaryDirectory() as td,contextlib.redirect_stdout(io.StringIO()):
            db=Path(td)/'q.duckdb'
            store_dq_results(db,rows);store_dq_results(db,rows)
            store_dq_results(db,[dict(r,run_id='second') for r in rows])
            with duckdb.connect(str(db)) as c:
                self.assertEqual(c.execute('SELECT count(*) FROM dq_results').fetchone()[0],len(rows)*2)
            with self.assertRaisesRegex(ValueError,'actual run_id'):store_dq_results(db,[dict(rows[0],run_id=None)])
    def test_master_quarantine_has_required_reason_and_masks_contacts(self):
        fixture=fixtures.ValidationChecksTests();fixture.setUp();fixture.tables['customers'].loc[0,'signup_date']='2027-01-01'
        result=fixture.validate()
        with TemporaryDirectory() as td:
            write_quarantine(result,td)
            import pandas as pd
            f=pd.read_csv(next(Path(td).glob('customers_failed_*.csv')))
            self.assertIn('customer_future_signup',f.iloc[0].failure_reason)
            self.assertEqual(f.iloc[0].email,'[REDACTED]')
if __name__=='__main__':unittest.main()
