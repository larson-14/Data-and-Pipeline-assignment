"""Persist non-PII rejection metadata for the ETL business report."""
from io import StringIO
import duckdb
import pandas as pd


def store_quarantine_results(database, result, run_id, dag_id):
    records=[]
    for name, payload in [('orders',result['failed']),*result.get('source_quarantine',{}).items()]:
        rows=pd.read_json(StringIO(payload))
        key={'orders':'order_id','payments':'payment_id','customers':'customer_id','products':'product_id','stores':'store_id'}[name]
        for row in rows.to_dict('records'):
            for reason in row.get('failure_reasons',[]):
                records.append((run_id,dag_id,row['source_file'],int(row['source_row']),str(row.get(key,'')),
                                str(reason),result['checked_at']))
    with duckdb.connect(str(database)) as con:
        con.execute('''CREATE TABLE IF NOT EXISTS etl_rejected_rows (
            run_id VARCHAR, dag_id VARCHAR, source_file VARCHAR, source_row BIGINT,
            row_key VARCHAR, failure_reason VARCHAR, checked_at TIMESTAMPTZ)''')
        con.execute('BEGIN TRANSACTION')
        try:
            if result.get('phase') == 'increment':
                con.execute("DELETE FROM etl_rejected_rows WHERE run_id=? AND dag_id=? AND source_file='orders_2026-10-01_increment.csv'",[run_id,dag_id])
            else:
                con.execute("DELETE FROM etl_rejected_rows WHERE run_id=? AND dag_id=? AND source_file<>'orders_2026-10-01_increment.csv'",[run_id,dag_id])
            if records:
                con.executemany('INSERT INTO etl_rejected_rows VALUES (?,?,?,?,?,?,?)',records)
            con.execute('COMMIT')
        except Exception:
            con.execute('ROLLBACK')
            raise
