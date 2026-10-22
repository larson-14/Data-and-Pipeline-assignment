"""Repeatable ETL/ELT verification using isolated warehouses."""
from datetime import datetime, timezone
from pathlib import Path
import contextlib, io, json, tempfile
import duckdb
from include.etl_pipeline import extract_csv, transform_snapshot, publish_snapshot
from include.validation_checks import run_validation_checks, read_frame
from include.dq_scorecard import store_dq_results
from include.quarantine_results import store_quarantine_results
from include.elt_warehouse import load_raw_files, run_sql
ROOT=Path(__file__).resolve().parents[1]
DATA_DIR=ROOT/'include/data/nammamart'
SQL_DIR=ROOT/'include/sql'

def build_etl_reference(database, include_increment=True, data_dir=DATA_DIR, run_id='verification', checked_at=None):
    checked_at=checked_at or datetime.now(timezone.utc)
    payloads=[extract_csv(data_dir,n+'.csv',run_id,archive=False) for n in ('orders','payments','customers','products','stores')]
    base=run_validation_checks(*payloads,run_id=run_id,checked_at=checked_at)
    store_dq_results(database,base['dq_results']);store_quarantine_results(database,base,run_id,'nammamart_etl_sudatta')
    increment=None
    if include_increment:
        incoming=extract_csv(data_dir,'orders_2026-10-01_increment.csv',run_id,archive=False)
        ids=set(read_frame(base['orders']).order_id)|set(read_frame(incoming).order_id)
        increment=run_validation_checks(incoming,*payloads[1:],run_id=run_id,checked_at=checked_at,phase='increment',reference_order_ids=ids)
        store_dq_results(database,increment['dq_results']);store_quarantine_results(database,increment,run_id,'nammamart_etl_sudatta')
    loaded=publish_snapshot(database,transform_snapshot(base,increment),run_id)
    return dict(base,increment=increment,load_result=loaded)

def build_elt_reference(database,include_increment=True,data_dir=DATA_DIR,run_id='verification',checked_at=None):
    checked_at=checked_at or datetime.now(timezone.utc)
    load_raw_files(database,[Path(data_dir)/(n+'.csv') for n in ('orders','payments','customers','products','stores')],run_id)
    run_sql(database,SQL_DIR,['stg_masters.sql','stg_orders.sql','dq_results.sql','batch_observability.sql','marts.sql','baseline_snapshot.sql'],run_id,checked_at=checked_at)
    if include_increment:
        load_raw_files(database,[Path(data_dir)/'orders_2026-10-01_increment.csv'],run_id)
        run_sql(database,SQL_DIR,['stg_orders.sql','dq_results.sql'],run_id,phase='increment',checked_at=checked_at)
    run_sql(database,SQL_DIR,['marts.sql'],run_id,phase='final',checked_at=checked_at)

def verify():
    evidence=[]
    with tempfile.TemporaryDirectory() as td,contextlib.redirect_stdout(io.StringIO()):
        for incremental in (False,True):
            etl=Path(td)/f'etl_{incremental}.duckdb';elt=Path(td)/f'elt_{incremental}.duckdb'
            build_etl_reference(etl,include_increment=incremental)
            build_elt_reference(elt,include_increment=incremental)
            with duckdb.connect(str(etl)) as a,duckdb.connect(str(elt)) as b:
                left=a.execute('SELECT order_id,net_revenue,gross_margin FROM etl_fact_orders ORDER BY 1').fetchall()
                right=b.execute('SELECT order_id,net_revenue,gross_margin FROM elt_reconciled_orders ORDER BY 1').fetchall()
                assert left==right, f'Finance parity failed: ETL={len(left)} ELT={len(right)}'
                recon_a=a.execute('SELECT order_id,order_amount,amount_paid,amount_difference,reason FROM etl_payment_reconciliation ORDER BY 1').fetchall()
                recon_b=b.execute('SELECT order_id,order_amount,amount_paid,amount_difference,reason FROM mart_payment_reconciliation ORDER BY 1').fetchall()
                assert recon_a==recon_b,'Reconciliation parity failed'
                revenue=sum(r[1] for r in left);margin=sum(r[2] for r in left)
                evidence.append(dict(increment=incremental,orders=len(left),revenue=str(revenue),margin=str(margin),mismatches=len(recon_a),parity=True))
    (ROOT/'assignment/verification_results.json').write_text(json.dumps(evidence,indent=2),encoding='utf-8')
    lines=['# Current ETL / ELT verification','','Both finance order rows and complete reconciliation tables match exactly.','','| Snapshot | Finance orders | Revenue INR | Margin INR | Mismatches |','| --- | ---: | ---: | ---: | ---: |']
    for r in evidence: lines.append(f"| {'With increment' if r['increment'] else 'Base only'} | {r['orders']} | {r['revenue']} | {r['margin']} | {r['mismatches']} |")
    lines+=['','Run: `python -m include.verify_elt`. Uses temporary databases; never changes the CSV sources.','Both pipelines retain original source evidence, mask reporting contacts, exclude non-delivered/unreconciled rows from finance, and measure delivery independently of payments.']
    (ROOT/'assignment/ELT_VERIFICATION.md').write_text('\n'.join(lines),encoding='utf-8')
    print(json.dumps(evidence,indent=2))
if __name__=='__main__':verify()
