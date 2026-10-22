"""NammaMart ETL: parallel extracts, Python validation, quarantine, pandas transform, atomic load."""
import sys
from pathlib import Path
from datetime import timedelta
import pendulum
import duckdb
import pandas as pd
from airflow.sdk import dag, task

INCLUDE_DIR=Path(__file__).resolve().parents[1]/'include'
sys.path.insert(0,str(INCLUDE_DIR))
from run_audit import audit_run,runtime_path,batch_checks
from failure_alert import slack_failure_alert
from etl_pipeline import extract_csv,transform_snapshot,publish_snapshot,write_quarantine
from validation_checks import run_validation_checks,read_frame
from dq_scorecard import store_dq_results
from quarantine_results import store_quarantine_results

DATA_DIR=INCLUDE_DIR/'data/nammamart'
WAREHOUSE_DB=INCLUDE_DIR/'warehouse_sudatta.duckdb'
QUARANTINE_DIR=INCLUDE_DIR/'quarantine/sudatta'
default_args=dict(owner='sudatta',retries=2,retry_delay=timedelta(seconds=30),execution_timeout=timedelta(minutes=5))

@dag(dag_id='nammamart_etl_sudatta',schedule='0 9 * * 1-6',start_date=pendulum.datetime(2026,7,1,tz='Asia/Kolkata'),
     catchup=False,max_active_runs=1,default_args=default_args,on_failure_callback=slack_failure_alert,tags=['fde','nammamart','etl'])
def nammamart_etl_sudatta():
    @task
    def extract_file(filename,**context):
        return extract_csv(runtime_path(DATA_DIR,'data_dir'),filename,context['run_id'])

    @task
    def extract_increment(**context):
        if not (context['dag_run'].conf or {}).get('include_increment',True): return None
        return extract_csv(runtime_path(DATA_DIR,'data_dir'),'orders_2026-10-01_increment.csv',context['run_id'])

    @task
    def validate_data(orders,payments,customers,products,stores,**context):
        db=runtime_path(WAREHOUSE_DB,'warehouse_db')
        result=run_validation_checks(orders,payments,customers,products,stores,run_id=context['run_id'],checked_at=context['dag_run'].start_date)
        times=pd.to_datetime(read_frame(result['operational']).order_ts,format='mixed',errors='coerce')
        now=pd.Timestamp(context['dag_run'].start_date)
        latest=times.max()
        if pd.notna(latest): latest=latest.tz_localize('Asia/Kolkata').tz_convert('UTC')
        prices=read_frame(orders)
        prices['unit_price']=pd.to_numeric(prices.unit_price,errors='coerce')
        medians=prices.groupby(prices.store_id.astype('string').str.strip().str.upper()).unit_price.median().to_dict()
        result['dq_results'].extend(batch_checks(db,context['run_id'],context['dag'].dag_id,len(read_frame(orders)),None if pd.isna(latest) else latest,now,store_prices=medians))
        store_dq_results(db,result['dq_results'])
        store_quarantine_results(db,result,context['run_id'],context['dag'].dag_id)
        return result

    @task
    def validate_increment(base,increment,payments,customers,products,stores,**context):
        if increment is None: return None
        ids=set(read_frame(base['orders']).order_id)|set(read_frame(increment).order_id.astype('string').str.strip().str.upper())
        result=run_validation_checks(increment,payments,customers,products,stores,run_id=context['run_id'],checked_at=context['dag_run'].start_date,phase='increment',reference_order_ids=ids)
        db=runtime_path(WAREHOUSE_DB,'warehouse_db')
        store_dq_results(db,result['dq_results'])
        store_quarantine_results(db,result,context['run_id'],context['dag'].dag_id)
        return result

    @task
    def quarantine(base,increment):
        for result in (base,increment):
            if result: write_quarantine(result,runtime_path(QUARANTINE_DIR,'quarantine_dir'))

    @task
    def transform(base,increment):
        return transform_snapshot(base,increment)

    @task
    def load_to_warehouse(bundle,**context):
        return publish_snapshot(runtime_path(WAREHOUSE_DB,'warehouse_db'),bundle,context['run_id'])

    @task
    def report():
        with duckdb.connect(runtime_path(WAREHOUSE_DB,'warehouse_db'),read_only=True) as con:
            for name in ('etl_daily_zone_revenue','etl_category_margin'):
                print(f'[Report] {name}\n{con.execute(f"SELECT * FROM {name} ORDER BY ALL").fetchdf().to_string(index=False)}')

    @task(trigger_rule='all_done',retries=0)
    def run_audit(**context):
        return audit_run(runtime_path(WAREHOUSE_DB,'warehouse_db'),'etl',context)

    orders=extract_file.override(task_id='extract_orders')('orders.csv')
    payments=extract_file.override(task_id='extract_payments')('payments.csv')
    customers=extract_file.override(task_id='extract_customers')('customers.csv')
    products=extract_file.override(task_id='extract_products')('products.csv')
    stores=extract_file.override(task_id='extract_stores')('stores.csv')
    base=validate_data(orders,payments,customers,products,stores)
    incoming=extract_increment()
    base >> incoming
    corrected=validate_increment(base,incoming,payments,customers,products,stores)
    quarantined=quarantine(base,corrected)
    transformed=transform(base,corrected)
    loaded=load_to_warehouse(transformed)
    quarantined >> loaded
    reported=report()
    loaded >> reported
    audited=run_audit()
    for output in (orders,payments,customers,products,stores,base,incoming,corrected,quarantined,transformed,loaded,reported): output >> audited

nammamart_etl_sudatta()
