"""ETL business transformations in pandas, followed by atomic DuckDB publication."""
import csv
import hashlib
import json
import re
from pathlib import Path
from datetime import datetime, timezone
import duckdb
import pandas as pd
try:
    from include.data_contract import SCHEMAS, KEYS, check_schema, money, order_money
    from include.validation_checks import read_frame, run_validation_checks
except ImportError:
    from data_contract import SCHEMAS, KEYS, check_schema, money, order_money
    from validation_checks import read_frame, run_validation_checks

def extract_csv(data_dir,filename,run_id,archive=True):
    path=Path(data_dir)/filename
    source='orders' if filename.startswith('orders') else path.stem
    content=path.read_bytes()
    with path.open(encoding='utf-8-sig',newline='') as f:
        reader=csv.reader(f); columns=next(reader); rows=list(reader)
    check_schema(source,columns)
    if any(len(r)!=len(columns) for r in rows): raise ValueError(f'{filename}: inconsistent field count')
    if archive:
        target=Path(data_dir)/'private'/'archive'/re.sub(r'[^\w-]','_',run_id)
        target.mkdir(parents=True,exist_ok=True)
        dest=target/filename
        if dest.exists() and dest.read_bytes()!=content: raise ValueError(f'{filename}: source changed during this run')
        dest.write_bytes(content)
        (target/(filename+'.sha256')).write_text(hashlib.sha256(content).hexdigest(),encoding='utf-8')
    print(f'[Extract] {filename}: {len(rows)} rows')
    return pd.DataFrame(rows,columns=columns).to_json()

def effective(base,increment,key='order_id'):
    if increment is None: return base.copy()
    return pd.concat([base[~base[key].isin(increment[key])],increment],ignore_index=True)

def reconcile(orders,payments):
    """Both sides, including rejected orders and orphan payments; returned != refunded."""
    orders=orders.drop_duplicates('order_id').set_index('order_id',drop=False)
    records=[]
    for order_id in sorted(set(orders.index)|set(payments.order_id)):
        matches=payments[payments.order_id.eq(order_id)]
        success=matches[matches.payment_status.eq('SUCCESS')].drop_duplicates('payment_id')
        paid=sum((money(v) or money(0) for v in success.amount_paid),money(0)) if len(success) else None
        order=orders.loc[order_id] if order_id in orders.index else None
        status=None if order is None else order.order_status
        expected=None if order is None else money(order.order_amount)
        reasons=[]
        if order is None: reasons.append('orphan payment')
        if order is not None:
            if status=='DELIVERED' and matches.empty: reasons.append('missing payment')
            elif status=='DELIVERED' and success.empty: reasons.append('no successful payment')
            target=money(0) if status=='CANCELLED' else expected
            if len(success) and (target is None or paid!=target): reasons.append('payment amount mismatch')
        if matches.payment_id.duplicated(keep=False).any(): reasons.append('duplicate payment_id')
        if len(matches) and matches['_critical'].any(): reasons.append('payment quality failure')
        if reasons:
            target=money(0) if order is None or status=='CANCELLED' else expected
            records.append(dict(order_id=order_id,order_status=status,payment_mode=None if order is None else order.payment_mode,
                order_amount=target,amount_paid=paid,amount_difference=(target or money(0))-(paid or money(0)),reason='; '.join(reasons)))
    return pd.DataFrame(records,columns=['order_id','order_status','payment_mode','order_amount','amount_paid','amount_difference','reason'])

def transform_snapshot(base,increment=None):
    raw_orders=read_frame(base['orders'])
    accepted=read_frame(base['passed']); operational=read_frame(base['operational'])
    if increment:
        incoming=read_frame(increment['orders'])
        raw_orders=effective(raw_orders,incoming)
        accepted=pd.concat([accepted[~accepted.order_id.isin(incoming.order_id)],read_frame(increment['passed'])],ignore_index=True)
        operational=pd.concat([operational[~operational.order_id.isin(incoming.order_id)],read_frame(increment['operational'])],ignore_index=True)
    masters={}
    for source in ('customers','products','stores'):
        frame=read_frame(base[source]); frame=frame[~frame['_critical']].drop_duplicates(KEYS[source]).drop(columns='_critical')
        for col in set(frame)&{'full_name','city','area','product_name','category','sub_category','brand','store_name','zone','manager_name'}:
            frame[col]=frame[col].astype('string').str.title()
        masters[source]=frame
    c=masters['customers']
    c['email']=c.email.map(lambda v: v.split('@',1)[0][:2].lower()+'***@'+v.split('@',1)[1].lower() if '@' in v else '***')
    c['phone']=c.phone.map(lambda v:v[:2]+'*'*(len(v)-4)+v[-2:] if len(v)>4 else '****')
    reconciliation=reconcile(raw_orders,read_frame(base['payments']))
    def derived(frame):
        frame=frame.copy();frame['order_ts']=pd.to_datetime(frame.order_ts,format='mixed',errors='coerce')
        frame['order_date']=frame.order_ts.dt.date
        frame['net_revenue']=[order_money(q,p,d) if s=='DELIVERED' else money(0) for q,p,d,s in frame[['quantity','unit_price','discount_pct','order_status']].itertuples(index=False,name=None)]
        costs=masters['products'].set_index('product_id').cost_price
        frame['gross_margin']=[money(r-money(q)*money(costs[pid])) if status=='DELIVERED' else money(0) for r,q,pid,status in frame[['net_revenue','quantity','product_id','order_status']].itertuples(index=False,name=None)]
        frame['within_15_min']=frame.order_status.eq('DELIVERED')&frame.delivery_minutes.le(15)
        return frame
    accepted=derived(accepted);operational=derived(operational)
    fact=accepted[accepted.order_status.eq('DELIVERED')&~accepted.order_id.isin(reconciliation.order_id)].copy()
    current=raw_orders.drop_duplicates('order_id').copy()
    current['order_ts']=pd.to_datetime(current.order_ts,format='mixed',errors='coerce')
    tables={'etl_dim_customer':c,'etl_dim_product':masters['products'],'etl_dim_store':masters['stores'],
            'etl_current_orders':current,'etl_orders':operational,'etl_fact_orders':fact,'etl_payment_reconciliation':reconciliation}
    return {name:frame.to_json(date_format='iso') for name,frame in tables.items()}

def publish_snapshot(database,bundle,run_id):
    """MERGE current tables, remove obsolete versions, and publish reports in one transaction."""
    tables={n:read_frame(v) for n,v in bundle.items()}
    for name in ('etl_fact_orders','etl_orders','etl_current_orders'):
        tables[name]['order_ts']=pd.to_datetime(tables[name].order_ts,format='mixed',errors='coerce')
        if 'order_date' in tables[name]: tables[name]['order_date']=pd.to_datetime(tables[name].order_date).dt.date
    with duckdb.connect(str(database)) as con:
        con.execute('BEGIN')
        # One-time migration of the previous insert-only schema (facts held FK constraints).
        if con.execute("SELECT count(*) FROM duckdb_constraints() WHERE table_name='etl_fact_orders' AND constraint_type='FOREIGN KEY'").fetchone()[0]:
            con.execute('DROP TABLE etl_fact_orders')
            for name in ('etl_dim_customer','etl_dim_product','etl_dim_store'): con.execute(f'DROP TABLE {name}')
        old=set()
        if con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name='etl_current_orders'").fetchone()[0]:
            old={r[0] for r in con.execute('SELECT order_id FROM etl_current_orders').fetchall()}
        new=set(tables['etl_current_orders'].order_id)
        for name,frame in tables.items():
            for col in set(frame)&{'order_id','customer_id','product_id','store_id','payment_mode','order_status','reason','_source_file','_run_id','pincode'}:
                frame[col]=frame[col].astype('string')
            con.register('incoming',frame)
            key={'etl_dim_customer':'customer_id','etl_dim_product':'product_id','etl_dim_store':'store_id'}.get(name,'order_id')
            columns=con.execute('DESCRIBE incoming').fetchall()
            definitions=[]
            for col,kind,*_ in columns:
                if col in {'unit_price','order_amount','amount_paid','amount_difference','net_revenue','gross_margin','mrp','cost_price'}: kind='DECIMAL(18,2)'
                elif col in {'order_id','customer_id','product_id','store_id','payment_mode','order_status','reason','_source_file','_run_id','pincode'}: kind='VARCHAR'
                elif col in {'quantity','_source_row','capacity_orders_per_day'}: kind='BIGINT'
                elif col=='order_date': kind='DATE'
                elif col=='order_ts': kind='TIMESTAMP'
                definitions.append(f'"{col}" {kind}')
            existing=con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name=?",[name]).fetchone()[0]
            if existing and [r[0] for r in con.execute(f'DESCRIBE {name}').fetchall()]!=list(frame.columns):
                con.execute(f'DROP TABLE {name}')
            con.execute(f'CREATE TABLE IF NOT EXISTS {name} ({", ".join(definitions)}, PRIMARY KEY ({key}))')
            con.execute(f'MERGE INTO {name} t USING incoming s ON t.{key}=s.{key} WHEN MATCHED THEN UPDATE BY NAME WHEN NOT MATCHED THEN INSERT BY NAME')
            con.execute(f'DELETE FROM {name} WHERE {key} NOT IN (SELECT {key} FROM incoming)')
            con.unregister('incoming')
        for dimension,key in [('customer','customer_id'),('product','product_id'),('store','store_id')]:
            assert con.execute(f'SELECT count(*) FROM etl_fact_orders f LEFT JOIN etl_dim_{dimension} d USING({key}) WHERE d.{key} IS NULL').fetchone()[0]==0
        con.execute("CREATE OR REPLACE TABLE etl_daily_zone_revenue AS SELECT order_date,zone,count(*) order_count,sum(net_revenue) net_revenue FROM etl_fact_orders JOIN etl_dim_store USING(store_id) WHERE order_status='DELIVERED' GROUP BY ALL")
        con.execute("CREATE OR REPLACE TABLE etl_category_margin AS SELECT category,count(*) order_count,sum(net_revenue) net_revenue,sum(gross_margin) gross_margin FROM etl_fact_orders JOIN etl_dim_product USING(product_id) WHERE order_status='DELIVERED' GROUP BY ALL")
        con.execute('CREATE TABLE IF NOT EXISTS etl_finance_history AS SELECT ?::VARCHAR AS snapshot_run_id,* FROM etl_fact_orders WHERE false',[run_id])
        con.execute('DELETE FROM etl_finance_history WHERE snapshot_run_id=?',[run_id])
        con.execute('INSERT INTO etl_finance_history SELECT ?,* FROM etl_fact_orders',[run_id])
        con.execute('COMMIT')
    result=dict(rows_loaded=len(tables['etl_fact_orders']),inserted=len(new-old),updated=len(old&new),current_orders=len(new))
    print('[Load]',result)
    return result

def write_quarantine(result,directory):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=True)
    run=re.sub(r'[^\w-]','_',str(result['run_id']))
    sources={'orders':result['failed'],**result['source_quarantine']}
    for source,payload in sources.items():
        frame=read_frame(payload)
        if frame.empty: continue
        if source=='customers':
            try: from include.pii_masking import mask_customer_evidence
            except ImportError: from pii_masking import mask_customer_evidence
            frame=mask_customer_evidence(frame)
        frame.to_csv(directory/f'{source}_failed_{result["phase"]}_{run}.csv',index=False)
    print(f'[Quarantine] {result["phase"]}: {result["failed_count"]} orders; source archives retain exact received text')
