"""Pandas quality checks. Original text is preserved in rejection evidence."""
from datetime import datetime, timezone
from io import StringIO
import numpy as np
import pandas as pd
try:
    from include.data_contract import KEYS, MODES, SCHEMAS, check_schema, money, order_money
except ImportError:
    from data_contract import KEYS, MODES, SCHEMAS, check_schema, money, order_money

def read_frame(payload):
    return pd.read_json(StringIO(payload), convert_dates=False, dtype=False).reset_index(drop=True)

def run_validation_checks(orders_json, payments_json, customers_json, products_json, stores_json,
                          run_id=None, dag_id='nammamart_etl_sudatta', checked_at=None,
                          phase='base', reference_order_ids=None):
    now=pd.Timestamp(checked_at or datetime.now(timezone.utc))
    now=now.tz_localize('UTC') if now.tzinfo is None else now.tz_convert('UTC')
    raw={n:read_frame(v) for n,v in zip(SCHEMAS,(orders_json,payments_json,customers_json,products_json,stores_json))}
    for n,f in raw.items(): check_schema(n,f.columns)
    frames={n:f.copy() for n,f in raw.items()}
    for frame in frames.values():
        for col in frame: frame[col]=frame[col].astype('string').fillna('').str.strip()
        for col in set(frame)&{'order_id','payment_id','customer_id','product_id','store_id','payment_mode','payment_status','order_status','loyalty_tier','zone'}:
            frame[col]=frame[col].str.upper()
    for name,cols in {'orders':['quantity','unit_price','discount_pct','order_amount','delivery_minutes'],
                      'payments':['amount_paid'],'products':['mrp','cost_price'],'stores':['capacity_orders_per_day']}.items():
        for col in cols: frames[name][col]=pd.to_numeric(frames[name][col],errors='coerce').astype(float)
    o,p,c,pr,s=(frames[n] for n in SCHEMAS)
    failures={n:[[] for _ in range(len(f))] for n,f in frames.items()}
    warnings={n:[[] for _ in range(len(f))] for n,f in frames.items()}
    results,summary=[],{}
    def check(source,name,dimension,severity,mask):
        mask=pd.Series(mask,index=frames[source].index).fillna(True).astype(bool)
        records=failures if severity=='Critical' else warnings
        for i in mask[mask].index: records[source][i].append(name)
        results.append(dict(run_id=run_id,dag_id=dag_id,check_name=f'{phase}.{name}',dimension=dimension,severity=severity,
            rows_checked=len(mask),rows_failed=int(mask.sum()),status=('Failed' if severity=='Critical' else 'Warning') if mask.any() else 'Passed',checked_at=now.isoformat()))
        if mask.any(): summary[name]=[f'{int(mask.sum())} {source} rows: {severity}']
    def dates(source,column):
        values=raw[source][column]
        if pd.api.types.is_numeric_dtype(values): return pd.to_datetime(values,unit='ms',utc=True,errors='coerce')
        def parse(v):
            try:
                t=pd.Timestamp(v)
                if pd.isna(t): return pd.NaT
                return (t.tz_localize('Asia/Kolkata') if t.tzinfo is None else t).tz_convert('UTC')
            except (ValueError,TypeError): return pd.NaT
        return pd.to_datetime(values.map(parse),utc=True,errors='coerce')
    for name,frame in frames.items():
        prefix=KEYS[name].split('_')[0]
        check(name,prefix+'_key_required','Completeness','Critical',frame[KEYS[name]].eq(''))
        check(name,prefix+'_duplicate','Uniqueness','Critical' if name in ('orders','payments') else 'Warning',frame[KEYS[name]].duplicated(keep=False))
    check('customers','customer_phone','Validity','Warning',~c.phone.str.fullmatch(r'[0-9]{10}'))
    check('customers','customer_email','Validity','Warning',~c.email.str.fullmatch(r'[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}'))
    check('customers','customer_pincode','Validity','Warning',~c.pincode.str.fullmatch(r'[0-9]{6}'))
    check('customers','customer_loyalty_casing','Consistency','Warning',raw['customers'].loyalty_tier.astype('string').fillna('').ne(c.loyalty_tier.str.capitalize()))
    signup=dates('customers','signup_date')
    check('customers','customer_signup_date','Validity','Critical',signup.isna())
    check('customers','customer_future_signup','Timeliness','Critical',signup>now)
    check('products','product_category_required','Completeness','Critical',pr.category.eq(''))
    check('products','product_mrp','Accuracy','Critical',~(np.isfinite(pr.mrp)&pr.mrp.gt(0)))
    check('products','product_cost_mrp','Accuracy','Critical',~(np.isfinite(pr.cost_price)&pr.cost_price.ge(0)&pr.cost_price.le(pr.mrp)))
    check('stores','store_zone','Validity','Critical',~s.zone.isin(['NORTH','SOUTH','EAST','WEST']))
    check('stores','store_capacity','Accuracy','Critical',~(np.isfinite(s.capacity_orders_per_day)&s.capacity_orders_per_day.gt(0)&s.capacity_orders_per_day.mod(1).eq(0)))
    check('stores','store_manager','Completeness','Warning',s.manager_name.eq(''))
    check('stores','store_pincode','Validity','Warning',~s.pincode.str.fullmatch(r'[0-9]{6}'))
    check('payments','payment_order_key','Consistency','Critical',~p.order_id.isin(reference_order_ids if reference_order_ids is not None else set(o.order_id)))
    check('payments','payment_mode','Validity','Critical',~p.payment_mode.isin(MODES))
    check('payments','payment_status','Validity','Critical',~p.payment_status.isin(['SUCCESS','FAILED','REFUNDED']))
    check('payments','payment_amount','Accuracy','Critical',~(np.isfinite(p.amount_paid)&p.amount_paid.ge(0)))
    check('payments','payment_timestamp','Validity','Critical',dates('payments','payment_ts').isna())
    check('orders','order_customer_required','Completeness','Critical',o.customer_id.eq(''))
    check('orders','order_payment_mode','Validity','Critical',~o.payment_mode.isin(MODES))
    check('orders','order_status','Validity','Critical',~o.order_status.isin(['DELIVERED','CANCELLED','RETURNED']))
    ts=dates('orders','order_ts')
    check('orders','order_timestamp','Validity','Critical',ts.isna())
    check('orders','order_future_timestamp','Timeliness','Critical',ts>now)
    check('orders','order_quantity','Accuracy','Critical',~(np.isfinite(o.quantity)&o.quantity.gt(0)&o.quantity.mod(1).eq(0)))
    check('orders','order_unit_price','Accuracy','Critical',~(np.isfinite(o.unit_price)&o.unit_price.ge(0)))
    check('orders','order_discount','Validity','Critical',~(np.isfinite(o.discount_pct)&o.discount_pct.between(0,100)))
    expected=pd.Series([order_money(*v) for v in o[['quantity','unit_price','discount_pct']].itertuples(index=False,name=None)],index=o.index,dtype=object)
    actual=o.order_amount.map(money)
    check('orders','order_amount','Accuracy','Critical',expected.isna()|actual.isna()|actual.ne(expected))
    check('orders','order_delivery_required','Completeness','Critical',o.order_status.eq('DELIVERED')&o.delivery_minutes.isna())
    check('orders','order_delivery_limit','Accuracy','Critical',o.delivery_minutes.notna()&(~np.isfinite(o.delivery_minutes)|o.delivery_minutes.lt(0)|o.delivery_minutes.ge(120)))
    for name,key in [('customers','customer_id'),('products','product_id'),('stores','store_id')]:
        prefix=key.split('_')[0]
        check('orders','order_'+prefix+'_key','Consistency','Critical',~o[key].isin(frames[name][key]))
        invalid=set(frames[name].loc[[bool(v) for v in failures[name]],key])
        check('orders','order_'+prefix+'_quality','Consistency','Critical',o[key].isin(invalid))
        warned=set(frames[name].loc[[bool(v) for v in warnings[name]],key])
        check('orders','order_'+prefix+'_warning','Consistency','Warning',o[key].isin(warned))
    bad_payments=set(p.loc[[bool(v) for v in failures['payments']],'order_id'])
    check('orders','order_payment_quality','Consistency','Critical',o.order_id.isin(bad_payments))
    success=p[p.payment_status.eq('SUCCESS')].merge(o[['order_id','order_amount']],on='order_id')
    bad=success.amount_paid.map(money).ne(success.order_amount.map(money))|success.amount_paid.map(money).isna()
    check('orders','order_payment_amount','Consistency','Critical',o.order_id.isin(set(success.loc[bad,'order_id'])))
    source_file='orders.csv' if phase=='base' else 'orders_2026-10-01_increment.csv'
    def evidence(name,masks,severity):
        mask=[bool(v) for v in masks]; frame=raw[name].loc[mask].copy()
        field='failure' if severity=='Critical' else 'warning'
        frame[field+'_reasons']=[v for v in masks if v]
        frame[field+'_reason']=['; '.join(v) for v in masks if v]
        frame['source_file']=source_file if name=='orders' else name+'.csv'
        frame['source_row']=np.arange(1,len(raw[name])+1)[mask]; frame['severity']=severity
        return frame
    failed=evidence('orders',failures['orders'],'Critical')
    warning=evidence('orders',[w if not f else [] for w,f in zip(warnings['orders'],failures['orders'])],'Warning')
    def accepted_rows(mask):
        frame=o.loc[mask].copy()
        frame['order_ts']=ts.loc[frame.index].dt.tz_convert('Asia/Kolkata').dt.tz_localize(None)
        frame['_source_file']=source_file; frame['_source_row']=frame.index+1; frame['_run_id']=run_id
        return frame
    accepted=accepted_rows([not bool(v) for v in failures['orders']])
    operational=accepted_rows([not any(r not in ('order_payment_quality','order_payment_amount') for r in v) for v in failures['orders']])
    quarantine={n:evidence(n,failures[n],'Critical').to_json(date_format='iso') for n in frames if n!='orders' and any(failures[n]) and phase=='base'}
    metrics={n+'.csv':dict(rows_extracted=len(f),rows_passed=sum(not v for v in failures[n]),rows_quarantined=sum(bool(v) for v in failures[n]),
        rows_warning=sum(bool(w) and not v for w,v in zip(warnings[n],failures[n]))) for n,f in frames.items()}
    if phase=='increment':
        results=[r for r in results if r['check_name'].split('.',1)[1].startswith('order_')]
        metrics={source_file:metrics['orders.csv']}
    result=dict(passed=accepted.to_json(date_format='iso'),operational=operational.to_json(date_format='iso'),failed=failed.to_json(date_format='iso'),warning=warning.to_json(date_format='iso'),
        passed_count=len(accepted),failed_count=len(failed),warning_count=len(warning),summary=summary,checked_at=now.isoformat(),
        dq_results=results,source_quarantine=quarantine,source_metrics=metrics,phase=phase,run_id=run_id)
    for name,frame in frames.items():
        frame['_critical']=[bool(v) for v in failures[name]]
        result[name]=frame.to_json(date_format='iso')
    print(f'[Validate] {phase}: passed={len(accepted)}, failed={len(failed)}, warning={len(warning)}')
    return result
