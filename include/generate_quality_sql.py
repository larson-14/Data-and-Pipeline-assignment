"""Build reviewable SQL files from the SQL rule catalogue; never reads source data.

Run after changing this catalogue: python -m include.generate_quality_sql.
ELT tasks execute the resulting .sql files, with no pandas transformations.
"""
from pathlib import Path

RULES = {}
def rule(source,name,dimension,severity,condition):
    RULES.setdefault(source,[]).append((name,dimension,severity,condition))

KEYS=dict(customers='customer_id',products='product_id',stores='store_id',payments='payment_id',orders='order_id')
for source,key in KEYS.items():
    prefix=key.split('_')[0]
    rule(source,prefix+'_key_required','Completeness','Critical',f"trim({key}) = ''")
    rule(source,prefix+'_duplicate','Uniqueness','Critical' if source in ('orders','payments') else 'Warning',f'count(*) OVER (PARTITION BY upper(trim({key}))) > 1')
for name,col,pattern in [('phone','phone','[0-9]{10}'),('email','email',r'[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}'),('pincode','pincode','[0-9]{6}')]:
    rule('customers','customer_'+name,'Validity','Warning',f"NOT regexp_full_match(trim({col}), '{pattern}')")
rule('customers','customer_loyalty_casing','Consistency','Warning',"loyalty_tier <> title_case(upper(trim(loyalty_tier)))")
rule('customers','customer_signup_date','Validity','Critical','try_cast(signup_date AS TIMESTAMPTZ) IS NULL')
rule('customers','customer_future_signup','Timeliness','Critical','try_cast(signup_date AS TIMESTAMPTZ) > (SELECT checked_at FROM pipeline_context)')
rule('products','product_category_required','Completeness','Critical',"trim(category) = ''")
rule('products','product_mrp','Accuracy','Critical','NOT coalesce(isfinite(try_cast(mrp AS DOUBLE)) AND try_cast(mrp AS DOUBLE)>0, false)')
rule('products','product_cost_mrp','Accuracy','Critical','NOT coalesce(isfinite(try_cast(cost_price AS DOUBLE)) AND try_cast(cost_price AS DOUBLE)>=0 AND try_cast(cost_price AS DOUBLE)<=try_cast(mrp AS DOUBLE),false)')
rule('stores','store_zone','Validity','Critical',"upper(trim(zone)) NOT IN ('NORTH','SOUTH','EAST','WEST')")
rule('stores','store_capacity','Accuracy','Critical','NOT coalesce(isfinite(try_cast(capacity_orders_per_day AS DOUBLE)) AND try_cast(capacity_orders_per_day AS DOUBLE)>0 AND try_cast(capacity_orders_per_day AS DOUBLE)%1=0,false)')
rule('stores','store_manager','Completeness','Warning',"trim(manager_name) = ''")
rule('stores','store_pincode','Validity','Warning',"NOT regexp_full_match(trim(pincode),'[0-9]{6}')")
rule('payments','payment_order_key','Consistency','Critical',"NOT EXISTS (SELECT 1 FROM raw_orders o WHERE o._run_id=source._run_id AND upper(trim(o.order_id))=upper(trim(source.order_id)))")
rule('payments','payment_mode','Validity','Critical',"upper(trim(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET')")
rule('payments','payment_status','Validity','Critical',"upper(trim(payment_status)) NOT IN ('SUCCESS','FAILED','REFUNDED')")
rule('payments','payment_amount','Accuracy','Critical','NOT coalesce(isfinite(try_cast(amount_paid AS DOUBLE)) AND try_cast(amount_paid AS DOUBLE)>=0,false)')
rule('payments','payment_timestamp','Validity','Critical','try_cast(payment_ts AS TIMESTAMPTZ) IS NULL')
rule('orders','order_customer_required','Completeness','Critical',"trim(customer_id) = ''")
rule('orders','order_payment_mode','Validity','Critical',"upper(trim(payment_mode)) NOT IN ('UPI','CARD','CASH ON DELIVERY','WALLET')")
rule('orders','order_status','Validity','Critical',"upper(trim(order_status)) NOT IN ('DELIVERED','CANCELLED','RETURNED')")
rule('orders','order_timestamp','Validity','Critical','try_cast(order_ts AS TIMESTAMPTZ) IS NULL')
rule('orders','order_future_timestamp','Timeliness','Critical','try_cast(order_ts AS TIMESTAMPTZ) > (SELECT checked_at FROM pipeline_context)')
rule('orders','order_quantity','Accuracy','Critical','NOT coalesce(isfinite(try_cast(quantity AS DOUBLE)) AND try_cast(quantity AS DOUBLE)>0 AND try_cast(quantity AS DOUBLE)%1=0,false)')
rule('orders','order_unit_price','Accuracy','Critical','NOT coalesce(isfinite(try_cast(unit_price AS DOUBLE)) AND try_cast(unit_price AS DOUBLE)>=0,false)')
rule('orders','order_discount','Validity','Critical','NOT coalesce(isfinite(try_cast(discount_pct AS DOUBLE)) AND try_cast(discount_pct AS DOUBLE) BETWEEN 0 AND 100,false)')
MONEY="round(try_cast(try_cast(quantity AS DOUBLE)*try_cast(unit_price AS DOUBLE)*(100-try_cast(discount_pct AS DOUBLE))/100 AS DECIMAL(30,8)),2)::DECIMAL(18,2)"
rule('orders','order_amount','Accuracy','Critical',f'NOT coalesce(try_cast(order_amount AS DECIMAL(18,2)) = {MONEY},false)')
rule('orders','order_delivery_required','Completeness','Critical',"upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE) IS NULL")
rule('orders','order_delivery_limit','Accuracy','Critical',"try_cast(delivery_minutes AS DOUBLE) IS NOT NULL AND (NOT isfinite(try_cast(delivery_minutes AS DOUBLE)) OR try_cast(delivery_minutes AS DOUBLE)<0 OR try_cast(delivery_minutes AS DOUBLE)>=120)")
for source,key in [('customers','customer_id'),('products','product_id'),('stores','store_id')]:
    prefix=key.split('_')[0]
    rule('orders','order_'+prefix+'_key','Consistency','Critical',f'NOT EXISTS (SELECT 1 FROM stg_{source} m WHERE m.{key}=upper(trim(source.{key})))')
    for suffix,severity in [('quality','Critical'),('warning','Warning')]:
        rule('orders','order_'+prefix+'_'+suffix,'Consistency',severity,f"EXISTS (SELECT 1 FROM elt_quality_rows q WHERE q.run_id=source._run_id AND q.source_file='{source}.csv' AND upper(trim(q.row_key))=upper(trim(source.{key})) AND q.failed AND q.severity='{severity}')")
rule('orders','order_payment_quality','Consistency','Critical',"EXISTS (SELECT 1 FROM stg_payments p WHERE p._run_id=source._run_id AND p.order_id=upper(trim(source.order_id)) AND p.dq_status='Critical')")
rule('orders','order_payment_amount','Consistency','Critical',"EXISTS (SELECT 1 FROM raw_payments p WHERE p._run_id=source._run_id AND upper(trim(p.order_id))=upper(trim(source.order_id)) AND upper(trim(p.payment_status))='SUCCESS' AND NOT coalesce(try_cast(p.amount_paid AS DECIMAL(18,2))=try_cast(source.order_amount AS DECIMAL(18,2)),false))")

SELECTS = {
'customers': """upper(trim(customer_id)) AS customer_id, title_case(full_name) AS full_name,
CASE WHEN contains(email,'@') THEN left(lower(trim(split_part(email,'@',1))),2)||'***@'||lower(trim(split_part(email,'@',2))) ELSE '***' END AS email,
CASE WHEN length(trim(phone))>4 THEN left(trim(phone),2)||repeat('*',length(trim(phone))-4)||right(trim(phone),2) ELSE '****' END AS phone,
title_case(city) AS city,title_case(area) AS area,trim(pincode) AS pincode,try_cast(signup_date AS DATE) AS signup_date,
upper(trim(loyalty_tier)) AS loyalty_tier,upper(trim(is_active)) AS is_active""",
'products': """upper(trim(product_id)) AS product_id,title_case(product_name) AS product_name,title_case(category) AS category,
title_case(sub_category) AS sub_category,title_case(brand) AS brand,try_cast(mrp AS DECIMAL(18,2)) AS mrp,
try_cast(cost_price AS DECIMAL(18,2)) AS cost_price,lower(trim(unit)) AS unit,try_cast(is_perishable AS BOOLEAN) AS is_perishable,try_cast(launch_date AS DATE) AS launch_date""",
'stores': """upper(trim(store_id)) AS store_id,title_case(store_name) AS store_name,title_case(zone) AS zone,title_case(area) AS area,
trim(pincode) AS pincode,try_cast(open_date AS DATE) AS open_date,title_case(manager_name) AS manager_name,try_cast(capacity_orders_per_day AS BIGINT) AS capacity_orders_per_day""",
'payments': """upper(trim(payment_id)) AS payment_id,upper(trim(order_id)) AS order_id,try_cast(payment_ts AS TIMESTAMPTZ) AS payment_ts,
upper(trim(payment_mode)) AS payment_mode,try_cast(amount_paid AS DECIMAL(18,2)) AS amount_paid,upper(trim(payment_status)) AS payment_status,trim(gateway_ref) AS gateway_ref""",
'orders': f"""upper(trim(order_id)) AS order_id,timezone('Asia/Kolkata',try_cast(order_ts AS TIMESTAMPTZ)) AS order_ts,
cast(timezone('Asia/Kolkata',try_cast(order_ts AS TIMESTAMPTZ)) AS DATE) AS order_date,
upper(trim(customer_id)) AS customer_id,upper(trim(product_id)) AS product_id,upper(trim(store_id)) AS store_id,
try_cast(quantity AS BIGINT) AS quantity,try_cast(unit_price AS DECIMAL(18,2)) AS unit_price,try_cast(discount_pct AS DOUBLE) AS discount_pct,
try_cast(order_amount AS DECIMAL(18,2)) AS order_amount,upper(trim(payment_mode)) AS payment_mode,upper(trim(order_status)) AS order_status,
title_case(channel) AS channel,try_cast(delivery_minutes AS DOUBLE) AS delivery_minutes,
CASE WHEN upper(trim(order_status))='DELIVERED' THEN {MONEY} ELSE 0::DECIMAL(18,2) END AS net_revenue,
upper(trim(order_status))='DELIVERED' AND try_cast(delivery_minutes AS DOUBLE)<=15 AS within_15_min"""
}

def stage(source):
    key=KEYS[source]
    checks=',\n'.join(f"dq_check('{n}','{d}','{s}',{c})" for n,d,s,c in RULES[source])
    where="_run_id=(SELECT run_id FROM pipeline_context)"
    if source=='orders': where+=" AND _source_file=CASE WHEN (SELECT phase FROM pipeline_context)='base' THEN 'orders.csv' ELSE 'orders_2026-10-01_increment.csv' END"
    script=f"""
CREATE TEMP TABLE {source}_checks AS
SELECT *, [{checks}] AS checks FROM
(SELECT rowid AS source_row,row_number() OVER (ORDER BY rowid) AS source_csv_row,* FROM raw_{source} WHERE {where}) source;
INSERT INTO elt_quality_rows
SELECT _run_id,(SELECT phase FROM pipeline_context),_source_file,source_row,{key},
x.check_name,x.dimension,x.severity,x.failed,(SELECT checked_at FROM pipeline_context)
FROM {source}_checks,unnest(checks) t(x);
CREATE TEMP TABLE incoming_{source} AS
WITH flagged AS (SELECT *,list_transform(list_filter(checks,x->x.failed AND x.severity='Critical'),x->x.check_name) AS critical,
list_transform(list_filter(checks,x->x.failed AND x.severity='Warning'),x->x.check_name) AS warnings FROM {source}_checks)
SELECT {SELECTS[source]},
CASE WHEN len(critical)>0 THEN 'Critical' WHEN len(warnings)>0 THEN 'Warning' ELSE 'Passed' END AS dq_status,
array_to_string(critical,'; ') AS failure_reason,array_to_string(warnings,'; ') AS warning_reason,
source_csv_row AS _source_row,_source_file,_loaded_at,_run_id
FROM flagged QUALIFY row_number() OVER (PARTITION BY upper(trim({key})) ORDER BY source_row)=1;
"""
    if source!='orders': return script+f'CREATE OR REPLACE TABLE stg_{source} AS SELECT * FROM incoming_{source};\n'
    return script+"""
CREATE TABLE IF NOT EXISTS stg_orders AS SELECT * FROM incoming_orders WHERE false;
DELETE FROM stg_orders WHERE (SELECT phase FROM pipeline_context)='base';
MERGE INTO stg_orders t USING incoming_orders s ON t.order_id=s.order_id
WHEN MATCHED THEN UPDATE BY NAME WHEN NOT MATCHED THEN INSERT BY NAME;
"""

def generate():
    directory=Path(__file__).resolve().parent/'sql'
    pre="""-- Generated by include/generate_quality_sql.py. All transformations execute in DuckDB.
CREATE OR REPLACE MACRO title_case(v) AS array_to_string(list_transform(string_split(trim(v),' '),w->upper(left(w,1))||lower(substr(w,2))),' ');
CREATE OR REPLACE MACRO dq_check(n,d,s,f) AS struct_pack(check_name:=n,dimension:=d,severity:=s,failed:=coalesce(f,false));
CREATE TABLE IF NOT EXISTS elt_quality_rows(run_id VARCHAR,phase VARCHAR,source_file VARCHAR,source_row BIGINT,row_key VARCHAR,check_name VARCHAR,dimension VARCHAR,severity VARCHAR,failed BOOLEAN,checked_at TIMESTAMPTZ);
DELETE FROM elt_quality_rows WHERE run_id=(SELECT run_id FROM pipeline_context) AND phase='base';
"""
    quarantine=[]
    for source in KEYS:
        quarantine.append(f"SELECT q.*,q.check_name AS failure_reason,to_json(r) AS raw_record FROM elt_quality_rows q JOIN raw_{source} r ON q.source_row=r.rowid AND q.run_id=r._run_id AND q.source_file=r._source_file WHERE q.severity='Critical' AND q.failed")
    (directory/'stg_masters.sql').write_text(pre+''.join(stage(s) for s in KEYS if s!='orders')+'CREATE OR REPLACE VIEW elt_quarantine AS\n'+'\nUNION ALL\n'.join(quarantine)+';\n',encoding='utf-8')
    (directory/'stg_orders.sql').write_text("DELETE FROM elt_quality_rows WHERE run_id=(SELECT run_id FROM pipeline_context) AND phase=(SELECT phase FROM pipeline_context) AND source_file LIKE 'orders%';\n"+stage('orders'),encoding='utf-8')
    catalog=',\n'.join(f"('{n}','{d}','{s}')" for rules in RULES.values() for n,d,s,c in rules)
    (directory/'dq_results.sql').write_text("""CREATE TABLE IF NOT EXISTS dq_results(run_id VARCHAR,dag_id VARCHAR,check_name VARCHAR,dimension VARCHAR,severity VARCHAR,rows_checked BIGINT,rows_failed BIGINT,status VARCHAR,checked_at TIMESTAMPTZ);
DELETE FROM dq_results WHERE run_id=(SELECT run_id FROM pipeline_context) AND starts_with(check_name,(SELECT phase||'.' FROM pipeline_context));
INSERT INTO dq_results WITH catalog(check_name,dimension,severity) AS (VALUES """+catalog+""")
SELECT c.run_id,c.dag_id,c.phase||'.'||k.check_name,k.dimension,k.severity,count(q.source_row),count(q.source_row) FILTER(WHERE q.failed),
CASE WHEN coalesce(bool_or(q.failed),false) THEN CASE WHEN k.severity='Critical' THEN 'Failed' ELSE 'Warning' END ELSE 'Passed' END,c.checked_at
FROM pipeline_context c CROSS JOIN catalog k LEFT JOIN elt_quality_rows q ON q.run_id=c.run_id AND q.phase=c.phase AND q.check_name=k.check_name
WHERE c.phase='base' OR starts_with(k.check_name,'order_') GROUP BY c.run_id,c.dag_id,c.phase,k.check_name,k.dimension,k.severity,c.checked_at;
""",encoding='utf-8')

if __name__=='__main__': generate()
