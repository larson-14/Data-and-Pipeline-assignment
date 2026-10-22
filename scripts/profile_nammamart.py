import csv, json, re, shutil, hashlib
from pathlib import Path
from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / 'include/data/nammamart'
OUT = ROOT / 'assignment'
tables = {p.name: list(csv.DictReader(p.open(newline=''))) for p in sorted(DATA.glob('*.csv'))}
issues = []
keys = {'customers.csv':'customer_id','products.csv':'product_id','stores.csv':'store_id','payments.csv':'payment_id'}
def add(f, n, col, problem, dim, severity='ERROR'):
    r = tables[f][n-2]
    issues.append(dict(file=f, source_row=n, row_key=r.get(keys.get(f,'order_id'),''), column=col, problem=problem, quality_dimension=dim, severity=severity))
def number(s):
    try:
        d = Decimal(s)
        return d if d.is_finite() else None
    except InvalidOperation: return None
def date(s):
    for fmt in ('%Y-%m-%d %H:%M:%S','%Y-%m-%d'):
        try: return datetime.strptime(s,fmt)
        except ValueError: pass
    return None
mode = {'UPI','Card','Cash on Delivery','Wallet'}
indexes = {}
profile = {}
for f, rows in tables.items():
    key = keys.get(f,'order_id')
    idx = defaultdict(list)
    for n,r in enumerate(rows,2): idx[r[key]].append((n,r))
    indexes[f] = idx
    for k, group in idx.items():
        if len(group)>1:
            exact = len({tuple(r.values()) for _,r in group}) == 1
            for n,r in group: add(f,n,key,f'Duplicate key across {len(group)} rows ({"identical" if exact else "conflicting"} records)','Uniqueness')
    profile[f] = dict(rows=len(rows), columns=len(rows[0]), distinct_keys=len(idx), duplicate_excess=len(rows)-len(idx), blanks={c:sum(not r[c].strip() for r in rows) for c in rows[0]}, sha256=hashlib.sha256((DATA/f).read_bytes()).hexdigest())
    for n,r in enumerate(rows,2):
        for c,v in r.items():
            optional = c=='gateway_ref' and r.get('payment_mode')=='Cash on Delivery' or c=='delivery_minutes' and r.get('order_status','').upper()!='DELIVERED'
            if not v.strip() and not optional: add(f,n,c,'Missing value','Completeness')
            if v and v!=v.strip(): add(f,n,c,'Leading/trailing whitespace','Consistency','REVIEW')
            if v and (c.endswith('_date') or c.endswith('_ts')) and not date(v): add(f,n,c,'Invalid ISO date/timestamp','Validity')
            if v and c in ['quantity','unit_price','discount_pct','order_amount','delivery_minutes','amount_paid','mrp','cost_price','capacity_orders_per_day']:
                d=number(v)
                if d is None: add(f,n,c,'Invalid finite number','Validity')
                elif c in ['quantity','capacity_orders_per_day'] and (d<=0 or d!=int(d)): add(f,n,c,'Must be a positive integer','Validity')
                elif c=='discount_pct' and not 0<=d<=100: add(f,n,c,'Discount outside 0â€“100','Validity')
                elif c not in ['discount_pct','quantity','capacity_orders_per_day'] and d<0: add(f,n,c,'Negative value','Validity')
            if c=='payment_mode' and v not in mode: add(f,n,c,'Not in client payment-mode allowlist','Validity')
            if c=='order_status' and v not in {'DELIVERED','CANCELLED','RETURNED'}: add(f,n,c,'Not in client order-status allowlist','Validity')
            if c=='pincode' and not re.fullmatch(r'[1-9][0-9]{5}',v): add(f,n,c,'Not a six-digit Indian pincode','Validity')
        if f=='customers.csv':
            dt=date(r['signup_date'])
            if dt and dt>datetime(2026,9,30): add(f,n,'signup_date','Signup after September snapshot','Timeliness','REVIEW')
            if not re.fullmatch(r'[^\s@]+@[^\s@]+\.[^\s@]+',r['email']): add(f,n,'email','Malformed email (value redacted)','Validity')
            if not re.fullmatch(r'[6-9][0-9]{9}',r['phone']): add(f,n,'phone','Not a canonical 10-digit Indian mobile number (value redacted)','Validity','REVIEW')
            if r['loyalty_tier'] not in {'Bronze','Silver','Gold','Platinum'}: add(f,n,'loyalty_tier','Noncanonical tier spelling/case; inferred catalogue','Consistency','REVIEW')
        if f=='products.csv':
            dt=date(r['launch_date'])
            if dt and dt>datetime(2026,9,30): add(f,n,'launch_date','Launch after September snapshot','Timeliness','REVIEW')
            a,b=number(r['mrp']),number(r['cost_price'])
            if a is not None and b is not None and b>a: add(f,n,'cost_price','Cost price exceeds MRP','Consistency','REVIEW')
        if f.startswith('orders'):
            for c, parent in [('customer_id','customers.csv'),('product_id','products.csv'),('store_id','stores.csv')]:
                if r[c] and r[c] not in indexes.get(parent,{k:1 for k in [x[c] for x in tables[parent]]}): add(f,n,c,'No matching dimension key','Integrity')
            vals=[number(r[c]) for c in ['quantity','unit_price','discount_pct','order_amount']]
            if all(v is not None for v in vals):
                q,u,d,a=vals; expected=(q*u*(1-d/100)).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
                if a!=expected: add(f,n,'order_amount',f'Formula mismatch: expected {expected}, recorded {a}','Accuracy')
            dm=number(r['delivery_minutes'])
            if dm is not None and not 8<=dm<=35: add(f,n,'delivery_minutes','Outside normal 8â€“35 minute range','Validity','REVIEW')
            ts=date(r['order_ts'])
            if ts and not (datetime(2026,9,1)<=ts<datetime(2026,10,1)) and f=='orders.csv': add(f,n,'order_ts','Outside September reporting window','Timeliness')
            if ts and f.startswith('orders_') and r['order_id'] not in {x['order_id'] for x in tables['orders.csv']} and ts.date()!=datetime(2026,10,1).date(): add(f,n,'order_ts','New increment order not dated October 1','Timeliness')

orders=indexes['orders.csv']
for n,r in enumerate(tables['payments.csv'],2):
    matches=orders.get(r['order_id'],[])
    if not matches: add('payments.csv',n,'order_id','No matching base order','Integrity'); continue
    signatures={tuple(o.values()) for _,o in matches}
    if len(signatures)>1: add('payments.csv',n,'order_id','Conflicting parent order versions; reconciliation ambiguous','Integrity','REVIEW'); continue
    o=matches[0][1]
    if o['order_status']=='CANCELLED': add('payments.csv',n,'order_id','Payment exists for cancelled order','Consistency')
    if r['payment_status']=='SUCCESS' and number(r['amount_paid'])!=number(o['order_amount']): add('payments.csv',n,'amount_paid','SUCCESS payment does not equal base order amount','Accuracy')
    if r['payment_mode']!=o['payment_mode']: add('payments.csv',n,'payment_mode','Payment mode differs from base order','Consistency','REVIEW')
    pt,ot=date(r['payment_ts']),date(o['order_ts'])
    if pt and ot and pt<ot: add('payments.csv',n,'payment_ts','Payment precedes order timestamp','Consistency','REVIEW')
paid={r['order_id'] for r in tables['payments.csv']}
for n,o in enumerate(tables['orders.csv'],2):
    if o['order_status']=='DELIVERED' and o['order_id'] not in paid: add('orders.csv',n,'order_id','Delivered order has no payment export record','Completeness','REVIEW')
area_pin={r['area']:r['pincode'] for r in tables['stores.csv']}
for n,r in enumerate(tables['customers.csv'],2):
    if r['area'] in area_pin and r['pincode']!=area_pin[r['area']]: add('customers.csv',n,'pincode','Pincode differs from store master for same area','Consistency','REVIEW')
for c in ['email']:
    groups=defaultdict(list)
    for n,r in enumerate(tables['customers.csv'],2): groups[r[c].strip().lower()].append((n,r))
    for group in groups.values():
        if len({r['customer_id'] for _,r in group})>1:
            for n,r in group: add('customers.csv',n,c,'Shared email across distinct customer IDs (redacted); review identity','Uniqueness','REVIEW')
inc=tables['orders_2026-10-01_increment.csv']
updates=[]
for n,r in enumerate(inc,2):
    if r['order_id'] in orders:
        changes=sorted({c for _,old in orders[r['order_id']] for c in r if old[c]!=r[c]})
        updates.append(dict(order_id=r['order_id'],source_row=n,changed_columns=changes))
        if not changes: add('orders_2026-10-01_increment.csv',n,'order_id','Unchanged replay of base order','Consistency','REVIEW')
with (OUT/'DATA_ISSUES_LOG.csv').open('w',newline='') as h:
    w=csv.DictWriter(h,fieldnames=['file','source_row','row_key','column','problem','quality_dimension','severity']);w.writeheader();w.writerows(issues)
(OUT/'DATA_PROFILE.json').write_text(json.dumps(dict(files=profile,issues=len(issues),affected_rows=len({(i['file'],i['source_row']) for i in issues}),increment=dict(new=len(inc)-len(updates),corrections=len(updates),changes=updates)),indent=2))
lines=['# NammaMart source data profile','', 'Raw exports are copied byte-for-byte into `include/data/nammamart/`; no source values were edited. This profile is reproducible from unchanged source exports.','', '| File | Rows | Unique keys | Duplicate excess | Issue entries | Affected rows |','|---|---:|---:|---:|---:|---:|']
for f,p in profile.items():
    subset=[i for i in issues if i['file']==f]
    lines.append(f"| {f} | {p['rows']} | {p['distinct_keys']} | {p['duplicate_excess']} | {len(subset)} | {len({i['source_row'] for i in subset})} |")
lines += ['',f'Total: {sum(p["rows"] for p in profile.values()):,} source rows; {len(issues):,} issue entries on {len({(i["file"],i["source_row"]) for i in issues}):,} source rows. One row may have multiple issues. Duplicate groups flag every participating row; duplicate excess counts only copies beyond the first.','', '## Findings by check','', '| File | Problem | Severity | Entries |','|---|---|---|---:|']
for (f,problem,sev),count in sorted(Counter((i['file'],i['problem'],i['severity']) for i in issues).items()):
    if problem.startswith('Formula mismatch'): continue
    lines.append(f'| {f} | {problem} | {sev} | {count} |')
for f in tables:
    count=sum(i['file']==f and i['problem'].startswith('Formula mismatch') for i in issues)
    if count: lines.append(f'| {f} | Order amount formula mismatch | ERROR | {count} |')
lines += ['', '## Increment behavior', '',f'{len(updates)} existing order IDs are corrections, and {len(inc)-len(updates)} IDs are new. An overlap across feeds is expected and is not a uniqueness defect. Apply an upsert by order_id after resolving duplicates within the base feed. Payment reconciliation here uses the September base snapshot; a later returned status does not retroactively invalidate a September payment.', '', '## Scope and interpretation','', '- Source row is the CSV record number with the header counted as row 1. Keys are retained; names, emails and phone values are excluded from the logs.', '- ERROR denotes an explicit client rule or structural/format defect. REVIEW denotes an inferred convention, anomaly, or business question; do not automatically reject these rows.', '- Delivery outside 8â€“35 minutes is an anomaly, not proof of error. Missing delivery values are required only for delivered orders. Blank gateway references on Cash on Delivery are expected.', '- Money uses Decimal and half-up rounding to two decimals. The client should confirm its rounding convention.', '- Customer contact formats and tier spelling, positive store capacity, cost above MRP, shared emails, and area/pincode mappings need confirmation. Store area mappings are internal reference evidence, not authoritative postal validation.', '- No authoritative catalogue was supplied for channel, loyalty tiers, payment status, category, unit or zones; observed values cannot prove correctness. Factual accuracy and freshness require source-owner verification.', '- Resolve conflicting key versions with the source owner. Normalize approved aliases in a derived layer, quarantine invalid foreign keys/numeric values, retain rejected records and reconcile revenue only for canonical DELIVERED orders.', '- Raw customer data contains personal information; restrict access and use masked values in downstream reports. The full source bundle preserves the original personal data.', '', '## Reproduce','', 'Run `python scripts/profile_nammamart.py` from this project. Output is deterministic for unchanged inputs. SHA-256 hashes and per-column blank counts are in the JSON profile.']
(OUT/'DATA_PROFILE.md').write_text('\n'.join(lines)+'\n')
print(json.dumps(dict(files=profile,issues=len(issues),affected_rows=len({(i['file'],i['source_row']) for i in issues}),corrections=len(updates)),indent=2))

# Complete per-column statistics required by Part 0; no raw contacts are emitted.
numeric_columns={'quantity','unit_price','discount_pct','order_amount','delivery_minutes','amount_paid','mrp','cost_price','capacity_orders_per_day'}
date_columns={'order_ts','payment_ts','signup_date','launch_date','open_date'}
categories={'order_status','payment_mode','payment_status','loyalty_tier','zone','category','channel','is_active','is_perishable','unit'}
for filename,rows in tables.items():
    profile[filename]['numeric_ranges']={}
    profile[filename]['date_ranges']={}
    profile[filename]['categorical_values']={}
    for column in rows[0]:
        if column in numeric_columns:
            values=[v for r in rows if (v:=number(r[column])) is not None]
            profile[filename]['numeric_ranges'][column]={'min':str(min(values)) if values else None,'max':str(max(values)) if values else None}
        if column in date_columns:
            values=[v for r in rows if (v:=date(r[column])) is not None]
            profile[filename]['date_ranges'][column]={'min':min(values).isoformat() if values else None,'max':max(values).isoformat() if values else None}
        if column in categories:
            profile[filename]['categorical_values'][column]=sorted({r[column] for r in rows})
profile_document=dict(files=profile,issues=len(issues),affected_rows=len({(i['file'],i['source_row']) for i in issues}),increment=dict(new=len(inc)-len(updates),corrections=len(updates),changes=updates))
(OUT/'DATA_PROFILE.json').write_text(json.dumps(profile_document,indent=2),encoding='utf-8')
with (OUT/'DATA_PROFILE.md').open('a',encoding='utf-8') as out:
    out.write('\n## Complete column statistics\n\n[Machine-readable statistics](DATA_PROFILE.json). Blank means an empty/whitespace CSV field. Invalid numeric/date strings are separately listed in the issues log.\n')
    for filename,stats in profile.items():
        out.write('\n### '+filename+'\n\n| Column | Blank count |\n| --- | ---: |\n')
        for col,count in stats['blanks'].items():out.write(f'| {col} | {count} |\n')
        out.write('\n| Numeric/date column | Minimum | Maximum |\n| --- | --- | --- |\n')
        for col,bounds in {**stats['numeric_ranges'],**stats['date_ranges']}.items():out.write(f"| {col} | {bounds['min']} | {bounds['max']} |\n")
        out.write('\n| Category | Distinct source values |\n| --- | --- |\n')
        for col,values in stats['categorical_values'].items():out.write(f"| {col} | {', '.join(repr(v) for v in values)} |\n")
for issue in issues:
    issue['profiling_severity']=issue['severity']
    issue['severity']='Critical' if issue['severity']=='ERROR' else 'Warning'
    issue['proposed_rule']=issue['problem']+'; see the implemented rule contract and governance policy'
with (OUT/'DATA_ISSUES_LOG.csv').open('w',encoding='utf-8',newline='') as out:
    writer=csv.DictWriter(out,fieldnames=['file','source_row','row_key','column','problem','quality_dimension','proposed_rule','severity','profiling_severity'])
    writer.writeheader();writer.writerows(issues)
