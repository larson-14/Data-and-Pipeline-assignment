"""Generate the data dictionary and worked lineage/audit examples in temporary DBs.

Run: python -m include.governance_examples
"""

import contextlib
import csv
import hashlib
import io
import json
import warnings
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pandas as pd

from include.elt_warehouse import load_raw_files, run_sql
from include.verify_elt import build_etl_reference, ROOT, DATA_DIR, SQL_DIR


def generate():
    exports = {}
    for name in ('orders', 'payments', 'customers', 'products', 'stores'):
        with (DATA_DIR / (name + '.csv')).open(encoding='utf-8-sig', newline='') as source:
            exports[name] = list(csv.DictReader(source))
    with (DATA_DIR / 'orders_2026-10-01_increment.csv').open(encoding='utf-8-sig', newline='') as source:
        exports['orders_2026-10-01_increment'] = list(csv.DictReader(source))
    with TemporaryDirectory() as directory, warnings.catch_warnings():
        warnings.simplefilter('ignore')
        etl_db, elt_db = Path(directory) / 'etl.duckdb', Path(directory) / 'elt.duckdb'
        with contextlib.redirect_stdout(io.StringIO()):
            etl_result = build_etl_reference(etl_db)
            load_raw_files(elt_db, [DATA_DIR / (name + '.csv') for name in ('orders', 'payments', 'customers', 'products', 'stores')], 'governance')
            run_sql(elt_db, SQL_DIR, ['stg_masters.sql', 'stg_orders.sql', 'dq_results.sql', 'marts.sql', 'baseline_snapshot.sql'],
                    'governance', checked_at=datetime.now(timezone.utc))
            load_raw_files(elt_db, [DATA_DIR / 'orders_2026-10-01_increment.csv'], 'governance')
            run_sql(elt_db, SQL_DIR, ['stg_orders.sql', 'dq_results.sql', 'marts.sql'], 'governance',
                    phase='increment', checked_at=datetime.now(timezone.utc))
        with duckdb.connect(str(elt_db), read_only=True) as con:
            chosen = con.execute("""
                SELECT order_date, zone, store_id, store_name, payment_mode, order_count, net_revenue
                FROM mart_daily_store_kpis WHERE order_date = DATE '2026-09-03'
                ORDER BY order_count DESC, store_id, payment_mode LIMIT 1
            """).fetchone()
            date, zone, store_id, store_name, mode, count, amount = chosen
            contributions = con.execute("""
                SELECT order_id, _source_file, _source_row, customer_id, product_id, store_id, net_revenue
                FROM elt_reconciled_orders WHERE order_date = ? AND store_id = ? AND payment_mode = ?
                ORDER BY order_id
            """, [date, store_id, mode]).fetchall()
            assert len(contributions) == count
            assert abs(sum(row[-1] for row in contributions) - amount) < 0.000001
            rejected_position, rejected_raw = next((position, row) for position, row in enumerate(exports['orders'], 1)
                                                   if row['quantity'] and float(row['quantity']) <= 0)
            rejected_id = rejected_raw['order_id']
            raw_json, rule = con.execute("""
                SELECT raw_record, check_name FROM elt_quarantine
                WHERE source_file = 'orders.csv' AND row_key = ? AND check_name = 'order_quantity' LIMIT 1
            """, [rejected_id]).fetchone()
            raw_evidence = json.loads(raw_json)
            assert all(raw_evidence[column] == value for column, value in rejected_raw.items())
            elt_in_revenue = con.execute('SELECT count(*) FROM elt_reconciled_orders WHERE order_id = ?', [rejected_id]).fetchone()[0]
            assert elt_in_revenue == 0
        with duckdb.connect(str(etl_db), read_only=True) as con:
            dictionary = {table: con.execute('DESCRIBE ' + table).fetchall() for table in
                          ('etl_fact_orders', 'etl_dim_customer', 'etl_dim_product', 'etl_dim_store')}
            etl_in_revenue = con.execute('SELECT count(*) FROM etl_fact_orders WHERE order_id = ?', [rejected_id]).fetchone()[0]
            assert etl_in_revenue == 0
        failed = pd.read_json(io.StringIO(etl_result['failed']))
        rejected_etl = failed[failed['order_id'] == rejected_id].iloc[0]
        assert 'order_quantity' in rejected_etl['failure_reasons']
        assert rejected_etl['source_row'] == rejected_position
        ledger = []
        for order_id, source_file, source_row, customer_id, product_id, store_id, revenue in contributions:
            raw = exports[Path(source_file).stem][source_row - 1]
            assert raw['order_id'] == order_id
            customer_row = next(index for index, row in enumerate(exports['customers'], 1) if row['customer_id'].strip().upper() == customer_id)
            product_row = next(index for index, row in enumerate(exports['products'], 1) if row['product_id'].strip().upper() == product_id)
            store_row = next(index for index, row in enumerate(exports['stores'], 1) if row['store_id'].strip().upper() == store_id)
            payment_rows = [index for index, row in enumerate(exports['payments'], 1) if row['order_id'].strip().upper() == order_id]
            ledger.append([order_id, source_file, source_row, raw['quantity'], raw['unit_price'], raw['discount_pct'],
                           raw['order_amount'], revenue, customer_row, product_row, store_row,
                           ';'.join(map(str, payment_rows))])

    output_dir = ROOT / 'assignment'
    headers = ['order_id', 'orders_source_file', 'orders_data_row', 'quantity_as_received', 'unit_price_as_received',
               'discount_pct_as_received', 'order_amount_as_received', 'net_revenue', 'customers_data_row',
               'products_data_row', 'stores_data_row', 'payments_data_rows']
    with (output_dir / 'LINEAGE_EXAMPLE.csv').open('w', encoding='utf-8', newline='') as output:
        writer = csv.writer(output)
        writer.writerow(headers)
        writer.writerows(ledger)
    audit = {'order_id':rejected_id, 'source_file':'orders.csv', 'source_data_row':rejected_position,
             'source_sha256':hashlib.sha256((DATA_DIR / 'orders.csv').read_bytes()).hexdigest(),
             'as_received':rejected_raw, 'etl_failure_reasons':rejected_etl['failure_reasons'],
             'elt_rule':rule, 'etl_fact_rows':etl_in_revenue, 'elt_revenue_rows':elt_in_revenue}
    (output_dir / 'AUDIT_EXAMPLE.json').write_text(json.dumps(audit, indent=2), encoding='utf-8')

    meanings = {
        'order_id':'Order key; one row per accepted order', 'customer_id':'Customer key; links orders to CRM',
        'product_id':'Product key; links orders to catalogue', 'store_id':'Store key; links orders to store master',
        'order_ts':'Order creation timestamp', 'order_date':'Calendar date derived from order_ts',
        'quantity':'Units ordered', 'unit_price':'Selling price per unit in INR',
        'discount_pct':'Discount applied to the order', 'order_amount':'Source billed amount in INR',
        'payment_mode':'Standardised payment method', 'order_status':'Standardised order state',
        'channel':'Order entry channel', 'delivery_minutes':'Delivery duration in minutes',
        'net_revenue':'quantity Ã— unit_price Ã— (1 âˆ’ discount_pct / 100)',
        'gross_margin':'net_revenue âˆ’ quantity Ã— product cost_price',
        'within_15_min':'Whether delivery_minutes â‰¤ 15', 'ingested_at':'Warehouse insertion timestamp',
        'full_name':'Customer name, trimmed and title-cased; unmasked', 'email':'Masked email; first two local characters and domain retained',
        'phone':'Masked phone; first two and last two characters retained', 'city':'Customer city',
        'area':'Customer or store locality', 'pincode':'Postal code; ETL type is inferred from the input column',
        'signup_date':'CRM account creation date', 'loyalty_tier':'Standardised loyalty level',
        'is_active':'CRM active-account indicator', 'product_name':'Catalogue item name',
        'category':'Product category', 'sub_category':'Product subcategory', 'brand':'Product brand',
        'mrp':'Maximum retail price per unit in INR', 'cost_price':'Product acquisition cost per unit in INR',
        'unit':'Selling unit', 'is_perishable':'Whether the product is perishable', 'launch_date':'Product launch date',
        'store_name':'Dark-store name', 'zone':'Store operating zone', 'open_date':'Store opening date',
        'manager_name':'Store manager name; unmasked employee personal data',
        'capacity_orders_per_day':'Planned daily order capacity',
        '_source_file':'Original export filename','_source_row':'One-based data row in source file','_run_id':'Airflow run identifier',
    }
    expected = {
        'order_id':'ORDâ€¦; unique primary key', 'customer_id':'Câ€¦; primary / foreign key',
        'product_id':'Pâ€¦; primary / foreign key', 'store_id':'Sâ€¦; primary / foreign key',
        'quantity':'> 0', 'unit_price':'Finite, nonnegative INR amount',
        'discount_pct':'Business contract: 0â€“100; no dedicated ETL range check',
        'order_amount':'Equals half-up rounded formula, two decimals', 'payment_mode':'UPI, CARD, CASH ON DELIVERY, WALLET',
        'order_status':'Finance fact: DELIVERED; operational layer also CANCELLED/RETURNED',
        'delivery_minutes':'0?<120; duration required for DELIVERED in both pipelines',
        'within_15_min':'true / false / null', 'is_active':'Y / N', 'loyalty_tier':'BRONZE, SILVER, GOLD, PLATINUM',
        'signup_date':'ISO date/timestamp text; no future signup', 'launch_date':'Source YYYY-MM-DD text; not type-enforced',
        'open_date':'Source YYYY-MM-DD text; not type-enforced', 'ingested_at':'ISO8601 UTC timestamp text',
        'unit':'kg, pack, unit', 'category':'Fruits & Vegetables, Dairy & Bakery, Staples, Snacks & Beverages, Personal Care, Household',
        'pincode':'6 digits; format problems produce warnings', 'zone':'North, South, East, West',
        'is_perishable':'Source True / False; inferred ETL string or boolean type shown below',
        'mrp':'Finite and > 0',
        'cost_price':'â‰¤ mrp', 'email':'Masked string / missing', 'phone':'Masked string / missing',
        'net_revenue':'Derived INR; business revenue counts DELIVERED only',
        'gross_margin':'Derived INR, may be negative',
    }
    lines = ['# Part E appendix: dictionary, lineage and audit', '',
             'Generated using current ETL functions and ELT SQL in temporary databases. Types below are the actual DuckDB schema produced by this dataset, not assumed types.', '',
             'The fact table grain is one accepted order_id. Dimensions use customer_id, product_id and store_id as natural keys. PII includes indirect identification: a customer-linked transaction can be personal even without a name.', '']
    for table, columns in dictionary.items():
        default_source = {'etl_fact_orders':'orders.csv', 'etl_dim_customer':'customers.csv',
                          'etl_dim_product':'products.csv', 'etl_dim_store':'stores.csv'}[table]
        lines += ['## ' + table, '', '| Column | Type | Meaning | Source | Expected values / current enforcement | PII |',
                  '| --- | --- | --- | --- | --- | --- |']
        for name, dtype, *_ in columns:
            source = default_source
            if name in ('order_date','within_15_min','net_revenue'):
                source = 'Derived from orders.csv'
            elif name == 'gross_margin':
                source = 'Derived from orders.csv + products.csv'
            elif name == 'ingested_at':
                source = 'Pipeline clock'
            pii = 'Yes, linked transaction' if table == 'etl_fact_orders' and name != 'ingested_at' else 'No'
            if table == 'etl_dim_customer':
                pii = 'Yes (masked, still linkable)' if name in ('email','phone') else 'Yes'
            if table == 'etl_dim_store' and name == 'manager_name':
                pii = 'Yes'
            allowed = expected.get(name, 'Business text' if dtype == 'VARCHAR' else 'Valid business-local date/timestamp' if 'DATE' in dtype or 'TIME' in dtype else 'Source numeric value')
            lines.append(f'| {name} | {dtype} | {meanings[name]} | {source} | {allowed} | {pii} |')
        lines += ['']
    lines += ['## Worked lineage number', '',
              f'`mart_daily_store_kpis.net_revenue` = **{amount:.2f} INR** for `{date}`, `{store_name}` (`{store_id}`), zone `{zone}`, payment mode `{mode}`. The value sums {count} accepted, delivered, reconciled orders.', '',
              'Exact order rows and corresponding master/payment CSV rows are listed in [LINEAGE_EXAMPLE.csv](LINEAGE_EXAMPLE.csv). Data rows are counted from 1 after the header; these supplied files have one physical line per data row, so physical line = data row + 1.', '',
              '| Order | Orders CSV data row | Revenue contribution |', '| --- | ---: | ---: |']
    for row in ledger:
        lines.append(f'| {row[0]} | {row[2]} | {row[7]:.2f} |')
    lines += ['', 'Formula for each contribution: `quantity Ã— unit_price Ã— (1 âˆ’ discount_pct / 100)`. Each contributing stg_orders row retains `_source_file`, `_source_row`, `_run_id`; the immutable raw snapshot and load ledger identify the input version. ETL facts now retain source file/row/run metadata; extraction archives exact CSV bytes and SHA-256 under include/data/nammamart/private/archive/<run>/.', '',
              '## Worked rejected order', '', f'`{rejected_id}`, `orders.csv` data row {rejected_position}, was rejected for `order_quantity` in ETL and `order_quantity` in ELT.', '',
              'Exact source fields and the SHA-256 of the original input are in [AUDIT_EXAMPLE.json](AUDIT_EXAMPLE.json). ETL quarantine contains its source row, severity and reasons; ELT `elt_quarantine.raw_record` exactly matches the original CSV fields. Neither query below contributes an order to revenue:', '',
              '```sql', '-- ETL warehouse: expected 0', f"SELECT COUNT(*) FROM etl_fact_orders WHERE order_id = '{rejected_id}';", '',
              '-- ELT warehouse: exact raw source record and broken rule', f"SELECT source_file, row_key, check_name, raw_record FROM elt_quarantine WHERE row_key = '{rejected_id}';", '',
              '-- ELT revenue input: expected 0', f"SELECT COUNT(*) FROM elt_reconciled_orders WHERE order_id = '{rejected_id}';", '```', '',
              'The evidence JSON records both counts as 0. ETL extraction preserves original fields and archives exact CSV bytes with SHA-256 per run. ELT retains all original fields in raw tables with file/run metadata.', '',
              '## Personal-data inventory', '', '| Source | Personal columns / linkage |', '| --- | --- |',
              '| customers.csv | customer_id, full_name, email, phone, city, area, pincode, signup_date, loyalty_tier, is_active |',
              '| orders.csv and increment | order_id, order_ts, customer_id, store_id, product_id, quantity, unit_price, discount_pct, order_amount, payment_mode, order_status, channel, delivery_minutes: personal as a linked customer transaction |',
              '| payments.csv | payment_id, order_id, payment_ts, payment_mode, amount_paid, payment_status, gateway_ref: personal when linked to the customer order |',
              '| stores.csv | manager_name is direct employee personal data; other columns describe the business store |',
              '| products.csv | No direct personal columns in this dataset; purchased product IDs become personal within linked order records |', '',
              'Only customer email and phone are masked today. Names, customer keys, localities, account attributes, transaction history and manager_name remain linkable or directly identifying; masking two fields does not anonymise the dataset. Raw tables, input CSVs, XCom payloads and restricted source quarantine can contain unmasked information.', '']
    (output_dir / 'GOVERNANCE_APPENDIX.md').write_text('\n'.join(lines), encoding='utf-8')
    print(f'[Governance] Lineage: {date} {store_name} {mode}: {amount:.2f} INR from {count} source orders')
    print(f'[Governance] Rejected order: {rejected_id}, CSV data row {rejected_position}; both revenue counts = 0')
    print('[Governance] Dictionary and evidence artifacts generated in assignment/')


if __name__ == '__main__':
    generate()
