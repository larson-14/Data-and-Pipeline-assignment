"""Contract regressions for pandas validation (full-schema synthetic records)."""
import contextlib,io,unittest
import pandas as pd
from include.validation_checks import run_validation_checks,read_frame
from include.data_contract import SCHEMAS

class ValidationChecksTests(unittest.TestCase):
    def setUp(self):
        records={
        'orders':dict(order_id='O1',order_ts='2020-01-01 12:00:00',customer_id='C1',store_id='S1',product_id='P1',quantity='2',unit_price='10',discount_pct='10',order_amount='18',payment_mode='UPI',order_status='DELIVERED',channel='App',delivery_minutes='15'),
        'payments':dict(payment_id='PAY1',order_id='O1',payment_ts='2020-01-01 12:00:00',payment_mode='UPI',amount_paid='18',payment_status='SUCCESS',gateway_ref='test'),
        'customers':dict(customer_id='C1',full_name='Fixture Person',email='fixture@example.invalid',phone='1010101010',city='Test',area='Test',pincode='101010',signup_date='2019-01-01',loyalty_tier='Silver',is_active='Y'),
        'products':dict(product_id='P1',product_name='Test',category='Food',sub_category='Test',brand='Test',mrp='10',cost_price='5',unit='pack',is_perishable='False',launch_date='2018-01-01'),
        'stores':dict(store_id='S1',store_name='Test',zone='North',area='Test',pincode='101010',open_date='2018-01-01',manager_name='Fixture Manager',capacity_orders_per_day='100')}
        self.tables={n:pd.DataFrame([records[n]],columns=SCHEMAS[n]) for n in SCHEMAS}
    def validate(self,**kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return run_validation_checks(*(self.tables[n].to_json() for n in SCHEMAS),run_id='test',checked_at='2026-10-21T03:30:00Z',**kwargs)
    def test_valid_order_and_warning_retention(self):
        self.assertEqual(self.validate()['passed_count'],1)
        self.tables['customers'].loc[0,'phone']='bad'
        r=self.validate();self.assertEqual((r['passed_count'],r['warning_count']),(1,1))
    def test_status_timestamp_delivery_and_discount_contract(self):
        for col,value in [('order_status','SHIPPED'),('order_ts','invalid'),('delivery_minutes',''),('delivery_minutes','-1'),('delivery_minutes','120'),('discount_pct','101'),('quantity','1.5'),('unit_price','-10')]:
            with self.subTest(column=col,value=value):
                self.setUp();self.tables['orders'].loc[0,col]=value
                self.assertEqual(self.validate()['failed_count'],1)
    def test_required_schema_fails_loudly(self):
        self.tables['orders'].drop(columns='quantity',inplace=True)
        with self.assertRaisesRegex(ValueError,'orders: missing required columns: quantity'): self.validate()
    def test_future_dates_and_local_timezone(self):
        self.tables['orders'].loc[0,'order_ts']='2027-01-01'
        self.assertEqual(self.validate()['failed_count'],1)
        self.tables['orders'].loc[0,'order_ts']='2026-10-21 09:00:00'
        self.assertEqual(self.validate()['passed_count'],1)
    def test_success_only_amount_check_and_operational_independence(self):
        self.tables['payments'].loc[0,['payment_status','amount_paid']]=['FAILED','0']
        self.assertEqual(self.validate()['passed_count'],1)
        self.tables['payments'].loc[0,'payment_status']='SUCCESS'
        r=self.validate();self.assertEqual(r['failed_count'],1)
        self.assertEqual(len(read_frame(r['operational'])),1)
    def test_half_up_money_and_exact_cent_comparison(self):
        self.tables['orders'].loc[0,['quantity','unit_price','discount_pct','order_amount']]=['1','1.005','0','1.01']
        self.tables['payments'].loc[0,'amount_paid']='1.01'
        self.assertEqual(self.validate()['passed_count'],1)
        self.tables['orders'].loc[0,'order_amount']='1.02'
        self.assertEqual(self.validate()['failed_count'],1)
    def test_duplicates_and_master_quality(self):
        self.tables['orders']=pd.concat([self.tables['orders']]*2,ignore_index=True)
        self.assertEqual(self.validate()['failed_count'],2)
        self.setUp();self.tables['stores'].loc[0,'zone']=''
        self.assertEqual(self.validate()['failed_count'],1)
        self.setUp();self.tables['customers']=pd.concat([self.tables['customers']]*2,ignore_index=True)
        r=self.validate();self.assertEqual(r['passed_count'],1)
        self.assertTrue(any(x['check_name']=='base.customer_duplicate' and x['rows_failed']==2 for x in r['dq_results']))
    def test_original_rejected_text_and_empty_orders(self):
        self.tables['orders'].loc[0,'quantity']=' bad '
        r=self.validate();self.assertEqual(read_frame(r['failed']).iloc[0].quantity,' bad ')
        self.assertIn('failure_reason',read_frame(r['failed']))
        self.tables['orders']=self.tables['orders'].iloc[:0]
        self.assertEqual(self.validate()['failed_count'],0)
if __name__=='__main__':unittest.main()
