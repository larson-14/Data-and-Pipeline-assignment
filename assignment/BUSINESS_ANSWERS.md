# Six business questions - SQL and measured results

Executed against successful Airflow runs using unchanged inputs, including the October increment in both pipelines. Finance uses delivered, validated, reconciled orders; delivery SLA uses operationally valid delivered orders independently of payments.

Both pipelines publish 1,628 finance orders: revenue INR 878,583.90 and gross margin INR 216,122.30. September before corrections: 1,635 orders and INR 882,163.60. The seven finance-eligible returned corrections reduce revenue by INR 3,579.70; the other three corrections were already excluded. New October orders await matching payment exports.

Question 1 has separate ZONE and STORE grains: do not sum both. Question 4 includes all reconciliation exceptions, including orphan payments and rejected orders, and sums absolute differences without netting. Question 6 uses the execution date in Asia/Kolkata across all runs; one row can break multiple rules. These captured results remain evidence for their run date.

## ETL - `submission_20261022_035604_etl`

### 1. Daily delivered, reconciled revenue by zone and store (separate grains).

```sql
SELECT CASE WHEN grouping(store_id)=1 THEN 'ZONE' ELSE 'STORE' END grain,order_date,zone,store_id,store_name,count(*) orders,sum(net_revenue) net_revenue
FROM etl_fact_orders JOIN etl_dim_store USING(store_id) GROUP BY GROUPING SETS((order_date,zone),(order_date,zone,store_id,store_name)) ORDER BY ALL;
```

| grain | order_date | zone | store_id | store_name | orders | net_revenue |
| --- | --- | --- | --- | --- | --- | --- |
| STORE | 2026-09-01 | East | S08 | Nammamart Whitefield | 4 | 2984.70 |
| STORE | 2026-09-01 | East | S09 | Nammamart Indiranagar | 4 | 2089.60 |
| STORE | 2026-09-01 | East | S10 | Nammamart Marathahalli | 3 | 2141.60 |
| STORE | 2026-09-01 | East | S11 | Nammamart Hsr Layout | 6 | 2121.15 |
| STORE | 2026-09-01 | North | S01 | Nammamart Hebbal | 3 | 1410.80 |
| STORE | 2026-09-01 | North | S02 | Nammamart Yelahanka | 6 | 2385.55 |
| STORE | 2026-09-01 | North | S03 | Nammamart Rt Nagar | 6 | 4583.80 |
| STORE | 2026-09-01 | South | S05 | Nammamart Jp Nagar | 2 | 803.00 |
| STORE | 2026-09-01 | South | S06 | Nammamart Banashankari | 6 | 2812.00 |
| STORE | 2026-09-01 | West | S13 | Nammamart Malleshwaram | 3 | 1469.10 |

504 result rows. [Complete result](evidence/etl_question_1.csv).

### 2. Category revenue and gross margin.

```sql
SELECT category,count(*) orders,sum(net_revenue) net_revenue,sum(gross_margin) gross_margin FROM etl_fact_orders JOIN etl_dim_product USING(product_id) GROUP BY category ORDER BY net_revenue DESC;
```

| category | orders | net_revenue | gross_margin |
| --- | --- | --- | --- |
| Staples | 221 | 199109.80 | 51414.81 |
| Personal Care | 259 | 168524.10 | 38130.72 |
| Household | 268 | 158996.05 | 40273.76 |
| Snacks & Beverages | 272 | 142114.15 | 32677.61 |
| Dairy & Bakery | 318 | 138904.90 | 36519.07 |
| Fruits & Vegetables | 290 | 70934.90 | 17106.33 |

6 result rows. [Complete result](evidence/etl_question_2.csv).

### 3. All operationally valid delivered orders, independent of payment collection.

```sql
SELECT store_id,store_name,count(*) delivered_orders,round(avg(delivery_minutes),2) average_delivery_minutes,
round(100.0*count(*) FILTER(WHERE within_15_min)/count(*),2) within_15_min_pct
FROM etl_orders JOIN etl_dim_store USING(store_id) WHERE order_status='DELIVERED' GROUP BY store_id,store_name ORDER BY store_id;
```

| store_id | store_name | delivered_orders | average_delivery_minutes | within_15_min_pct |
| --- | --- | --- | --- | --- |
| S01 | Nammamart Hebbal | 140 | 20.69 | 30.71 |
| S02 | Nammamart Yelahanka | 138 | 20.93 | 31.16 |
| S03 | Nammamart Rt Nagar | 138 | 20.69 | 28.99 |
| S04 | Nammamart Jayanagar | 129 | 21.67 | 26.36 |
| S05 | Nammamart Jp Nagar | 133 | 21.34 | 28.57 |
| S06 | Nammamart Banashankari | 145 | 22.33 | 22.07 |
| S08 | Nammamart Whitefield | 155 | 20.6 | 35.48 |
| S09 | Nammamart Indiranagar | 127 | 21.14 | 27.56 |
| S10 | Nammamart Marathahalli | 139 | 21.77 | 27.34 |
| S11 | Nammamart Hsr Layout | 144 | 20.97 | 27.78 |

13 result rows. [Complete result](evidence/etl_question_3.csv).

### 4. Finance revenue and every reconciliation exception, including rejected/orphan records.

```sql
WITH r AS (SELECT payment_mode,sum(net_revenue) revenue FROM etl_fact_orders GROUP BY payment_mode),
m AS (SELECT coalesce(payment_mode,'UNKNOWN') payment_mode,sum(abs(amount_difference)) mismatch,count(*) mismatch_orders FROM etl_payment_reconciliation GROUP BY 1),
x AS (SELECT coalesce(r.payment_mode,m.payment_mode) payment_mode,coalesce(revenue,0) revenue,coalesce(mismatch,0) mismatch,coalesce(mismatch_orders,0) mismatch_orders FROM r FULL JOIN m USING(payment_mode))
SELECT coalesce(payment_mode,'ALL') payment_mode,sum(revenue) net_revenue,sum(mismatch) absolute_unreconciled_amount,sum(mismatch_orders) mismatch_orders FROM x GROUP BY GROUPING SETS((payment_mode),()) ORDER BY payment_mode;
```

| payment_mode | net_revenue | absolute_unreconciled_amount | mismatch_orders |
| --- | --- | --- | --- |
| ALL | 878583.90 | 127937.17 | 254 |
| CARD | 174952.30 | 31776.10 | 67 |
| CASH ON DELIVERY | 114091.75 | 30777.65 | 49 |
| CASHH | 0.00 | 25.00 | 2 |
| COD | 0.00 | 0.00 | 1 |
| UNKNOWN | 0.00 | 4580.57 | 8 |
| UPI | 491377.50 | 43260.55 | 91 |
| WALLET | 98162.35 | 17517.30 | 36 |

8 result rows. [Complete result](evidence/etl_question_4.csv).

### 5. Top ten customers by reconciled spend, with loyalty tier and no contacts.

```sql
SELECT customer_id,loyalty_tier,count(*) orders,sum(net_revenue) total_spend FROM etl_fact_orders JOIN etl_dim_customer USING(customer_id) GROUP BY customer_id,loyalty_tier ORDER BY total_spend DESC,customer_id LIMIT 10;
```

| customer_id | loyalty_tier | orders | total_spend |
| --- | --- | --- | --- |
| C00310 | BRONZE | 8 | 7258.95 |
| C00072 | SILVER | 11 | 6433.95 |
| C00236 | PLATINUM | 8 | 6105.80 |
| C00202 | SILVER | 6 | 5988.75 |
| C00115 | BRONZE | 6 | 5783.70 |
| C00105 | GOLD | 5 | 5702.85 |
| C00135 | BRONZE | 7 | 5693.20 |
| C00163 | PLATINUM | 5 | 5671.50 |
| C00064 | BRONZE | 8 | 5609.25 |
| C00209 | PLATINUM | 7 | 5315.40 |

10 result rows. [Complete result](evidence/etl_question_5.csv).

### 6. Rejection occurrences across all runs today in IST, grouped by source and reason.

```sql
SELECT source_file,failure_reason,count(*) rejected_rows FROM etl_rejected_rows WHERE cast(timezone('Asia/Kolkata',checked_at) AS DATE)=cast(timezone('Asia/Kolkata',current_timestamp) AS DATE) GROUP BY source_file,failure_reason ORDER BY ALL;
```

| source_file | failure_reason | rejected_rows |
| --- | --- | --- |
| customers.csv | customer_future_signup | 3 |
| orders.csv | order_amount | 18 |
| orders.csv | order_customer_key | 18 |
| orders.csv | order_customer_quality | 18 |
| orders.csv | order_customer_required | 18 |
| orders.csv | order_delivery_limit | 6 |
| orders.csv | order_delivery_required | 8 |
| orders.csv | order_discount | 3 |
| orders.csv | order_duplicate | 24 |
| orders.csv | order_future_timestamp | 5 |

32 result rows. [Complete result](evidence/etl_question_6.csv).

## ELT - `submission_20261022_035604_elt`

### 1. Daily delivered, reconciled revenue by zone and store (separate grains).

```sql
SELECT CASE WHEN grouping(store_id)=1 THEN 'ZONE' ELSE 'STORE' END grain,order_date,zone,store_id,store_name,count(*) orders,sum(net_revenue) net_revenue
FROM elt_reconciled_orders GROUP BY GROUPING SETS((order_date,zone),(order_date,zone,store_id,store_name)) ORDER BY ALL;
```

| grain | order_date | zone | store_id | store_name | orders | net_revenue |
| --- | --- | --- | --- | --- | --- | --- |
| STORE | 2026-09-01 | East | S08 | Nammamart Whitefield | 4 | 2984.70 |
| STORE | 2026-09-01 | East | S09 | Nammamart Indiranagar | 4 | 2089.60 |
| STORE | 2026-09-01 | East | S10 | Nammamart Marathahalli | 3 | 2141.60 |
| STORE | 2026-09-01 | East | S11 | Nammamart Hsr Layout | 6 | 2121.15 |
| STORE | 2026-09-01 | North | S01 | Nammamart Hebbal | 3 | 1410.80 |
| STORE | 2026-09-01 | North | S02 | Nammamart Yelahanka | 6 | 2385.55 |
| STORE | 2026-09-01 | North | S03 | Nammamart Rt Nagar | 6 | 4583.80 |
| STORE | 2026-09-01 | South | S05 | Nammamart Jp Nagar | 2 | 803.00 |
| STORE | 2026-09-01 | South | S06 | Nammamart Banashankari | 6 | 2812.00 |
| STORE | 2026-09-01 | West | S13 | Nammamart Malleshwaram | 3 | 1469.10 |

504 result rows. [Complete result](evidence/elt_question_1.csv).

### 2. Category revenue and gross margin.

```sql
SELECT category,count(*) orders,sum(net_revenue) net_revenue,sum(gross_margin) gross_margin FROM elt_reconciled_orders GROUP BY category ORDER BY net_revenue DESC;
```

| category | orders | net_revenue | gross_margin |
| --- | --- | --- | --- |
| Staples | 221 | 199109.80 | 51414.81 |
| Personal Care | 259 | 168524.10 | 38130.72 |
| Household | 268 | 158996.05 | 40273.76 |
| Snacks & Beverages | 272 | 142114.15 | 32677.61 |
| Dairy & Bakery | 318 | 138904.90 | 36519.07 |
| Fruits & Vegetables | 290 | 70934.90 | 17106.33 |

6 result rows. [Complete result](evidence/elt_question_2.csv).

### 3. All operationally valid delivered orders, independent of payment collection.

```sql
SELECT store_id,store_name,count(*) delivered_orders,round(avg(delivery_minutes),2) average_delivery_minutes,
round(100.0*count(*) FILTER(WHERE within_15_min)/count(*),2) within_15_min_pct
FROM elt_operational_orders WHERE order_status='DELIVERED' GROUP BY store_id,store_name ORDER BY store_id;
```

| store_id | store_name | delivered_orders | average_delivery_minutes | within_15_min_pct |
| --- | --- | --- | --- | --- |
| S01 | Nammamart Hebbal | 140 | 20.69 | 30.71 |
| S02 | Nammamart Yelahanka | 138 | 20.93 | 31.16 |
| S03 | Nammamart Rt Nagar | 138 | 20.69 | 28.99 |
| S04 | Nammamart Jayanagar | 129 | 21.67 | 26.36 |
| S05 | Nammamart Jp Nagar | 133 | 21.34 | 28.57 |
| S06 | Nammamart Banashankari | 145 | 22.33 | 22.07 |
| S08 | Nammamart Whitefield | 155 | 20.6 | 35.48 |
| S09 | Nammamart Indiranagar | 127 | 21.14 | 27.56 |
| S10 | Nammamart Marathahalli | 139 | 21.77 | 27.34 |
| S11 | Nammamart Hsr Layout | 144 | 20.97 | 27.78 |

13 result rows. [Complete result](evidence/elt_question_3.csv).

### 4. Finance revenue and every reconciliation exception, including rejected/orphan records.

```sql
WITH r AS (SELECT payment_mode,sum(net_revenue) revenue FROM elt_reconciled_orders GROUP BY payment_mode),
m AS (SELECT coalesce(payment_mode,'UNKNOWN') payment_mode,sum(abs(amount_difference)) mismatch,count(*) mismatch_orders FROM mart_payment_reconciliation GROUP BY 1),
x AS (SELECT coalesce(r.payment_mode,m.payment_mode) payment_mode,coalesce(revenue,0) revenue,coalesce(mismatch,0) mismatch,coalesce(mismatch_orders,0) mismatch_orders FROM r FULL JOIN m USING(payment_mode))
SELECT coalesce(payment_mode,'ALL') payment_mode,sum(revenue) net_revenue,sum(mismatch) absolute_unreconciled_amount,sum(mismatch_orders) mismatch_orders FROM x GROUP BY GROUPING SETS((payment_mode),()) ORDER BY payment_mode;
```

| payment_mode | net_revenue | absolute_unreconciled_amount | mismatch_orders |
| --- | --- | --- | --- |
| ALL | 878583.90 | 127937.17 | 254 |
| CARD | 174952.30 | 31776.10 | 67 |
| CASH ON DELIVERY | 114091.75 | 30777.65 | 49 |
| CASHH | 0.00 | 25.00 | 2 |
| COD | 0.00 | 0.00 | 1 |
| UNKNOWN | 0.00 | 4580.57 | 8 |
| UPI | 491377.50 | 43260.55 | 91 |
| WALLET | 98162.35 | 17517.30 | 36 |

8 result rows. [Complete result](evidence/elt_question_4.csv).

### 5. Top ten customers by reconciled spend, with loyalty tier and no contacts.

```sql
SELECT customer_id,loyalty_tier,count(*) orders,sum(net_revenue) total_spend FROM elt_reconciled_orders GROUP BY customer_id,loyalty_tier ORDER BY total_spend DESC,customer_id LIMIT 10;
```

| customer_id | loyalty_tier | orders | total_spend |
| --- | --- | --- | --- |
| C00310 | BRONZE | 8 | 7258.95 |
| C00072 | SILVER | 11 | 6433.95 |
| C00236 | PLATINUM | 8 | 6105.80 |
| C00202 | SILVER | 6 | 5988.75 |
| C00115 | BRONZE | 6 | 5783.70 |
| C00105 | GOLD | 5 | 5702.85 |
| C00135 | BRONZE | 7 | 5693.20 |
| C00163 | PLATINUM | 5 | 5671.50 |
| C00064 | BRONZE | 8 | 5609.25 |
| C00209 | PLATINUM | 7 | 5315.40 |

10 result rows. [Complete result](evidence/elt_question_5.csv).

### 6. Rejection occurrences across all runs today in IST, grouped by source and reason.

```sql
SELECT source_file,failure_reason,sum(rejected_rows) rejected_rows FROM elt_rejected_rows WHERE checked_date=cast(timezone('Asia/Kolkata',current_timestamp) AS DATE) GROUP BY source_file,failure_reason ORDER BY ALL;
```

| source_file | failure_reason | rejected_rows |
| --- | --- | --- |
| customers.csv | customer_future_signup | 3 |
| orders.csv | order_amount | 18 |
| orders.csv | order_customer_key | 18 |
| orders.csv | order_customer_quality | 18 |
| orders.csv | order_customer_required | 18 |
| orders.csv | order_delivery_limit | 6 |
| orders.csv | order_delivery_required | 8 |
| orders.csv | order_discount | 3 |
| orders.csv | order_duplicate | 24 |
| orders.csv | order_future_timestamp | 5 |

32 result rows. [Complete result](evidence/elt_question_6.csv).
