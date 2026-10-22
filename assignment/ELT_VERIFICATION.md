# Current ETL / ELT verification

Both finance order rows and complete reconciliation tables match exactly.

| Snapshot | Finance orders | Revenue INR | Margin INR | Mismatches |
| --- | ---: | ---: | ---: | ---: |
| Base only | 1635 | 882163.60 | 217377.92 | 104 |
| With increment | 1628 | 878583.90 | 216122.30 | 254 |

Run: `python -m include.verify_elt`. Uses temporary databases; never changes the CSV sources.
Both pipelines retain original source evidence, mask reporting contacts, exclude non-delivered/unreconciled rows from finance, and measure delivery independently of payments.