# Submission security scan

Scanned 98 text/document files outside include/data/ in the selected submission file set.
The scan checks known Slack/OpenAI token patterns, suspicious quoted credential assignments, and exact source customer full-name/email/canonical-phone matches. It does not print matching values. Screenshots were generated only from noncontact query/CSV evidence and Airflow state/schedule pages.

Findings requiring review: 0.

| File | Finding |
| --- | --- |

Raw source exports are allowed only under include/data/. Private warehouses, original quarantine evidence, .env, operational experiment artifacts and Git internals are excluded from the ZIP. Customer/order IDs remain pseudonymous personal data, so publication still needs the appropriate audience and governance.

Existing Git history contains the originally tracked fictional customer export and demo database. The ZIP has no .git directory or history and omits those old artifact paths; create a fresh repository from it for a clean-history submission. This scan does not prove that arbitrary binary files or all historical commits are free of sensitive data. No remote push or destructive history rewrite was performed.
