"""Assemble the submission and render actual, redacted DuckDB evidence."""
import csv,html,json,re,shutil
from pathlib import Path
from include.observability_submission import observed,EXPECTED,CHANGES,NAMES

ROOT=Path(__file__).resolve().parents[1]
ASSIGN=ROOT/'assignment'
EVIDENCE=ASSIGN/'evidence'


def table(columns,rows):
    clean=lambda value:str(value if value is not None else 'NULL').replace('|','\\|').replace('\n',' ')
    return '\n'.join(['| '+' | '.join(columns)+' |','| '+' | '.join('---' for _ in columns)+' |',
                      *['| '+' | '.join(clean(v) for v in row)+' |' for row in rows]])


def evidence_page(name,title,sql,columns,rows,note):
    cells='<tr>'+''.join('<th>'+html.escape(c)+'</th>' for c in columns)+'</tr>'
    cells+=''.join('<tr>'+''.join('<td>'+html.escape(str(v))+'</td>' for v in row)+'</tr>' for row in rows)
    page=f'''<!doctype html><meta charset="utf-8"><title>{html.escape(title)}</title>
<style>body{{font:16px Arial;margin:32px;color:#16283c}}pre{{white-space:pre-wrap;background:#edf2f7;padding:16px}}table{{border-collapse:collapse;font-size:14px}}td,th{{border:1px solid #b7c4d2;padding:8px;text-align:left}}th{{background:#e4edf6}}</style>
<h1>{html.escape(title)}</h1><p>{html.escape(note)}</p><pre>{html.escape(sql)}</pre><table>{cells}</table>'''
    (EVIDENCE/(name+'.html')).write_text(page,encoding='utf-8')


def linked(text):
    return re.sub(r'\]\((?!https?://)([^)#]+\.\w+)([^)]*)\)',r'](assignment/\1\2)',text)


def build():
    EVIDENCE.mkdir(exist_ok=True)
    records=[]
    business=['# Six business questions - SQL and measured results','',
      'Executed against successful Airflow runs using unchanged inputs, including the October increment in both pipelines. Finance uses delivered, validated, reconciled orders; delivery SLA uses operationally valid delivered orders independently of payments.','',
      'Both pipelines publish 1,628 finance orders: revenue INR 878,583.90 and gross margin INR 216,122.30. September before corrections: 1,635 orders and INR 882,163.60. The seven finance-eligible returned corrections reduce revenue by INR 3,579.70; the other three corrections were already excluded. New October orders await matching payment exports.','',
      'Question 1 has separate ZONE and STORE grains: do not sum both. Question 4 includes all reconciliation exceptions, including orphan payments and rejected orders, and sums absolute differences without netting. Question 6 uses the execution date in Asia/Kolkata across all runs; one row can break multiple rules. These captured results remain evidence for their run date.','']
    for pipeline in ('etl','elt'):
        source=ROOT/'include/submission_evidence'
        record=json.loads((source/f'{pipeline}_results.json').read_text())
        assert record['state']=='success'
        records.append(record)
        public={k:v for k,v in record.items() if k!='warehouse'}
        (EVIDENCE/f'{pipeline}_results.json').write_text(json.dumps(public,indent=2),encoding='utf-8')
        business += [f"## {pipeline.upper()} - `{record['run_id']}`",'']
        for query in record['queries']:
            number=query['number'];dest=f'{pipeline}_question_{number}.csv'
            shutil.copy2(source/dest,EVIDENCE/dest)
            business += [f"### {number}. {query['title']}",'','```sql',query['sql'],'```','',table(query['columns'],query['rows'][:10]),'',f"{len(query['rows'])} result rows. [Complete result](evidence/{dest}).",'']
        for name,result in record['table_evidence'].items():
            evidence_page(f'{pipeline}_{name}',f'{pipeline.upper()}: {name}',result['sql'],result['columns'],result['rows'][:12],f"Actual DuckDB result; run {record['run_id']}. Rendered query evidence, not a SQLTools screenshot.")
    for a,b in zip(records[0]['queries'],records[1]['queries']):
        assert a['columns']==b['columns']
        assert sorted(a['rows'],key=str)==sorted(b['rows'],key=str), f"Business question {a['number']} differs"
    quarantine=ROOT/'include/data/private/submission/quarantine'/f"orders_failed_base_{records[0]['run_id']}.csv"
    with quarantine.open(encoding='utf-8-sig',newline='') as h:selected=list(csv.DictReader(h))[:8]
    columns=['order_id','source_file','source_row','severity','failure_reason']
    rows=[[row[c] for c in columns] for row in selected]
    with (EVIDENCE/'quarantine_example.csv').open('w',encoding='utf-8',newline='') as h:
        writer=csv.writer(h);writer.writerow(columns);writer.writerows(rows)
    evidence_page('quarantine','Sanitized quarantine-file excerpt',f'Source: {quarantine.name}',columns,rows,'Actual CSV rows; customer identifiers and contact attributes omitted.')
    (ASSIGN/'BUSINESS_ANSWERS.md').write_text('\n'.join(business),encoding='utf-8')
    lines=['# NammaMart - Sudatta submission','',
      'Current local Airflow UI: http://localhost:16492 (container port 8080).','',
      'Both TaskFlow DAGs implement the assignment and pass the recorded checks. Start with [implementation verification](assignment/IMPLEMENTATION_VERIFICATION.md). The trainer DAG and six supplied CSVs are unchanged.','',
      '## Submission contents','',
      '| Part | Evidence |','| --- | --- |',
      '| 0: Discovery | [Profile](assignment/DATA_PROFILE.md), [287-entry issues log](assignment/DATA_ISSUES_LOG.csv) covering 262 source rows |',
      '| A: ETL | [TaskFlow DAG](dags/nammamart_etl_sudatta.py), [Python transforms](include/etl_pipeline.py), [validation](include/validation_checks.py) |',
      '| B: ELT | [TaskFlow DAG](dags/nammamart_elt_sudatta.py), [SQL layers](include/sql/), [exact parity](assignment/ELT_VERIFICATION.md) |',
      '| C: Schedule | [IST schedule and SLA explanation](assignment/SCHEDULING.md) |',
      '| D/E: Quality and governance | [Answers](assignment/DATA_QUALITY_AND_GOVERNANCE.md), [dictionary and worked evidence](assignment/GOVERNANCE_APPENDIX.md) |',
      '| F: Observability | [Twenty experiments](assignment/OBSERVABILITY_TESTS.md), [captured results](assignment/OBSERVABILITY_RESULTS.json), [task durations](assignment/observability_task_durations.csv) |',
      '| G: Recommendation | [Recommendation to Sameer](assignment/CLIENT_RECOMMENDATION.md) |',
      '| Six business questions | [SQL and actual results for both pipelines](assignment/BUSINESS_ANSWERS.md) |','',
      '## Discovery questions for the client','',
      '1. Should October returns restate September revenue, and when does a refund become an accounting adjustment?',
      '2. Which delivery, contact and catalogue anomalies should quarantine a row versus warn?',
      '3. How should conflicting master versions and missing or late payment exports be resolved?','',
      'The increment has 150 new IDs and ten legitimate corrections, with no discovery defects. Store completeness/capacity checks are enforced. Discovery source_row includes the header; pipeline source_row counts the first data record as 1.','',
      '## Screenshots','',
      '| Evidence | ETL | ELT |','| --- | --- | --- |',
      '| Successful Graph | [Graph](assignment/screenshots/etl_graph.png) | [Graph](assignment/screenshots/elt_graph.png) |',
      '| Schedule and next run | [Schedule](assignment/screenshots/etl_schedule.png) | [Schedule](assignment/screenshots/elt_schedule.png) |',
      '| Quality scorecard | [dq_results](assignment/screenshots/etl_dq_results.png) | [dq_results](assignment/screenshots/elt_dq_results.png) |',
      '| Run audit | [pipeline_runs](assignment/screenshots/etl_pipeline_runs.png) | [pipeline_runs](assignment/screenshots/elt_pipeline_runs.png) |',
      '| Reconciliation | [Payment exceptions](assignment/screenshots/etl_etl_payment_reconciliation.png) | [Payment exceptions](assignment/screenshots/elt_mart_payment_reconciliation.png) |','',
      '[Sanitized quarantine screenshot](assignment/screenshots/quarantine.png) and [CSV](assignment/evidence/quarantine_example.csv). Graph/schedule images are from real Airflow. Query/quarantine images render actual captured DuckDB/CSV results in labelled HTML pages.','',
      '## Package','',
      '[submission_bundle.zip](submission_bundle.zip) contains source, documentation and evidence without Git history or operational private files. [Scoped scan](assignment/SECURITY_SCAN.md) and [manifest](assignment/PACKAGE_MANIFEST.json) document the package. Source exports stay under include/data/; derived public evidence omits names/contacts. IDs remain pseudonymous. No remote push or Git history rewrite was performed.','',
      'The assignment has a timing contradiction: a 09:00 start cannot guarantee data ready before 09:00. The implementation follows Part C: start at 09:00 IST Monday-Saturday, targeting the 09:30 review. Keep Docker and the scheduler running. Production scale and external notification delivery are not established by these local tests.','']
    (ROOT/'SUBMISSION.md').write_text('\n'.join(lines),encoding='utf-8')
    print('Submission built; all six business-query results match exactly.')

if __name__=='__main__':build()
