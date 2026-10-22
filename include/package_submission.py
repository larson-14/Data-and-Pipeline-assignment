"""Scan submission files without revealing secrets, then create a clean-history ZIP."""
import csv,hashlib,json,re,subprocess,zipfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]


def candidates():
    output=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],cwd=ROOT,text=True)
    files=[]
    for name in sorted(set(output.splitlines())):
        path=ROOT/name
        if not path.is_file() or name in ('submission_bundle.zip','assignment/PACKAGE_MANIFEST.json'):continue
        if path.suffix in ('.duckdb','.wal') or name.startswith(('include/quarantine/','include/observability/','include/submission_evidence/','include/data/private/','include/data/nammamart/private/')):continue
        files.append(path)
    return files


def scan(files):
    with (ROOT/'include/data/nammamart/customers.csv').open(encoding='utf-8-sig',newline='') as h:
        customers=list(csv.DictReader(h))
    values=set()
    for row in customers:
        for key in ('full_name','email','phone'):
            value=row.get(key,'').strip()
            if key=='phone' and not re.fullmatch(r'\d{10}',value):continue
            if len(value)>6:values.add(value)
    patterns=[r'https://hooks\.slack\.com/services/[A-Za-z0-9/]{10,}',r'\bsk-[A-Za-z0-9_-]{20,}',
              r'\bxox[baprs]-[A-Za-z0-9-]{20,}',r'(?i)(?:api_key|password|webhook_url)\s*[:=]\s*[\"\x27]([^\"\x27]{16,})[\"\x27]']
    findings=[];checked=0
    for path in files:
        relative=path.relative_to(ROOT).as_posix()
        if relative.startswith('include/data/'):continue
        if path.suffix=='.docx':
            with zipfile.ZipFile(path) as z:text='\n'.join(z.read(n).decode('utf-8','replace') for n in z.namelist() if n.endswith('.xml'))
        elif path.suffix in ('.py','.md','.json','.csv','.txt','.sql','.yaml','.yml','.html'):
            text=path.read_text(encoding='utf-8-sig',errors='replace')
        else:continue
        checked+=1
        if any(re.search(pattern,text) for pattern in patterns):findings.append((relative,'credential-pattern review'))
        if any(re.search(r'(?<!\w)'+re.escape(value)+r'(?!\w)',text) for value in values):
            findings.append((relative,'source customer name/contact match'))
    return checked,findings


def main():
    files=candidates();checked,findings=scan(files)
    lines=['# Submission security scan','',f'Scanned {checked} text/document files outside include/data/ in the selected submission file set.',
        'The scan checks known Slack/OpenAI token patterns, suspicious quoted credential assignments, and exact source customer full-name/email/canonical-phone matches. It does not print matching values. Screenshots were generated only from noncontact query/CSV evidence and Airflow state/schedule pages.','',
        f'Findings requiring review: {len(findings)}.','',
        '| File | Finding |','| --- | --- |',*['| '+name+' | '+kind+' |' for name,kind in findings],'',
        'Raw source exports are allowed only under include/data/. Private warehouses, original quarantine evidence, .env, operational experiment artifacts and Git internals are excluded from the ZIP. Customer/order IDs remain pseudonymous personal data, so publication still needs the appropriate audience and governance.','',
        'Existing Git history contains the originally tracked fictional customer export and demo database. The ZIP has no .git directory or history and omits those old artifact paths; create a fresh repository from it for a clean-history submission. This scan does not prove that arbitrary binary files or all historical commits are free of sensitive data. No remote push or destructive history rewrite was performed.','']
    (ROOT/'assignment/SECURITY_SCAN.md').write_text('\n'.join(lines),encoding='utf-8')
    if findings:
        for name,kind in findings:print(name,kind)
        raise RuntimeError('Review the finding locations before packaging; values were not printed')
    files=candidates()
    archive=ROOT/'submission_bundle.zip'
    with zipfile.ZipFile(archive,'w',compression=zipfile.ZIP_DEFLATED) as z:
        for path in files:z.write(path,path.relative_to(ROOT).as_posix())
    with zipfile.ZipFile(archive) as z:
        assert not any(name.startswith('.git/') or name.endswith('.duckdb') or name=='.env' or '/private/' in name for name in z.namelist())
    manifest=dict(file_count=len(files),sha256=hashlib.sha256(archive.read_bytes()).hexdigest(),security_findings=0)
    (ROOT/'assignment/PACKAGE_MANIFEST.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    print('Clean-history ZIP created:',len(files),'files; no source customer contacts or credential patterns detected outside include/data/.')


if __name__=='__main__':main()
