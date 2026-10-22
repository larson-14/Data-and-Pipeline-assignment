"""Capture the real local Airflow UI with an installed Chrome browser."""
import argparse
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1]
OUT=ROOT/'assignment/screenshots'


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--url',default='http://localhost:8080')
    parser.add_argument('--name',default='airflow_inspection')
    parser.add_argument('--graph',action='store_true')
    parser.add_argument('--evidence',action='store_true')
    args=parser.parse_args()
    OUT.mkdir(parents=True,exist_ok=True)
    with sync_playwright() as p:
        browser=p.chromium.launch(channel='chrome',headless=True)
        page=browser.new_page(viewport={'width':1600,'height':1000})
        if args.evidence:
            for source in sorted((ROOT/'assignment/evidence').glob('*.html')):
                page.goto(source.resolve().as_uri(),wait_until='networkidle')
                page.screenshot(path=str(OUT/(source.stem+'.png')),full_page=True)
                print('Captured actual query/CSV evidence:',source.stem)
            browser.close()
            return
        page.goto(args.url,wait_until='networkidle')
        if args.graph:
            graph=page.get_by_role('link',name='Graph',exact=False)
            if graph.count():
                graph.click()
                page.wait_for_load_state('networkidle')
            else:
                graph=page.get_by_role('button',name='Graph',exact=False)
                if graph.count():graph.click()
            collapse=page.get_by_role('button',name='Collapse Details Panel',exact=True)
            if collapse.count():collapse.click()
            page.wait_for_timeout(300)
            fit=page.get_by_role('button',name='Fit View',exact=False)
            if fit.count():fit.first.click()
        page.wait_for_timeout(1500)
        print('URL:',page.url)
        print('Visible navigation:',page.locator('body').inner_text()[:1800])
        print('View controls:',page.locator('button').evaluate_all("els=>els.slice(0,14).map(e=>({aria:e.getAttribute('aria-label'),title:e.getAttribute('title'),text:e.innerText}))"))
        page.screenshot(path=str(OUT/(args.name+'.png')),full_page=True)
        browser.close()


if __name__=='__main__':
    main()
