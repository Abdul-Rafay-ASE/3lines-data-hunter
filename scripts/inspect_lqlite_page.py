"""DEVELOPER DIAGNOSTIC TOOL — NOT PART OF THE PRODUCTION APP.

Inspect lqlite.com result-page DOM for a given NSN. Useful when investigating
DOM structure for new scraper features (e.g., this was used to plan the
Management Information / Unit Price extraction). Read-only — never modifies
production data or the scraper modules.

Run with:

    .venv/bin/python -u scripts/inspect_lqlite_page.py 5305011048393

The -u flag is recommended so progress prints appear in real time.

Outputs:
- Counts and metadata of every <table> on the result page.
- Headers and first few rows of each table.
- Any cell containing "$" or "Unit Price" (likely-price candidates).
- Full HTML saved to /tmp/lqlite_inspection.html for offline analysis.

NOT imported by the running app. Do not deploy with the production image.
"""
import os
import sys
import time

# Make repo modules importable when run from scripts/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from scraper.driver import make_driver
from scraper.scrape import _smart_wait


TARGET_URL = "https://www.lqlite.com"


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def describe_table(idx, tbl):
    tag_id = tbl.get_attribute("id") or ""
    tag_class = tbl.get_attribute("class") or ""
    rows = tbl.find_elements(By.TAG_NAME, "tr")
    print(f"\n— Table #{idx}  id={tag_id!r}  class={tag_class!r}  rows={len(rows)}", flush=True)
    captions = tbl.find_elements(By.TAG_NAME, "caption")
    if captions:
        cap_text = captions[0].text.strip()
        if cap_text:
            print(f"  caption: {cap_text!r}", flush=True)
    for j, r in enumerate(rows[:6]):
        ths = [h.text.strip() for h in r.find_elements(By.TAG_NAME, "th")]
        tds = [c.text.strip()[:60] for c in r.find_elements(By.TAG_NAME, "td")]
        if ths:
            print(f"  row {j}  TH: {ths}", flush=True)
        if tds:
            print(f"  row {j}  TD: {tds}", flush=True)
    if len(rows) > 6:
        print(f"  ...({len(rows) - 6} more rows)", flush=True)


def main():
    stock = sys.argv[1] if len(sys.argv) > 1 else "5305011048393"
    log(f"Inspecting lqlite.com result page for stock {stock}")
    log(f"Target URL: {TARGET_URL}")

    log("booting driver…")
    drv = make_driver()
    wt = WebDriverWait(drv, 15)
    log("driver up")
    try:
        log("drv.get(TARGET_URL)…")
        drv.get(TARGET_URL)
        log(f"page loaded; title={drv.title!r}, current_url={drv.current_url!r}")

        log("_smart_wait input…")
        _smart_wait(drv, "input", 8)

        log("waiting for #nALL search box…")
        box = wt.until(EC.presence_of_element_located((By.ID, "nALL")))
        log("box found")

        log(f"typing stock {stock} + RETURN")
        box.clear()
        box.send_keys(stock)
        box.send_keys(Keys.RETURN)

        log("_smart_wait tr after submit…")
        _smart_wait(drv, "tr", 6)
        log(f"results page loaded; title={drv.title!r}, current_url={drv.current_url!r}")

        # Drill-down behavior matches scrape_one
        src = drv.page_source
        if "Search Results:" in src or "results found" in src.lower():
            log("detected Search Results listing — drilling into first NIIN link")
            lks = (
                drv.find_elements(By.XPATH, "//a[contains(@href,'NIIN') or contains(@href,'niin')]")
                or drv.find_elements(By.XPATH, "//a[string-length(normalize-space(text()))=9 and translate(text(),'0123456789','')='']")
                or drv.find_elements(By.XPATH, "//table//tr//td//a")
            )
            log(f"found {len(lks)} candidate links")
            for lk in lks:
                t = lk.text.strip()
                if t and len(t) >= 5:
                    log(f"clicking link {t!r}")
                    lk.click()
                    _smart_wait(drv, "tr", 5)
                    log(f"drilled in; current_url={drv.current_url!r}")
                    break
        else:
            log("appears to be direct detail page — no drill-down needed")

        log("waiting for td (data table) to load…")
        _smart_wait(drv, "td", 4)

        tables = drv.find_elements(By.TAG_NAME, "table")
        print(f"\n========================================", flush=True)
        print(f"Total tables on page: {len(tables)}", flush=True)
        print(f"========================================", flush=True)
        for i, t in enumerate(tables):
            try:
                describe_table(i, t)
            except Exception as e:
                print(f"\n— Table #{i}  inspection error: {e}", flush=True)

        print(f"\n========================================", flush=True)
        print("Cells containing '$' or 'unit price' or 'price'", flush=True)
        print("========================================", flush=True)
        all_tds = drv.find_elements(By.TAG_NAME, "td")
        print(f"  (scanning {len(all_tds)} <td> elements)", flush=True)
        hits = 0
        for td in all_tds:
            try:
                txt = td.text.strip()
            except Exception:
                continue
            tl = txt.lower()
            if "$" in txt or "unit price" in tl or tl == "price":
                hits += 1
                print(f"  hit: {txt[:100]!r}", flush=True)
                if hits > 30:
                    print("  ...(truncated, too many hits)", flush=True)
                    break
        if not hits:
            print("  (no cells matched)", flush=True)

        out_path = "/tmp/lqlite_inspection.html"
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(drv.page_source)
        print(f"\n→ Full HTML saved to {out_path} ({len(drv.page_source)} bytes)", flush=True)

    finally:
        try:
            drv.quit()
        except Exception:
            pass
        log("driver quit; inspection complete")


if __name__ == "__main__":
    main()
