"""DEVELOPER DIAGNOSTIC TOOL — NOT PART OF THE PRODUCTION APP.

Minimal page-saver: navigate to an lqlite.com result page for the given NSN
and save the full rendered HTML to /tmp/ for offline DOM analysis. Useful
when designing new extractors and you want to grep / view-source / dev-tools
the saved file without re-running the scraper.

Run with:

    .venv/bin/python -u scripts/save_lqlite_html.py 5305011048393

Outputs /tmp/lqlite_{stock}.html. NOT imported by the running app. Do not
deploy with the production image.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from scraper.driver import make_driver
from scraper.scrape import _smart_wait


def log(m):
    print(f"[{time.strftime('%H:%M:%S')}] {m}", flush=True)


def main():
    stock = sys.argv[1] if len(sys.argv) > 1 else "5305011048393"
    out_path = f"/tmp/lqlite_{stock}.html"

    log(f"booting driver…")
    drv = make_driver()
    wt = WebDriverWait(drv, 15)
    try:
        log(f"GET https://www.lqlite.com")
        drv.get("https://www.lqlite.com")
        _smart_wait(drv, "input", 8)
        box = wt.until(EC.presence_of_element_located((By.ID, "nALL")))
        log(f"searching {stock}")
        box.clear()
        box.send_keys(stock)
        box.send_keys(Keys.RETURN)
        _smart_wait(drv, "tr", 6)
        log(f"results page; url={drv.current_url}")
        _smart_wait(drv, "td", 4)
        # Small extra wait for nested tables to render
        time.sleep(2)
        html = drv.page_source
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html)
        log(f"saved {len(html):,} bytes → {out_path}")
    finally:
        drv.quit()
        log("done")


if __name__ == "__main__":
    main()
