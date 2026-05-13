"""Per-stock scraping logic for lqlite.com result pages.

`scrape_one(drv, wt, stock, target_url, priority_targets,
blacklisted_companies)` performs one lookup against the catalog and
returns `(result_dict, status, blacklisted_count)` where status is one
of:

    'ok'   — scraper completed normally; result_dict is a row dict
              keyed by Stock Number / P.NO N / MFG N.
    'err'  — scraper threw a non-fatal Python exception; result_dict
              is the same shape but with empty P.NO/MFG fields.
    'dead' — driver session went away mid-scrape; result_dict is None
              and the caller is expected to reboot the driver.

`scrape_one_with_timeout` wraps scrape_one with a wall-clock cap so a
hung page can't block the worker forever.

`_smart_wait` is a thin WebDriverWait helper that returns the moment
the page has >3 elements of the given tag, instead of rounding up to
the next manual poll tick.
"""
import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as _FuturesTimeoutError

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import STATIC_BLACKLIST
from utils.parsing import matches_company_list


def _smart_wait(drv, target_text="tr", timeout=8):
    """Wait until the page has >3 elements of the given tag, or until
    `timeout` seconds elapse. Uses Selenium's WebDriverWait so the call
    returns the moment the condition is met, instead of rounding up to
    the next manual poll tick. poll_frequency=0.2 keeps the wait tight
    enough to match (and beat) the previous 0.5 s polling loop."""
    try:
        WebDriverWait(drv, timeout, poll_frequency=0.2).until(
            lambda d: len(d.find_elements(By.TAG_NAME, target_text)) > 3
        )
        return True
    except Exception:
        return False


def scrape_one(drv, wt, stock, target_url, priority_targets, blacklisted_companies):
    s = stock.strip()
    try:
        # Smart: try to find search box, reload page if needed
        for attempt in range(2):
            try:
                box = wt.until(EC.presence_of_element_located((By.ID, "nALL")))
                break
            except Exception:
                drv.get(target_url)
                _smart_wait(drv, "input", 6)
                try:
                    box = wt.until(EC.presence_of_element_located((By.ID, "nALL")))
                    break
                except Exception:
                    if attempt == 1:
                        return {"Stock Number": s, "P.NO 1": "", "MFG 1": ""}, "err", 0

        # Selenium's clear() and send_keys() are synchronous; no settle delay needed.
        box.clear()
        box.send_keys(s)
        box.send_keys(Keys.RETURN)

        # Smart: wait for results instead of fixed sleep
        _smart_wait(drv, "tr", 6)

        src = drv.page_source
        if "Search Results:" in src or "results found" in src.lower():
            try:
                lks = (drv.find_elements(By.XPATH, "//a[contains(@href,'NIIN') or contains(@href,'niin')]") or
                       drv.find_elements(By.XPATH, "//a[string-length(normalize-space(text()))=9 and translate(text(),'0123456789','')='']") or
                       drv.find_elements(By.XPATH, "//table//tr//td//a"))
                for lk in (lks or []):
                    if lk.text.strip() and len(lk.text.strip()) >= 5:
                        lk.click()
                        _smart_wait(drv, "tr", 5)
                        break
            except Exception:
                pass

        rows = drv.find_elements(By.TAG_NAME, "tr")
        fstock, niin = "", ""
        for r in rows:
            cells = r.find_elements(By.TAG_NAME, "td")
            if len(cells) >= 2:
                t = [c.text.strip() for c in cells]
                if t[0] == "NIIN:" and len(t) > 1:
                    niin = t[1]
                if t[0] == "FSC:" and len(t) > 1 and niin:
                    fstock = f"{t[1]}{niin}"

        # Smart: wait for data tables to fully load
        _smart_wait(drv, "td", 4)
        rows = drv.find_elements(By.TAG_NAME, "tr")
        raw = []
        for r in rows:
            cells = r.find_elements(By.TAG_NAME, "td")
            if len(cells) < 3:
                continue
            t = [c.text.strip() for c in cells]
            fc = t[0].upper()
            if any(x in fc for x in ["NIIN", "FSC", "NSN", "MOE", "AAC", ":"]):
                continue
            if any(x in fc for x in STATIC_BLACKLIST):
                continue
            cage = -1
            for i, tx in enumerate(t):
                if tx and len(tx) == 5 and re.match(r'^[A-Z0-9]{5}$', tx):
                    cage = i
                    break
            if cage <= 0:
                continue
            pn = t[0].strip()
            if any(pn.upper().startswith(b) for b in ["HUES", "ABGL", "SHPE", "FSC", "NIIN", "NSN", "MOE", "AAC", "RNCC", "RNVC", "DAC", "RNAAC", "CAGE"]):
                continue
            if len(pn) <= 3:
                continue
            co = ""
            for j in range(cage + 1, min(cage + 4, len(t))):
                cd = t[j].strip()
                if len(cd) <= 5:
                    continue
                if cd.upper() in ["NATURAL", "BLACK", "RECTANGULAR", "MINIMUM"]:
                    continue
                if "INCH" in cd.upper():
                    continue
                co = cd
                break
            if pn and co and len(co) > 5:
                raw.append((pn, t[cage], co))
        if not fstock:
            fstock = s
        fstock = fstock.replace("-", "")
        priority_entries, other_entries, seen = [], [], set()
        blacklisted_count = 0
        for pn, _, co in raw:
            pn, co = pn.strip(), co.strip()
            if not pn or pn in seen:
                continue
            cu = co.upper()
            if any(w in cu for w in ["HUES", "ABGL", "SHPE", "CRF,", "NATURAL", "BLACK", "RECTANGULAR", "FSC", "NIIN"]):
                continue
            if len(co) <= 5:
                continue
            seen.add(pn)
            if matches_company_list(co, blacklisted_companies):
                blacklisted_count += 1
                continue
            if matches_company_list(co, priority_targets):
                priority_entries.append((pn, co))
            else:
                other_entries.append((pn, co))
        res = {"Stock Number": fstock}
        slot = 1
        for pn, mfg in priority_entries:
            res[f"P.NO {slot}"] = pn
            res[f"MFG {slot}"] = mfg
            slot += 1
        for pn, mfg in other_entries:
            res[f"P.NO {slot}"] = pn
            res[f"MFG {slot}"] = mfg
            slot += 1
        if slot == 1:
            res["P.NO 1"] = ""
            res["MFG 1"] = ""
        return res, "ok", blacklisted_count
    except Exception as e:
        em = str(e).lower()
        if "session" in em or "invalid session" in em:
            return None, "dead", 0
        return {"Stock Number": s, "P.NO 1": "", "MFG 1": ""}, "err", 0


def scrape_one_with_timeout(drv, wt, stock, target_url, priority_targets,
                            blacklisted_companies, timeout_s):
    """Wall-clock wrapper around scrape_one. If a single stock exceeds
    timeout_s, the call is abandoned and we return ("dead", 0) so the worker's
    existing dead-handling path will reboot the driver and continue.

    The runaway thread inside the inner pool is left to clean up on its own
    when the underlying selenium call eventually returns; we shut the pool
    down without waiting so the worker thread is not blocked.
    """
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dh-to")
    try:
        fut = pool.submit(scrape_one, drv, wt, stock, target_url,
                          priority_targets, blacklisted_companies)
        try:
            return fut.result(timeout=timeout_s)
        except _FuturesTimeoutError:
            return None, "dead", 0
    finally:
        # wait=False so a hung scrape doesn't trap the worker.
        pool.shutdown(wait=False)
