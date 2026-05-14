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
from datetime import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from config import STATIC_BLACKLIST
from utils.logger import logger
from utils.parsing import matches_company_list


# Management Information table column indices (positional; the header row is
# verified by text below so this is anchored, not blind):
#   [0]  MOEC    [4]  SLC    [8]  Unit Price   [12] Action Date
#   [1]  SOS     [5]  CIIC   [9]  UI           [13] C/H
#   [2]  AAC     [6]  UPQ    [10] UICF
#   [3]  RC      [7]  USC    [11] MCD
_MI_PRICE_COL = 8
_MI_DATE_COL = 12
_MI_MIN_CELLS = 13  # need at least up to col 12 (Action Date)


def _select_best_price_row(candidates):
    """Given a list of (price_str, date_str) tuples, return the one with the
    most recent Action Date. Ties broken by highest numeric price. Returns
    ("", "") for an empty input. Unparseable dates sort to the oldest.
    Unparseable prices count as 0.0 for tie-breaking but are still returned
    as their original string."""
    if not candidates:
        return "", ""
    parsed = []
    for price_str, date_str in candidates:
        try:
            dt = datetime.strptime(date_str, "%b-%d-%Y")
        except Exception:
            dt = datetime.min
        try:
            num = float(price_str.replace("$", "").replace(",", "").strip())
        except Exception:
            num = 0.0
        parsed.append((dt, num, price_str, date_str))
    # Most recent date first; among same-date rows, highest price first.
    parsed.sort(key=lambda x: (x[0], x[1]), reverse=True)
    _, _, price_str, date_str = parsed[0]
    return price_str, date_str


def _extract_mgmt_info(drv):
    """Find the Management Information KeyTable (identified by 'Unit Price' in
    its KeyHead row) and return (unit_price, action_date) strings. Multiple
    rows are resolved by _select_best_price_row. Returns ("", "") on any
    failure — never raises into the caller."""
    try:
        tables = drv.find_elements(By.CLASS_NAME, "KeyTable")
    except Exception:
        logger.debug("Mgmt-info: KeyTable lookup failed", exc_info=True)
        return "", ""
    for tbl in tables:
        try:
            head = tbl.find_elements(By.CSS_SELECTOR, "tr.KeyHead")
            if not head or "Unit Price" not in head[0].text:
                continue
            # Found the Management Information table. Pull data rows.
            rows = tbl.find_elements(By.CSS_SELECTOR, "tr.KeyRow")
            candidates = []
            for r in rows:
                cells = r.find_elements(By.TAG_NAME, "td")
                if len(cells) < _MI_MIN_CELLS:
                    continue
                price = cells[_MI_PRICE_COL].text.strip()
                date = cells[_MI_DATE_COL].text.strip()
                if price:
                    candidates.append((price, date))
            return _select_best_price_row(candidates)
        except Exception:
            logger.debug("Mgmt-info: per-table parse failed", exc_info=True)
            continue
    return "", ""


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
        static_filtered_count = 0
        for r in rows:
            cells = r.find_elements(By.TAG_NAME, "td")
            if len(cells) < 3:
                continue
            t = [c.text.strip() for c in cells]
            fc = t[0].upper()
            if any(x in fc for x in ["NIIN", "FSC", "NSN", "MOE", "AAC", ":"]):
                continue
            if any(x in fc for x in STATIC_BLACKLIST):
                logger.debug("Static blacklist filtered row (stock=%s, first_cell=%r)", s, t[0])
                static_filtered_count += 1
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
        # Unit Price + Action Date come from the Management Information table,
        # which is a separate KeyTable from the Part Information rows above.
        # Insertion order here puts them right after Stock Number so they
        # render in the right column order in Excel/CSV/JSON exports.
        res["Unit Price"], res["Action Date"] = _extract_mgmt_info(drv)
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
        # Roll static-blacklist hits into the same counter the UI shows as
        # "Blacklisted Entries" — they're functionally exclusions too,
        # they were just silent before.
        return res, "ok", blacklisted_count + static_filtered_count
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
