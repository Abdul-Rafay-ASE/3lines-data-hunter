"""Top-level scrape orchestration: worker pool, retry pass, finalization.

run_scraper() is the only public entry point. It's called from the
Scraper tab on Start, mutates st.session_state to publish live progress
(processed counter, ETA, log entries, etc.), and persists every per-stock
outcome to run_progress as it goes. On exit it calls db_finalize_run to
flip the run to terminal state and copy results into run_results.

The function is tightly coupled to Streamlit (st.session_state and the
placeholder objects status_ph / bar_ph / mN / log_ph passed in from the
caller) — by design, since it owns live UI updates while spawning worker
threads.
"""
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import streamlit as st
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from config import (
    AUTOSAVE_INTERVAL, DH_PER_STOCK_TIMEOUT, MAX_LOG_LINES,
    MINUTES_PER_ITEM_MANUAL,
)
from database.db import (
    _get_db,
    db_finalize_run,
    db_get_recently_scraped_stocks,
    db_get_run_progress_results,
    db_get_run_progress_stocks,
    db_record_stock,
    db_start_run,
)
from exports.builders import build_excel
from scraper.driver import make_driver
from scraper.scrape import _smart_wait, scrape_one_with_timeout
from ui.components import render_log, rmetric
from utils.logger import logger
from utils.parsing import load_stocks_strict, row_has_priority


def run_scraper(file_bytes, num_workers, limit, target_url,
        priority_targets, blacklisted_companies,
        stop_flag, status_ph, bar_ph, m1, m2, m3, m4, m5, stop_ph, log_ph,
        skip_recent=False):
    ss = st.session_state
    ss.running=True; ss.completed=False; ss.stopped=False
    ss.processed=0; ss.priority_matches=0; ss.blacklisted=0; ss.errors=0
    ss.output_bytes=None; ss.output_name=""; ss.autosave_bytes=None; ss.autosave_name=""
    ss.perf_data=[]; ss.failed_stocks=[]; ss.final_data=[]; ss.smart_skipped=False; stop_flag.clear()

    stocks, err_msg = load_stocks_strict(file_bytes)
    if not stocks:
        status_ph.error(err_msg or "❌ Invalid file"); ss.running=False; return

    # Optional smart-skip: drop stocks already in the DB from a successful run
    # within the last 24h. Off by default (opt-in via the Advanced Settings checkbox).
    skipped_count = 0
    if skip_recent:
        try:
            recent = db_get_recently_scraped_stocks(within_hours=24)
            if recent:
                before = len(stocks)
                stocks = [s for s in stocks if s not in recent]
                skipped_count = before - len(stocks)
        except Exception:
            skipped_count = 0
        if not stocks:
            logger.info("Smart-skip: all stocks were scraped recently; nothing to do")
            status_ph.markdown('<div class="sbox">All stocks were scraped recently — nothing to do. Disable smart-skip to re-run them.</div>', unsafe_allow_html=True)
            ss.smart_skipped=True
            ss.running=False; ss.completed=True; ss.stopped=False; return

    if 0 < limit < len(stocks): stocks = stocks[:limit]
    total_uploaded = len(stocks)

    # ── Resume support ──
    # If the user clicked "Resume" on the unfinished-run banner, ss.resume_run_id
    # is set. We reuse that run_id, filter out stocks already 'ok' under it, and
    # seed in-memory results from run_progress so the live UI / final export
    # reflect the full run (old + new). Stocks that were 'err'/'dead' are
    # retried — their old progress rows are cleared first.
    save_name = ss.get("custom_name", "3LINES_Results").strip() or "3LINES_Results"
    seed_results = []
    seed_priority = 0
    seed_blacklisted = 0
    resume_run_id = (ss.get("resume_run_id") or "").strip()
    if resume_run_id:
        try:
            already_ok = db_get_run_progress_stocks(resume_run_id)
            seed_results = db_get_run_progress_results(resume_run_id)
            for r in seed_results:
                if row_has_priority(r, priority_targets): seed_priority += 1
            # Wipe old non-'ok' progress rows so retried stocks land cleanly.
            _conn = _get_db()
            _conn.execute("DELETE FROM run_progress WHERE run_id=? AND status != 'ok'", (resume_run_id,))
            _conn.commit(); _conn.close()
            run_id = resume_run_id
            before = len(stocks)
            stocks = [s for s in stocks if s not in already_ok]
            resumed_skipped = before - len(stocks)
            logger.info("Resume: run=%s, %d already-ok skipped, %d to scrape", run_id, resumed_skipped, len(stocks))
        except Exception:
            logger.exception("Resume setup failed for run=%s; starting fresh", resume_run_id)
            run_id = f"run_{datetime.now():%Y%m%d_%H%M%S}"
            seed_results = []; seed_priority = 0; seed_blacklisted = 0
    else:
        run_id = f"run_{datetime.now():%Y%m%d_%H%M%S}"

    total = len(stocks) + len(seed_results)
    ss.target = total; t0 = time.time(); ss.start_time = t0
    logger.info("Run started: run_id=%s, total=%d (uploaded=%d, seeded=%d), workers=%d, target_url=%s, skip_recent=%s, skipped=%d",
                run_id, total, total_uploaded, len(seed_results), num_workers, target_url, skip_recent, skipped_count)
    try:
        db_start_run(run_id, save_name, total)
    except Exception:
        logger.exception("db_start_run failed; in-memory run continues without progress persistence")

    if resume_run_id and seed_results:
        status_ph.markdown(f'<div class="sbox">Resuming run: {len(seed_results):,} already complete, {len(stocks):,} remaining.</div>', unsafe_allow_html=True)
    elif skipped_count:
        status_ph.markdown(f'<div class="sbox">Smart-skip: {skipped_count:,} stock(s) skipped (already scraped in last 24h). Processing {total:,}.</div>', unsafe_allow_html=True)

    lock = threading.Lock()
    results = list(seed_results)
    ctr = {"done": len(seed_results), "priority": seed_priority,
           "blacklisted": seed_blacklisted, "errors": 0}
    last_as = {"count": ctr["done"]}; log_entries = deque(maxlen=MAX_LOG_LINES)
    perf_pts = []; failed = []

    def do_autosave():
        with lock: snap = list(results)
        if not snap: return
        try:
            xb,_,_,_ = build_excel(snap, priority_targets, blacklisted_companies)
            if xb: ss.autosave_bytes=xb; ss.autosave_name=f"AutoSave_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
        except Exception:
            logger.exception("Autosave excel build failed")

    def worker(wid, chunk, si):
        drv=None; restarts=0; consecutive_errors=0
        def boot():
            nonlocal drv
            try:
                if drv: drv.quit()
            except Exception:
                logger.debug("Worker %d: best-effort quit of prior driver failed", wid, exc_info=True)
            with lock: log_entries.append({"bot":wid,"stock":"","status":"start","num":"BOOT"})
            logger.info("Worker %d: booting driver", wid)
            d=make_driver(); w=WebDriverWait(d,15)
            for att in range(3):
                try:
                    d.get(target_url)
                    _smart_wait(d, "input", 8)
                    d.find_element(By.ID,"nALL")
                    with lock: log_entries.append({"bot":wid,"stock":"","status":"ok","num":"READY"})
                    return d,w
                except Exception as pe:
                    with lock: log_entries.append({"bot":wid,"stock":str(pe)[:40],"status":"err","num":f"LOAD#{att+1}"})
                    if att<2: time.sleep(3 + att*2)  # Progressive delay
            return d,w
        try: drv,wt=boot()
        except Exception:
            logger.exception("Worker %d: initial driver boot failed; chunk of %d marked failed", wid, len(chunk))
            with lock: ctr["done"]+=len(chunk); ctr["errors"]+=len(chunk); failed.extend(chunk)
            return
        try:
            for ci,stk in enumerate(chunk):
                if stop_flag.is_set(): break
                rn=si+ci+1

                # Smart: if too many consecutive errors, slow down (likely IP block)
                if consecutive_errors >= 3:
                    with lock: log_entries.append({"bot":wid,"stock":"slowdown","status":"retry","num":f"Cooling {consecutive_errors}s"})
                    time.sleep(consecutive_errors * 2)  # Adaptive cooldown
                    if consecutive_errors >= 6:
                        # Too many errors - restart browser (new IP/session)
                        logger.info("Worker %d: %d consecutive errors, rebooting driver", wid, consecutive_errors)
                        try: drv,wt=boot()
                        except Exception:
                            logger.exception("Worker %d: cooldown reboot failed; exiting", wid)
                            break
                        consecutive_errors = 0

                with lock: log_entries.append({"bot":wid,"stock":stk,"status":"start","num":rn})
                try:
                    res,status,bl = scrape_one_with_timeout(drv,wt,stk,target_url,priority_targets,blacklisted_companies,DH_PER_STOCK_TIMEOUT)
                    if status=="ok" and res and res.get("Stock Number","").strip():
                        consecutive_errors = 0  # Reset on success
                        ip=row_has_priority(res,priority_targets)
                        with lock:
                            results.append(res); ctr["done"]+=1; ctr["blacklisted"]+=bl
                            if bl>0: log_entries.append({"bot":wid,"stock":stk,"status":"blocked","num":rn})
                            if ip: ctr["priority"]+=1; log_entries.append({"bot":wid,"stock":stk,"status":"priority","num":rn})
                            else: log_entries.append({"bot":wid,"stock":stk,"status":"ok","num":rn})
                        db_record_stock(run_id, stk, "ok", res)
                    elif status=="dead":
                        consecutive_errors += 1
                        with lock: log_entries.append({"bot":wid,"stock":stk,"status":"dead","num":rn}); failed.append(stk)
                        db_record_stock(run_id, stk, "dead")
                        restarts+=1
                        logger.warning("Worker %d: dead driver on stock %s (restart #%d)", wid, stk, restarts)
                        if restarts>10:
                            logger.error("Worker %d: exceeded restart budget (%d); exiting", wid, restarts)
                            break
                        time.sleep(2 + restarts)  # Longer wait each restart
                        try: drv,wt=boot()
                        except Exception:
                            logger.exception("Worker %d: post-dead reboot failed; exiting", wid)
                            break
                    elif status=="err":
                        consecutive_errors += 1
                        with lock:
                            if res: results.append(res)
                            ctr["done"]+=1; ctr["errors"]+=1; failed.append(stk)
                            log_entries.append({"bot":wid,"stock":stk,"status":"err","num":rn})
                        db_record_stock(run_id, stk, "err", res if res else None)
                except Exception:
                    logger.exception("Worker %d: unexpected error scraping stock %s", wid, stk)
                    consecutive_errors += 1
                    with lock: ctr["done"]+=1; ctr["errors"]+=1; failed.append(stk)
                    log_entries.append({"bot":wid,"stock":stk,"status":"err","num":rn})
                    db_record_stock(run_id, stk, "err")
        except Exception:
            logger.exception("Worker %d: chunk loop terminated by exception", wid)
        finally:
            try: drv.quit()
            except Exception:
                logger.debug("Worker %d: best-effort final quit failed", wid, exc_info=True)
            logger.info("Worker %d: finished", wid)

    n_to_scrape = len(stocks)
    chunks=[]; sis=[]
    if n_to_scrape > 0:
        cs = max(1, n_to_scrape // num_workers)
        for i in range(num_workers):
            s = i*cs; e = s+cs if i < num_workers-1 else n_to_scrape
            if s < n_to_scrape:
                chunks.append(stocks[s:e]); sis.append(s)
    with stop_ph:
        if st.button("STOP & SAVE",use_container_width=True,key="stop_btn"): stop_flag.set()
    if chunks:
        pool=ThreadPoolExecutor(max_workers=len(chunks))
        futs={pool.submit(worker,i+1,ch,si):i+1 for i,(ch,si) in enumerate(zip(chunks,sis))}
    else:
        # Resume case: nothing left to scrape; jump straight to finalize.
        pool = None
        futs = {}
        logger.info("Resume: nothing remaining to scrape; finalizing")
    try:
        while any(not f.done() for f in futs):
            time.sleep(1)
            if stop_flag.is_set(): break
            with lock: d,p,bl,e,ls=ctr["done"],ctr["priority"],ctr["blacklisted"],ctr["errors"],list(log_entries)
            ss.processed=d; ss.priority_matches=p; ss.blacklisted=bl; ss.errors=e
            en=time.time()-t0; perf_pts.append({"elapsed":round(en,1),"records":d})
            pct=min(d/total,1.0) if total else 0; bar_ph.progress(pct)
            status_ph.markdown(f'<div class="sbox">Processing Record <b>#{d:,}</b> of <b>{total:,}</b></div>',unsafe_allow_html=True)
            ts2=d*MINUTES_PER_ITEM_MANUAL
            if d>0:
                rem=total-d; eta=int(rem*(en/d)); em2,es2=divmod(eta,60); eh2,em2=divmod(em2,60)
                eta_s=f"{eh2}h {em2:02d}m" if eh2 else f"{em2}m {es2:02d}s"
            else: eta_s="..."
            m1.markdown(rmetric("Records",f"{d:,}/{total:,}","g"),unsafe_allow_html=True)
            m2.markdown(rmetric("Priority",f"{p:,}","b"),unsafe_allow_html=True)
            m3.markdown(rmetric("Blacklisted",f"{bl:,}","r"),unsafe_allow_html=True)
            m4.markdown(rmetric("Time Saved",f"{ts2:,}m","p"),unsafe_allow_html=True)
            m5.markdown(rmetric("ETA",eta_s,"b"),unsafe_allow_html=True)
            log_ph.markdown(render_log(ls),unsafe_allow_html=True)
            if d-last_as["count"]>=AUTOSAVE_INTERVAL: last_as["count"]=d; do_autosave()
        was_stopped=stop_flag.is_set()
        if was_stopped:
            status_ph.markdown('<div class="sbox">Stopping... collecting data</div>',unsafe_allow_html=True)
            ws2=time.time()
            while time.time()-ws2<15:
                if all(f.done() for f in futs): break
                time.sleep(0.5)
                with lock: d=ctr["done"]
                ss.processed=d; bar_ph.progress(min(d/total,1.0) if total else 0)
        for f in as_completed(futs):
            try: f.result(timeout=5)
            except Exception:
                logger.debug("Worker future raised on join", exc_info=True)
    except BaseException: stop_flag.set(); raise

    # Auto-retry
    if not was_stopped: was_stopped=stop_flag.is_set()
    with lock: retry_s=list(set(failed))
    recovered_in_retry = set()
    if retry_s and not was_stopped and len(retry_s)<=total*0.5:
        logger.info("Retry pass starting for %d failed stock(s)", len(retry_s))
        status_ph.markdown(f'<div class="sbox">Retrying <b>{len(retry_s)}</b> failed...</div>',unsafe_allow_html=True)
        try:
            rd2=make_driver(); rw2=WebDriverWait(rd2,15); rd2.get(target_url); time.sleep(3)
            for stk in retry_s:
                if stop_flag.is_set(): break
                try:
                    res,status,bl=scrape_one_with_timeout(rd2,rw2,stk,target_url,priority_targets,blacklisted_companies,DH_PER_STOCK_TIMEOUT)
                    if status=="ok" and res and res.get("Stock Number","").strip():
                        recovered_in_retry.add(stk)
                        with lock:
                            results[:]=[r for r in results if r.get("Stock Number","")!=stk or r.get("P.NO 1","")!=""]
                            results.append(res); ctr["blacklisted"]+=bl
                            if row_has_priority(res,priority_targets): ctr["priority"]+=1
                            ctr["errors"]=max(0,ctr["errors"]-1)
                        # Replace any prior err/dead row for this stock with an 'ok' row.
                        try:
                            _c = _get_db()
                            _c.execute("DELETE FROM run_progress WHERE run_id=? AND stock_number=?",
                                       (run_id, str(stk)))
                            _c.commit(); _c.close()
                        except Exception:
                            logger.exception("Retry pass: failed to clear prior progress row for %s", stk)
                        db_record_stock(run_id, stk, "ok", res)
                except Exception:
                    logger.exception("Retry pass: failed to scrape %s", stk)
            rd2.quit()
            logger.info("Retry pass complete: recovered %d/%d", len(recovered_in_retry), len(retry_s))
        except Exception:
            logger.exception("Retry pass: driver setup or teardown failed")

    # Finalize
    with lock: final=list(results); d,p,bl,e=ctr["done"],ctr["priority"],ctr["blacklisted"],ctr["errors"]; ls=list(log_entries)
    final_failed = sorted(set(failed) - recovered_in_retry)
    ss.failed_stocks = final_failed
    elapsed=time.time()-t0; ss.elapsed=elapsed; ss.perf_data=perf_pts; ss.final_data=final
    if final:
        try:
            xb,pc,tr,ex=build_excel(final,priority_targets,blacklisted_companies)
            ts3=datetime.now().strftime("%Y%m%d_%H%M%S")
            base=ss.get("custom_name","3LINES_Results").strip() or "3LINES_Results"
            lbl="Partial" if was_stopped else "Result"
            ss.output_bytes=xb; ss.output_name=f"{base}_{lbl}_{ts3}.xlsx"
            ss.processed=tr; ss.priority_matches=pc; ss.blacklisted=bl+ex; ss.errors=e
        except Exception:
            logger.exception("Finalize: build_excel failed; falling back to live counters")
            ss.processed=d; ss.priority_matches=p; ss.blacklisted=bl; ss.errors=e
    else: ss.processed=d; ss.priority_matches=p; ss.blacklisted=bl; ss.errors=e
    ss.final_log=list(ls); ss.running=False; ss.completed=True; ss.stopped=was_stopped
    if pool is not None:
        pool.shutdown(wait=False)
    em3,es3=divmod(int(elapsed),60); eh3,em3=divmod(em3,60)
    ed=f"{eh3}h {em3:02d}m {es3:02d}s" if eh3 else f"{em3}m {es3:02d}s"
    ss.run_history.append({"date":datetime.now().strftime("%Y-%m-%d %H:%M"),"records":ss.processed,
        "total":total,"priority":ss.priority_matches,"blacklisted":ss.blacklisted,
        "errors":ss.errors,"elapsed":ed,"stopped":was_stopped})
    try:
        db_finalize_run(run_id, save_name, total,
                        ss.processed, ss.priority_matches, ss.blacklisted, ss.errors,
                        ed, was_stopped, final)
    except Exception:
        logger.exception("db_finalize_run failed; results stay in session only")
    # Clear resume flag — the run has been finalized one way or another.
    ss.resume_run_id = ""
    logger.info("Run finished: processed=%d, priority=%d, blacklisted=%d, errors=%d, failed=%d, elapsed=%s, stopped=%s",
                ss.processed, ss.priority_matches, ss.blacklisted, ss.errors,
                len(final_failed), ed, was_stopped)
    bar_ph.progress(min(d/total,1.0) if total else 0)
    ts4=d*MINUTES_PER_ITEM_MANUAL
    m1.markdown(rmetric("Records",f"{ss.processed:,}/{total:,}","g"),unsafe_allow_html=True)
    m2.markdown(rmetric("Priority",f"{ss.priority_matches:,}","b"),unsafe_allow_html=True)
    m3.markdown(rmetric("Blacklisted",f"{ss.blacklisted:,}","r"),unsafe_allow_html=True)
    m4.markdown(rmetric("Time Saved",f"{ts4:,}m","p"),unsafe_allow_html=True)
    m5.markdown(rmetric("Elapsed",ed,"b"),unsafe_allow_html=True)
    log_ph.markdown(render_log(ls),unsafe_allow_html=True)
