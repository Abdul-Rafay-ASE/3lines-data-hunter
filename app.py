"""
3LINES DataHunter v16.0 - Elite Edition
Dynamic hardware inspection: Available RAM + Live CPU Load via psutil.
Safe Bots = Available_RAM / 0.6 GB per bot, halved if CPU > 70%.
Features: Run History, Multi-Format Export, Performance Chart,
Auto-Retry, Dark/Light Theme Toggle, Data Preview.
Strict Column A validation from Row 2. Dual filtering preserved.
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import re
import io
import os
import sys
import time
import json
import base64
import shutil
import sqlite3
import threading
from datetime import datetime, timedelta

from config import (
    DH_DEFAULT_URL, DH_MAX_BOTS, DH_AUTOSAVE_INTERVAL, DH_PER_STOCK_TIMEOUT,
    DEFAULT_URL, AUTOSAVE_INTERVAL,
    STATIC_BLACKLIST, MINUTES_PER_ITEM_MANUAL, MAX_LOG_LINES,
)
from utils.parsing import (
    load_stocks_strict, parse_comma_list, matches_company_list,
    row_has_priority, row_is_blacklisted,
)
from utils.logger import logger
from utils.system import (
    PSUTIL_OK, get_system_status,
    AVAILABLE_GB, TOTAL_GB, CPU_LOAD, CPU_CORES, SMART_LIMIT,
)
from database.db import (
    _get_db,
    db_clear_all,
    db_discard_run,
    db_finalize_run,
    db_get_all_results,
    db_get_all_runs,
    db_get_recently_scraped_stocks,
    db_get_run_progress_results,
    db_get_run_progress_stocks,
    db_get_run_results,
    db_get_total_stats,
    db_get_unfinished_runs,
    db_record_stock,
    db_start_run,
)
from exports.builders import (
    build_excel, build_csv, build_json, build_failed_excel,
)
from scraper.driver import _CHROME_BIN, _CHROME_DRV, _CHROME_DEBUG
from ui.components import render_log, rmetric
from ui.theme import apply_theme

# Selenium availability flag. The orchestrator (and the scraper modules it
# imports) hard-depend on selenium; if that import chain fails, the
# Scraper tab degrades to a "Selenium not installed" state instead of
# crashing the whole app.
SELENIUM_OK = False
try:
    from scraper.orchestrator import run_scraper
    SELENIUM_OK = True
except ImportError:
    pass

# ── Page Config ──
st.set_page_config(page_title="3LINES DataHunter", page_icon="3L",
                   layout="wide", initial_sidebar_state="collapsed")

# ── Optional Password Gate ──
# .env is loaded by config.py at import time; DH_PASSWORD is read directly
# from os.environ rather than re-exported because the value is only used
# right here.
_DH_PASSWORD = os.environ.get("DH_PASSWORD", "").strip()
if _DH_PASSWORD:
    if not st.session_state.get("_dh_authenticated"):
        st.markdown("## 3LINES DataHunter")
        st.caption("Enter the access password to continue.")
        with st.form("_dh_login", clear_on_submit=False):
            _pw = st.text_input("Password", type="password")
            _ok = st.form_submit_button("Sign in")
        if _ok:
            if _pw == _DH_PASSWORD:
                st.session_state["_dh_authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password.")
        st.stop()

# ── Session State ──
defaults = dict(
    running=False, completed=False, stopped=False,
    output_bytes=None, output_name="",
    processed=0, target=0, priority_matches=0, blacklisted=0, errors=0,
    start_time=0.0, elapsed=0.0,
    autosave_bytes=None, autosave_name="",
    stock_count=0, file_bytes=None, final_log=[],
    perf_data=[], failed_stocks=[], final_data=[], auto_downloaded=False,
    resume_run_id="",  # set when user clicks Resume on the unfinished-run banner
    smart_skipped=False,  # True when run_scraper early-returns because all stocks were recently scraped
)
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v
if "stop_flag" not in st.session_state:
    st.session_state.stop_flag = threading.Event()
if "run_history" not in st.session_state:
    st.session_state.run_history = []
if "theme" not in st.session_state:
    st.session_state.theme = "dark"

is_dark = st.session_state.theme == "dark"

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  THEME COLORS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Palette and CSS injection live in ui/theme.py. We unpack the colors
# dict back into module-level _bg / _text / etc. so the rest of app.py
# can interpolate them into inline st.markdown calls unchanged.
_colors = apply_theme(is_dark)
_bg=_colors["bg"]; _card=_colors["card"]; _card_solid=_colors["card_solid"]; _border=_colors["border"]; _border2=_colors["border2"]
_text=_colors["text"]; _text2=_colors["text2"]; _muted=_colors["muted"]; _input=_colors["input"]
_accent=_colors["accent"]; _accent2=_colors["accent2"]
_green=_colors["green"]; _green2=_colors["green2"]; _red=_colors["red"]; _red2=_colors["red2"]
_yellow=_colors["yellow"]; _yellow2=_colors["yellow2"]; _purple=_colors["purple"]; _purple2=_colors["purple2"]
_glass_bg=_colors["glass_bg"]; _glass_border=_colors["glass_border"]; _shadow=_colors["shadow"]














# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MAIN UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ss = st.session_state

# ── Load logo as base64 ──
_logo_b64 = ""
_logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logo.png")
if os.path.isfile(_logo_path):
    with open(_logo_path, "rb") as _lf:
        _logo_b64 = base64.b64encode(_lf.read()).decode()

_logo_html = f'<img src="data:image/png;base64,{_logo_b64}" height="34" style="margin-right:8px;">' if _logo_b64 else ''

# ── Header ──
st.markdown(f'''
<div class="elite-header">
    <div class="eh-brand">
        {_logo_html}
        <div class="eh-logo">3LINES <b>DataHunter</b></div>
        <div class="eh-sep"></div>
        <div class="eh-sub">Automated Data Collection &mdash; Smart Filtering &mdash; One-Click Export</div>
    </div>
    <div class="eh-right">
        <div class="eh-pill"><div class="dot"></div>Online</div>
        <div class="eh-ver">v16.0</div>
    </div>
</div>
''', unsafe_allow_html=True)

# ── Theme Toggle (small, clean, top-right) ──
_, tc = st.columns([11, 1])
with tc:
    tl = "Light" if is_dark else "Dark"
    if st.button(tl, key="thm"):
        ss.theme = "light" if is_dark else "dark"
        st.rerun()

# ── Tabs (native Streamlit ONLY) ──
tab_scraper, tab_dashboard, tab_database, tab_settings = st.tabs([
    "Scraper", "Dashboard", "Database", "Settings"
])


# ━━━━━━━━━ TAB: SCRAPER ━━━━━━━━━
with tab_scraper:

    # ── Unfinished-run banner ──
    # Surface any in_progress run older than 5 minutes so a crashed/closed
    # session can be resumed. No auto-resume — user must click Resume to opt in.
    if not ss.running:
        try:
            _unfinished = db_get_unfinished_runs(min_age_minutes=5)
        except Exception:
            logger.exception("Failed to query unfinished runs")
            _unfinished = []
        for _ur in _unfinished:
            _ur_id = _ur["run_id"]
            # runs.processed is only written on finalize, so it stays 0 for an
            # interrupted run. Read the live count from run_progress instead.
            try:
                _processed = len(db_get_run_progress_stocks(_ur_id))
            except Exception:
                logger.exception("Failed to count run_progress rows for %s", _ur_id)
                _processed = _ur.get("processed", 0) or 0
            _total = _ur.get("total_stocks", 0) or 0
            _name = _ur.get("save_name") or "(untitled)"
            _when = (_ur.get("created_at") or "")[:16].replace("T", " ")
            with st.container():
                st.warning(
                    f"**Unfinished run detected:** `{_name}` — "
                    f"{_processed:,}/{_total:,} stocks recorded, started {_when}. "
                    "Re-upload the original file and click Resume to continue, or Discard to dismiss."
                )
                _bc1, _bc2, _ = st.columns([1, 1, 6])
                with _bc1:
                    if st.button("Resume", key=f"resume_{_ur_id}", use_container_width=True):
                        ss.resume_run_id = _ur_id
                        logger.info("User clicked Resume for run %s", _ur_id)
                        st.rerun()
                with _bc2:
                    if st.button("Discard", key=f"discard_{_ur_id}", use_container_width=True):
                        try:
                            db_discard_run(_ur_id)
                            logger.info("User discarded unfinished run %s", _ur_id)
                        except Exception:
                            logger.exception("Failed to discard run %s", _ur_id)
                        if ss.get("resume_run_id") == _ur_id:
                            ss.resume_run_id = ""
                        st.rerun()
        if ss.get("resume_run_id"):
            st.info(f"Ready to resume run `{ss.resume_run_id}` — upload the original Excel file below to continue.")

    # Step indicators at the top
    has_file = ss.get("file_bytes") is not None or ss.completed
    s1_done = has_file
    s3_active = ss.running
    s3_done = ss.completed

    s1_cls = "done" if s1_done else "active"
    s2_cls = "done" if s1_done else ""
    s3_cls = "done" if s3_done else ("active" if s3_active else "")

    st.markdown(f'''
    <div style="display:flex; align-items:flex-start; gap:0; margin-bottom:1.5rem; position:relative;">
        <div class="step-card {s1_cls}" style="flex:1; animation: fadeInUp 0.4s ease-out;">
            <span class="step-num {'done' if s1_done else ''}">{'&#10003;' if s1_done else '1'}</span>
            <span class="step-icon">&#128196;</span>
            <div class="step-title">Upload File</div>
            <div class="step-desc">Excel file with stock numbers</div>
        </div>
        <div style="display:flex;align-items:center;padding-top:2.5rem;color:{_muted};font-size:1.5rem;margin:0 -0.3rem;">&#10132;</div>
        <div class="step-card {s2_cls}" style="flex:1; animation: fadeInUp 0.5s ease-out;">
            <span class="step-num {'done' if s1_done else ''}">{'&#10003;' if s1_done else '2'}</span>
            <span class="step-icon">&#9889;</span>
            <div class="step-title">Choose Speed</div>
            <div class="step-desc">Select search speed</div>
        </div>
        <div style="display:flex;align-items:center;padding-top:2.5rem;color:{_muted};font-size:1.5rem;margin:0 -0.3rem;">&#10132;</div>
        <div class="step-card {s3_cls}" style="flex:1; animation: fadeInUp 0.6s ease-out;">
            <span class="step-num {'done' if s3_done else ''}">{'&#10003;' if s3_done else '3'}</span>
            <span class="step-icon">&#128640;</span>
            <div class="step-title">Start & Download</div>
            <div class="step-desc">Run search & get results</div>
        </div>
    </div>
    ''', unsafe_allow_html=True)

    # ── Step 1: Upload & Config ──
    st.markdown('<div class="sec">Step 1 - Upload &amp; Configure</div>', unsafe_allow_html=True)

    cu, cf, cn = st.columns([2, 1, 1])
    with cu:
        target_url = st.text_input("Target Website URL", value=DEFAULT_URL)
    with cf:
        uploaded_file = st.file_uploader("Upload Excel", type=["xlsx","xls"])
    with cn:
        custom_name = st.text_input("Save File As:", value="3LINES_Results")
        ss.custom_name = custom_name

    with st.expander("Advanced Settings (Priority & Blacklist)", expanded=False):
        f1, f2 = st.columns(2)
        with f1:
            priority_input = st.text_input("Priority Companies", value="", placeholder="e.g. AMETEK, SAMI, BOEING")
        with f2:
            blacklist_input = st.text_input("Blacklisted Companies", value="", placeholder="e.g. HARSCO, ACME")
            st.markdown('<p class="blwarn">Blacklist adds extra processing time.</p>', unsafe_allow_html=True)
        skip_recent_chk = st.checkbox(
            "Skip stocks already successfully scraped within the last 24 hours",
            value=False,
            key="skip_recent_chk",
            help="OFF by default. When ON, stocks already in the local database from a successful run within the last 24h are skipped — useful when resuming a partial job.",
        )
        priority_targets = parse_comma_list(priority_input)
        blacklisted_companies = parse_comma_list(blacklist_input)

    if "priority_input" not in dir(): priority_input=""
    if "blacklist_input" not in dir(): blacklist_input=""
    priority_targets = parse_comma_list(priority_input)
    blacklisted_companies = parse_comma_list(blacklist_input)

    if uploaded_file:
        file_bytes = uploaded_file.getvalue(); ss.file_bytes = file_bytes
        detected_stocks, validation_error = load_stocks_strict(file_bytes)
        total_records = len(detected_stocks); ss.stock_count = total_records
        if validation_error and not ss.running and not ss.completed: st.error(validation_error)
        elif total_records == 0 and not ss.running and not ss.completed:
            st.error("File Rejected: Stock numbers must start from Row 2 in Column A")

        # ── Step 2: Speed ──
        st.markdown('<div class="sec">Step 2 - Choose Speed</div>', unsafe_allow_html=True)
        if "num_bots" not in ss: ss.num_bots = SMART_LIMIT
        if "speed_mode" not in ss: ss.speed_mode = "safe"
        # Each preset's bot count is capped at DH_MAX_BOTS so deployments can
        # tighten the ceiling via env var without code changes. "Maximum" maps
        # to whatever the configured cap is.
        _b_slow   = min(1,  DH_MAX_BOTS)
        _b_safe   = min(3,  DH_MAX_BOTS)
        _b_medium = min(6,  DH_MAX_BOTS)
        _b_fast   = DH_MAX_BOTS
        spm = {
            "slow":   {"b":_b_slow,   "l":"Careful",     "e":"\U0001f422",       "d":f"{_b_slow} bot{'s' if _b_slow>1 else ''} - safest"},
            "safe":   {"b":_b_safe,   "l":"Recommended", "e":"\U0001f6e1\ufe0f", "d":f"{_b_safe} bot{'s' if _b_safe>1 else ''} - stable"},
            "medium": {"b":_b_medium, "l":"Faster",      "e":"\u26a1",           "d":f"{_b_medium} bot{'s' if _b_medium>1 else ''} - quicker"},
            "fast":   {"b":_b_fast,   "l":"Maximum",     "e":"\U0001f680",       "d":f"{_b_fast} bot{'s' if _b_fast>1 else ''} - full power"},
        }
        s1,s2,s3,s4 = st.columns(4)
        for col,mk in zip([s1,s2,s3,s4],["slow","safe","medium","fast"]):
            m = spm[mk]; sel = mk==ss.speed_mode
            best = " *" if mk=="safe" else ""
            check = "\u2705 " if sel else ""
            with col:
                if st.button(f"{check}{m['e']} {m['l']}{best}\n{m['b']} bot{'s' if m['b']>1 else ''} - {m['d']}",
                             key=f"sp_{mk}", use_container_width=True,
                             type="primary" if sel else "secondary"):
                    ss.speed_mode=mk; ss.num_bots=m["b"]; st.rerun()
        num_bots = ss.num_bots; sm = spm[ss.speed_mode]
        st.markdown(f'<div class="apbox"><span class="apt">{sm["e"]} {sm["l"]} Mode &mdash; {sm["b"]} bot{"s" if sm["b"]>1 else ""}</span><br>'
                    f'<span class="apd">{sm["d"]}</span></div>', unsafe_allow_html=True)

        # ── Step 3: Controls ──
        st.markdown('<div class="sec">Step 3 - Start Search</div>', unsafe_allow_html=True)
        c2,c3,c4 = st.columns([2,1,1])
        with c2:
            mx2 = max(total_records,1)
            process_limit = st.number_input("How many to process (0 = all)", min_value=0, max_value=mx2, value=0, step=100, help="0 means process everything")
            st.caption(f"Will process all {total_records:,} records" if process_limit==0 else f"Will process first {process_limit:,} of {total_records:,}")
        with c3:
            can = total_records>0 and SELENIUM_OK and not ss.running and not ss.completed and not validation_error
            start_btn = st.button("START SEARCH", use_container_width=True, disabled=not can, type="primary")
            if not SELENIUM_OK: st.caption("Selenium not installed")
        with c4:
            st.markdown('<div class="stop-btn-wrap">', unsafe_allow_html=True)
            if st.button("STOP", use_container_width=True, key="stop_m"): ss.stop_flag.set()
            st.markdown('</div>', unsafe_allow_html=True)

        st.markdown('<div class="hr2"></div>', unsafe_allow_html=True)

        # ── Live Tracker ──
        st.markdown('<div class="sec">Live Progress</div>', unsafe_allow_html=True)
        c1,c2,c3,c4,c5 = st.columns(5)
        m1_ph,m2_ph,m3_ph,m4_ph,m5_ph = c1.empty(),c2.empty(),c3.empty(),c4.empty(),c5.empty()
        cd2 = ss.processed; ct2 = ss.target if ss.target>0 else total_records
        m1_ph.markdown(rmetric("Records",f"{cd2:,}/{ct2:,}","g"),unsafe_allow_html=True)
        m2_ph.markdown(rmetric("Priority",f"{ss.priority_matches:,}","b"),unsafe_allow_html=True)
        m3_ph.markdown(rmetric("Blacklisted",f"{ss.blacklisted:,}","r"),unsafe_allow_html=True)
        m4_ph.markdown(rmetric("Time Saved",f"{cd2*MINUTES_PER_ITEM_MANUAL:,}m","p"),unsafe_allow_html=True)
        m5_ph.markdown(rmetric("ETA","--","b"),unsafe_allow_html=True)

        init_pct = min(ss.processed/ss.target,1.0) if ss.target>0 else 0
        progress_bar = st.progress(init_pct)
        status_ph = st.empty(); log_ph = st.empty(); stop_ph = st.empty()
        if not ss.running and not ss.completed:
            status_ph.markdown(f'<div class="sbox">{total_records:,} records ready to process</div>',unsafe_allow_html=True)
            log_ph.markdown(render_log([]),unsafe_allow_html=True)
        if start_btn and not ss.running and not ss.completed:
            try:
                run_scraper(file_bytes,num_bots,process_limit,target_url,priority_targets,blacklisted_companies,
                    ss.stop_flag,status_ph,progress_bar,m1_ph,m2_ph,m3_ph,m4_ph,m5_ph,stop_ph,log_ph,
                    skip_recent=bool(skip_recent_chk))
                st.rerun()
            except Exception as e:
                ss.running=False; st.error(f"Crashed: {e}")
                import traceback; st.code(traceback.format_exc())

    elif not ss.completed:
        st.markdown(f'''<div class="upload-placeholder">
            <div class="up-icon">&#128196; &#10132; &#128194;</div>
            <div class="up-title">Upload Your Excel File</div>
            <div class="up-sub">
                Click <b>"Browse files"</b> above or drag & drop your file here<br>
                <span style="color:{_accent2}!important;">Supported: .xlsx and .xls</span> &mdash; Stock numbers in Column A, starting from Row 2
            </div>
        </div>''', unsafe_allow_html=True)

    # ── Completion ──
    if ss.completed:
        elapsed=ss.elapsed; total=ss.target; ts5=ss.processed*MINUTES_PER_ITEM_MANUAL
        em4,es4=divmod(int(elapsed),60); eh4,em4=divmod(em4,60)
        ed2=f"{eh4}h {em4:02d}m {es4:02d}s" if eh4 else f"{em4}m {es4:02d}s"
        if not uploaded_file:
            st.markdown('<div class="sec">Final Results</div>',unsafe_allow_html=True)
            r1,r2,r3,r4,r5=st.columns(5)
            r1.markdown(rmetric("Records",f"{ss.processed:,}/{total:,}","g"),unsafe_allow_html=True)
            r2.markdown(rmetric("Priority",f"{ss.priority_matches:,}","b"),unsafe_allow_html=True)
            r3.markdown(rmetric("Blacklisted",f"{ss.blacklisted:,}","r"),unsafe_allow_html=True)
            r4.markdown(rmetric("Time Saved",f"{ts5:,}m","p"),unsafe_allow_html=True)
            r5.markdown(rmetric("Elapsed",ed2,"b"),unsafe_allow_html=True)
        if ss.final_log: st.markdown(render_log(ss.final_log),unsafe_allow_html=True)
        if ss.perf_data and len(ss.perf_data)>2:
            with st.expander("Performance Chart",expanded=False):
                pdf=pd.DataFrame(ss.perf_data)
                pdf["rpm"]=pdf["records"].diff().fillna(0)*60
                if "elapsed" in pdf.columns:
                    ed3=pdf["elapsed"].diff().fillna(1).replace(0,1)
                    pdf["rpm"]=(pdf["records"].diff().fillna(0)/ed3*60)
                pdf["rpm"]=pdf["rpm"].clip(lower=0)
                st.line_chart(pdf[["elapsed","rpm"]].rename(columns={"elapsed":"Time(s)","rpm":"Rec/min"}).set_index("Time(s)"))
        if ss.output_bytes:
            if ss.stopped:
                st.balloons(); rem2=total-ss.processed if total>ss.processed else 0
                st.markdown(f'<div class="sbanner"><div class="st2">Stopped &amp; Saved</div>'
                    f'<div class="sm">{ss.processed:,} rows &bull; {rem2:,} remaining &bull; {ss.priority_matches:,} priority &bull; {ed2}</div></div>',unsafe_allow_html=True)
            else:
                st.balloons()
                st.markdown(f'<div class="dbanner"><div class="dt">Search Complete!</div>'
                    f'<div class="dm">{ss.processed:,} rows &bull; {ss.priority_matches:,} priority &bull; {ss.blacklisted:,} blacklisted &bull; {ss.errors:,} errors</div></div>',unsafe_allow_html=True)
            if not ss.auto_downloaded:
                b64=base64.b64encode(ss.output_bytes).decode()
                components.html(f'<script>var a=document.createElement("a");a.href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}";a.download="{ss.output_name}";document.body.appendChild(a);a.click();</script>',height=0)
                ss.auto_downloaded=True
            if ss.final_data:
                with st.expander("Preview Results (first 10 rows)",expanded=False):
                    pf=pd.DataFrame(ss.final_data[:10])
                    cs2=["Stock Number"]+[c for c in pf.columns if c!="Stock Number"]
                    st.dataframe(pf[cs2],use_container_width=True)
            st.markdown('<div class="sec">Download Your Files</div>',unsafe_allow_html=True)
            d1,d2,d3=st.columns(3)
            with d1: st.download_button(f"Download Excel",data=ss.output_bytes,file_name=ss.output_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)
            with d2:
                cv=build_csv(ss.final_data,priority_targets,blacklisted_companies)
                if cv: st.download_button(f"Download CSV",data=cv,file_name=ss.output_name.replace(".xlsx",".csv"),mime="text/csv",use_container_width=True)
            with d3:
                jv=build_json(ss.final_data,priority_targets,blacklisted_companies)
                if jv: st.download_button(f"Download JSON",data=jv,file_name=ss.output_name.replace(".xlsx",".json"),mime="application/json",use_container_width=True)
            # Failed-stocks export — only shown when at least one stock failed
            # to produce a result after the retry pass.
            if ss.failed_stocks:
                _failed_xlsx = build_failed_excel(ss.failed_stocks)
                if _failed_xlsx:
                    st.download_button(
                        f"Download Failed Stocks ({len(ss.failed_stocks):,})",
                        data=_failed_xlsx,
                        file_name=ss.output_name.replace(".xlsx", "_FAILED.xlsx"),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        help="Stocks that did not produce a result, even after the auto-retry pass. Re-feed this file into the Scraper tab to try again.",
                    )
        elif ss.smart_skipped:
            st.info("All stocks were scraped recently — nothing to do. Disable smart-skip to re-run them.")
        else:
            st.error(f"No results found. Errors: {ss.errors:,} | Time: {int(elapsed)}s")
        if st.button("Run Again",use_container_width=True, type="primary"):
            for k,v in defaults.items(): ss[k]=v
            ss.stop_flag.clear(); st.rerun()
    if ss.run_history:
        with st.expander(f"Run History ({len(ss.run_history)} runs)",expanded=False):
            for i,h in enumerate(reversed(ss.run_history)):
                ic="Stopped" if h.get("stopped") else "Done"
                st.markdown(f'<div class="hrow"><span>#{len(ss.run_history)-i} ({ic})</span>'
                    f'<span class="hd">{h["date"]}</span><span class="hr2c">{h["records"]:,}/{h["total"]:,}</span>'
                    f'<span class="hp">{h["priority"]:,} priority</span><span class="ht">{h["elapsed"]}</span></div>',unsafe_allow_html=True)
            if st.button("Clear History"): ss.run_history=[]; st.rerun()


# ━━━━━━━━━ TAB: DASHBOARD ━━━━━━━━━
with tab_dashboard:
    stats = db_get_total_stats()

    st.markdown('<div class="sec">Overview</div>', unsafe_allow_html=True)
    d1,d2,d3,d4 = st.columns(4)
    d1.markdown(rmetric("Total Runs", f"{stats['total_runs']}", "b"), unsafe_allow_html=True)
    d2.markdown(rmetric("All Records", f"{stats['total_records']:,}", "g"), unsafe_allow_html=True)
    d3.markdown(rmetric("Priority Found", f"{stats['total_priority']:,}", "p"), unsafe_allow_html=True)
    d4.markdown(rmetric("Total Errors", f"{stats['total_errors']:,}", "r"), unsafe_allow_html=True)

    st.markdown('<div class="sec">Recent Jobs</div>', unsafe_allow_html=True)
    runs = db_get_all_runs()
    if runs:
        for r in runs[:10]:
            status_label = "Stopped" if r["was_stopped"] else "Completed"
            st.markdown(f'<div class="hrow"><span>{status_label} - {r["save_name"]}</span><span class="hd">{r["created_at"][:16]}</span>'
                f'<span class="hr2c">{r["processed"]:,}/{r["total_stocks"]:,}</span><span class="hp">{r["priority_count"]:,} priority</span>'
                f'<span class="ht">{r["elapsed"]}</span></div>',unsafe_allow_html=True)
    else:
        st.info("No jobs yet. Go to the Scraper tab to start your first search.")


# ━━━━━━━━━ TAB: DATABASE ━━━━━━━━━
with tab_database:
    stats = db_get_total_stats()

    st.markdown('<div class="sec">Database Overview</div>', unsafe_allow_html=True)
    db1,db2,db3,db4 = st.columns(4)
    db1.markdown(rmetric("Runs", f"{stats['total_runs']}", "b"), unsafe_allow_html=True)
    db2.markdown(rmetric("Records", f"{stats['total_records']:,}", "g"), unsafe_allow_html=True)
    db3.markdown(rmetric("Priority", f"{stats['total_priority']:,}", "p"), unsafe_allow_html=True)
    db4.markdown(rmetric("Errors", f"{stats['total_errors']:,}", "r"), unsafe_allow_html=True)

    if stats["total_runs"]==0:
        st.info("No data saved yet. Run a search first, and results will appear here.")
    else:
        vm = st.radio("View Mode", ["All Combined","By Run"], horizontal=True, key="dbvm")
        if vm=="All Combined":
            ar = db_get_all_results()
            if ar:
                df=pd.DataFrame(ar)
                dc=["Stock Number"]+[c for c in df.columns if c not in ("Stock Number","_run_id","_date","_save_name")]+["_date","_save_name"]
                dc=[c for c in dc if c in df.columns]; df=df[dc].rename(columns={"_date":"Date","_save_name":"File"})
                st.dataframe(df,use_container_width=True,height=400)
                st.markdown('<div class="sec">Download All Data</div>', unsafe_allow_html=True)
                d1,d2=st.columns(2)
                with d1: st.download_button("Download as CSV",data=df.to_csv(index=False).encode("utf-8"),
                    file_name=f"ALL_{datetime.now():%Y%m%d}.csv",mime="text/csv",use_container_width=True)
                with d2:
                    xb2=io.BytesIO(); df.to_excel(xb2,index=False,engine="openpyxl"); xb2.seek(0)
                    st.download_button("Download as Excel",data=xb2.getvalue(),
                        file_name=f"ALL_{datetime.now():%Y%m%d}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)
        else:
            for r in db_get_all_runs():
                status_label = "Stopped" if r["was_stopped"] else "Completed"
                with st.expander(f'{status_label} - {r["save_name"]} | {r["processed"]:,} records | {r["created_at"][:16]}',expanded=False):
                    c1,c2,c3,c4=st.columns(4)
                    c1.metric("Processed",f"{r['processed']:,}"); c2.metric("Priority",f"{r['priority_count']:,}")
                    c3.metric("Blacklisted",f"{r['blacklisted']:,}"); c4.metric("Elapsed",r["elapsed"])
                    rr=db_get_run_results(r["run_id"])
                    if rr:
                        rdf=pd.DataFrame(rr); st.dataframe(rdf,use_container_width=True,height=300)
                        st.download_button(f"Download CSV",data=rdf.to_csv(index=False).encode("utf-8"),
                            file_name=f"{r['save_name']}_{r['run_id']}.csv",mime="text/csv",
                            key=f"dl_{r['run_id']}",use_container_width=True)
        st.markdown('<div class="hr2"></div>',unsafe_allow_html=True)
        if st.button("Clear All Database",key="clrdb",type="secondary"): db_clear_all(); st.rerun()


# ━━━━━━━━━ TAB: SETTINGS ━━━━━━━━━
with tab_settings:
    st.markdown('<div class="sec">Appearance</div>',unsafe_allow_html=True)
    st.markdown(f'''<div class="stat-cell" style="margin:0.5rem 0">
        <span class="sl">Current Theme</span>
        <span class="sv">{"Dark Mode" if is_dark else "Light Mode"}</span>
    </div>''',unsafe_allow_html=True)
    st.caption("Use the button in the top-right corner to switch between dark and light themes.")

    st.markdown('<div class="sec">System Status</div>',unsafe_allow_html=True)
    st.markdown(f'''
    <div class="stat-grid">
        <div class="stat-cell"><span class="sl">Selenium</span><span class="sv" style="color:{_green2 if SELENIUM_OK else _red2}!important">{"Ready" if SELENIUM_OK else "Not Found"}</span></div>
        <div class="stat-cell"><span class="sl">Chrome Binary</span><span class="sv" style="color:{_green2 if _CHROME_BIN else _red2}!important">{_CHROME_BIN or "Not Found"}</span></div>
        <div class="stat-cell"><span class="sl">ChromeDriver</span><span class="sv" style="color:{_green2 if _CHROME_DRV else _red2}!important">{_CHROME_DRV or "Not Found"}</span></div>
        <div class="stat-cell"><span class="sl">Installed Packages</span><span class="sv" style="font-size:0.65rem">{_CHROME_DEBUG}</span></div>
        <div class="stat-cell"><span class="sl">Hosting</span><span class="sv">Streamlit Cloud</span></div>
    </div>
    ''',unsafe_allow_html=True)

    st.markdown('<div class="sec">About</div>',unsafe_allow_html=True)
    st.markdown(f'''<div style="text-align:center; padding:2rem; background:{_glass_bg}; backdrop-filter:blur(12px);
        border:1px solid {_glass_border}; border-radius:16px;">
        <div style="font-size:1.6rem; font-weight:900; margin-bottom:0.5rem;">3LINES DataHunter</div>
        <div style="font-size:0.82rem; color:{_muted}!important; line-height:1.8;">
            v16.0 Elite Edition<br>
            Smart Filtering &bull; Auto-Retry &bull; Multi-Format Export<br>
            Priority Targets &bull; Blacklist Exclusion &bull; Auto-Save
        </div>
    </div>''', unsafe_allow_html=True)


# ── Footer ──
st.markdown(f'<div class="footer">3LINES DataHunter v16.0 &mdash; Automated Data Collection &bull; Smart Filtering &bull; One-Click Export</div>',unsafe_allow_html=True)
