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
from collections import deque
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as _FuturesTimeoutError,
    as_completed,
)
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
from scraper.driver import make_driver, _CHROME_BIN, _CHROME_DRV, _CHROME_DEBUG
from scraper.scrape import _smart_wait, scrape_one_with_timeout




try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_OK = True
except ImportError:
    SELENIUM_OK = False

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
if is_dark:
    _bg="#0a0e1a"; _card="rgba(15,23,42,0.7)"; _card_solid="#0f172a"; _border="rgba(30,41,59,0.6)"; _border2="#334155"
    _text="#f1f5f9"; _text2="#cbd5e1"; _muted="#64748b"; _input="#0f172a"
    _accent="#3b82f6"; _accent2="#60a5fa"
    _green="#10b981"; _green2="#34d399"; _red="#ef4444"; _red2="#f87171"
    _yellow="#f59e0b"; _yellow2="#fbbf24"; _purple="#8b5cf6"; _purple2="#a78bfa"
    _glass_bg="rgba(15,23,42,0.6)"; _glass_border="rgba(59,130,246,0.15)"
    _shadow="rgba(0,0,0,0.4)"
else:
    _bg="#f0f4f8"; _card="rgba(255,255,255,0.75)"; _card_solid="#ffffff"; _border="rgba(226,232,240,0.8)"; _border2="#cbd5e1"
    _text="#0f172a"; _text2="#334155"; _muted="#64748b"; _input="#ffffff"
    _accent="#2563eb"; _accent2="#3b82f6"
    _green="#059669"; _green2="#10b981"; _red="#dc2626"; _red2="#ef4444"
    _yellow="#d97706"; _yellow2="#f59e0b"; _purple="#7c3aed"; _purple2="#8b5cf6"
    _glass_bg="rgba(255,255,255,0.6)"; _glass_border="rgba(37,99,235,0.12)"
    _shadow="rgba(0,0,0,0.06)"

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');

/* ── Keyframes ── */
@keyframes gradientShift {{
    0% {{ background-position: 0% 50%; }}
    50% {{ background-position: 100% 50%; }}
    100% {{ background-position: 0% 50%; }}
}}
@keyframes blink {{ 0%,100%{{opacity:1}} 50%{{opacity:.2}} }}
@keyframes fadeInUp {{
    from {{ opacity: 0; transform: translateY(12px); }}
    to {{ opacity: 1; transform: translateY(0); }}
}}
@keyframes pulseGlow {{
    0%,100% {{ box-shadow: 0 0 5px rgba(16,185,129,0.3); }}
    50% {{ box-shadow: 0 0 20px rgba(16,185,129,0.5); }}
}}
@keyframes shimmer {{
    0% {{ background-position: -200% 0; }}
    100% {{ background-position: 200% 0; }}
}}

/* ── Base ── */
.stApp {{
    background: {_bg} !important;
    font-family: 'Inter', sans-serif;
}}
section[data-testid="stSidebar"], #MainMenu, footer, header {{ display: none !important; }}
.stApp, .stApp p, .stApp span, .stApp label, .stApp div,
.stApp li, .stApp h1, .stApp h2, .stApp h3, .stApp h4,
.stApp summary, .stApp td, .stApp th, .stApp a,
.stApp strong, .stApp em, .stApp code {{ color: {_text} !important; }}

/* ── Premium Header ── */
.elite-header {{
    background: linear-gradient(135deg, #0a1628 0%, #0f2340 25%, #132d5e 50%, #0f2340 75%, #0a1628 100%);
    background-size: 300% 300%;
    animation: gradientShift 8s ease infinite;
    padding: 1.2rem 2.2rem;
    margin: -1rem -1rem 0 -1rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: relative;
    overflow: hidden;
    border-bottom: 1px solid rgba(59,130,246,0.2);
}}
.elite-header::before {{
    content: '';
    position: absolute;
    top: 0; left: -50%; width: 200%; height: 100%;
    background: linear-gradient(90deg, transparent, rgba(59,130,246,0.03), transparent);
    animation: shimmer 6s linear infinite;
}}
.elite-header::after {{
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, transparent 0%, {_accent} 20%, {_accent2} 50%, {_accent} 80%, transparent 100%);
    opacity: 0.6;
}}
.eh-brand {{
    display: flex; align-items: center; gap: 16px; z-index: 1;
}}
.eh-logo {{
    font-size: 1.7rem; font-weight: 900; color: #fff !important;
    letter-spacing: -0.5px;
    text-shadow: 0 0 30px rgba(59,130,246,0.3);
}}
.eh-logo b {{ color: #60a5fa !important; font-weight: 400; }}
.eh-sep {{ width: 1px; height: 28px; background: rgba(255,255,255,0.1); }}
.eh-sub {{ font-size: 0.72rem; color: rgba(180,200,230,0.6) !important; font-weight: 500; letter-spacing: 0.5px; }}
.eh-right {{ display: flex; align-items: center; gap: 14px; z-index: 1; }}
.eh-pill {{
    display: inline-flex; align-items: center; gap: 7px;
    background: rgba(16,185,129,0.08);
    border: 1px solid rgba(16,185,129,0.2);
    backdrop-filter: blur(10px);
    padding: 6px 16px; border-radius: 99px;
    font-size: 0.68rem; font-weight: 600; color: #34d399 !important;
    font-family: 'JetBrains Mono', monospace;
    animation: pulseGlow 3s ease-in-out infinite;
}}
.eh-pill .dot {{
    width: 6px; height: 6px; border-radius: 50%; background: #34d399;
    animation: blink 2s infinite;
}}
.eh-ver {{ font-size: 0.58rem; color: rgba(120,150,190,0.4) !important;
    font-family: 'JetBrains Mono', monospace; }}

/* ── Streamlit native tabs override ── */
.stTabs [data-baseweb="tab-list"] {{
    background: {_glass_bg} !important;
    backdrop-filter: blur(12px);
    border-bottom: 1px solid {_border} !important;
    gap: 0 !important;
    padding: 0 0.5rem !important;
    border-radius: 12px 12px 0 0;
}}
.stTabs [data-baseweb="tab"] {{
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 0.85rem !important;
    color: {_muted} !important;
    padding: 0.9rem 1.8rem !important;
    border-bottom: 2px solid transparent !important;
    background: transparent !important;
    transition: all 0.3s ease !important;
    letter-spacing: 0.3px !important;
}}
.stTabs [data-baseweb="tab"]:hover {{
    color: {_text} !important;
    background: rgba(59,130,246,0.05) !important;
}}
.stTabs [aria-selected="true"] {{
    color: {_accent2} !important;
    border-bottom: 2.5px solid {_accent2} !important;
    background: transparent !important;
}}
.stTabs [data-baseweb="tab-highlight"] {{
    background: {_accent2} !important;
}}
.stTabs [data-baseweb="tab-border"] {{
    display: none !important;
}}

/* ── Metric Cards (Premium Glass) ── */
.mc {{
    background: {_glass_bg};
    backdrop-filter: blur(12px);
    border: 1px solid {_glass_border};
    border-radius: 16px; padding: 1.3rem 1rem;
    text-align: center; position: relative; overflow: hidden;
    transition: all 0.3s cubic-bezier(0.4,0,0.2,1);
    animation: fadeInUp 0.5s ease-out;
}}
.mc:hover {{
    transform: translateY(-4px);
    box-shadow: 0 12px 40px {_shadow};
    border-color: rgba(59,130,246,0.3);
}}
.mc::before {{
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px;
}}
.mc.g::before {{ background: linear-gradient(90deg,{_green},{_green2}); }}
.mc.b::before {{ background: linear-gradient(90deg,{_accent},{_accent2}); }}
.mc.r::before {{ background: linear-gradient(90deg,{_red},{_red2}); }}
.mc.p::before {{ background: linear-gradient(90deg,{_purple},{_purple2}); }}
.mc .mv {{
    font-size: 1.8rem; font-weight: 800; line-height: 1.2; margin-top: 4px;
    font-family: 'JetBrains Mono', monospace;
}}
.mc .ml {{
    font-size: 0.6rem; text-transform: uppercase; letter-spacing: 2.5px;
    color: {_muted} !important; margin-top: 8px; font-weight: 700;
}}
.cv-green {{ color: {_green2} !important; }}
.cv-blue  {{ color: {_accent2} !important; }}
.cv-red   {{ color: {_red2} !important; }}
.cv-purple{{ color: {_purple2} !important; }}

/* ── Stat Grid ── */
.stat-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(155px,1fr));
    gap: 0.8rem; margin: 0.8rem 0;
}}
.stat-cell {{
    background: {_glass_bg};
    backdrop-filter: blur(10px);
    border: 1px solid {_glass_border}; border-radius: 12px;
    padding: 1rem 1.1rem; display: flex; justify-content: space-between; align-items: center;
    transition: all 0.25s ease;
}}
.stat-cell:hover {{
    transform: translateY(-2px);
    box-shadow: 0 8px 24px {_shadow};
}}
.stat-cell .sl {{ font-size: 0.72rem; color: {_muted} !important; font-weight: 600; }}
.stat-cell .sv {{
    font-size: 0.88rem; font-weight: 700;
    font-family: 'JetBrains Mono', monospace;
}}

/* ── Section Divider ── */
.sec {{
    font-size: 0.72rem; font-weight: 800; color: {_muted} !important;
    text-transform: uppercase; letter-spacing: 2.5px;
    margin: 1.4rem 0 0.7rem; display: flex; align-items: center; gap: 10px;
}}
.sec::after {{ content: ''; flex: 1; height: 1px; background: linear-gradient(90deg, {_border}, transparent); }}

/* ── Step Cards ── */
.step-card {{
    background: {_glass_bg};
    backdrop-filter: blur(12px);
    border: 1px solid {_glass_border};
    border-radius: 16px;
    padding: 1.6rem 1.2rem;
    text-align: center;
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}}
.step-card:hover {{
    transform: translateY(-3px);
    box-shadow: 0 10px 30px {_shadow};
    border-color: {_accent};
}}
.step-card.active {{
    border-color: {_accent};
    box-shadow: 0 0 20px rgba(59,130,246,0.15);
}}
.step-card.done {{
    border-color: {_green};
    box-shadow: 0 0 15px rgba(16,185,129,0.12);
}}
.step-num {{
    display: inline-flex; align-items: center; justify-content: center;
    width: 36px; height: 36px; border-radius: 50%;
    background: linear-gradient(135deg, {_accent}, {_accent2});
    color: #fff !important; font-weight: 800; font-size: 0.9rem;
    margin-bottom: 0.7rem;
}}
.step-num.done {{
    background: linear-gradient(135deg, {_green}, {_green2});
}}
.step-icon {{ font-size: 2rem; margin-bottom: 0.5rem; display: block; }}
.step-title {{ font-size: 0.9rem; font-weight: 700; color: {_text} !important; margin-bottom: 0.3rem; }}
.step-desc {{ font-size: 0.72rem; color: {_muted} !important; line-height: 1.5; }}

/* ── Inputs ── */
div[data-testid="stTextInput"] input,
div[data-testid="stNumberInput"] input,
div[data-testid="stTextArea"] textarea {{
    background: {_input} !important; color: {_text} !important;
    -webkit-text-fill-color: {_text} !important;
    border: 1px solid {_border} !important; border-radius: 10px !important;
    font-weight: 600 !important; font-size: 0.92rem !important;
    transition: all 0.25s ease !important;
}}
div[data-testid="stTextInput"] input:focus,
div[data-testid="stNumberInput"] input:focus {{
    border-color: {_accent2} !important;
    box-shadow: 0 0 0 3px rgba(59,130,246,0.12) !important;
}}
div[data-testid="stNumberInput"] button {{
    color: {_text} !important; background: {_card_solid} !important;
    border: 1px solid {_border} !important;
}}
.stApp label, div[data-testid="stWidgetLabel"] label,
div[data-testid="stWidgetLabel"] p {{
    color: {_text2} !important; font-weight: 700 !important; font-size: 0.82rem !important;
}}
.stApp .stCaption, .stApp small {{ color: {_muted} !important; }}
.stApp input::placeholder, .stApp textarea::placeholder {{
    color: {_muted} !important; opacity: 0.6 !important;
}}

/* ── Selectbox / Popover ── */
div[data-testid="stSelectbox"] > div > div {{
    background: {_input} !important; border: 1px solid {_border} !important; border-radius: 10px !important;
}}
div[data-testid="stSelectbox"] span {{ color: {_text} !important; }}
[data-baseweb="popover"] {{ background: {_input} !important; border: 1px solid {_border} !important; }}
[data-baseweb="popover"] ul {{ background: {_input} !important; }}
[data-baseweb="popover"] li, [data-baseweb="menu"] li, ul[role="listbox"] li {{
    background: {_input} !important; color: {_text} !important;
}}
[data-baseweb="popover"] li:hover, [data-baseweb="menu"] li:hover, ul[role="listbox"] li:hover {{
    background: {_card_solid} !important;
}}
ul[role="listbox"] {{ background: {_input} !important; }}

/* ── File Uploader (Premium Drop Zone) ── */
div[data-testid="stFileUploader"] > div {{
    background: {_glass_bg} !important;
    border: 2px dashed {_accent} !important;
    border-radius: 16px !important;
    transition: all 0.3s ease !important;
    padding: 1rem !important;
}}
div[data-testid="stFileUploader"] > div:hover {{
    border-color: {_green} !important;
    background: {'rgba(16,185,129,0.05)' if is_dark else 'rgba(16,185,129,0.03)'} !important;
    box-shadow: 0 0 30px rgba(16,185,129,0.1) !important;
}}
div[data-testid="stFileUploader"] span, div[data-testid="stFileUploader"] small,
div[data-testid="stFileUploader"] p, div[data-testid="stFileUploader"] div {{ color: {_text2} !important; }}
div[data-testid="stFileUploader"] button {{
    color: {_text} !important; background: {_card_solid} !important; border: 1px solid {_border} !important;
}}

/* ── Expander ── */
div[data-testid="stExpander"] {{
    background: {_card_solid} !important; border: 1px solid {_border} !important; border-radius: 12px !important;
}}
div[data-testid="stExpander"] details summary {{ color: {_text2} !important; font-weight: 700 !important; }}

/* ── Alert ── */
.stAlert, div[data-testid="stAlert"] {{
    background: {_card_solid} !important; border-color: {_border} !important; border-radius: 10px !important;
}}
.stAlert p, div[data-testid="stAlert"] p {{ color: {_text2} !important; }}

/* ── Progress ── */
.stProgress > div > div > div > div {{
    background: linear-gradient(90deg, {_accent}, {_green2}) !important; border-radius: 8px;
    box-shadow: 0 0 12px rgba(59,130,246,0.3);
}}
.stProgress > div > div > div {{ background: {_card_solid} !important; border-radius: 8px; }}

/* ── Dataframe ── */
.stDataFrame, div[data-testid="stDataFrame"] {{ background: {_card_solid} !important; border-radius: 12px; }}

/* ── Buttons ── */
.stApp button {{
    color: {_text} !important; background: {_card_solid} !important;
    border: 1px solid {_border} !important; border-radius: 10px !important;
    font-weight: 600 !important; transition: all 0.25s cubic-bezier(0.4,0,0.2,1) !important;
    white-space: pre-line !important; line-height: 1.4 !important;
    padding: 0.5rem 1rem !important;
}}
.stApp button:hover {{
    background: {_card_solid} !important;
    border-color: {_accent} !important;
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 12px {_shadow} !important;
}}
.stApp button p {{ color: inherit !important; white-space: pre-line !important; }}

/* ── Primary (START) Button - Big & Prominent ── */
.stApp button[kind="primary"] {{
    background: linear-gradient(135deg, {_green}, #047857) !important;
    color: #fff !important; font-weight: 900 !important; font-size: 16px !important;
    border: none !important; height: 3.5em !important;
    border-radius: 14px !important;
    box-shadow: 0 6px 20px rgba(16,185,129,0.35) !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
}}
.stApp button[kind="primary"]:hover {{
    background: linear-gradient(135deg,#047857,#065f46) !important;
    box-shadow: 0 8px 30px rgba(16,185,129,0.45) !important;
    transform: translateY(-2px) !important;
}}
.stApp button[kind="primary"] p {{ color: #fff !important; }}

/* ── Download Button ── */
div[data-testid="stDownloadButton"] button {{
    background: linear-gradient(135deg,{_accent},#1d4ed8) !important;
    color: #fff !important; font-weight: 700 !important; border: none !important;
    border-radius: 12px !important;
    box-shadow: 0 4px 14px rgba(59,130,246,0.25) !important;
}}
div[data-testid="stDownloadButton"] button:hover {{
    background: linear-gradient(135deg,#1d4ed8,#1e40af) !important;
    box-shadow: 0 6px 20px rgba(59,130,246,0.35) !important;
}}
div[data-testid="stDownloadButton"] button p {{ color: #fff !important; }}

/* ── STOP Button ── */
.stop-btn-wrap button {{
    background: linear-gradient(135deg, {_red}, #b91c1c) !important;
    color: #fff !important; font-weight: 900 !important; font-size: 15px !important;
    border: none !important; height: 3.5em !important;
    border-radius: 14px !important;
    box-shadow: 0 6px 20px rgba(239,68,68,0.3) !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
}}
.stop-btn-wrap button:hover {{
    background: linear-gradient(135deg,#b91c1c,#991b1b) !important;
    box-shadow: 0 8px 30px rgba(239,68,68,0.4) !important;
    transform: translateY(-2px) !important;
}}
.stop-btn-wrap button p {{ color: #fff !important; }}

/* ── Status Box ── */
.sbox {{
    padding: 14px 20px; border-radius: 12px;
    background: {_glass_bg};
    backdrop-filter: blur(10px);
    border: 1px solid {_glass_border};
    text-align: center;
    font-size: 0.92rem; font-weight: 700; color: {_accent2} !important;
}}

/* ── Live Log (Terminal Style) ── */
.llog {{
    background: {'#040810' if is_dark else '#1a1a2e'};
    border: 1px solid {'rgba(59,130,246,0.15)' if is_dark else 'rgba(0,0,0,0.1)'};
    border-radius: 14px;
    padding: 1rem 1.2rem; margin-top: 0.5rem;
    max-height: 220px; overflow-y: auto;
    font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; line-height: 1.8;
    box-shadow: inset 0 2px 8px rgba(0,0,0,0.3);
}}
.llog .lt {{
    color: {'#4a5568' if is_dark else '#a0aec0'} !important;
    font-size: 0.58rem; text-transform: uppercase;
    letter-spacing: 3px; margin-bottom: 0.5rem; font-weight: 700;
    padding-bottom: 0.4rem;
    border-bottom: 1px solid {'rgba(255,255,255,0.05)' if is_dark else 'rgba(255,255,255,0.1)'};
}}
.llog .ll {{ color: {'#a0aec0' if is_dark else '#cbd5e1'} !important; }}
.llog .ll .bi {{ color: {_accent2} !important; font-weight: 700; }}
.llog .ll .sn {{ color: {_green2} !important; }}
.llog .ll .lok {{ color: {_green2} !important; }}
.llog .ll .ler {{ color: {_red2} !important; font-weight: 700; }}
.llog .ll .lpr {{ color: {_yellow2} !important; font-weight: 700; }}
.llog .ll .lbl {{ color: {_red2} !important; font-weight: 700; }}

/* ── Banners ── */
.dbanner {{
    background: {'linear-gradient(135deg,rgba(7,26,18,0.9),rgba(10,38,24,0.9))' if is_dark else 'linear-gradient(135deg,#ecfdf5,#d1fae5)'};
    border: 1px solid {'rgba(22,101,52,0.5)' if is_dark else '#6ee7b7'};
    border-radius: 16px; padding: 2rem; margin: 1rem 0; text-align: center;
    backdrop-filter: blur(10px);
}}
.dbanner .dt {{ color: {_green2} !important; font-size: 1.3rem; font-weight: 800; }}
.dbanner .dm {{ color: {_muted} !important; font-size: 0.8rem; margin-top: 0.5rem;
    font-family: 'JetBrains Mono', monospace; }}

.sbanner {{
    background: {'linear-gradient(135deg,rgba(26,21,0,0.9),rgba(31,26,0,0.9))' if is_dark else 'linear-gradient(135deg,#fffbeb,#fef3c7)'};
    border: 1px solid {'rgba(133,77,14,0.5)' if is_dark else '#fcd34d'};
    border-radius: 16px; padding: 2rem; margin: 1rem 0; text-align: center;
    backdrop-filter: blur(10px);
}}
.sbanner .st2 {{ color: {_yellow2} !important; font-size: 1.3rem; font-weight: 800; }}
.sbanner .sm {{ color: {_muted} !important; font-size: 0.8rem; margin-top: 0.5rem;
    font-family: 'JetBrains Mono', monospace; }}

/* ── Misc ── */
.apbox {{
    background: {'rgba(7,26,18,0.7)' if is_dark else '#ecfdf5'};
    border: 1px solid {'rgba(22,101,52,0.4)' if is_dark else '#6ee7b7'};
    border-radius: 12px; padding: 12px 16px; font-size: 0.8rem; font-weight: 600; color: {_green2} !important;
    backdrop-filter: blur(8px);
}}
.apbox .apt {{ font-weight: 800; font-size: 0.85rem; color: {_green2} !important; }}
.apbox .apd {{ color: {_muted} !important; font-weight: 500; font-size: 0.72rem;
    font-family: 'JetBrains Mono', monospace; }}

.ramalert {{
    background: {'rgba(26,8,8,0.8)' if is_dark else '#fef2f2'}; border-left: 3px solid {_red};
    border-radius: 0 10px 10px 0; padding: 12px 16px; margin: 0.4rem 0;
    font-size: 0.82rem; font-weight: 700; color: {_red2} !important;
}}
.blwarn {{
    color: {_yellow2} !important; font-size: 0.76rem; font-weight: 600;
    padding: 0.4rem 0.7rem; background: {'rgba(26,21,0,0.7)' if is_dark else '#fffbeb'};
    border-left: 3px solid {_yellow}; border-radius: 0 6px 6px 0; margin-top: 0.3rem;
}}
.hr2 {{ height: 1px; background: linear-gradient(90deg,transparent,{_border},transparent);
    margin: 1.2rem 0; border: none; }}
.hrow {{
    background: {_glass_bg}; backdrop-filter: blur(8px);
    border: 1px solid {_glass_border}; border-radius: 12px;
    padding: 0.7rem 1.2rem; margin: 0.4rem 0; font-size: 0.8rem;
    display: flex; justify-content: space-between; align-items: center;
    transition: all 0.2s ease;
}}
.hrow:hover {{ border-color: {_accent}; transform: translateX(3px); }}
.hrow span {{ color: {_text} !important; }}
.hrow .hd {{ color: {_muted} !important; font-family: 'JetBrains Mono', monospace; font-size: 0.72rem; }}
.hrow .hr2c {{ color: {_green2} !important; font-weight: 700; }}
.hrow .hp {{ color: {_accent2} !important; font-weight: 700; }}
.hrow .ht {{ color: {_purple2} !important; font-family: 'JetBrains Mono', monospace; }}

.upload-placeholder {{
    background: {_glass_bg}; backdrop-filter: blur(12px);
    border: 2px dashed {_accent}; border-radius: 20px;
    padding: 3.5rem 2rem; text-align: center;
    transition: all 0.3s ease;
    animation: fadeInUp 0.6s ease-out;
}}
.upload-placeholder:hover {{ border-color: {_green}; }}
.upload-placeholder .up-icon {{ font-size: 3rem; margin-bottom: 1rem; }}
.upload-placeholder .up-title {{ font-size: 1.1rem; font-weight: 800; color: {_text} !important; }}
.upload-placeholder .up-sub {{ font-size: 0.8rem; color: {_muted} !important; margin-top: 0.4rem; }}

/* ── Speed Button Selected ── */
.speed-selected {{
    border: 2px solid {_green} !important;
    box-shadow: 0 0 15px rgba(16,185,129,0.2) !important;
}}

/* ── Health Bar ── */
.health-bar-track {{
    width: 100%; height: 8px; border-radius: 4px;
    background: {_card_solid}; overflow: hidden; margin-top: 6px;
}}
.health-bar-fill {{
    height: 100%; border-radius: 4px; transition: width 0.5s ease;
}}

/* ── Footer ── */
.footer {{
    text-align: center; padding: 1.5rem 0; margin-top: 2rem;
    border-top: 1px solid {_border}; font-size: 0.65rem;
    color: {_muted} !important; letter-spacing: 0.5px;
}}

/* ── Scrollbar ── */
::-webkit-scrollbar {{ width: 5px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: {_border2}; border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: {_accent}; }}
</style>
""", unsafe_allow_html=True)










# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UI RENDERERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def render_log(entries):
    lines = ""
    for e in entries:
        b, s, st2, n = e.get("bot","?"), e.get("stock",""), e.get("status",""), e.get("num","")
        if st2=="start": lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; Scraping <span class="sn">{s}</span>...</div>'
        elif st2=="ok": lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lok">Done</span></div>'
        elif st2=="priority": lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lpr">PRIORITY</span></div>'
        elif st2=="blocked": lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lbl">BLACKLISTED</span></div>'
        elif st2=="err": lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="ler">Error</span></div>'
        elif st2=="dead": lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> <span class="ler">CRASHED: {s} &#8212; restarting</span></div>'
        elif st2=="retry": lines += f'<div class="ll"><span class="bi">[Bot {b}]</span> #{n} &#8594; <span class="lpr">RETRY</span> <span class="sn">{s}</span></div>'
    return f'<div class="llog"><div class="lt">Live Processing Log</div>{lines}</div>'

def rmetric(label, value, color="g"):
    cmap = {"g": "cv-green", "b": "cv-blue", "r": "cv-red", "p": "cv-purple"}
    return f'<div class="mc {color}"><div class="mv {cmap.get(color,"cv-green")}">{value}</div><div class="ml">{label}</div></div>'


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ORCHESTRATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
        status_ph.error(err_msg or "\u274c Invalid file"); ss.running=False; return

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
            status_ph.markdown('<div class="sbox">All stocks were scraped recently \u2014 nothing to do. Disable smart-skip to re-run them.</div>', unsafe_allow_html=True)
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
