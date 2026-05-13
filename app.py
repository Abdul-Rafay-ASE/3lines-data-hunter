"""
3LINES DataHunter v16.0 - Elite Edition
Dynamic hardware inspection: Available RAM + Live CPU Load via psutil.
Safe Bots = Available_RAM / 0.6 GB per bot, halved if CPU > 70%.
Features: Run History, Multi-Format Export, Performance Chart,
Auto-Retry, Dark/Light Theme Toggle, Data Preview.
Strict Column A validation from Row 2. Dual filtering preserved.
"""

import base64
import os
import threading

import streamlit as st

from ui.theme import apply_theme
from ui.tabs import (
    scraper as ui_scraper_tab,
    dashboard as ui_dashboard_tab,
    database as ui_database_tab,
    settings as ui_settings_tab,
)

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

# Apply theme CSS + collect color palette (used by tabs that render inline colors).
_colors = apply_theme(is_dark)


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
    ui_scraper_tab.render(defaults, _colors, SELENIUM_OK)

with tab_dashboard:
    ui_dashboard_tab.render()

with tab_database:
    ui_database_tab.render()

with tab_settings:
    ui_settings_tab.render(_colors, is_dark, SELENIUM_OK)


# ── Footer ──
st.markdown(f'<div class="footer">3LINES DataHunter v16.0 &mdash; Automated Data Collection &bull; Smart Filtering &bull; One-Click Export</div>',unsafe_allow_html=True)
