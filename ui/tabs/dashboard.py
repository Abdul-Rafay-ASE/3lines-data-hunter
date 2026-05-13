"""Dashboard tab — top-level stats + recent runs roster."""
import streamlit as st

from database.db import db_get_all_runs, db_get_total_stats
from ui.components import rmetric


def render():
    stats = db_get_total_stats()

    st.markdown('<div class="sec">Overview</div>', unsafe_allow_html=True)
    d1, d2, d3, d4 = st.columns(4)
    d1.markdown(rmetric("Total Runs", f"{stats['total_runs']}", "b"), unsafe_allow_html=True)
    d2.markdown(rmetric("All Records", f"{stats['total_records']:,}", "g"), unsafe_allow_html=True)
    d3.markdown(rmetric("Priority Stocks", f"{stats['total_priority']:,}", "p",
        help="Stocks where at least one supplier matched your priority list. Counted once per stock."),
        unsafe_allow_html=True)
    d4.markdown(rmetric("Total Errors", f"{stats['total_errors']:,}", "r"), unsafe_allow_html=True)

    st.markdown('<div class="sec">Recent Jobs</div>', unsafe_allow_html=True)
    runs = db_get_all_runs()
    if runs:
        for r in runs[:10]:
            status_label = "Stopped" if r["was_stopped"] else "Completed"
            st.markdown(
                f'<div class="hrow"><span>{status_label} - {r["save_name"]}</span><span class="hd">{r["created_at"][:16]}</span>'
                f'<span class="hr2c">{r["processed"]:,}/{r["total_stocks"]:,}</span><span class="hp">{r["priority_count"]:,} priority</span>'
                f'<span class="ht">{r["elapsed"]}</span></div>',
                unsafe_allow_html=True,
            )
    else:
        st.info("No jobs yet. Go to the Scraper tab to start your first search.")
