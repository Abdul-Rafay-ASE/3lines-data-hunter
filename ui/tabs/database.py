"""Database tab — combined / per-run view of stored results, plus clear-all."""
import io
from datetime import datetime

import pandas as pd
import streamlit as st

from database.db import (
    db_clear_all,
    db_get_all_results,
    db_get_all_runs,
    db_get_run_results,
    db_get_total_stats,
)
from ui.components import rmetric


def render():
    stats = db_get_total_stats()

    st.markdown('<div class="sec">Database Overview</div>', unsafe_allow_html=True)
    db1, db2, db3, db4 = st.columns(4)
    db1.markdown(rmetric("Runs", f"{stats['total_runs']}", "b"), unsafe_allow_html=True)
    db2.markdown(rmetric("Records", f"{stats['total_records']:,}", "g"), unsafe_allow_html=True)
    db3.markdown(rmetric("Priority Stocks", f"{stats['total_priority']:,}", "p",
        help="Stocks where at least one supplier matched your priority list. Counted once per stock."),
        unsafe_allow_html=True)
    db4.markdown(rmetric("Errors", f"{stats['total_errors']:,}", "r"), unsafe_allow_html=True)

    if stats["total_runs"] == 0:
        st.info("No data saved yet. Run a search first, and results will appear here.")
        return

    vm = st.radio("View Mode", ["All Combined", "By Run"], horizontal=True, key="dbvm")
    if vm == "All Combined":
        ar = db_get_all_results()
        if ar:
            df = pd.DataFrame(ar)
            dc = ["Stock Number"] + [c for c in df.columns if c not in ("Stock Number", "_run_id", "_date", "_save_name")] + ["_date", "_save_name"]
            dc = [c for c in dc if c in df.columns]
            df = df[dc].rename(columns={"_date": "Date", "_save_name": "File"})
            st.dataframe(df, use_container_width=True, height=400)
            st.markdown('<div class="sec">Download All Data</div>', unsafe_allow_html=True)
            d1, d2 = st.columns(2)
            with d1:
                st.download_button(
                    "Download as CSV",
                    data=df.to_csv(index=False).encode("utf-8"),
                    file_name=f"ALL_{datetime.now():%Y%m%d}.csv",
                    mime="text/csv",
                    use_container_width=True,
                )
            with d2:
                xb2 = io.BytesIO()
                df.to_excel(xb2, index=False, engine="openpyxl")
                xb2.seek(0)
                st.download_button(
                    "Download as Excel",
                    data=xb2.getvalue(),
                    file_name=f"ALL_{datetime.now():%Y%m%d}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
    else:
        for r in db_get_all_runs():
            status_label = "Stopped" if r["was_stopped"] else "Completed"
            with st.expander(f'{status_label} - {r["save_name"]} | {r["processed"]:,} records | {r["created_at"][:16]}', expanded=False):
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Processed", f"{r['processed']:,}")
                c2.metric("Priority", f"{r['priority_count']:,}")
                c3.metric("Blacklisted", f"{r['blacklisted']:,}")
                c4.metric("Elapsed", r["elapsed"])
                rr = db_get_run_results(r["run_id"])
                if rr:
                    rdf = pd.DataFrame(rr)
                    st.dataframe(rdf, use_container_width=True, height=300)
                    st.download_button(
                        "Download CSV",
                        data=rdf.to_csv(index=False).encode("utf-8"),
                        file_name=f"{r['save_name']}_{r['run_id']}.csv",
                        mime="text/csv",
                        key=f"dl_{r['run_id']}",
                        use_container_width=True,
                    )
    st.markdown('<div class="hr2"></div>', unsafe_allow_html=True)
    if st.button("Clear All Database", key="clrdb", type="secondary"):
        db_clear_all()
        st.rerun()
