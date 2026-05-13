"""SQLite persistence layer for 3LINES DataHunter.

Three tables:
    runs          one row per scrape session, with terminal status
                  ('in_progress', 'completed', 'stopped').
    run_results   final per-stock results, written by db_finalize_run.
    run_progress  live per-stock outcomes ('ok' / 'err' / 'dead'),
                  written by db_record_stock as workers complete each
                  stock, used by Resume and the unfinished-run banner.

Schema is initialised at import time (CREATE TABLE IF NOT EXISTS), so
any module that does `from database.db import db_get_all_runs` etc.
gets a ready-to-query DB without needing a separate setup call.

No Streamlit / Selenium dependencies — pure data layer.
"""
import json
import os
import sqlite3
from datetime import datetime, timedelta

from config import APP_DIR
from utils.logger import logger


DB_PATH = os.path.join(APP_DIR, "datahunter_local.db")


def _get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_local_db():
    conn = _get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id          TEXT PRIMARY KEY,
            created_at      TEXT,
            status          TEXT DEFAULT 'completed',
            save_name       TEXT,
            total_stocks    INTEGER DEFAULT 0,
            processed       INTEGER DEFAULT 0,
            priority_count  INTEGER DEFAULT 0,
            blacklisted     INTEGER DEFAULT 0,
            errors          INTEGER DEFAULT 0,
            elapsed         TEXT,
            was_stopped     INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS run_results (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id      TEXT REFERENCES runs(run_id),
            stock_number TEXT,
            result_data TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_run_results ON run_results(run_id);

        -- Per-stock progress, written live during a run so an interrupted run
        -- can be resumed (or at minimum, not lost). Source of truth while a
        -- run is in_progress; finalize copies result_data into run_results
        -- so existing dashboard/export queries are unchanged.
        CREATE TABLE IF NOT EXISTS run_progress (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id       TEXT,
            stock_number TEXT,
            status       TEXT,           -- 'ok' | 'err' | 'dead'
            scraped_at   TEXT,
            result_data  TEXT             -- JSON, NULL when status != 'ok'
        );
        CREATE INDEX IF NOT EXISTS idx_run_progress_run ON run_progress(run_id);
        CREATE INDEX IF NOT EXISTS idx_run_progress_stock ON run_progress(run_id, stock_number);
    """)
    conn.commit()
    conn.close()


def db_get_all_runs():
    conn = _get_db()
    rows = conn.execute(
        "SELECT * FROM runs WHERE status != 'in_progress' ORDER BY created_at DESC"
    ).fetchall()
    result = [dict(r) for r in rows]
    conn.close()
    return result


def db_get_all_results():
    conn = _get_db()
    rows = conn.execute("""
        SELECT rr.stock_number, rr.result_data, r.run_id, r.created_at, r.save_name
        FROM run_results rr JOIN runs r ON rr.run_id = r.run_id
        ORDER BY r.created_at DESC, rr.id ASC
    """).fetchall()
    result = []
    for row in rows:
        data = json.loads(row["result_data"])
        data["_run_id"] = row["run_id"]
        data["_date"] = row["created_at"][:10]
        data["_save_name"] = row["save_name"]
        result.append(data)
    conn.close()
    return result


def db_get_run_results(run_id):
    conn = _get_db()
    rows = conn.execute(
        "SELECT result_data FROM run_results WHERE run_id=? ORDER BY id", (run_id,)
    ).fetchall()
    result = [json.loads(r["result_data"]) for r in rows]
    conn.close()
    return result


def db_get_total_stats():
    conn = _get_db()
    row = conn.execute("""
        SELECT COUNT(*) as total_runs,
               COALESCE(SUM(processed), 0) as total_processed,
               COALESCE(SUM(priority_count), 0) as total_priority,
               COALESCE(SUM(blacklisted), 0) as total_blacklisted,
               COALESCE(SUM(errors), 0) as total_errors,
               (SELECT COUNT(*) FROM run_results) as total_records
        FROM runs
        WHERE status != 'in_progress'
    """).fetchone()
    result = dict(row)
    conn.close()
    return result


def db_delete_run(run_id):
    conn = _get_db()
    conn.execute("DELETE FROM run_results WHERE run_id=?", (run_id,))
    conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
    conn.commit()
    conn.close()


def db_clear_all():
    conn = _get_db()
    conn.execute("DELETE FROM run_results")
    conn.execute("DELETE FROM run_progress")
    conn.execute("DELETE FROM runs")
    conn.commit()
    conn.close()


def db_start_run(run_id, save_name, total_stocks):
    """Create a runs row at status='in_progress' so an interrupted run is
    visible on next launch. Idempotent on resume — uses INSERT OR IGNORE
    so a resumed run keeps its original created_at and total_stocks."""
    conn = _get_db()
    conn.execute(
        """INSERT OR IGNORE INTO runs
           (run_id, created_at, status, save_name, total_stocks, processed)
           VALUES (?, ?, 'in_progress', ?, ?, 0)""",
        (run_id, datetime.now().isoformat(), save_name, total_stocks),
    )
    conn.execute(
        "UPDATE runs SET status='in_progress' WHERE run_id=?",
        (run_id,),
    )
    conn.commit()
    conn.close()


def db_record_stock(run_id, stock_number, status, result_dict=None):
    """Persist a single stock's outcome. Called from inside worker threads
    once per stock; SQLite WAL handles concurrent writers."""
    payload = json.dumps(result_dict, ensure_ascii=False) if result_dict else None
    try:
        conn = _get_db()
        conn.execute(
            """INSERT INTO run_progress
               (run_id, stock_number, status, scraped_at, result_data)
               VALUES (?, ?, ?, ?, ?)""",
            (run_id, str(stock_number), status, datetime.now().isoformat(), payload),
        )
        conn.commit()
        conn.close()
    except Exception:
        # Per-stock persistence failure must not abort the worker; log and
        # continue. Result is still in the in-memory results list and will
        # be picked up by the existing autosave / finalize paths.
        logger.exception("db_record_stock failed for run=%s stock=%s", run_id, stock_number)


def db_get_unfinished_runs(min_age_minutes=5):
    """Return runs still marked in_progress and at least min_age_minutes old.
    The age guard prevents the banner from firing on a run that's actively
    executing in another browser tab / process."""
    cutoff = (datetime.now() - timedelta(minutes=min_age_minutes)).isoformat()
    conn = _get_db()
    rows = conn.execute(
        """SELECT run_id, created_at, save_name, total_stocks, processed
           FROM runs WHERE status='in_progress' AND created_at < ?
           ORDER BY created_at DESC""",
        (cutoff,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def db_get_run_progress_stocks(run_id):
    """Set of normalized stock numbers already recorded under this run."""
    conn = _get_db()
    rows = conn.execute(
        "SELECT DISTINCT stock_number FROM run_progress WHERE run_id=?",
        (run_id,),
    ).fetchall()
    conn.close()
    return {str(r["stock_number"]).replace("-", "").strip() for r in rows if r["stock_number"]}


def db_get_run_progress_results(run_id):
    """List of successfully-scraped result dicts recorded under this run.
    Used to seed the in-memory results list when resuming a run."""
    conn = _get_db()
    rows = conn.execute(
        """SELECT result_data FROM run_progress
           WHERE run_id=? AND status='ok' AND result_data IS NOT NULL
           ORDER BY id""",
        (run_id,),
    ).fetchall()
    conn.close()
    out = []
    for r in rows:
        try:
            out.append(json.loads(r["result_data"]))
        except Exception:
            logger.exception("Failed to decode run_progress.result_data for run=%s", run_id)
    return out


def db_finalize_run(run_id, save_name, total_stocks, processed,
                    priority_count, blacklisted, errors, elapsed,
                    was_stopped, results):
    """Mark a run terminal and copy results into run_results so the
    dashboard / export code (which reads run_results) is unchanged."""
    final_status = "stopped" if was_stopped else "completed"
    conn = _get_db()
    conn.execute(
        """UPDATE runs
           SET status=?, save_name=?, total_stocks=?, processed=?,
               priority_count=?, blacklisted=?, errors=?, elapsed=?, was_stopped=?
           WHERE run_id=?""",
        (final_status, save_name, total_stocks, processed,
         priority_count, blacklisted, errors, elapsed,
         int(was_stopped), run_id),
    )
    if results:
        conn.execute("DELETE FROM run_results WHERE run_id=?", (run_id,))
        conn.executemany(
            "INSERT INTO run_results (run_id, stock_number, result_data) VALUES (?, ?, ?)",
            [(run_id, r.get("Stock Number", ""), json.dumps(r, ensure_ascii=False))
             for r in results],
        )
    conn.commit()
    conn.close()


def db_discard_run(run_id):
    """Fully remove an unfinished run: drops its progress rows, any partial
    run_results rows, and the runs row itself. The user explicitly chose to
    discard, so we leave no trace in the dashboard."""
    conn = _get_db()
    conn.execute("DELETE FROM run_progress WHERE run_id=?", (run_id,))
    conn.execute("DELETE FROM run_results WHERE run_id=?", (run_id,))
    conn.execute("DELETE FROM runs WHERE run_id=?", (run_id,))
    conn.commit()
    conn.close()


def db_get_recently_scraped_stocks(within_hours=24):
    """Return a set of stock numbers that were saved to the DB within the
    given window. Used by the optional smart-skip feature to avoid redoing
    recent successful work. Stocks are matched in their normalized form."""
    cutoff = (datetime.now() - timedelta(hours=within_hours)).isoformat()
    conn = _get_db()
    rows = conn.execute(
        """SELECT DISTINCT rr.stock_number
           FROM run_results rr
           JOIN runs r ON r.run_id = rr.run_id
           WHERE r.created_at >= ?""",
        (cutoff,),
    ).fetchall()
    conn.close()
    return {str(r["stock_number"]).replace("-", "").strip() for r in rows if r["stock_number"]}


# Initialise the schema at import time. Idempotent (CREATE TABLE IF NOT EXISTS).
init_local_db()
