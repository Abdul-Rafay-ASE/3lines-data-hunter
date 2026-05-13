# 3LINES DataHunter — Project Handoff Note

Hi Claude. I am continuing work on this repo on a new machine after cloning.
This note brings you up to date.

---

## A. Project context

- **Repo:** 3lines-data-hunter
- **Remote:** https://github.com/Abdul-Rafay-ASE/3lines-data-hunter
- **Branch:** master

3LINES DataHunter is a Streamlit web app that automates parts-data lookups
against `lqlite.com`.

**Flow:**

1. Upload Excel (NSNs / part numbers in column A from row 2).
2. A pool of headless Chromium bots (Selenium) hits the catalog.
3. Result rows are parsed into a structured record.
4. Priority list (e.g. `AMETEK` first) and blacklist (e.g. `MILITARY, FEDERAL,
   FINLAND, A486G`) applied.
5. Excel / CSV / JSON exports offered on completion.
6. Run history + per-stock results saved to local SQLite
   (`datahunter_local.db`).

Primary entrypoint: `app.py` (Streamlit). Legacy POC scripts live in `legacy/`
for reference only and are **NOT** part of the live app.

The user is at 3LINES Saudi Arabia (`subscriptions@3lines.com.sa`).
Repo memory captures these guardrails:

- No secrets in source (env vars only).
- Scraping ethics: optimize per-bot, don't scale by adding bots; cap max bots
  conservatively; make it configurable.
- Change cadence: gradual changes, validate each step, surprising new
  behaviors default OFF.
- AI policy: deterministic core; rapidfuzz/rules before LLM; never LLM in the
  scrape loop.

---

## B. What was completed (4 commits already on `master`)

### Phase 1 — `d1cd658` "Reorganize legacy POC files and add env-driven config skeleton"

- Moved `main_task.py`, `scraper.py`, `transformer.py` into `legacy/` with a
  `legacy/README.md` marking them reference-only.
- Sanitized `legacy/scraper.py`: removed hard-coded session URL.
- Added `.env.example` documenting `DEFAULT_TARGET_URL`, `DH_MAX_BOTS`,
  `DH_AUTOSAVE_INTERVAL`, `DH_PER_STOCK_TIMEOUT`, `DH_PASSWORD`,
  `DH_LOG_LEVEL`, `DH_LOG_DIR`.
- Added `.gitignore` covering `__pycache__/`, `.venv/`, `*.db`, `*.xlsx`,
  `.chrome-for-testing/`, `logs/`, `.env`.
- Trimmed `requirements.txt`; tightened `Dockerfile` / `.dockerignore`.
- Refreshed `README.md`.

### Phase 2 — `0aa0432` "Add per-stock timeout, failed-stocks export, smart-skip, env-driven bot cap"

All in `app.py`:

- `_env_int` helper + read `DEFAULT_TARGET_URL`, `DH_MAX_BOTS`,
  `DH_AUTOSAVE_INTERVAL`, `DH_PER_STOCK_TIMEOUT` from env.
- Speed-preset Maximum now equals `DH_MAX_BOTS`; lowering the cap shrinks
  every preset that exceeds it. Default `DH_MAX_BOTS=10` reproduces the prior
  UI exactly (1 / 3 / 6 / 10).
- New `scrape_one_with_timeout` wraps `scrape_one` with a wall-clock cap
  (default 60 s). On timeout the stock is marked `dead` and routes through
  the existing dead-handling path.
- Track `recovered_in_retry`; final `ss.failed_stocks` excludes
  retry-recovered stocks.
- New "Download Failed Stocks" Excel button on completion screen (only shown
  when ≥1 failed stock). Filename suffix `_FAILED.xlsx`.
- New checkbox under Advanced Settings: "Skip stocks already successfully
  scraped within the last 24 hours". Default OFF.
- New `db_get_recently_scraped_stocks(within_hours=24)` helper.

### Phase 3a — `a22afb9` "Add structured logging and instrument silent except blocks"

- New `_configure_logger()` at module load: stdout handler always on;
  `RotatingFileHandler` at `logs/datahunter.log` (5 MB × 3 backups) when the
  dir is writable.
- Configurable via env: `DH_LOG_LEVEL` (default `INFO`; `""` or `DISABLED` =
  file logging off, console at WARNING) and `DH_LOG_DIR` (default `logs`,
  relative paths resolved against `app.py`).
- All 12 silent `except: pass` / bare `except:` blocks in `run_scraper`
  replaced with `except Exception:` + `logger.exception` (or `logger.debug`
  with `exc_info=True` for best-effort cleanup). **NO control-flow change.**
  Failures now leave a traceback on disk.
- Lifecycle INFO logs at run start/finish, worker boot/finish, driver reboots
  (cooldown + post-dead), retry pass start/end, smart-skip early return.

### Phase 3b — `c46d726` "Add incremental progress persistence and resume-after-crash"

- New SQLite table `run_progress` (run_id, stock_number, status, scraped_at,
  result_data) written one row per stock as the worker completes it. WAL
  handles concurrent writers.
- `runs.status` now meaningful: `in_progress` (set at start), `completed` /
  `stopped` (set at finalize). New `db_start_run` opens the run; new
  `db_finalize_run` flips to terminal state and copies progress rows into
  `run_results` so dashboard / export queries are unchanged.
- `db_get_all_runs` and `db_get_total_stats` filter `in_progress` out so live
  runs don't ghost-list themselves.
- New unfinished-run banner at the top of the Scraper tab. Fires only for
  runs older than 5 minutes still `in_progress`. Yellow banner with Resume /
  Discard buttons. **NO auto-resume.**
- Resume reuses the same `run_id`, skips already-`ok` stocks, retries
  `err`/`dead` ones (their old progress rows are cleared first), and seeds
  in-memory results from `run_progress` so the live UI + final export include
  both old and new.
- Discard fully removes the run (`runs` row + `run_progress` rows + any
  partial `run_results` rows).
- Removed unused `db_save_run`; `db_finalize_run` is the single
  terminal-state writer.

---

## C. Current state

- `app.py` is still the primary Streamlit app (~2,190 lines after the
  2026-05-12 fixes).
- The four phase commits **plus** three follow-up fixes are pushed to
  `origin/master`.
- The app has been validated end-to-end on macOS Apple Silicon and inside
  the Docker image. **Production-ready for local internal use.**

### Validation pass — 2026-05-12 (new Mac)

End-to-end smoke test completed against real lqlite.com NSNs. All of these
now work:

- Streamlit launch on macOS (no traceback at startup).
- 4 tabs render: Scraper / Dashboard / Database / Settings.
- 7-stock scrape on Slower(1) preset — populated MFG/P.NO fields, 0 errors.
- Excel / CSV / JSON downloads open cleanly with correct data.
- `logs/datahunter.log` written with lifecycle INFO lines (run start/finish,
  worker boot/finish, smart-skip early return, Resume, Discard).
- Smart-skip checkbox filters all recently-scraped stocks (24h window),
  short-circuits with 0s elapsed.
- Resume + Discard flow: kill mid-run, wait >5 min, banner appears with
  correct live count from `run_progress`, Resume reuses same `run_id` and
  skips already-`ok` stocks, Discard fully removes the run from all three
  DB tables.
- `DH_MAX_BOTS=3` correctly collapses speed presets to 1 / 3 / 3 / 3.
- Docker image builds (~1.86 GB on disk, 467 MB compressed) and scrapes
  successfully inside the container.
- **Failed-stocks export path** (2026-05-13). Forced 5/7 timeouts by
  setting `DH_PER_STOCK_TIMEOUT=8` so real NSNs (which take 10-17 s) all
  fired the wall-clock cap. `scrape_one_with_timeout` marked each as
  `dead` and rebooted the driver. The `>50%-failed` guardrail correctly
  skipped the retry pass (5/7 = 71 %). Completion screen rendered the
  `Download Failed Stocks (5)` button, the `_FAILED.xlsx` download
  contained exactly the 5 real NSNs (gibberish entries that returned
  empty-but-fast were correctly excluded as `ok`). Restored `.env` to
  `DH_PER_STOCK_TIMEOUT=60` after the test.

### Three fixes added on top of phase commits

- **`d0ba1f3`** — Fix `--single-process` crashing Chrome renderer on macOS.
  Pre-existing bug. `make_driver()` gated Linux-only Chrome flags on
  `os.name != "nt"`, which is True on both Linux and macOS. macOS crashed
  every Selenium session at start with "invalid session id: ... browser has
  closed the connection". Switched the guard to
  `sys.platform.startswith("linux")`. **Blocking fix — without it the app
  cannot run on macOS at all.**
- **`c7e7ee8`** — Preserve smart-skip success message across Streamlit
  rerun. The success banner was set on a placeholder then immediately wiped
  by `st.rerun()`, leaving a misleading red "No results found" error as the
  only visible feedback. Added `ss.smart_skipped` flag and branched the
  completion screen on it.
- **`5149e57`** — Show live `run_progress` count in unfinished-run banner.
  The banner read from `runs.processed` (only set on finalize), so crashed
  runs always displayed "0/N stocks recorded" even when several stocks were
  already saved. Banner now reads the live count from `run_progress`; the
  resume logic itself was already correct.

### Still NOT validated

- **Retry pass (`<50 %`-failure path).** Today's failed-stocks test crossed
  the `>50 %` guardrail, so the retry pass was deliberately skipped. The
  *successful* retry path — where 1-2 stocks fail, the retry pass runs
  with a fresh single driver, and some failures get recovered into
  `recovered_in_retry` — was never exercised. To test it, induce a small
  number of failures (e.g. 1-2 unreachable URLs in a 10-stock file).
- **Concurrent-worker writes to `run_progress`** — only validated with 1
  worker. Should test with 3 workers on a larger batch before assuming
  WAL handles the contention cleanly.

---

## D. What the user should do on the new device

```bash
# 1. Clone
git clone https://github.com/Abdul-Rafay-ASE/3lines-data-hunter.git
cd 3lines-data-hunter

# 2. Virtualenv
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Optional .env (only if you want to override defaults)
cp .env.example .env
# edit .env if needed

# 5. Run Streamlit
streamlit run app.py

# 6. Open the URL Streamlit prints (usually http://localhost:8501)

# 7. Watch logs in another terminal
tail -f logs/datahunter.log

# 8. Inspect SQLite
sqlite3 datahunter_local.db
# .tables
# SELECT run_id, status, total_stocks, processed
#   FROM runs ORDER BY created_at DESC LIMIT 5;
# .quit

# 9. Docker (only after Docker Desktop is running)
open -a Docker
# wait ~30-60s for the whale icon to settle
docker build -t 3lines-datahunter:phase3 .
docker run --rm -p 8501:8501 --env-file .env 3lines-datahunter:phase3
```

---

## E. What Claude Code should do on the new device

**DO NOT START NEW FEATURES IMMEDIATELY. Validate first.**

Order of operations:

1. Verify the app runs locally with no traceback at launch.
2. Run the manual smoke checklist in section F.
3. Verify `logs/datahunter.log` gets written during a real run (timestamped
   INFO lines for run start/finish, worker boot/finish, retry pass).
4. Verify SQLite history: terminated runs show `status='completed'` or
   `'stopped'` (not `'in_progress'`).
5. Verify the failed-stocks download button appears when failures exist and
   produces a valid Excel.
6. Verify smart-skip: re-running the same file with the checkbox ON should
   report "all stocks were scraped recently".
7. Verify resume-after-crash: kill mid-run, wait >5 min, restart, confirm the
   yellow banner appears with correct counts, click Resume, re-upload the
   same file, confirm only unscraped stocks are processed and the same
   `run_id` is reused.
8. Verify Docker build + run only AFTER Docker Desktop is up.
9. If a smoke test fails: prefer fix-forward, but if a single commit is the
   obvious culprit, `git revert <sha>` cleanly rolls it back without
   disturbing the others. The four phase commits are deliberately small and
   independently revertible.

**DO NOT** start P3c (lower Selenium page-load timeout), P3d (replace
`time.sleep` with `WebDriverWait`), or Phase 4 (modular refactor of `app.py`
into `ui/`, `scraper/`, `database/`, `exports/`, `utils/`, `config.py`)
**UNTIL P3a + P3b are validated in a real browser.** Refactoring unverified
code wastes effort if the verification reveals bugs.

---

## F. Manual test checklist (concise)

- [ ] **App launch:** `streamlit run app.py` → page renders, four tabs
      present, no traceback in terminal.
- [ ] **Tabs render:** Scraper / Dashboard / Database / Settings all open
      without errors.
- [ ] **Upload small Excel** (5–10 stocks in column A from row 2): counter
      shows the correct count.
- [ ] **Run 5–10 stock scrape** using "Slower (1)" preset: progress bar
      advances, completion screen shows Excel/CSV/JSON download buttons.
- [ ] **Check downloads:** each format opens cleanly and contains the
      scraped rows.
- [ ] **Failed-stocks download:** if any stocks failed, the "Download Failed
      Stocks (N)" button appears and produces `<save_name>_FAILED.xlsx`.
- [ ] **`logs/datahunter.log`:** tail it during the run; expect lines like
      `Run started: ...`, `Worker 1: booting driver`, `Run finished: ...`.
- [ ] **`DH_MAX_BOTS=3`:** stop, restart with this env, confirm speed
      presets show 1 / 3 / 3 / 3.
- [ ] **Smart-skip:** run a file successfully, then enable the checkbox,
      re-run, expect "all stocks were scraped recently".
- [ ] **Resume / Discard:** described in section E step 7.
- [ ] **Docker:** build + run AFTER Docker Desktop is up.

---

## G. Next recommended phases (after validation)

If all smoke tests pass:

- ~~**P3c — Lower the Selenium page-load timeout** closer to
  `DH_PER_STOCK_TIMEOUT - 15`.~~ **Shipped 2026-05-13 as `c4ce3e7`.**
  `make_driver()` now calls `set_page_load_timeout(max(5, DH_PER_STOCK_TIMEOUT - 15))`.
  At the default `DH_PER_STOCK_TIMEOUT=60` the formula evaluates to 45 —
  byte-identical to the previous hardcoded value, so default behaviour is
  unchanged. The change only matters when the operator tightens the
  wall-clock cap (e.g. when forcing the failed-stocks path).

- ~~**P3d — Replace remaining `time.sleep(...)` calls in `scrape_one` with
  `WebDriverWait` polls.**~~ **Shipped 2026-05-13 as `e91a528`.** Rewrote
  `_smart_wait` with `WebDriverWait(..., poll_frequency=0.2)` and removed
  three cargo-cult sleeps inside `scrape_one`. Validated against the same
  7-NSN file — 7/7 still `ok`, data still populated, wall-clock went from
  1m 07s to 1m 05s (only -3 %). **The dominant cost is now confirmed to be
  lqlite.com's server-side render time (9-15 s per real NSN), not local
  polling overhead.** Further per-bot client-side optimization will have
  diminishing returns; if more speed is needed, the answer is either
  parallel workers (with care for scraping ethics) or asking lqlite.com to
  expose a faster API.

- **Phase 4 — Modular refactor of `app.py`** (~1900 lines) into:

  - `ui/` — Streamlit tabs and rendering
  - `scraper/` — Selenium worker + `scrape_one`
  - `database/` — SQLite layer (helpers + schema)
  - `exports/` — Excel / CSV / JSON builders
  - `utils/` — logging, env helpers, etc.
  - `config.py` — all env-driven settings centralized

  Best done **LAST**, on stable validated code. Don't refactor before
  P3c/P3d ship; refactoring multiplies the risk of regressing behavior we
  haven't yet confirmed.

**ABSOLUTE RULE:** do not refactor before validating current behavior.
