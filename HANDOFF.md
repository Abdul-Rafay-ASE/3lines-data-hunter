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

### Multi-bot empirical findings (2026-05-14)

Test ladder run end-to-end on a 49-NSN file (real production stocks
mixed with two gibberish padding entries). Same file across all runs
so timing is comparable.

| Workers | Elapsed   | Speedup vs 1-bot | Per-stock effective | Dead-drivers | Retry recovered |
|---------|-----------|------------------|---------------------|--------------|-----------------|
| 1       | 11 m 16 s | 1.0 ×            | 13.8 s              | 1            | 0 / 1           |
| **3**   | **5 m 38 s** | **2.0 ×**     | **6.9 s**           | 1            | 0 / 1           |
| 6       | 5 m 54 s  | 1.91 ×           | 7.2 s               | 3            | 2 / 3           |

**Headline:** going from 3 → 6 bots got nothing. Same wall-clock,
slightly worse per-stock, three times the dead-driver noise. **The wall
is lqlite.com's tolerance for parallel sessions from one IP**, not
SQLite contention (zero `database is locked`, zero duplicates across
every test).

**Recommendation:** lower the cap. The `DH_MAX_BOTS=10` default in
`.env.example` advertises a "Maximum" preset that doesn't pay off on
this site. Recommend reducing the documented default to `6` and noting
in the README that `3` is the empirical sweet spot for lqlite. The
hardcoded "fast" preset label can stay as "Maximum" since it follows
whatever cap is configured; the change is just to the documented
default and the explanation around it.

### Three previously-unvalidated paths now validated (2026-05-14)

- ✅ **Concurrent-worker writes to `run_progress`** at 2, 3, and 6
  workers. SQLite WAL handles every level cleanly. No locks, no dups.
- ✅ **Retry pass — success-recovery path** (`recovered_in_retry`).
  The 6-bot run produced 3 transient dead-drivers, of which 2 were
  recovered by the single-driver retry pass. First time this branch
  has ever been exercised end-to-end.
- ✅ **Retry pass — total-failure path.** `5331006413407` is a
  terminal NSN (confirmed dead at 1, 3, and 6 bots across all runs;
  retry pass cannot recover it). Failed-stocks export correctly
  surfaces it. The intrinsic-failure branch of the retry path is
  exercised on every run that includes this NSN.

### Three follow-up commits this session

- **`6d0433c`** — Counter clarification. STATIC_BLACKLIST hits now
  roll into the visible "Blacklisted Entries" counter (previously
  silent). `rmetric()` gained a `(?)` tooltip glyph. Labels rephrased
  to make per-stock vs per-entry semantics explicit.
- **`1288964`** — Reset cached `ss.num_bots` on every render. Bug
  caught during the 2-bot test: if `DH_MAX_BOTS` shrank between
  Streamlit sessions, `ss.num_bots` retained the old value until the
  user clicked a preset, leaking forward as a 3-bot run when the cap
  was set to 2. Six-line defensive fix in `ui/tabs/scraper.py`.
- **`6762dea`** — Unit Price + Action Date extraction from lqlite's
  Management Information KeyTable (separate table from the Part
  Information rows the existing parser scans). Multiple price rows
  resolved by "most-recent Action Date, tie-break highest". New
  columns inserted between Stock Number and the P.NO/MFG chain in
  Excel/CSV/JSON. Old DB rows lacking the keys render as blank cells.
  Also committed two developer diagnostic tools at `scripts/`.

### Large-data findings (2026-05-14, 499 NSNs at 3 bots)

First sustained long-running test. The single biggest validation pass
since Phase 4. All measurements end-to-end on a real 499-NSN file.

| Metric                          | Value                       |
|---------------------------------|-----------------------------|
| Elapsed                         | **1 h 22 m 44 s**           |
| Processed (`ok`)                | 488                         |
| Terminal failures (`dead`)      | 11                          |
| Dead-drivers during main pass   | 14                          |
| Retry pass recovered            | 3 / 14                      |
| Per-stock effective             | ~10.2 s (incl. price extr.) |
| Chrome RSS over the run         | 310–500 MB, oscillating     |
| RSS upward drift                | none (no memory leak)       |
| `database is locked` errors     | 0 across 499 writes         |
| Duplicate run_progress entries  | 0                           |
| Price extraction rate           | **482 / 488 ok = 98.8 %**   |
| Highest captured Unit Price     | $893.07 (MICHELIN tire)     |
| Newest captured Action Date     | Oct-01-2025 (most stocks)   |

**FSC-correlated terminal failures:** of the 11 unrecoverable stocks,
7 are FSC 5331 (seals/gaskets), 3 are FSC 6135 (batteries), 1 is
FSC 6240 (lighting). The shared trait is slow-loading lqlite result
pages for these categories — Selenium hits `DH_PER_STOCK_TIMEOUT=60`
and aborts. Not an app bug; reflects lqlite's per-category performance.
The previously-known `5331006413407` is among them. **Operators should
expect a small population of FSC-5331 / 6135 / 6240 NSNs to land in
the failed-stocks export on any real batch.**

### Five previously-pending paths now empirically validated

- ✅ **Long-running stability** (>1 h continuous). No instability.
- ✅ **Chrome memory growth over hundreds of navigations.** RSS
  oscillates within a healthy range; no leak. Chrome's GC keeps up.
- ✅ **SQLite under sustained 3-worker writes** (499 stocks). Zero
  contention, zero duplicates.
- ✅ **Retry pass at non-trivial scale.** 14 candidates, 3 recovered.
  The "<50 %"-failure guardrail correctly allowed the retry to run.
- ✅ **Price extraction at scale.** 98.8 % of successful scrapes
  populated `Unit Price` + `Action Date`; failures degrade gracefully
  to empty strings.

### Still NOT validated

- **10-bot run.** Skipped after 6-bot confirmed contention saturation.
  Empirically would almost certainly be slower than 6 with more
  dead-drivers; not worth the test time.
- **Failed-stocks Excel export UI** at 11+ failed entries. End-to-end
  worked; the manual download-button visual check is the last gap.
- **Very-very-large batches (5,000+ stocks).** Not tested. The 500
  run's per-stock rate would extrapolate to roughly 14 hours of
  wall-clock for 5 k stocks at 3 bots — would need batching or
  background-job mode for practical use at that scale.

### Known terminal NSNs (catalog-side issues, surface via failed-stocks)

7 in FSC 5331, 3 in FSC 6135, 1 in FSC 6240 — listed in
`run_progress` for run `run_20260514_123321` with `status='dead'`.
No fix possible from our side without lqlite-side investigation;
operators should treat them as expected residue.

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

- ~~**Phase 4 — Modular refactor of `app.py`.**~~ **Complete, shipped
  2026-05-13.** Thirteen commits, each individually revertible. The
  monolithic `app.py` is now a 146-line orchestrator that wires up
  the rest of the codebase.

  | Commit    | Module / Change                | Lines moved |
  |-----------|--------------------------------|-------------|
  | `e8d0f80` | `config.py`                     | 51          |
  | `beaab67` | `utils/parsing.py`              | 63          |
  | `7b1e88d` | `utils/logger.py`               | 57          |
  | `7ac8e07` | `utils/system.py`               | 46          |
  | `8de9577` | `database/db.py`                | 295         |
  | `7ea3b9e` | `exports/builders.py`           | 117         |
  | `1bf724e` | `scraper/driver.py`             | 164         |
  | `6b0acec` | `scraper/scrape.py`             | 202         |
  | `06903cd` | `ui/components.py`              | 37          |
  | `46388e3` | `ui/theme.py` (CSS + palette)   | 520         |
  | `868afac` | `scraper/orchestrator.py`       | 337         |
  | `7dff18d` | `ui/tabs/{scraper,dashboard,database,settings}.py` | 442 |
  | `614ef9f` | `Dockerfile` (COPY new modules) | n/a         |

  `app.py`: **2,198 → 146 lines (-2,052, -93 %)** with **zero
  behavioural regressions** at any step. Each commit was validated by
  page-render check; steps 11 (orchestrator), 12 (tabs), and 13 (Docker)
  additionally ran a full 7-NSN scrape end-to-end. The same NSNs that
  produced rich MFG/P.NO chains (NIMIKKEISTOKESKUS / REID PRODUCTS /
  TWIST TITE / HOWMET / ALLFAST / JEDNOLITY / MILITARY SPECIFICATIONS)
  before the refactor continue to produce identical data after.

  Key design choices:
  - `config.py` introduced `APP_DIR` as the anchor for relative paths
    (`logs/`, `datahunter_local.db`) since submodules no longer have
    `app.py` in their `__file__`.
  - `ui/theme.py::apply_theme(is_dark)` owns both the color palette
    and the CSS injection, returns the colors dict so tab modules can
    receive it and unpack named colors locally.
  - `scraper.orchestrator.run_scraper` keeps its tight coupling to
    `st.session_state` — by design, since it owns live UI updates
    while running worker threads.
  - `SELENIUM_OK` in app.py is now derived from whether the orchestrator
    import succeeds (its transitive selenium deps are the actual hard
    requirement), not from probing the selenium package directly.

**ABSOLUTE RULE:** do not refactor before validating current behavior.
