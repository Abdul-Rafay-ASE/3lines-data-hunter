# 3LINES DataHunter

Internal data-collection tool for 3LINES. It takes an Excel file containing
NSNs (military/aerospace stock numbers), searches them on the FLIS site
(`lqlite.com`) using one or more headless Chromium "bots" via Selenium,
extracts supplier / manufacturer / part-number information, applies priority
and blacklist rules, persists results to a local SQLite database, and exports
clean Excel / CSV / JSON reports.

The product is delivered as a single-page **Streamlit** web app
(`app.py`) with four tabs: Scraper, Dashboard, Database, Settings.

---

## Quick start (local)

Requires Python 3.11+ and a local Chromium / chromedriver pair (or the app's
auto-download fallback will fetch Chrome for Testing into `~/.chrome-for-testing/`).

```bash
python -m venv .venv
source .venv/bin/activate                # on Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Optional: copy the example env file and edit it
cp .env.example .env

streamlit run app.py
```

The app will open at <http://localhost:8501>.

---

## Docker (recommended deployment)

```bash
docker build -t 3lines-datahunter .
docker run --rm -p 8501:8501 \
    -e DH_PASSWORD=change-me \
    -v "$(pwd)/data:/app/data" \
    3lines-datahunter
```

Then open <http://localhost:8501>.

The image is based on `python:3.11-slim` and bundles Debian's `chromium` +
`chromium-driver`, so no host-side Chrome installation is required.

---

## Configuration

All variables are **optional**. Defaults work out of the box. See
[`.env.example`](.env.example) for the full list.

| Variable | Purpose | Default |
|---|---|---|
| `DEFAULT_TARGET_URL` | URL pre-filled in the Scraper tab | `https://www.lqlite.com` |
| `DH_MAX_BOTS` | UI ceiling for parallel scraper bots | `10` |
| `DH_AUTOSAVE_INTERVAL` | Records between autosave snapshots | `50` |
| `DH_PASSWORD` | Optional password gate. Empty ⇒ no gate. | _(empty)_ |

Set `DH_PASSWORD` whenever the app is reachable outside a trusted local
network.

---

## Project layout

```
3lines-data-hunter/
├── app.py              # Streamlit app: UI, scraper, DB, exports — the live product
├── logo.png            # Embedded as base64 in the header
├── Dockerfile          # python:3.11-slim + chromium + chromium-driver
├── requirements.txt    # Top-level Python deps (pip resolves transitives)
├── packages.txt        # Streamlit Cloud apt packages
├── .env.example        # Documented configuration variables
├── .gitignore
├── .dockerignore
├── legacy/             # Archived POC scripts — NOT part of the running product
│   ├── README.md
│   ├── scraper.py
│   ├── main_task.py
│   └── transformer.py
└── README.md
```

`legacy/` exists for historical reference only — the live app does not import
from it. See [`legacy/README.md`](legacy/README.md) for details.

---

## How it works (brief)

1. User uploads an Excel file with NSNs in **Column A starting at Row 2**.
2. User picks a speed preset (1 / 3 / 6 / 10 parallel bots).
3. The orchestrator fans the work out across worker threads. Each worker:
   - Boots a hardened headless Chromium with anti-bot flags.
   - Searches the target site, scrapes the result table, applies priority
     and blacklist filters, and yields per-stock rows.
4. Auto-save snapshots are written to disk every `DH_AUTOSAVE_INTERVAL`
   records. A retry pass re-runs failed stocks at the end (skipped if more
   than half failed).
5. Final results land in a local SQLite database and are downloadable as
   Excel, CSV, or JSON.

---

## Notes on responsible scraping

The default UI ceiling is 10 parallel bots, but you should usually run with
fewer. Increasing parallelism is **not** the recommended way to make scrapes
faster — per-bot efficiency improvements are. If you don't have explicit
authorization to scrape the target site at high concurrency, lower
`DH_MAX_BOTS` (or pick a slower preset in the UI) before deploying.

---

## License

Internal use only — see repository owner for terms.
