"""Environment-driven configuration for 3LINES DataHunter.

Reads .env (via python-dotenv when available) and exposes:

    DH_DEFAULT_URL          → target URL pre-filled in the Scraper tab
    DH_MAX_BOTS             → ceiling on parallel scraper bots
    DH_AUTOSAVE_INTERVAL    → records between autosave snapshots
    DH_PER_STOCK_TIMEOUT    → per-stock wall-clock timeout (s)

    DEFAULT_URL             → alias for DH_DEFAULT_URL (legacy)
    AUTOSAVE_INTERVAL       → alias for DH_AUTOSAVE_INTERVAL (legacy)
    STATIC_BLACKLIST        → hardcoded substrings always filtered
    MINUTES_PER_ITEM_MANUAL → minutes-saved metric in the UI
    MAX_LOG_LINES           → in-memory live-log buffer size

All env-derived values fall back to safe defaults when unset or invalid.
"""
import os

# Best-effort load of a local .env so DH_PASSWORD (and other DH_* vars) are
# picked up during local development. Production deployments typically inject
# env vars directly and won't have python-dotenv as a hard requirement.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def _env_int(name, default):
    try:
        v = os.environ.get(name, "").strip()
        return int(v) if v else default
    except (TypeError, ValueError):
        return default


# ── Env-driven scraper config ──
DH_DEFAULT_URL       = os.environ.get("DEFAULT_TARGET_URL", "").strip() or "https://www.lqlite.com"
DH_MAX_BOTS          = max(1, _env_int("DH_MAX_BOTS", 10))
DH_AUTOSAVE_INTERVAL = max(1, _env_int("DH_AUTOSAVE_INTERVAL", 50))
DH_PER_STOCK_TIMEOUT = max(10, _env_int("DH_PER_STOCK_TIMEOUT", 60))

# ── Legacy aliases ──
DEFAULT_URL       = DH_DEFAULT_URL
AUTOSAVE_INTERVAL = DH_AUTOSAVE_INTERVAL

# ── Hardcoded application constants ──
STATIC_BLACKLIST        = ["A486G", "FINLAND"]
MINUTES_PER_ITEM_MANUAL = 2
MAX_LOG_LINES           = 30
