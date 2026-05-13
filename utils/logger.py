"""Application logger configuration.

Exposes a single module-level `logger` named "datahunter". Console handler
is always attached; a RotatingFileHandler at <DH_LOG_DIR>/datahunter.log
(5 MB × 3 backups) is attached when the directory is writable.

Controllable via env:
    DH_LOG_LEVEL  default INFO; "" or "DISABLED" → console-only at WARNING.
    DH_LOG_DIR    default "logs" (relative paths resolved against APP_DIR).
"""
import logging
import os
from logging.handlers import RotatingFileHandler

from config import APP_DIR


def _configure_logger():
    lvl_name = (os.environ.get("DH_LOG_LEVEL") or "INFO").strip().upper()
    log = logging.getLogger("datahunter")
    log.propagate = False
    if log.handlers:
        return log  # already configured (e.g. on Streamlit reload)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if lvl_name in ("", "DISABLED"):
        log.setLevel(logging.WARNING)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        log.addHandler(sh)
        return log
    try:
        log.setLevel(getattr(logging, lvl_name, logging.INFO))
    except Exception:
        log.setLevel(logging.INFO)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(sh)
    log_dir = os.environ.get("DH_LOG_DIR", "logs").strip() or "logs"
    if not os.path.isabs(log_dir):
        log_dir = os.path.join(APP_DIR, log_dir)
    try:
        os.makedirs(log_dir, exist_ok=True)
        fh = RotatingFileHandler(
            os.path.join(log_dir, "datahunter.log"),
            maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8",
        )
        fh.setFormatter(fmt)
        log.addHandler(fh)
    except Exception as _e:
        log.warning("File logging disabled (%s): %s", log_dir, _e)
    return log


logger = _configure_logger()
