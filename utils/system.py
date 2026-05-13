"""Host-system inspection: memory, CPU, and a derived 'safe_bots' hint.

`get_system_status()` returns a dict with current memory/CPU stats and a
recommended worker count. The module also caches a one-shot snapshot at
import time as `_SYS` and exposes individual fields (AVAILABLE_GB, etc.)
for backwards compatibility with the original top-of-app.py layout.

Falls back to conservative defaults when psutil isn't installed.
"""
import os


PSUTIL_OK = False
try:
    import psutil
    PSUTIL_OK = True
except ImportError:
    pass


def get_system_status():
    if PSUTIL_OK:
        mem = psutil.virtual_memory()
        available_gb = round(mem.available / (1024 ** 3), 1)
        total_gb = round(mem.total / (1024 ** 3), 1)
        cpu_load = psutil.cpu_percent(interval=1)
        cpu_cores = psutil.cpu_count(logical=True) or os.cpu_count() or 2
    else:
        available_gb, total_gb, cpu_load = 4.0, 4.0, 0.0
        cpu_cores = os.cpu_count() or 2
    safe_bots = max(1, min(int(available_gb / 0.6), 5))
    if cpu_load > 70:
        safe_bots = max(1, safe_bots // 2)
    safe_bots = min(safe_bots, 5)  # Hard cap at 5 for stability
    return {"available_gb": available_gb, "total_gb": total_gb,
            "cpu_load": cpu_load, "cpu_cores": cpu_cores, "safe_bots": safe_bots}


# One-shot snapshot at import time; exposed as module-level constants for
# backwards compatibility with how app.py used to expose them.
_SYS = get_system_status()
AVAILABLE_GB = _SYS["available_gb"]
TOTAL_GB     = _SYS["total_gb"]
CPU_LOAD     = _SYS["cpu_load"]
CPU_CORES    = _SYS["cpu_cores"]
SMART_LIMIT  = _SYS["safe_bots"]
