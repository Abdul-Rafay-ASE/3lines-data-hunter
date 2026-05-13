"""Chrome binary / chromedriver discovery and Selenium driver factory.

`make_driver()` returns a hardened headless Chrome driver. The Chrome
binary and chromedriver are discovered in this priority order:

    1. Common Linux system paths (/usr/bin/chromium, etc.) — used by
       the Docker image where Debian's `chromium` + `chromium-driver`
       packages are installed.
    2. `shutil.which()` lookup on PATH.
    3. ~/.chrome-for-testing/ auto-downloaded by `_auto_install_chrome`
       (Linux-only — the download URL filter is linux64).
    4. None / None → Selenium Manager will fetch a matching chromedriver
       and use system Chrome (the macOS dev path).

The Linux-only headless flags (`--single-process` etc.) are gated on
sys.platform.startswith('linux'); on macOS these crash the renderer
on launch.
"""
import json
import os
import shutil
import subprocess as _sp
import sys

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

from config import DH_PER_STOCK_TIMEOUT


def _auto_install_chrome():
    """Auto-download Chrome for Testing if not found on system."""
    home = os.path.expanduser("~")
    chrome_dir = os.path.join(home, ".chrome-for-testing")
    chrome_bin = os.path.join(chrome_dir, "chrome-linux64", "chrome")
    driver_bin = os.path.join(chrome_dir, "chromedriver-linux64", "chromedriver")

    if os.path.isfile(chrome_bin) and os.path.isfile(driver_bin):
        return chrome_bin, driver_bin

    os.makedirs(chrome_dir, exist_ok=True)

    # Get latest stable version
    import urllib.request
    try:
        ver_url = "https://googlechromelabs.github.io/chrome-for-testing/LATEST_VERSIONS_PER_MILESTONE_WITH_DOWNLOADS.json"
        with urllib.request.urlopen(ver_url, timeout=15) as resp:
            data = json.loads(resp.read().decode())

        # Find latest milestone
        milestones = sorted(data["milestones"].keys(), key=int, reverse=True)
        for ms in milestones:
            ms_data = data["milestones"][ms]
            chrome_url = None
            driver_url = None
            for d in ms_data.get("downloads", {}).get("chrome", []):
                if d["platform"] == "linux64":
                    chrome_url = d["url"]
                    break
            for d in ms_data.get("downloads", {}).get("chromedriver", []):
                if d["platform"] == "linux64":
                    driver_url = d["url"]
                    break
            if chrome_url and driver_url:
                break

        if not chrome_url or not driver_url:
            return None, None

        # Download and extract Chrome
        chrome_zip = os.path.join(chrome_dir, "chrome.zip")
        urllib.request.urlretrieve(chrome_url, chrome_zip)
        _sp.run(["unzip", "-o", "-q", chrome_zip, "-d", chrome_dir], timeout=60)
        os.remove(chrome_zip)

        # Download and extract ChromeDriver
        driver_zip = os.path.join(chrome_dir, "driver.zip")
        urllib.request.urlretrieve(driver_url, driver_zip)
        _sp.run(["unzip", "-o", "-q", driver_zip, "-d", chrome_dir], timeout=60)
        os.remove(driver_zip)

        # Make executable
        if os.path.isfile(chrome_bin):
            os.chmod(chrome_bin, 0o755)
        if os.path.isfile(driver_bin):
            os.chmod(driver_bin, 0o755)

        if os.path.isfile(chrome_bin) and os.path.isfile(driver_bin):
            return chrome_bin, driver_bin
    except Exception:
        pass
    return None, None


def _find_binary():
    for p in ["/usr/bin/chromium", "/usr/bin/chromium-browser",
              "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
              "/usr/lib/chromium/chromium"]:
        if os.path.isfile(p):
            return p
    for name in ["chromium", "chromium-browser", "google-chrome"]:
        found = shutil.which(name)
        if found and "completion" not in found:
            return found
    return None


def _find_driver():
    for p in ["/usr/bin/chromedriver", "/usr/lib/chromium/chromedriver",
              "/usr/lib/chromium-browser/chromedriver"]:
        if os.path.isfile(p):
            return p
    return shutil.which("chromedriver")


# Try system first, then auto-download
_CHROME_BIN = _find_binary()
_CHROME_DRV = _find_driver()

if not _CHROME_BIN or not _CHROME_DRV:
    _auto_bin, _auto_drv = _auto_install_chrome()
    if not _CHROME_BIN and _auto_bin:
        _CHROME_BIN = _auto_bin
    if not _CHROME_DRV and _auto_drv:
        _CHROME_DRV = _auto_drv

_CHROME_DEBUG = f"Binary: {_CHROME_BIN or 'None'} | Driver: {_CHROME_DRV or 'None'}"


def make_driver():
    opts = Options()
    for flag in ["--headless=new", "--no-sandbox", "--disable-dev-shm-usage",
                 "--disable-gpu", "--disable-extensions", "--disable-notifications",
                 "--disable-popup-blocking", "--log-level=3", "--window-size=1200,800",
                 "--disable-software-rasterizer", "--disable-background-networking",
                 "--disable-default-apps", "--disable-sync", "--disable-translate",
                 "--metrics-recording-only", "--no-first-run"]:
        opts.add_argument(flag)
    # --single-process / --no-zygote / --disable-setuid-sandbox are Linux-only.
    # On macOS they crash the renderer on launch ("invalid session id: session
    # deleted as the browser has closed the connection"). Gate strictly on Linux.
    if sys.platform.startswith("linux"):
        opts.add_argument("--single-process")
        opts.add_argument("--no-zygote")
        opts.add_argument("--disable-setuid-sandbox")
    opts.add_experimental_option('excludeSwitches', ['enable-logging'])
    opts.page_load_strategy = 'eager'
    if _CHROME_BIN:
        opts.binary_location = _CHROME_BIN
    if _CHROME_DRV:
        drv = webdriver.Chrome(service=Service(_CHROME_DRV), options=opts)
    else:
        drv = webdriver.Chrome(options=opts)
    # Derive page-load timeout from DH_PER_STOCK_TIMEOUT so Selenium raises a
    # clean TimeoutException before the scrape_one_with_timeout wall-clock
    # wrapper would abandon the worker thread. 15 s buffer below the wall-
    # clock; floor of 5 s so very tight DH_PER_STOCK_TIMEOUT values still
    # leave the browser a moment to navigate. Default DH_PER_STOCK_TIMEOUT=60
    # → page-load timeout = 45 s (unchanged from previous hardcoded value).
    drv.set_page_load_timeout(max(5, DH_PER_STOCK_TIMEOUT - 15))
    drv.set_script_timeout(20)
    drv.implicitly_wait(8)
    return drv
