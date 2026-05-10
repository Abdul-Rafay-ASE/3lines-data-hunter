# legacy/ — reference-only POC scripts

These files are **not** part of the running product. They predate the current
Streamlit app (`app.py`) and are kept here only as historical reference.

## Files

| File | Original purpose | Why it's legacy |
|---|---|---|
| `scraper.py` | Single-threaded CLI prototype that read `~/Desktop/Abdullah/input.xlsx` and exported a horizontal Excel report. | Superseded by `app.py`'s built-in scraper + Excel builder. Also contains a known bug (`processed_results` undefined at line ~325) and a hard-coded Desktop path. The session token previously embedded in `BASE_URL` has been removed — see commit history if you need to rotate the original. |
| `main_task.py` | Multi-bot ("SAMI TURBO") CLI prototype with interactive prompts, headless Chrome, smart scaling, and a master DB layer. | Superseded by `app.py`. Imports a missing `server/master_db` module and is missing a `from datetime import datetime` import — will not run as-is. |
| `transformer.py` | Standalone helper that converted "vertical" supplier-data Excel files into the OEM/PN horizontal layout. | The same logic now lives inside `app.py`'s `build_excel(...)`. |

## Rules for this folder

1. **Don't import from `legacy/` in the live app.** The active product
   (`app.py`) must remain self-contained.
2. **Don't reintroduce secrets.** If you need to revive any of this code, pull
   configuration (URLs, paths, tokens) from environment variables — never
   hard-code session tokens into source.
3. **Treat as read-only.** If you find yourself patching files here, that is a
   signal the work belongs in the live app instead.

If you decide a script here is permanently dead, move the deletion into a
single dedicated commit so its history stays easy to recover.
