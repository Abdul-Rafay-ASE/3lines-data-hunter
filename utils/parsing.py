"""Pure helpers for input parsing and priority/blacklist matching.

No side effects, no Streamlit / Selenium / DB dependencies. Safe to import
from any module.
"""
import io

import pandas as pd


def load_stocks_strict(fb):
    """Parse an uploaded Excel file into a normalized list of stock numbers.

    Expects stock numbers in column A starting at row 2 (row 1 = header).
    Dashes and spaces are stripped so "5305-01-104-8393" becomes
    "5305011048393", matching how the DB and scraper normalize them.

    Returns (stocks, error_message). On any rejection the stocks list is
    empty and error_message describes why; on success error_message is "".
    """
    xl = pd.ExcelFile(io.BytesIO(fb))
    df = pd.read_excel(io.BytesIO(fb), sheet_name=xl.sheet_names[0], dtype=str, header=0)
    if df.empty or len(df.columns) == 0:
        return [], "❌ File Rejected: Stock numbers must start from Row 2 in Column A"
    col_a = df.iloc[:, 0]
    if len(col_a) == 0 or pd.isna(col_a.iloc[0]) or str(col_a.iloc[0]).strip() == "":
        return [], "❌ File Rejected: Stock numbers must start from Row 2 in Column A"
    stocks = [str(v).strip().replace("-", "").replace(" ", "")
              for v in col_a if pd.notna(v) and str(v).strip()]
    if not stocks:
        return [], "❌ File Rejected: Stock numbers must start from Row 2 in Column A"
    return stocks, ""


def parse_comma_list(text):
    """Normalize a user-entered comma-separated string to an upper-cased list."""
    if not text or not text.strip():
        return []
    return [t.strip().upper() for t in text.split(",") if t.strip()]


def matches_company_list(mfg_name, company_list):
    """True if any company_list entry is a substring of mfg_name (case-insensitive)."""
    if not mfg_name or not company_list:
        return False
    mu = mfg_name.strip().upper()
    return any(t in mu for t in company_list)


def row_has_priority(row_dict, priority_list):
    """True if any MFG-named field in row_dict matches the priority list."""
    if not priority_list:
        return False
    return any(matches_company_list(str(v), priority_list)
               for k, v in row_dict.items() if k.startswith("MFG ") and v)


def row_is_blacklisted(row_dict, blacklist):
    """True if any MFG-named field in row_dict matches the blacklist."""
    if not blacklist:
        return False
    return any(matches_company_list(str(v), blacklist)
               for k, v in row_dict.items() if k.startswith("MFG ") and v and str(v).strip())
