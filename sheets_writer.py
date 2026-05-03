"""
sheets_writer.py
----------------
AutoKnowMus — Google Sheets read/write client using a service account.

This module is the bridge between the Flask app and the AutoKnowMus Price
Data Google Sheet. It uses gspread + a service-account JSON credential
(stored in the GOOGLE_SERVICE_ACCOUNT_JSON env var on Render) to authenticate
with the Google Sheets API.

Architecture choice — why gspread:
  - gspread is a thin, well-maintained wrapper over the raw Google Sheets API.
  - It handles auth, retries, batching, range parsing, and error mapping for us.
  - Alternative: raw google-api-python-client. Same end result, ~3x more code.
  - Locked decision: use gspread.

Failure-mode design:
  - The service account credential is loaded once at module import time and
    cached. If the env var is missing or malformed, ALL functions in this
    module raise RuntimeError with a clear message. The caller (the test
    endpoint, the price tools admin pages) is responsible for handling.
  - The actual sheet open is also cached. If the sheet has been un-shared
    with the service account, the first call after that fails with a clear
    permission error.

Public API (used by app.py):
  - health_check()         → quick connection test (returns dict with details)
  - read_car_prices()      → returns the car_prices tab as a list of dicts
  - write_price_update(...) → updates one row's ex_showroom_price + notes
  - get_service_account_email() → for showing in admin UI / debug
  - get_sheet_metadata()   → sheet title + tab names
"""

import os
import json
import logging
import threading
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

# Lazy import — google libraries are heavy. We only import them inside
# the functions that need them. This means a pure import of this module
# (e.g. for ``from sheets_writer import health_check`` in app.py at boot time)
# does NOT crash if google libs aren't installed yet — the failure surfaces
# only when an actual call is made. That matches the rest of AutoKnowMus's
# fail-late philosophy (e.g. car_data.py's lazy fetch).

# ============================================================
# CONFIGURATION
# ============================================================

# Required scopes for read AND write access to a single sheet.
# - sheets/feeds = legacy v3 scope, kept for full compatibility with gspread.
# - drive.file  = lets us see only the sheets we're explicitly shared into,
#                 NOT every file in the service account's "drive". Safer than
#                 the full drive scope. Still allows reads + writes to shared
#                 files.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# The AutoKnowMus Price Data sheet. ID is hardcoded because there's exactly
# one of these per environment, and it never changes. If we ever need to
# point at a different sheet, change this constant + redeploy.
DEFAULT_SHEET_ID = "1ZGDlq-qWA17ChBf2KJy0pmpYMBaAPconwquVXqMh9Oc"
SHEET_ID = os.environ.get("AUTOKNOWMUS_SHEET_ID", DEFAULT_SHEET_ID)

# Tab names — must match exactly what's in the spreadsheet.
TAB_CAR_PRICES = "car_prices"
TAB_DEPRECIATION = "depreciation_curve"
TAB_MULTIPLIERS = "multipliers"
TAB_META = "meta"

# Expected column headers for car_prices tab. If the sheet structure changes,
# update here too. Order matters — index 0 = column A, index 1 = column B, etc.
CAR_PRICES_COLUMNS = [
    "make",                # A
    "model",               # B
    "variant",             # C
    "fuel",                # D
    "ex_showroom_price",   # E
    "active",              # F
    "notes",               # G
]


# ============================================================
# MODULE-LEVEL CACHES (thread-safe)
# ============================================================

_credentials = None
_gc_client = None
_spreadsheet = None
_lock = threading.Lock()


def _load_credentials():
    """
    Load service account credentials from the GOOGLE_SERVICE_ACCOUNT_JSON env var.
    Cached at module level after first successful load.

    Raises RuntimeError with a clear message on any failure — the caller
    catches and surfaces this to the admin UI.
    """
    global _credentials

    if _credentials is not None:
        return _credentials

    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is missing or empty. "
            "Set it in Render → Environment with the contents of the service "
            "account JSON key file."
        )

    try:
        info = json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {e}. "
            "Make sure you pasted the entire content of the .json file, "
            "from the opening { to the closing }."
        )

    # Sanity-check required fields. The full key file has many more, but
    # these are the four we ABSOLUTELY need to authenticate.
    required_keys = ["type", "client_email", "private_key", "project_id"]
    missing = [k for k in required_keys if k not in info]
    if missing:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON is missing required fields: {missing}. "
            "Make sure you pasted the COMPLETE service account JSON key, "
            "not a truncated version."
        )
    if info.get("type") != "service_account":
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON has type='{info.get('type')}', "
            f"expected 'service_account'. You may have pasted an OAuth client "
            "credential instead of a service account key."
        )

    # Lazy-import google.oauth2 only when we actually need it.
    try:
        from google.oauth2.service_account import Credentials
    except ImportError as e:
        raise RuntimeError(
            f"google-auth library not installed: {e}. "
            "Add `google-auth` to requirements.txt and redeploy."
        )

    try:
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    except Exception as e:
        raise RuntimeError(
            f"Could not build credentials from service account JSON: {e}. "
            "The JSON may be malformed, or the private_key field may be corrupted."
        )

    _credentials = creds
    return creds


def _get_client():
    """
    Build (and cache) a gspread client. Cached at module level — safe to
    call repeatedly. The underlying HTTP session inside gspread handles
    token refresh automatically.
    """
    global _gc_client

    with _lock:
        if _gc_client is not None:
            return _gc_client

        try:
            import gspread
        except ImportError as e:
            raise RuntimeError(
                f"gspread library not installed: {e}. "
                "Add `gspread` to requirements.txt and redeploy."
            )

        creds = _load_credentials()
        try:
            _gc_client = gspread.authorize(creds)
        except Exception as e:
            raise RuntimeError(f"Could not authorize gspread client: {e}")

        return _gc_client


def _get_spreadsheet():
    """
    Open (and cache) the AutoKnowMus Price Data spreadsheet. Cached at
    module level. If the sheet is later un-shared with the service account,
    the next API call (e.g. read_car_prices) will fail with a permission
    error — that's the right point to detect and report it.
    """
    global _spreadsheet

    with _lock:
        if _spreadsheet is not None:
            return _spreadsheet

        client = _get_client()
        try:
            _spreadsheet = client.open_by_key(SHEET_ID)
        except Exception as e:
            err_str = str(e).lower()
            # gspread/google API errors are messy strings. Catch the common
            # cases and rewrite them into something an admin can debug from.
            if "permission" in err_str or "denied" in err_str or "403" in err_str:
                raise RuntimeError(
                    f"Permission denied opening sheet {SHEET_ID}. "
                    f"The service account email may not be shared on the sheet "
                    f"as Editor. Open the sheet → Share → add "
                    f"{get_service_account_email()} with Editor permission. "
                    f"Original error: {e}"
                )
            if "not found" in err_str or "404" in err_str:
                raise RuntimeError(
                    f"Spreadsheet {SHEET_ID} not found. The sheet may have "
                    f"been deleted, or the SHEET_ID constant in sheets_writer.py "
                    f"may be wrong. Original error: {e}"
                )
            raise RuntimeError(f"Could not open spreadsheet {SHEET_ID}: {e}")

        return _spreadsheet


def _get_worksheet(tab_name: str):
    """Open a specific tab inside the spreadsheet. Not cached (tabs are cheap)."""
    spreadsheet = _get_spreadsheet()
    try:
        return spreadsheet.worksheet(tab_name)
    except Exception as e:
        err_str = str(e).lower()
        if "not found" in err_str or "no worksheet" in err_str:
            available = []
            try:
                available = [ws.title for ws in spreadsheet.worksheets()]
            except Exception:
                pass
            raise RuntimeError(
                f"Tab '{tab_name}' not found in sheet. Available tabs: {available}"
            )
        raise RuntimeError(f"Could not open tab '{tab_name}': {e}")


# ============================================================
# PUBLIC API
# ============================================================

def get_service_account_email() -> str:
    """
    Return the email address of the configured service account. Useful for
    the admin UI ("share the sheet with this email") and for debug output.

    Returns "<unknown>" if credentials haven't been loaded yet — does NOT
    raise, so it's safe to call from error-handling paths.
    """
    try:
        creds = _load_credentials()
        return getattr(creds, "service_account_email", "<unknown>")
    except Exception:
        return "<unknown>"


def get_sheet_metadata() -> Dict[str, Any]:
    """
    Return basic sheet info for diagnostics. Used by the test endpoint.

    Returns:
      {
        "sheet_id": str,
        "sheet_title": str,
        "tabs": [str, ...],       # in tab order
        "service_account_email": str,
      }

    Raises RuntimeError on any failure (caller catches).
    """
    spreadsheet = _get_spreadsheet()
    try:
        tabs = [ws.title for ws in spreadsheet.worksheets()]
        title = spreadsheet.title
    except Exception as e:
        raise RuntimeError(f"Could not read sheet metadata: {e}")

    return {
        "sheet_id": SHEET_ID,
        "sheet_title": title,
        "tabs": tabs,
        "service_account_email": get_service_account_email(),
    }


def health_check() -> Dict[str, Any]:
    """
    Quick end-to-end test: load credentials, open sheet, read one cell.
    Used by the /admin/test-sheets-connection endpoint to verify the entire
    Google Cloud setup works.

    Returns:
      {
        "ok": True,
        "sheet_id": str,
        "sheet_title": str,
        "tabs": [str, ...],
        "first_cell": str,         # contents of car_prices!A1, expected "make"
        "service_account_email": str,
      }

    Raises RuntimeError on any failure. The endpoint catches and converts
    to JSON: {"ok": false, "error": <message>}.
    """
    metadata = get_sheet_metadata()

    if TAB_CAR_PRICES not in metadata["tabs"]:
        raise RuntimeError(
            f"Tab '{TAB_CAR_PRICES}' not found in sheet. "
            f"Available tabs: {metadata['tabs']}. "
            f"This is unexpected — the sheet structure may have changed."
        )

    ws = _get_worksheet(TAB_CAR_PRICES)
    try:
        first_cell = ws.cell(1, 1).value
    except Exception as e:
        raise RuntimeError(f"Could not read cell A1 of '{TAB_CAR_PRICES}' tab: {e}")

    return {
        "ok": True,
        "sheet_id": metadata["sheet_id"],
        "sheet_title": metadata["sheet_title"],
        "tabs": metadata["tabs"],
        "first_cell": first_cell,
        "service_account_email": metadata["service_account_email"],
    }


def read_car_prices() -> List[Dict[str, Any]]:
    """
    Read the entire car_prices tab and return as a list of dicts. Each dict
    has keys matching CAR_PRICES_COLUMNS plus a `_row_number` key (1-indexed,
    matches the sheet's row number — useful for write_price_update).

    Header row (row 1) is excluded from the result.

    Returns:
      [
        {
          "make": "Maruti Suzuki",
          "model": "Swift",
          "variant": "VXi",
          "fuel": "Petrol",
          "ex_showroom_price": "700000",   # str — caller converts as needed
          "active": "TRUE",
          "notes": "5/1/2026 Claude",
          "_row_number": 2,                 # row 2 in the sheet
        },
        ...
      ]

    Raises RuntimeError on any failure.
    """
    ws = _get_worksheet(TAB_CAR_PRICES)
    try:
        all_values = ws.get_all_values()
    except Exception as e:
        raise RuntimeError(f"Could not read car_prices tab: {e}")

    if not all_values:
        return []

    # First row is the header; we ignore the header content but use its
    # length to know how many columns to expect. Subsequent rows are data.
    header = all_values[0]
    rows = []
    for idx, row in enumerate(all_values[1:], start=2):
        # Pad short rows with empty strings so column access never fails.
        padded = list(row) + [""] * (len(CAR_PRICES_COLUMNS) - len(row))
        rec = {col: padded[i] if i < len(padded) else "" for i, col in enumerate(CAR_PRICES_COLUMNS)}
        rec["_row_number"] = idx
        rows.append(rec)
    return rows


def find_row(make: str, model: str, variant: str, fuel: str) -> Optional[Dict[str, Any]]:
    """
    Find the row in car_prices that matches (make, model, variant, fuel)
    EXACTLY (case-sensitive). Returns the dict (with _row_number) or None.

    Why case-sensitive: the sheet IS the source of truth for spelling. If
    the caller passes "honda" but the sheet has "Honda", that's a caller
    bug, not a lookup bug. We surface it as None instead of silently fixing.
    """
    all_rows = read_car_prices()
    for r in all_rows:
        if (r["make"] == make
                and r["model"] == model
                and r["variant"] == variant
                and r["fuel"] == fuel):
            return r
    return None


def _format_notes(source: str, extra: Optional[str] = None) -> str:
    """
    Build the notes-cell value for an updated row.

    Format: "DD-MMM-YYYY {source}" matching the existing sheet pattern
    (per user's earlier convention seen in row 123: "5/1/2026 Claude").

    DD-MMM-YYYY is enforced per AutoKnowMus standing UI rule.
    """
    today_str = date.today().strftime("%d-%b-%Y")
    parts = [today_str, source]
    if extra:
        parts.append(extra)
    return " ".join(parts)


def write_price_update(make: str, model: str, variant: str, fuel: str,
                       new_price: int, source: str = "Manual",
                       extra_note: Optional[str] = None) -> Dict[str, Any]:
    """
    Update the ex_showroom_price (column E) and notes (column G) of a single
    row in car_prices. The (make, model, variant, fuel) tuple must already
    exist as a row — use `find_row()` first if you need to confirm.

    Args:
      make, model, variant, fuel: exact match keys (case-sensitive)
      new_price: int rupees (no commas, no decimal point)
      source: short tag for the notes column (e.g. "CarDekho", "CarWale", "Manual")
      extra_note: optional additional text appended to the notes cell

    Returns:
      {
        "ok": True,
        "row_number": int,        # which row was updated
        "old_price": int,         # what was there before
        "new_price": int,
        "old_notes": str,
        "new_notes": str,
      }

    Raises RuntimeError on any failure (row not found, write failed, etc.).
    """
    if not isinstance(new_price, int) or new_price < 0:
        raise RuntimeError(
            f"new_price must be a non-negative int, got {type(new_price).__name__}={new_price!r}"
        )

    target = find_row(make, model, variant, fuel)
    if target is None:
        raise RuntimeError(
            f"Row not found for ({make}, {model}, {variant}, {fuel}). "
            f"Cannot write update — row must exist already."
        )

    row_num = target["_row_number"]
    old_price_str = target.get("ex_showroom_price", "")
    try:
        old_price = int(old_price_str.replace(",", "")) if old_price_str else 0
    except (ValueError, TypeError):
        old_price = 0
    old_notes = target.get("notes", "")
    new_notes = _format_notes(source, extra_note)

    ws = _get_worksheet(TAB_CAR_PRICES)
    try:
        # Update column E (ex_showroom_price) and column G (notes) in a
        # single batch — saves one API call.
        ws.batch_update([
            {"range": f"E{row_num}", "values": [[str(new_price)]]},
            {"range": f"G{row_num}", "values": [[new_notes]]},
        ])
    except Exception as e:
        raise RuntimeError(
            f"Failed to write update for row {row_num} "
            f"({make} {model} {variant} {fuel}): {e}"
        )

    logger.info(
        "sheets_writer: updated row %d | %s %s %s %s | price %d -> %d | source=%s",
        row_num, make, model, variant, fuel, old_price, new_price, source,
    )

    return {
        "ok": True,
        "row_number": row_num,
        "old_price": old_price,
        "new_price": new_price,
        "old_notes": old_notes,
        "new_notes": new_notes,
    }


def reset_caches():
    """
    Force-clear module-level caches. The next call to any function will
    re-load credentials and re-open the sheet. Useful if credentials get
    rotated mid-runtime — though in practice the app would just be restarted.
    """
    global _credentials, _gc_client, _spreadsheet
    with _lock:
        _credentials = None
        _gc_client = None
        _spreadsheet = None
    logger.info("sheets_writer: caches reset; next call will re-authenticate")
