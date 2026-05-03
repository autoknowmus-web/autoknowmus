"""
sheets_writer.py
----------------
AutoKnowMus — Google Sheets read/write client using a service account.

This module is the bridge between the Flask app and the AutoKnowMus Price
Data Google Sheet. It uses gspread + a service-account JSON credential
(stored in the GOOGLE_SERVICE_ACCOUNT_JSON env var on Render) to authenticate
with the Google Sheets API.

----------------------------------------------------------------------
v3.6.3 — TIMEOUT IMPLEMENTATION REWRITE (the actual fix)
----------------------------------------------------------------------
Background: v3.6.1 + v3.6.2 tried to use `socket.setdefaulttimeout()`
to bound Google API calls. Neither worked:

  - v3.6.1: set socket timeout globally → broke Supabase httpx connections
            for all other routes.
  - v3.6.2: scoped the socket timeout to a context manager → didn't help
            because gspread + google-auth use `requests.Session` which
            bypasses `socket.setdefaulttimeout()` for its own HTTP calls.

The actual fix uses two layers of EXPLICIT timeouts:

  1. **OAuth handshake.** Build a `google.auth.transport.requests.Request`
     and monkey-patch its `session.request` method to inject a 10-second
     timeout for THIS Request instance only. Then call `creds.refresh(req)`
     ourselves. If the OAuth call hangs, we get a clean exception within
     10s — well before gunicorn's 30s worker timeout fires.

  2. **Subsequent sheet calls.** After `gspread.authorize(creds)` returns
     a client, call `client.set_timeout((10, 10))`. This wires the timeout
     into gspread's own `requests.Session` for every spreadsheet API call
     (open_by_key, get_all_values, batch_update, etc.) — no per-call
     wrapping needed.

Result: explicit timeouts where the actual network calls happen, with
zero impact on other parts of the app. Other Flask code paths keep their
normal blocking socket behavior because we never touch the global default.

----------------------------------------------------------------------
v3.6.2 — (superseded) scoped socket timeout via context manager
v3.6.1 — (superseded) global socket.setdefaulttimeout
----------------------------------------------------------------------
Previous version (v3.6.0) hung gunicorn workers for 30 seconds when the
GOOGLE_SERVICE_ACCOUNT_JSON env var contained a private_key with literal
"\\n" escape sequences instead of real newlines. This is the #1 known
issue with Google service account JSON in cloud env vars — Render, Heroku,
Railway, and others all mangle the embedded newlines differently depending
on how the value was pasted, edited, or copied.

Two fixes in this release:

1. **Auto-detect base64 OR raw JSON.** The env var can hold either:
     - Raw JSON (legacy, fragile)        → starts with `{`
     - Base64-encoded JSON (recommended) → ascii-only, no whitespace
   We try base64 first; if the decoded bytes don't parse as JSON, we fall
   back to treating the env var as raw JSON. Either format works.

2. **10-second socket timeout** on every Google API call. If something
   hangs (DNS, OAuth, sheets RPC), we raise a clean RuntimeError instead
   of letting gunicorn SIGKILL the worker after 30s. The user gets a
   clear "timed out" error in the test endpoint instead of a 502.

These changes are 100% backward-compatible. If your env var is already
working with raw JSON, this version still works. If you re-paste it as
base64 (recommended), you eliminate the whole class of newline-escaping
bugs forever.

----------------------------------------------------------------------
ARCHITECTURE NOTES (unchanged from v3.6.0)
----------------------------------------------------------------------
Why gspread:
  - Thin, well-maintained wrapper over the raw Google Sheets API
  - Handles auth, retries, batching, range parsing, error mapping
  - Alternative: raw google-api-python-client. Same end result, ~3x more
    code. Locked decision: gspread.

Failure-mode design:
  - The service account credential is loaded once at module import time
    and cached. If the env var is missing or malformed, ALL functions in
    this module raise RuntimeError with a clear message. The caller (the
    test endpoint, the price tools admin pages) is responsible for handling.
  - The actual sheet open is also cached. If the sheet has been un-shared
    with the service account, the first call after that fails with a clear
    permission error.
  - All HTTP calls have a 10-second timeout. Anything slower errors clean.

Public API (used by app.py):
  - health_check()         → quick connection test (returns dict with details)
  - read_car_prices()      → returns the car_prices tab as a list of dicts
  - write_price_update(...) → updates one row's ex_showroom_price + notes
  - get_service_account_email() → for showing in admin UI / debug
  - get_sheet_metadata()   → sheet title + tab names
"""

import os
import json
import socket
import logging
import threading
import base64
import binascii
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

# Lazy import — google libraries are heavy. We only import them inside
# the functions that need them. This means a pure import of this module
# (e.g. for ``from sheets_writer import health_check`` in app.py at boot
# time) does NOT crash if google libs aren't installed yet — the failure
# surfaces only when an actual call is made.

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

# v3.6.1: hard timeout on any sheet/auth network call. Gunicorn's default
# worker timeout is 30s; we cap our individual calls at 10s so we always
# error cleanly before gunicorn kills us. Set on the global socket layer
# because google-auth's HTTP client doesn't expose a per-request timeout
# we can pass through gspread.
_GOOGLE_API_TIMEOUT_SECONDS = 10


# ============================================================
# MODULE-LEVEL CACHES (thread-safe)
# ============================================================

_credentials = None
_gc_client = None
_spreadsheet = None
_lock = threading.Lock()


# ============================================================
# v3.6.2: SCOPED SOCKET TIMEOUT CONTEXT MANAGER
# ============================================================

import contextlib

@contextlib.contextmanager
def _with_sheets_timeout():
    """
    Context manager that temporarily sets the default socket timeout to
    _GOOGLE_API_TIMEOUT_SECONDS (10s) for the duration of the wrapped call,
    then ALWAYS restores the previous value — even if an exception is raised.

    Why scoped instead of global: setting socket.setdefaulttimeout() globally
    affects EVERY socket created afterward in the process, which breaks
    Supabase httpx connections and other long-running HTTP streams. With
    this context manager, only sockets created inside `with _with_sheets_timeout():`
    blocks inherit the 10s timeout. Everything else keeps its normal behavior.

    Caveat: if a network call STARTS inside the context but is still in
    progress when the context exits, the timeout is restored mid-stream and
    that specific socket keeps its 10s timeout (sockets snapshot the default
    at creation time). In practice this is fine for our usage — gspread
    completes its HTTP calls synchronously before returning.
    """
    previous = socket.getdefaulttimeout()
    socket.setdefaulttimeout(_GOOGLE_API_TIMEOUT_SECONDS)
    try:
        yield
    finally:
        socket.setdefaulttimeout(previous)


# ============================================================
# CREDENTIAL LOADING
# ============================================================

def _decode_env_payload(raw: str) -> Dict:
    """
    Accept the GOOGLE_SERVICE_ACCOUNT_JSON env var in either form:

      1. Base64-encoded JSON (recommended, robust to paste mangling).
         Example: 'ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIs...'
         Detected by trying base64 decode first.
      2. Raw JSON (legacy, fragile — multi-line newlines in private_key
         get mangled by some env-var UIs). Starts with `{`.

    Returns the parsed dict on success.
    Raises RuntimeError with a clear message on any decoding/parsing failure.
    """
    raw = raw.strip()
    if not raw:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is missing or empty. "
            "Set it in Render → Environment with the service account JSON, "
            "ideally base64-encoded for robustness."
        )

    # Heuristic: if the first non-whitespace character is `{`, it's raw JSON.
    # Otherwise, try base64 first (it's the recommended format).
    looks_like_json = raw[0] == '{'

    # ---- Try base64 first if it doesn't look like JSON ----
    if not looks_like_json:
        try:
            # Strip any whitespace/newlines that might have crept in during paste.
            cleaned = ''.join(raw.split())
            decoded_bytes = base64.b64decode(cleaned, validate=True)
            decoded_str = decoded_bytes.decode('utf-8')
            return json.loads(decoded_str)
        except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as e:
            # Base64 failed — fall through to raw-JSON attempt below
            logger.info(
                f"sheets_writer: base64 decode of env var failed ({e}); "
                f"trying raw JSON fallback"
            )

    # ---- Try raw JSON ----
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed as either "
            f"base64-encoded JSON or raw JSON. Last error: {e}. "
            f"RECOMMENDED FIX: re-encode your service account JSON file "
            f"as base64 and paste the result. On Mac/Linux: "
            f"`base64 -i autoknowmus-prod-XXXXXX.json | tr -d '\\n'`. "
            f"On Windows PowerShell: "
            f"`[Convert]::ToBase64String([IO.File]::ReadAllBytes('path/to/key.json'))`."
        )


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

    # Decode (handles both base64 and raw JSON formats).
    info = _decode_env_payload(raw)

    # Sanity-check required fields. The full key file has many more, but
    # these are the four we ABSOLUTELY need to authenticate.
    required_keys = ["type", "client_email", "private_key", "project_id"]
    missing = [k for k in required_keys if k not in info]
    if missing:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON is missing required fields: {missing}. "
            "Make sure you encoded the COMPLETE service account JSON key, "
            "not a truncated version."
        )
    if info.get("type") != "service_account":
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON has type='{info.get('type')}', "
            f"expected 'service_account'. You may have used an OAuth client "
            "credential instead of a service account key."
        )

    # v3.6.1 SAFETY CHECK: detect mangled private_key (literal "\n" instead
    # of real newlines). A real RSA key starts with "-----BEGIN PRIVATE KEY-----\n"
    # where that \n is a real newline character (ASCII 10). If the env var was
    # pasted as raw JSON and Render escaped the newlines, we end up with the
    # literal two-character string "\\n" inside private_key, which makes
    # google-auth hang silently during the OAuth handshake.
    pk = info.get("private_key", "")
    if "\\n" in pk and "\n" not in pk:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON private_key contains literal '\\n' "
            "escape sequences instead of real newlines. This is the #1 known "
            "Render env var bug — the OAuth library will hang forever trying "
            "to parse this. FIX: encode the JSON file as base64 and paste "
            "that instead. On Mac/Linux: "
            "`base64 -i autoknowmus-prod-XXXXXX.json | tr -d '\\n'`. "
            "On Windows PowerShell: "
            "`[Convert]::ToBase64String([IO.File]::ReadAllBytes('path/to/key.json'))`. "
            "Then update the Render env var with the base64 string."
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

    v3.6.3: socket-level timeout (v3.6.1/v3.6.2 attempts) didn't work
    because google-auth's HTTP client and gspread's `requests.Session`
    bypass `socket.setdefaulttimeout()` for their actual API calls. The
    OAuth handshake was hanging for the full gunicorn 30s window even
    inside a `with _with_sheets_timeout()` block.

    The right fix uses two layers of EXPLICIT timeouts:

      1. Pre-flight `credentials.refresh()` with an explicit 10s timeout
         on the underlying `Request`. This is the OAuth handshake — if it
         hangs (e.g. malformed private_key), we get a clean exception
         within 10 seconds instead of waiting for gunicorn to SIGKILL us.

      2. After credentials are valid, build the gspread client and call
         `client.set_timeout((connect_s, read_s))` so every subsequent
         spreadsheet API call (open_by_key, get_all_values, etc.) inherits
         an explicit per-request timeout via gspread's own HTTP layer.

    No `socket.setdefaulttimeout()` games. Other Flask code paths keep
    their normal socket behavior because we never touch the global default.
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

        try:
            from google.auth.transport.requests import Request as GoogleAuthRequest
        except ImportError as e:
            raise RuntimeError(
                f"google.auth.transport.requests not available: {e}. "
                "google-auth is missing a required submodule — check requirements.txt."
            )

        creds = _load_credentials()

        # ─── Step 1: Pre-flight OAuth refresh with explicit timeout ─────
        # If the private_key is malformed or Google's OAuth endpoint is
        # unreachable, this raises within ~10s instead of hanging forever.
        #
        # CRITICAL DETAIL: google.auth.transport.requests.Request.__call__
        # ALWAYS passes `timeout=_DEFAULT_TIMEOUT` (120s) to session.request,
        # regardless of whether the caller specified one. Earlier attempts
        # to set a timeout on socket-level or check `if 'timeout' not in kwargs`
        # didn't work because the kwarg is ALWAYS present. We have to
        # FORCE-override it on every call by replacing whatever value is
        # there with our 10s value.
        try:
            auth_req = GoogleAuthRequest()
            original_request = auth_req.session.request

            def _request_with_timeout(method, url, **kwargs):
                # Force-override regardless of whether timeout is already in
                # kwargs. google-auth's Request.__call__ always passes
                # timeout=120 explicitly, so we must always replace it.
                kwargs['timeout'] = (
                    _GOOGLE_API_TIMEOUT_SECONDS,  # connect timeout
                    _GOOGLE_API_TIMEOUT_SECONDS,  # read timeout
                )
                return original_request(method, url, **kwargs)

            auth_req.session.request = _request_with_timeout

            creds.refresh(auth_req)
            logger.info(
                "sheets_writer: OAuth refresh OK for %s",
                getattr(creds, "service_account_email", "<unknown>"),
            )
        except Exception as e:
            err_str = str(e).lower()
            if "timed out" in err_str or "timeout" in err_str or "read timed out" in err_str:
                raise RuntimeError(
                    f"OAuth handshake with Google timed out after "
                    f"{_GOOGLE_API_TIMEOUT_SECONDS}s. Most common causes: "
                    "(a) malformed private_key in GOOGLE_SERVICE_ACCOUNT_JSON "
                    "(re-encode as base64 — see _load_credentials() docs); "
                    "(b) Render's egress is blocked from oauth2.googleapis.com "
                    "(very rare); (c) Google Auth API is degraded. "
                    f"Original error: {e}"
                )
            if "invalid_grant" in err_str or "invalid grant" in err_str:
                raise RuntimeError(
                    f"Google rejected the service-account credentials "
                    f"(invalid_grant). The service-account JSON in the env var "
                    f"may be revoked, expired, or corrupted. Generate a fresh "
                    f"key in Google Cloud Console and update the env var. "
                    f"Original error: {e}"
                )
            if "could not deserialize key data" in err_str or "key data" in err_str:
                raise RuntimeError(
                    f"private_key field could not be parsed as a valid PEM key. "
                    f"This is the classic '\\n' escape-mangling bug. FIX: re-encode "
                    f"the JSON as base64 before pasting into the env var. "
                    f"Original error: {e}"
                )
            raise RuntimeError(f"OAuth credentials.refresh() failed: {e}")

        # ─── Step 2: Build gspread client with refreshed credentials ────
        try:
            _gc_client = gspread.authorize(creds)
            # Apply explicit timeout to every subsequent spreadsheet API
            # call. Tuple form = (connect_timeout, read_timeout). 10s each
            # is plenty for normal sheet operations on a healthy network.
            _gc_client.set_timeout(
                (_GOOGLE_API_TIMEOUT_SECONDS, _GOOGLE_API_TIMEOUT_SECONDS)
            )
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
