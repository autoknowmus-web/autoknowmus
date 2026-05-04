"""
sheets_writer.py
----------------
AutoKnowMus — Google Sheets read/write client using a service account.

Direct Google Sheets REST API calls via the `requests` library. No gspread,
no google-auth. Just JWT-grant OAuth (proven working in 238ms) + plain
HTTPS calls (proven working in 200ms via /admin/diag-egress).

----------------------------------------------------------------------
v3.7.0 — Phase 3 admin price tools support
----------------------------------------------------------------------
New in this version (additive — all v3.6.8 code unchanged):

  - CAR_PRICES_COLUMNS extended with two new headers:
      H: status                  (empty | "discontinued")
      I: last_known_price_date   (DD-MMM-YYYY)
  - write_price_update_v2() — like write_price_update() but ALSO sets
    column I to today's date. Used for "price_update" review approvals.
  - write_discontinued_flag() — sets H="discontinued" + I=today. Leaves
    price (col E) and notes (col G) unchanged. Used for "discontinued"
    review approvals to preserve the last known price for valuations.
  - write_new_variant() — appends a brand new row at the bottom of
    car_prices. Used for "new_variant" review approvals.

Used by:
  - app.py admin price tools routes (Phase 3)
  - The same module also continues serving Phase 2 features:
    health_check, find_row, write_price_update, read_model_slugs,
    write_model_slug, etc.

----------------------------------------------------------------------
v3.6.8 — model_slugs tab support (additive, no breaking changes)
v3.6.7 — Direct REST API (gspread/google-auth fully bypassed)
v3.6.5 — Manual JWT-grant OAuth
(History trimmed; see git log for full evolution.)
"""

import os
import json
import time
import logging
import threading
import base64
import binascii
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
SHEETS_API_BASE = "https://sheets.googleapis.com/v4"

JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"

TOKEN_REFRESH_SAFETY_MARGIN_SECONDS = 60

DEFAULT_SHEET_ID = "1ZGDlq-qWA17ChBf2KJy0pmpYMBaAPconwquVXqMh9Oc"
SHEET_ID = os.environ.get("AUTOKNOWMUS_SHEET_ID", DEFAULT_SHEET_ID)

# Tab names — must match exactly what's in the spreadsheet.
TAB_CAR_PRICES = "car_prices"
TAB_DEPRECIATION = "depreciation_curve"
TAB_MULTIPLIERS = "multipliers"
TAB_META = "meta"
TAB_MODEL_SLUGS = "model_slugs"

# Expected column headers for car_prices tab.
# v3.7.0: added H (status) and I (last_known_price_date).
CAR_PRICES_COLUMNS = [
    "make",                      # A
    "model",                     # B
    "variant",                   # C
    "fuel",                      # D
    "ex_showroom_price",         # E
    "active",                    # F
    "notes",                     # G
    "status",                    # H  (v3.7.0: empty or "discontinued")
    "last_known_price_date",     # I  (v3.7.0: DD-MMM-YYYY)
]

# Expected column headers for model_slugs tab (v3.6.8).
MODEL_SLUGS_COLUMNS = [
    "make",
    "model",
    "carwale_slug",
    "notes",
]

# Hard timeouts — bounded well below gunicorn's 30s worker timeout.
_API_CONNECT_TIMEOUT = 5
_API_READ_TIMEOUT = 10
_API_TIMEOUT = (_API_CONNECT_TIMEOUT, _API_READ_TIMEOUT)
_TOKEN_MINT_TIMEOUT = (5, 5)


# ============================================================
# MODULE-LEVEL CACHES (thread-safe)
# ============================================================

_sa_info: Optional[Dict[str, Any]] = None
_access_token: Optional[str] = None
_access_token_expires_at: Optional[float] = None
_session = None  # requests.Session for connection reuse

_sheet_metadata_cache: Optional[Dict[str, Any]] = None

_lock = threading.Lock()


# ============================================================
# CREDENTIAL LOADING
# ============================================================

def _decode_env_payload(raw: str) -> Dict:
    """Accept GOOGLE_SERVICE_ACCOUNT_JSON env var as base64 or raw JSON."""
    raw = raw.strip()
    if not raw:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON env var is missing or empty. "
            "Set it in Render -> Environment with the service account JSON, "
            "ideally base64-encoded for robustness."
        )

    looks_like_json = raw[0] == '{'

    if not looks_like_json:
        try:
            cleaned = ''.join(raw.split())
            decoded_bytes = base64.b64decode(cleaned, validate=True)
            decoded_str = decoded_bytes.decode('utf-8')
            return json.loads(decoded_str)
        except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as e:
            logger.info(
                f"sheets_writer: base64 decode of env var failed ({e}); "
                f"trying raw JSON fallback"
            )

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON could not be parsed as either "
            f"base64-encoded JSON or raw JSON. Last error: {e}. "
            f"RECOMMENDED FIX: re-encode your service account JSON file "
            f"as base64 and paste the result."
        )


def _load_sa_info() -> Dict[str, Any]:
    """Load + validate the service-account JSON dict. Cached."""
    global _sa_info

    if _sa_info is not None:
        return _sa_info

    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    info = _decode_env_payload(raw)

    required_keys = ["type", "client_email", "private_key", "project_id"]
    missing = [k for k in required_keys if k not in info]
    if missing:
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON is missing required fields: {missing}."
        )
    if info.get("type") != "service_account":
        raise RuntimeError(
            f"GOOGLE_SERVICE_ACCOUNT_JSON has type='{info.get('type')}', "
            f"expected 'service_account'."
        )

    pk = info.get("private_key", "")
    if "\\n" in pk and "\n" not in pk:
        raise RuntimeError(
            "GOOGLE_SERVICE_ACCOUNT_JSON private_key contains literal '\\n' "
            "escape sequences instead of real newlines. FIX: encode the JSON "
            "file as base64."
        )

    _sa_info = info
    return info


# ============================================================
# JWT-GRANT OAUTH (unchanged from v3.6.5 — proven working in 238ms)
# ============================================================

def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _build_jwt_assertion(sa_info: Dict[str, Any]) -> str:
    """RFC 7523 signed JWT using RS256."""
    try:
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import padding
        from cryptography.hazmat.backends import default_backend
    except ImportError as e:
        raise RuntimeError(
            f"cryptography library not available: {e}. "
            "Add `cryptography` to requirements.txt."
        )

    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iss": sa_info["client_email"],
        "scope": " ".join(SCOPES),
        "aud": GOOGLE_TOKEN_URL,
        "iat": now,
        "exp": now + 3600,
    }

    header_b64 = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    payload_b64 = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{header_b64}.{payload_b64}".encode("ascii")

    pem_bytes = sa_info["private_key"].encode("utf-8")
    try:
        private_key = serialization.load_pem_private_key(
            pem_bytes, password=None, backend=default_backend()
        )
    except Exception as e:
        raise RuntimeError(
            f"Could not load private_key as PEM: {e}. "
            "Re-encode the service account JSON as base64."
        )

    try:
        signature = private_key.sign(
            signing_input,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
    except Exception as e:
        raise RuntimeError(f"JWT signing failed: {e}")

    signature_b64 = _b64url_encode(signature)
    return f"{header_b64}.{payload_b64}.{signature_b64}"


def _mint_access_token_manually() -> Tuple[str, float]:
    try:
        import requests
    except ImportError as e:
        raise RuntimeError(f"requests library not available: {e}.")

    sa_info = _load_sa_info()
    sa_email = sa_info.get("client_email", "<unknown>")

    logger.info("sheets_writer: minting access token for %s (manual JWT-grant)", sa_email)

    assertion = _build_jwt_assertion(sa_info)

    body = {
        "grant_type": JWT_GRANT_TYPE,
        "assertion": assertion,
    }

    start = time.time()
    try:
        resp = requests.post(
            GOOGLE_TOKEN_URL,
            data=body,
            timeout=_TOKEN_MINT_TIMEOUT,
        )
    except requests.exceptions.Timeout as e:
        raise RuntimeError(
            f"Token mint timed out. Run /admin/diag-egress to diagnose. "
            f"Original error: {e}"
        )
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Token mint network error: {e}.")

    elapsed_ms = int((time.time() - start) * 1000)
    logger.info(
        "sheets_writer: token mint completed in %dms (status %d)",
        elapsed_ms, resp.status_code
    )

    if resp.status_code != 200:
        body_text = (resp.text or "")[:500]
        try:
            err_json = resp.json()
            err_code = err_json.get("error", "")
            err_desc = err_json.get("error_description", "")
        except Exception:
            err_code, err_desc = "", ""

        if err_code == "invalid_grant":
            raise RuntimeError(
                f"Google rejected the JWT assertion (invalid_grant). "
                f"Common causes: revoked SA, clock skew, wrong scopes. "
                f"Description: {err_desc}. Full: {body_text}"
            )
        if err_code == "invalid_client":
            raise RuntimeError(
                f"Google did not recognize the service account ({sa_email}). "
                f"Full response: {body_text}"
            )
        if err_code == "unauthorized_client":
            raise RuntimeError(
                f"Service account ({sa_email}) is not authorized for the "
                f"requested scopes. Full response: {body_text}"
            )
        raise RuntimeError(
            f"Token mint failed with HTTP {resp.status_code}. "
            f"Error: '{err_code}'. Desc: '{err_desc}'. Full: {body_text}"
        )

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(
            f"Token mint returned non-JSON body: {e}. "
            f"First 200 chars: {(resp.text or '')[:200]}"
        )

    access_token = data.get("access_token")
    expires_in = data.get("expires_in", 3600)

    if not access_token:
        raise RuntimeError(
            f"Token mint response had no access_token field. "
            f"Full response: {json.dumps(data)[:300]}"
        )

    expires_at = time.time() + int(expires_in)
    logger.info(
        "sheets_writer: minted access token (expires in %ds)",
        expires_in,
    )
    return access_token, expires_at


def _get_access_token() -> str:
    """Return a valid access token, minting a new one if needed. Thread-safe."""
    global _access_token, _access_token_expires_at

    with _lock:
        now = time.time()
        if (_access_token is not None
                and _access_token_expires_at is not None
                and now < (_access_token_expires_at - TOKEN_REFRESH_SAFETY_MARGIN_SECONDS)):
            return _access_token

    token, expires_at = _mint_access_token_manually()
    with _lock:
        _access_token = token
        _access_token_expires_at = expires_at
        return token


# ============================================================
# END OF PART 1/2 — sheets_writer.py v3.7.0
# Continue pasting Part 2/2 after this line.
# ============================================================
# ============================================================
# CONTINUATION OF sheets_writer.py v3.7.0 — PART 2/2
# Paste this AFTER Part 1/2 in the same file.
# ============================================================

# ============================================================
# DIRECT GOOGLE SHEETS REST API CLIENT (unchanged from v3.6.7)
# ============================================================

def _get_session():
    global _session

    with _lock:
        if _session is not None:
            return _session

        try:
            import requests
        except ImportError as e:
            raise RuntimeError(f"requests library not available: {e}.")

        _session = requests.Session()
        logger.info("sheets_writer: built requests.Session for REST API calls")
        return _session


def _sheets_request(method: str, path: str,
                    params: Optional[Dict[str, Any]] = None,
                    json_body: Optional[Dict[str, Any]] = None,
                    op_label: str = "request") -> Any:
    """Make an authenticated HTTP request to the Google Sheets REST API."""
    try:
        import requests
    except ImportError as e:
        raise RuntimeError(f"requests library not available: {e}.")

    session = _get_session()
    token = _get_access_token()

    url = SHEETS_API_BASE + path
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if json_body is not None:
        headers["Content-Type"] = "application/json"

    logger.info("sheets_writer: %s %s -> %s", op_label, method, path)

    start = time.time()
    try:
        if method.upper() == "GET":
            resp = session.get(
                url, headers=headers, params=params, timeout=_API_TIMEOUT,
            )
        elif method.upper() == "POST":
            resp = session.post(
                url, headers=headers, params=params, json=json_body, timeout=_API_TIMEOUT,
            )
        else:
            raise RuntimeError(f"Unsupported HTTP method: {method}")
    except requests.exceptions.Timeout as e:
        elapsed_ms = int((time.time() - start) * 1000)
        raise RuntimeError(
            f"Sheets API {op_label} timed out after {elapsed_ms}ms "
            f"(timeout={_API_TIMEOUT}). Run /admin/diag-egress to diagnose "
            f"network. Original error: {e}"
        )
    except requests.exceptions.RequestException as e:
        elapsed_ms = int((time.time() - start) * 1000)
        raise RuntimeError(
            f"Sheets API {op_label} network error after {elapsed_ms}ms: {e}"
        )

    elapsed_ms = int((time.time() - start) * 1000)
    logger.info(
        "sheets_writer: %s completed in %dms (status %d)",
        op_label, elapsed_ms, resp.status_code,
    )

    if resp.status_code == 401:
        logger.warning("sheets_writer: %s got 401, retrying with fresh token", op_label)
        global _access_token, _access_token_expires_at
        with _lock:
            _access_token = None
            _access_token_expires_at = None
        token = _get_access_token()
        headers["Authorization"] = f"Bearer {token}"

        start = time.time()
        try:
            if method.upper() == "GET":
                resp = session.get(
                    url, headers=headers, params=params, timeout=_API_TIMEOUT,
                )
            else:
                resp = session.post(
                    url, headers=headers, params=params, json=json_body, timeout=_API_TIMEOUT,
                )
        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Sheets API {op_label} retry network error: {e}"
            )
        elapsed_ms = int((time.time() - start) * 1000)
        logger.info(
            "sheets_writer: %s retry completed in %dms (status %d)",
            op_label, elapsed_ms, resp.status_code,
        )

    if not (200 <= resp.status_code < 300):
        body_text = (resp.text or "")[:500]
        try:
            err_json = resp.json()
            err_msg = err_json.get("error", {}).get("message", "")
            err_status = err_json.get("error", {}).get("status", "")
        except Exception:
            err_msg, err_status = "", ""

        if resp.status_code == 403:
            raise RuntimeError(
                f"Permission denied (403) on Sheets API {op_label}. "
                f"The service account ({get_service_account_email()}) may "
                f"not be shared as Editor on the sheet. Open the sheet -> "
                f"Share -> add the SA email with Editor permission. "
                f"API msg: {err_msg}. Full: {body_text}"
            )
        if resp.status_code == 404:
            raise RuntimeError(
                f"Sheet or range not found (404) on {op_label}. The "
                f"spreadsheet ID might be wrong, or the tab/range doesn't "
                f"exist. API msg: {err_msg}. Full: {body_text}"
            )
        if resp.status_code == 429:
            raise RuntimeError(
                f"Sheets API rate-limited (429) on {op_label}. Try again "
                f"in a moment. API msg: {err_msg}."
            )
        raise RuntimeError(
            f"Sheets API {op_label} returned HTTP {resp.status_code} "
            f"({err_status}). Message: {err_msg}. Full: {body_text}"
        )

    try:
        return resp.json()
    except Exception as e:
        raise RuntimeError(
            f"Sheets API {op_label} returned non-JSON body: {e}. "
            f"First 200 chars: {(resp.text or '')[:200]}"
        )


def _fetch_sheet_metadata_raw() -> Dict[str, Any]:
    global _sheet_metadata_cache

    with _lock:
        if _sheet_metadata_cache is not None:
            return _sheet_metadata_cache

    path = f"/spreadsheets/{SHEET_ID}"
    params = {"fields": "properties.title,sheets.properties.title"}
    data = _sheets_request("GET", path, params=params, op_label="fetch_metadata")

    title = (data.get("properties") or {}).get("title", "<unknown>")
    sheets_list = data.get("sheets") or []
    tabs = []
    for s in sheets_list:
        props = s.get("properties") or {}
        tab_name = props.get("title")
        if tab_name:
            tabs.append(tab_name)

    metadata = {
        "sheet_id": SHEET_ID,
        "sheet_title": title,
        "tabs": tabs,
    }

    with _lock:
        _sheet_metadata_cache = metadata

    logger.info(
        "sheets_writer: cached metadata (title='%s', %d tabs)",
        title, len(tabs),
    )
    return metadata


def _fetch_tab_values(tab_name: str) -> List[List[str]]:
    """Fetch all values from a tab as a 2D array of strings."""
    path = f"/spreadsheets/{SHEET_ID}/values/{tab_name}"
    params = {"majorDimension": "ROWS"}
    data = _sheets_request("GET", path, params=params, op_label=f"read_tab[{tab_name}]")
    return data.get("values") or []


def _write_cells_batch(updates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Write multiple cell ranges in a single batched API call."""
    path = f"/spreadsheets/{SHEET_ID}/values:batchUpdate"
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": updates,
    }
    return _sheets_request("POST", path, json_body=body, op_label="batch_update")


# ============================================================
# PUBLIC API — CORE
# ============================================================

def get_service_account_email() -> str:
    """Return the SA email. Returns "<unknown>" on failure — never raises."""
    try:
        info = _load_sa_info()
        return info.get("client_email", "<unknown>")
    except Exception:
        return "<unknown>"


def get_sheet_metadata() -> Dict[str, Any]:
    metadata = _fetch_sheet_metadata_raw()
    return {
        "sheet_id": metadata["sheet_id"],
        "sheet_title": metadata["sheet_title"],
        "tabs": metadata["tabs"],
        "service_account_email": get_service_account_email(),
    }


def health_check() -> Dict[str, Any]:
    metadata = get_sheet_metadata()

    if TAB_CAR_PRICES not in metadata["tabs"]:
        raise RuntimeError(
            f"Tab '{TAB_CAR_PRICES}' not found. Available: {metadata['tabs']}."
        )

    path = f"/spreadsheets/{SHEET_ID}/values/{TAB_CAR_PRICES}!A1"
    data = _sheets_request("GET", path, op_label="health_check_a1")
    values = data.get("values") or []
    first_cell = values[0][0] if (values and values[0]) else None

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
    Read the entire car_prices tab and return as list of dicts.
    Each dict has CAR_PRICES_COLUMNS keys plus _row_number (1-indexed).
    Header row excluded.

    v3.7.0: Each dict now includes 'status' and 'last_known_price_date'
    in addition to the original 7 keys. Old callers that don't read
    these keys are unaffected (they just get extra keys they ignore).
    """
    all_values = _fetch_tab_values(TAB_CAR_PRICES)
    if not all_values:
        return []

    rows = []
    for idx, row in enumerate(all_values[1:], start=2):
        padded = list(row) + [""] * (len(CAR_PRICES_COLUMNS) - len(row))
        rec = {col: padded[i] if i < len(padded) else "" for i, col in enumerate(CAR_PRICES_COLUMNS)}
        rec["_row_number"] = idx
        rows.append(rec)
    return rows


def find_row(make: str, model: str, variant: str, fuel: str) -> Optional[Dict[str, Any]]:
    """Find row matching (make, model, variant, fuel) EXACTLY (case-sensitive)."""
    all_rows = read_car_prices()
    for r in all_rows:
        if (r["make"] == make
                and r["model"] == model
                and r["variant"] == variant
                and r["fuel"] == fuel):
            return r
    return None


def _format_notes(source: str, extra: Optional[str] = None) -> str:
    today_str = date.today().strftime("%d-%b-%Y")
    parts = [today_str, source]
    if extra:
        parts.append(extra)
    return " ".join(parts)


def _today_ddmmmyyyy() -> str:
    """Returns today's date as DD-MMM-YYYY (e.g. '04-May-2026')."""
    return date.today().strftime("%d-%b-%Y")


def write_price_update(make: str, model: str, variant: str, fuel: str,
                       new_price: int, source: str = "Manual",
                       extra_note: Optional[str] = None) -> Dict[str, Any]:
    """
    LEGACY: Update the ex_showroom_price (col E) and notes (col G) of a single row.
    Does NOT touch status (col H) or last_known_price_date (col I).

    Kept for backward compatibility with any pre-v3.7.0 callers. New code
    in app.py admin price tools should use write_price_update_v2() instead,
    which also stamps the last_known_price_date column.
    """
    if not isinstance(new_price, int) or new_price < 0:
        raise RuntimeError(
            f"new_price must be a non-negative int, got "
            f"{type(new_price).__name__}={new_price!r}"
        )

    target = find_row(make, model, variant, fuel)
    if target is None:
        raise RuntimeError(
            f"Row not found for ({make}, {model}, {variant}, {fuel}). "
            f"Cannot write update."
        )

    row_num = target["_row_number"]
    old_price_str = target.get("ex_showroom_price", "")
    try:
        old_price = int(old_price_str.replace(",", "")) if old_price_str else 0
    except (ValueError, TypeError):
        old_price = 0
    old_notes = target.get("notes", "")
    new_notes = _format_notes(source, extra_note)

    updates = [
        {
            "range": f"{TAB_CAR_PRICES}!E{row_num}",
            "values": [[str(new_price)]],
        },
        {
            "range": f"{TAB_CAR_PRICES}!G{row_num}",
            "values": [[new_notes]],
        },
    ]

    try:
        _write_cells_batch(updates)
    except RuntimeError as e:
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


# ============================================================
# v3.7.0: Phase 3 admin price tools — write functions
# ============================================================
#
# Three new functions, one per review type:
#   write_price_update_v2 — for "price_update" reviews (existing variant, new price)
#   write_discontinued_flag — for "discontinued" reviews (existing variant, mark stale)
#   write_new_variant — for "new_variant" reviews (insert new row)
#
# All three write to the SAME car_prices sheet but in different ways:
#   price_update_v2 → updates cols E (price), G (notes), I (date)
#   discontinued    → updates cols H (status), I (date) only — leaves price/notes
#   new_variant     → appends a fresh row at the bottom with all 9 cols filled
# ============================================================

def write_price_update_v2(make: str, model: str, variant: str, fuel: str,
                          new_price: int, source: str = "CarWale",
                          extra_note: Optional[str] = None) -> Dict[str, Any]:
    """
    Phase 3: Update ex_showroom_price + notes + last_known_price_date of an
    existing row. Used when admin approves a "price_update" review.

    Updates 3 cells:
      col E (ex_showroom_price) -> new_price
      col G (notes)             -> "DD-MMM-YYYY {source} {extra_note}"
      col I (last_known_price_date) -> "DD-MMM-YYYY"

    Does NOT touch col H (status) — preserves any existing "discontinued" flag
    in the unlikely case that one was set then later overridden by a new price.

    Args:
      make, model, variant, fuel: must match an existing row exactly
      new_price: positive int (rupees, e.g. 658900 for 6.58 Lakh)
      source: tag for the notes column (default "CarWale" for scraper-sourced)
      extra_note: optional extra string appended to notes (e.g. "auto-approved")

    Raises RuntimeError if the row doesn't exist or the API call fails.
    """
    if not isinstance(new_price, int) or new_price < 0:
        raise RuntimeError(
            f"new_price must be a non-negative int, got "
            f"{type(new_price).__name__}={new_price!r}"
        )

    target = find_row(make, model, variant, fuel)
    if target is None:
        raise RuntimeError(
            f"Row not found for ({make}, {model}, {variant}, {fuel}). "
            f"Cannot write price update. Use write_new_variant() to insert."
        )

    row_num = target["_row_number"]
    old_price_str = target.get("ex_showroom_price", "")
    try:
        old_price = int(old_price_str.replace(",", "")) if old_price_str else 0
    except (ValueError, TypeError):
        old_price = 0
    old_notes = target.get("notes", "")
    today_str = _today_ddmmmyyyy()
    new_notes = _format_notes(source, extra_note)

    updates = [
        {
            "range": f"{TAB_CAR_PRICES}!E{row_num}",
            "values": [[str(new_price)]],
        },
        {
            "range": f"{TAB_CAR_PRICES}!G{row_num}",
            "values": [[new_notes]],
        },
        {
            "range": f"{TAB_CAR_PRICES}!I{row_num}",
            "values": [[today_str]],
        },
    ]

    try:
        _write_cells_batch(updates)
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to write price update for row {row_num} "
            f"({make} {model} {variant} {fuel}): {e}"
        )

    logger.info(
        "sheets_writer v3.7.0: write_price_update_v2 row %d | %s %s %s %s | "
        "price %d -> %d | date %s",
        row_num, make, model, variant, fuel, old_price, new_price, today_str,
    )

    return {
        "ok": True,
        "action": "price_update",
        "row_number": row_num,
        "old_price": old_price,
        "new_price": new_price,
        "old_notes": old_notes,
        "new_notes": new_notes,
        "last_known_price_date": today_str,
    }


def write_discontinued_flag(make: str, model: str, variant: str, fuel: str,
                            note: Optional[str] = None) -> Dict[str, Any]:
    """
    Phase 3: Mark an existing row as discontinued by setting:
      col H (status) -> "discontinued"
      col I (last_known_price_date) -> today's date

    Critical: does NOT touch col E (price) or col G (notes). The price stays
    available so existing owners of this car can still get valuations using
    the last known number. The dashboard shows a "Discontinued · last
    verified DATE" badge based on the col H + col I values.

    Args:
      make, model, variant, fuel: must match an existing row exactly
      note: optional admin note appended to notes column (col G).
            If provided, col G is also updated. If None, col G is untouched.

    Raises RuntimeError if the row doesn't exist or the API call fails.
    """
    target = find_row(make, model, variant, fuel)
    if target is None:
        raise RuntimeError(
            f"Row not found for ({make}, {model}, {variant}, {fuel}). "
            f"Cannot mark discontinued."
        )

    row_num = target["_row_number"]
    today_str = _today_ddmmmyyyy()

    updates = [
        {
            "range": f"{TAB_CAR_PRICES}!H{row_num}",
            "values": [["discontinued"]],
        },
        {
            "range": f"{TAB_CAR_PRICES}!I{row_num}",
            "values": [[today_str]],
        },
    ]

    # Optional: append a note to col G if admin provided one
    if note:
        existing_notes = target.get("notes", "")
        merged_notes = f"{existing_notes} | Discontinued {today_str}: {note}".strip(" |")
        updates.append({
            "range": f"{TAB_CAR_PRICES}!G{row_num}",
            "values": [[merged_notes]],
        })

    try:
        _write_cells_batch(updates)
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to write discontinued flag for row {row_num} "
            f"({make} {model} {variant} {fuel}): {e}"
        )

    logger.info(
        "sheets_writer v3.7.0: write_discontinued_flag row %d | %s %s %s %s | "
        "status=discontinued | date %s",
        row_num, make, model, variant, fuel, today_str,
    )

    return {
        "ok": True,
        "action": "discontinued",
        "row_number": row_num,
        "status": "discontinued",
        "last_known_price_date": today_str,
    }

def clear_discontinued_flag(make: str, model: str, variant: str, fuel: str,
                            note: Optional[str] = None) -> Dict[str, Any]:
    """
    Phase 3.2: Reverse a "discontinued" flag on an existing row.

    Clears:
      col H (status)                  -> "" (empty = active again)
      col I (last_known_price_date)   -> today's date (refresh the verified date)

    Critical: does NOT touch col E (price) or col G (notes). The price stays
    as-is — the variant just rejoins the catalog as active. The dashboard
    will stop showing the orange "Discontinued" badge for this variant.

    Use case: admin previously approved a NOT FOUND review and flagged a
    variant as discontinued, then realized the variant is actually still in
    production (e.g. CarWale just renamed it, or the data gap was temporary).
    Clicking "Un-discontinue" in the admin UI calls this function.

    Args:
      make, model, variant, fuel: must match an existing row exactly
      note: optional admin note appended to notes column (col G).
            If provided, col G is also updated. If None, col G is untouched.

    Raises RuntimeError if the row doesn't exist or the API call fails.
    """
    target = find_row(make, model, variant, fuel)
    if target is None:
        raise RuntimeError(
            f"Row not found for ({make}, {model}, {variant}, {fuel}). "
            f"Cannot un-discontinue."
        )

    row_num = target["_row_number"]
    today_str = _today_ddmmmyyyy()
    old_status = target.get("status", "")

    updates = [
        {
            "range": f"{TAB_CAR_PRICES}!H{row_num}",
            "values": [[""]],
        },
        {
            "range": f"{TAB_CAR_PRICES}!I{row_num}",
            "values": [[today_str]],
        },
    ]

    # Optional: append a note to col G if admin provided one
    if note:
        existing_notes = target.get("notes", "")
        merged_notes = f"{existing_notes} | Un-discontinued {today_str}: {note}".strip(" |")
        updates.append({
            "range": f"{TAB_CAR_PRICES}!G{row_num}",
            "values": [[merged_notes]],
        })

    try:
        _write_cells_batch(updates)
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to clear discontinued flag for row {row_num} "
            f"({make} {model} {variant} {fuel}): {e}"
        )

    logger.info(
        "sheets_writer v3.7.0: clear_discontinued_flag row %d | %s %s %s %s | "
        "status %r -> '' | date %s",
        row_num, make, model, variant, fuel, old_status, today_str,
    )

    return {
        "ok": True,
        "action": "un_discontinued",
        "row_number": row_num,
        "old_status": old_status,
        "status": "",
        "last_known_price_date": today_str,
    }

def write_new_variant(make: str, model: str, variant: str, fuel: str,
                      ex_showroom_price: int,
                      source: str = "CarWale",
                      extra_note: Optional[str] = None) -> Dict[str, Any]:
    """
    Phase 3: Append a brand new row to car_prices for a variant that doesn't
    exist yet. Used when admin approves a "new_variant" review.

    Inserts at the bottom of the tab with all 9 columns filled:
      A: make
      B: model
      C: variant     (CarWale's verbose name, e.g. "VXi Petrol Manual")
      D: fuel
      E: ex_showroom_price
      F: active                       -> "TRUE"
      G: notes                        -> "DD-MMM-YYYY {source} {extra_note}"
      H: status                       -> "" (empty = active)
      I: last_known_price_date        -> "DD-MMM-YYYY"

    Args:
      make, model, variant, fuel: car identity (variant is CarWale's verbose name)
      ex_showroom_price: positive int (rupees)
      source: tag for the notes column (default "CarWale")
      extra_note: optional extra string appended to notes

    Raises RuntimeError if a row with this (make, model, variant, fuel) already
    exists (in which case caller should use write_price_update_v2 instead).

    NOTE: Does NOT validate that the variant name doesn't already exist with a
    DIFFERENT fuel — that's allowed (e.g. "VXi Petrol" and "VXi Diesel" are two
    valid rows). The check is only on the full (make, model, variant, fuel) tuple.
    """
    if not isinstance(ex_showroom_price, int) or ex_showroom_price < 0:
        raise RuntimeError(
            f"ex_showroom_price must be a non-negative int, got "
            f"{type(ex_showroom_price).__name__}={ex_showroom_price!r}"
        )
    if not all([make, model, variant, fuel]):
        raise RuntimeError(
            "make, model, variant, fuel are all required (got empty value)"
        )

    # Guard: don't insert duplicate (make, model, variant, fuel)
    existing = find_row(make, model, variant, fuel)
    if existing is not None:
        raise RuntimeError(
            f"Row already exists for ({make}, {model}, {variant}, {fuel}) "
            f"at row {existing['_row_number']}. Use write_price_update_v2() "
            f"to update an existing row, not write_new_variant()."
        )

    # Find the next free row at the bottom by reading the tab.
    # Note: read_car_prices() returns rows starting from row 2 (header is row 1).
    # The next free row is max existing _row_number + 1, or 2 if tab is empty.
    all_rows = read_car_prices()
    last_row_num = max(
        (r["_row_number"] for r in all_rows),
        default=1,  # header is row 1
    )
    new_row_num = last_row_num + 1

    today_str = _today_ddmmmyyyy()
    new_notes = _format_notes(source, extra_note)

    # Single batch with all 9 cells in cols A through I
    updates = [
        {
            "range": f"{TAB_CAR_PRICES}!A{new_row_num}:I{new_row_num}",
            "values": [[
                make,
                model,
                variant,
                fuel,
                str(ex_showroom_price),
                "TRUE",
                new_notes,
                "",  # status = active
                today_str,
            ]],
        },
    ]

    try:
        _write_cells_batch(updates)
    except RuntimeError as e:
        raise RuntimeError(
            f"Failed to insert new variant row at row {new_row_num} "
            f"({make} {model} {variant} {fuel}): {e}"
        )

    logger.info(
        "sheets_writer v3.7.0: write_new_variant inserted row %d | "
        "%s %s %s %s | price=%d | date %s",
        new_row_num, make, model, variant, fuel, ex_showroom_price, today_str,
    )

    return {
        "ok": True,
        "action": "new_variant",
        "row_number": new_row_num,
        "make": make,
        "model": model,
        "variant": variant,
        "fuel": fuel,
        "ex_showroom_price": ex_showroom_price,
        "last_known_price_date": today_str,
        "notes": new_notes,
    }


# ============================================================
# PUBLIC API — model_slugs tab (v3.6.8, unchanged)
# ============================================================

def read_model_slugs() -> List[Dict[str, Any]]:
    """Read the model_slugs tab. Returns [] if tab doesn't exist."""
    metadata = _fetch_sheet_metadata_raw()
    if TAB_MODEL_SLUGS not in metadata.get("tabs", []):
        logger.info(
            "sheets_writer: model_slugs tab does not exist yet; returning []. "
            "Create the tab with column headers: make, model, carwale_slug, notes"
        )
        return []

    all_values = _fetch_tab_values(TAB_MODEL_SLUGS)
    if not all_values:
        return []

    rows = []
    for idx, row in enumerate(all_values[1:], start=2):
        padded = list(row) + [""] * (len(MODEL_SLUGS_COLUMNS) - len(row))
        rec = {
            col: padded[i] if i < len(padded) else ""
            for i, col in enumerate(MODEL_SLUGS_COLUMNS)
        }
        rec["_row_number"] = idx
        rows.append(rec)
    return rows


def write_model_slug(make: str, model: str, slug: str,
                     note: Optional[str] = None) -> Dict[str, Any]:
    """Insert or update a row in model_slugs keyed on (make, model)."""
    if not make or not model:
        raise RuntimeError("make and model are required to write a slug")
    if not slug:
        raise RuntimeError(
            "slug cannot be empty (use 'NEEDS_MANUAL_FIX' if unresolved)"
        )

    today_str = _today_ddmmmyyyy()
    if note is None:
        if slug == "NEEDS_MANUAL_FIX":
            note = f"Auto-derivation failed {today_str}"
        else:
            note = f"Auto-resolved {today_str}"

    existing = read_model_slugs()
    target_row = None
    for r in existing:
        if r.get("make") == make and r.get("model") == model:
            target_row = r
            break

    if target_row is not None:
        row_num = target_row["_row_number"]
        updates = [
            {
                "range": f"{TAB_MODEL_SLUGS}!C{row_num}",
                "values": [[slug]],
            },
            {
                "range": f"{TAB_MODEL_SLUGS}!D{row_num}",
                "values": [[note]],
            },
        ]
        _write_cells_batch(updates)
        logger.info(
            "sheets_writer: updated model_slugs row %d | %s/%s | slug=%s",
            row_num, make, model, slug,
        )
        return {
            "ok": True,
            "action": "updated",
            "row_number": row_num,
            "make": make,
            "model": model,
            "carwale_slug": slug,
            "notes": note,
        }

    last_row_num = max(
        (r["_row_number"] for r in existing),
        default=1,
    )
    new_row_num = last_row_num + 1

    updates = [
        {
            "range": f"{TAB_MODEL_SLUGS}!A{new_row_num}:D{new_row_num}",
            "values": [[make, model, slug, note]],
        },
    ]
    _write_cells_batch(updates)
    logger.info(
        "sheets_writer: appended model_slugs row %d | %s/%s | slug=%s",
        new_row_num, make, model, slug,
    )
    return {
        "ok": True,
        "action": "appended",
        "row_number": new_row_num,
        "make": make,
        "model": model,
        "carwale_slug": slug,
        "notes": note,
    }


# ============================================================
# CACHE RESET
# ============================================================

def reset_caches():
    """Force-clear module-level caches."""
    global _sa_info, _access_token, _access_token_expires_at
    global _session, _sheet_metadata_cache
    with _lock:
        _sa_info = None
        _access_token = None
        _access_token_expires_at = None
        _session = None
        _sheet_metadata_cache = None
    logger.info("sheets_writer: caches reset; next call will re-authenticate")

# ============================================================
# END OF PART 2/2 — sheets_writer.py v3.7.0
# ============================================================
