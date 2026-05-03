"""
sheets_writer.py
----------------
AutoKnowMus — Google Sheets read/write client using a service account.

Direct Google Sheets REST API calls via the `requests` library. No gspread,
no google-auth. Just JWT-grant OAuth (proven working in 238ms) + plain
HTTPS calls (proven working in 200ms via /admin/diag-egress).

----------------------------------------------------------------------
v3.6.8 — model_slugs tab support (additive, no breaking changes)
----------------------------------------------------------------------
New in this version (purely additive — all v3.6.7 code unchanged):

  - TAB_MODEL_SLUGS constant ("model_slugs")
  - MODEL_SLUGS_COLUMNS constant
  - read_model_slugs() public function — returns rows as list of dicts
  - write_model_slug(make, model, slug, note=None) public function —
    inserts or updates a row keyed on (make, model)

These are used by:
  - price_scraper.py (reads slugs to know which CarWale URL to fetch)
  - populate_slugs.py (one-time script: writes auto-resolved slugs)

If the model_slugs tab does not exist in the sheet, read_model_slugs()
returns [] gracefully so existing flows are not affected. The tab must
be created manually before populate_slugs.py is run for the first time:
  Sheet → add tab named "model_slugs" with column headers in row 1:
  A: make    B: model    C: carwale_slug    D: notes

----------------------------------------------------------------------
v3.6.7 — DIRECT REST API (gspread/google-auth fully bypassed)
----------------------------------------------------------------------
Background — what we tried before:

  v3.6.0  initial gspread integration                       → 30s hang
  v3.6.1  global socket.setdefaulttimeout                   → broke other routes
  v3.6.2  scoped socket timeout via context manager         → didn't help
  v3.6.3  monkey-patched google-auth Request.session        → didn't help
  v3.6.4  added SIGALRM safety net (still hangs at 15s)
  v3.6.5  manual JWT-grant OAuth (token mint = 238ms ✅)
                BUT open_by_key STILL hangs 15s             → gspread is the issue
  v3.6.6  HTTPAdapter timeout injection                     → still hangs 15s

Final root cause (from gspread v5.8 docs, confirmed empirically):

  > session – (optional) A session object capable of making HTTP requests...
  > Defaults to google.auth.transport.requests.AuthorizedSession.

In gspread 6.x, even when we pass `auth=None, session=requests.Session()`,
gspread internally wraps that session inside google-auth's
`AuthorizedSession` for some operations. That brings us right back to
google-auth's broken HTTP layer which hangs 15s on Render. The
`HTTPAdapter` we mounted on our session never gets a chance to apply
because gspread isn't using our session for the actual HTTP call.

Solution: stop using gspread. Stop using google-auth transport. Use the
Google Sheets REST API directly with `requests`.

The /admin/diag-egress test proved both required endpoints work with
plain `requests` calls and respond in <300ms:

  Step 5: POST oauth2.googleapis.com/token   → 270ms ✅
  Step 6: GET  sheets.googleapis.com/v4/...  → 200ms ✅

So this approach has zero unknowns left.

----------------------------------------------------------------------
ARCHITECTURE
----------------------------------------------------------------------
1. Manual JWT-grant flow (unchanged from v3.6.5):
     _build_jwt_assertion() → signed JWT
     _mint_access_token_manually() → POSTs to oauth2.googleapis.com/token
     _get_access_token() → cached token, auto-refresh near expiry

2. Google Sheets REST API client (v3.6.7+):
     _sheets_get(path, params=None) → GET with auth header + timeout
     _sheets_post(path, json_body) → POST with auth header + timeout
     Both use a module-level `requests.Session` for connection reuse.

3. Public API:
     v3.6.7: health_check, read_car_prices, find_row, write_price_update,
             get_service_account_email, get_sheet_metadata, reset_caches
     v3.6.8: + read_model_slugs, write_model_slug

The Google Sheets REST API endpoints we use:
  GET  /v4/spreadsheets/{id}?fields=properties.title,sheets.properties.title
       → sheet metadata (title, list of tab names)
  GET  /v4/spreadsheets/{id}/values/{range}
       → read a tab/range as 2D array
  POST /v4/spreadsheets/{id}/values:batchUpdate
       → write to specific cells in a single batched call

API docs: https://developers.google.com/sheets/api/reference/rest

requirements.txt:
  - `gspread` is no longer required (can be removed in a cleanup commit)
  - `google-auth` may still be a transitive dep of other packages but is
    no longer imported by this module
  - `requests` and `cryptography` are still needed (both already present)
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

# Required scopes for read AND write access to a single sheet.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
]

# Google API endpoints.
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
SHEETS_API_BASE = "https://sheets.googleapis.com/v4"

# JWT grant type as defined in RFC 7523. Service-account JSON keys use
# this exact assertion type to swap a signed JWT for an access token.
JWT_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"

# Access tokens from Google's OAuth2 endpoint live for 1 hour by default.
# We refresh slightly before expiry to avoid edge-case 401s.
TOKEN_REFRESH_SAFETY_MARGIN_SECONDS = 60

# The AutoKnowMus Price Data sheet. ID is hardcoded because there's exactly
# one of these per environment, and it never changes.
DEFAULT_SHEET_ID = "1ZGDlq-qWA17ChBf2KJy0pmpYMBaAPconwquVXqMh9Oc"
SHEET_ID = os.environ.get("AUTOKNOWMUS_SHEET_ID", DEFAULT_SHEET_ID)

# Tab names — must match exactly what's in the spreadsheet.
TAB_CAR_PRICES = "car_prices"
TAB_DEPRECIATION = "depreciation_curve"
TAB_MULTIPLIERS = "multipliers"
TAB_META = "meta"
TAB_MODEL_SLUGS = "model_slugs"  # v3.6.8

# Expected column headers for car_prices tab.
CAR_PRICES_COLUMNS = [
    "make",                # A
    "model",               # B
    "variant",             # C
    "fuel",                # D
    "ex_showroom_price",   # E
    "active",              # F
    "notes",               # G
]

# Expected column headers for model_slugs tab (v3.6.8).
MODEL_SLUGS_COLUMNS = [
    "make",                # A
    "model",               # B
    "carwale_slug",        # C   format: 'make-slug/model-slug'
    "notes",               # D   auto-resolved date or NEEDS_MANUAL_FIX reason
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

# Cached spreadsheet metadata so we don't fetch tab list on every call.
_sheet_metadata_cache: Optional[Dict[str, Any]] = None

_lock = threading.Lock()


# ============================================================
# CREDENTIAL LOADING (unchanged from v3.6.5)
# ============================================================

def _decode_env_payload(raw: str) -> Dict:
    """
    Accept the GOOGLE_SERVICE_ACCOUNT_JSON env var in either form:
      1. Base64-encoded JSON (recommended)
      2. Raw JSON (legacy, fragile)
    """
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
    """URL-safe base64 encoding without padding (RFC 7515)."""
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _build_jwt_assertion(sa_info: Dict[str, Any]) -> str:
    """
    Build a signed JWT to use as the assertion in a JWT-grant OAuth2 request.
    Per RFC 7523. Uses RS256 (RSASSA-PKCS1-v1_5 with SHA-256).
    """
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
    """
    Mint a fresh OAuth2 access token by JWT-grant flow.
    Proven working in 238ms on the Render production worker.
    """
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
    """
    Return a valid access token, minting a new one if the cached one is
    expired or near-expired. Thread-safe.
    """
    global _access_token, _access_token_expires_at

    with _lock:
        now = time.time()
        if (_access_token is not None
                and _access_token_expires_at is not None
                and now < (_access_token_expires_at - TOKEN_REFRESH_SAFETY_MARGIN_SECONDS)):
            return _access_token

    # Mint OUTSIDE the lock since it's a network call. Re-acquire to write.
    token, expires_at = _mint_access_token_manually()
    with _lock:
        _access_token = token
        _access_token_expires_at = expires_at
        return token



# ============================================================
# END OF PART 1/2 — sheets_writer.py v3.6.8
# Continue pasting Part 2/2 after this line.
# ============================================================
# ============================================================
# CONTINUATION OF sheets_writer.py v3.6.8 — PART 2/2
# Paste this AFTER Part 1/2 in the same file.
# ============================================================

# ============================================================
# v3.6.7: DIRECT GOOGLE SHEETS REST API CLIENT
# ============================================================

def _get_session():
    """
    Lazy-build (and cache) a single requests.Session for the module.
    Reusing a Session enables connection pooling — subsequent calls to
    the same host reuse the existing TCP/TLS connection.
    """
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
    """
    Make an authenticated HTTP request to the Google Sheets REST API.
    Returns the parsed JSON body on success.

    Args:
      method: 'GET' or 'POST'
      path: path relative to SHEETS_API_BASE (e.g. '/spreadsheets/{id}')
      params: query string params (for GET)
      json_body: JSON request body (for POST)
      op_label: short label for log messages (e.g. 'get_metadata')

    Raises RuntimeError on non-2xx response or network failure. The error
    message includes the HTTP status, response body, and a hint about the
    likely cause (permission denied, sheet not found, etc.).
    """
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
                url,
                headers=headers,
                params=params,
                timeout=_API_TIMEOUT,
            )
        elif method.upper() == "POST":
            resp = session.post(
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=_API_TIMEOUT,
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
        # Token might've expired between mint and request. Force a fresh
        # mint and retry once. Practical case: a token cached for 59 min
        # right when this request fires.
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
                    url, headers=headers, params=params, json=json_body,
                    timeout=_API_TIMEOUT,
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
    """
    Fetch sheet metadata (title + tab list) via the REST API.
    Cached at module level — first call hits the API, subsequent calls
    return cached value.
    """
    global _sheet_metadata_cache

    with _lock:
        if _sheet_metadata_cache is not None:
            return _sheet_metadata_cache

    # GET /v4/spreadsheets/{id}?fields=properties.title,sheets.properties.title
    # Using the `fields` mask makes the response tiny (~200 bytes) and fast.
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
    """
    Fetch all values from a tab as a 2D array of strings.

    Uses the values.get endpoint with the tab name as the range.
    Empty trailing rows/columns are NOT returned by the API by default —
    we get only the rectangle that actually has data.
    """
    # The range is just the tab name (with quoting if it contains spaces).
    # For our tabs (car_prices, depreciation_curve, etc.) no quoting needed.
    path = f"/spreadsheets/{SHEET_ID}/values/{tab_name}"
    # majorDimension=ROWS is the default but explicit is better.
    params = {"majorDimension": "ROWS"}
    data = _sheets_request("GET", path, params=params, op_label=f"read_tab[{tab_name}]")

    return data.get("values") or []


def _write_cells_batch(updates: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Write multiple cell ranges in a single batched API call.

    Args:
      updates: list of {"range": "TabName!A1:B2", "values": [["a","b"],["c","d"]]}

    Returns the API response dict (totalUpdatedCells, etc.).
    """
    path = f"/spreadsheets/{SHEET_ID}/values:batchUpdate"
    body = {
        "valueInputOption": "USER_ENTERED",
        "data": updates,
    }
    return _sheets_request("POST", path, json_body=body, op_label="batch_update")


# ============================================================
# PUBLIC API — CORE (UNCHANGED ACROSS VERSIONS)
# ============================================================

def get_service_account_email() -> str:
    """
    Return the SA email. Returns "<unknown>" on failure — never raises.
    Safe to call from error-handling paths.
    """
    try:
        info = _load_sa_info()
        return info.get("client_email", "<unknown>")
    except Exception:
        return "<unknown>"


def get_sheet_metadata() -> Dict[str, Any]:
    """
    Return basic sheet info for diagnostics.

    Returns:
      {
        "sheet_id": str,
        "sheet_title": str,
        "tabs": [str, ...],
        "service_account_email": str,
      }
    """
    metadata = _fetch_sheet_metadata_raw()
    return {
        "sheet_id": metadata["sheet_id"],
        "sheet_title": metadata["sheet_title"],
        "tabs": metadata["tabs"],
        "service_account_email": get_service_account_email(),
    }


def health_check() -> Dict[str, Any]:
    """
    Quick end-to-end test: load credentials, mint OAuth token, fetch sheet
    metadata, read first cell of car_prices tab.
    """
    metadata = get_sheet_metadata()

    if TAB_CAR_PRICES not in metadata["tabs"]:
        raise RuntimeError(
            f"Tab '{TAB_CAR_PRICES}' not found. Available: {metadata['tabs']}."
        )

    # Read just A1 of car_prices for the health check.
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
    """
    Find row matching (make, model, variant, fuel) EXACTLY (case-sensitive).
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
    """Build the notes-cell value: 'DD-MMM-YYYY {source}'."""
    today_str = date.today().strftime("%d-%b-%Y")
    parts = [today_str, source]
    if extra:
        parts.append(extra)
    return " ".join(parts)


def write_price_update(make: str, model: str, variant: str, fuel: str,
                       new_price: int, source: str = "Manual",
                       extra_note: Optional[str] = None) -> Dict[str, Any]:
    """
    Update the ex_showroom_price (col E) and notes (col G) of a single row.
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

    # Build the batch update request: column E (ex_showroom_price) and
    # column G (notes) for the target row, in a single API call.
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
# v3.6.8: PUBLIC API — model_slugs tab
# ============================================================
#
# Used by:
#   - price_scraper.py (reads slugs to know which CarWale URL to fetch)
#   - populate_slugs.py (one-time script: writes auto-resolved slugs)
#
# Sheet setup required (one time, manual):
#   In the AutoKnowMus Price Data Google Sheet, add a tab named
#   "model_slugs" with these column headers in row 1:
#     A: make    B: model    C: carwale_slug    D: notes
#
# Slug format stored in column C: 'make-slug/model-slug'
#   e.g. 'maruti-suzuki/swift'
#   The price_scraper splits this into the URL pattern:
#     https://www.carwale.com/{make-slug}-cars/{model-slug}/
#
# When auto-resolution fails, the slug column is set to 'NEEDS_MANUAL_FIX'
# so the admin knows which rows need a human visiting CarWale to find
# the right URL.
# ============================================================

def read_model_slugs() -> List[Dict[str, Any]]:
    """
    Read the model_slugs tab and return as a list of dicts.
    Each dict has MODEL_SLUGS_COLUMNS keys plus _row_number (1-indexed).
    Header row is excluded.

    If the tab doesn't exist yet, returns []. (This makes it safe to call
    before populate_slugs.py has run for the first time, or before the
    admin has manually created the tab.)

    Returns:
      [
        {"make": "Maruti Suzuki", "model": "Swift",
         "carwale_slug": "maruti-suzuki/swift", "notes": "Auto-resolved 03-May-2026",
         "_row_number": 2},
        ...
      ]
    """
    # Guard against the tab not existing. Cheaper than a 404 from the API
    # and avoids polluting logs with errors during normal startup.
    metadata = _fetch_sheet_metadata_raw()
    if TAB_MODEL_SLUGS not in metadata.get("tabs", []):
        logger.info(
            "sheets_writer: model_slugs tab does not exist yet; returning []. "
            "Create the tab with column headers: make, model, carwale_slug, notes "
            "to enable price scraping."
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
    """
    Insert or update a row in the model_slugs tab keyed on (make, model).

    If a row exists with matching (make, model) → update its carwale_slug
    and notes columns.
    If no row exists → append a new row at the bottom of the tab.

    Args:
      make:  the make name as it appears in car_prices (e.g. "Maruti Suzuki")
      model: the model name as it appears in car_prices (e.g. "Swift")
      slug:  the resolved slug "make-slug/model-slug" or "NEEDS_MANUAL_FIX"
      note:  optional override for the notes column. If omitted, a default
             is generated based on whether the slug is resolved or not.

    Returns:
      {
        "ok": True,
        "action": "updated" | "appended",
        "row_number": int,
        "make": str, "model": str, "carwale_slug": str, "notes": str,
      }
    """
    if not make or not model:
        raise RuntimeError("make and model are required to write a slug")
    if not slug:
        raise RuntimeError(
            "slug cannot be empty (use 'NEEDS_MANUAL_FIX' if unresolved)"
        )

    today_str = date.today().strftime("%d-%b-%Y")
    if note is None:
        if slug == "NEEDS_MANUAL_FIX":
            note = f"Auto-derivation failed {today_str}"
        else:
            note = f"Auto-resolved {today_str}"

    # Look up existing row by (make, model)
    existing = read_model_slugs()
    target_row = None
    for r in existing:
        if r.get("make") == make and r.get("model") == model:
            target_row = r
            break

    if target_row is not None:
        # Update existing row in place — only columns C and D.
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

    # Append new row at the bottom of the tab.
    # The "next" row number = max existing row + 1, or 2 if tab is empty
    # (row 1 is the header).
    last_row_num = max(
        (r["_row_number"] for r in existing),
        default=1,  # header row at row 1
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
    """
    Force-clear module-level caches.

    v3.6.8: also clears _sheet_metadata_cache so a freshly-added
    model_slugs tab gets picked up without restarting the worker.
    """
    global _sa_info, _access_token, _access_token_expires_at
    global _session, _sheet_metadata_cache
    with _lock:
        _sa_info = None
        _access_token = None
        _access_token_expires_at = None
        _session = None
        _sheet_metadata_cache = None
    logger.info("sheets_writer: caches reset; next call will re-authenticate")
