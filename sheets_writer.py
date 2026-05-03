"""
sheets_writer.py
----------------
AutoKnowMus — Google Sheets read/write client using a service account.

This module is the bridge between the Flask app and the AutoKnowMus Price
Data Google Sheet. It uses gspread + a manually-minted OAuth2 access token
to authenticate with the Google Sheets API.

----------------------------------------------------------------------
v3.6.5 — MANUAL JWT-GRANT OAuth (the real fix)
----------------------------------------------------------------------
Background: v3.6.3 tried to fix the OAuth hang by monkey-patching
google-auth's Request.session.request to inject a 10s timeout. That
patch was confirmed to be invoked correctly, but the hang persisted
anyway.

The /admin/diag-egress diagnostic endpoint (run on 03-May-2026) proved
the cause definitively:

  Step 5 (requests.post -> oauth2.googleapis.com/token):  270ms PASS
  Inside creds.refresh(auth_req):                          15s+ HANG

Same target URL, same network, same Render worker, same Python process.
The 270ms call works perfectly. The google-auth call hangs.

Conclusion: the hang is INSIDE google-auth's flow somewhere between
`creds.refresh()` being called and the actual `session.request()` line.
Our timeout patch was on `session.request`, but execution never reaches
that line. The hang is upstream — likely in google-auth's own retry-
wrapping logic or its credential-state machinery.

The fix: skip google-auth's broken refresh path entirely. Build the
JWT-grant flow ourselves using `cryptography` (a transitive dep of
google-auth, already installed) + `requests` (a direct dep of gspread,
already installed). Hand the resulting access token to gspread via a
small custom auth callable. Result: OAuth completes in ~300ms instead
of timing out at 15s.

Architecture:

  1. _mint_access_token_manually() builds a JWT assertion, signs it
     with the service-account's RSA private key, POSTs it to
     oauth2.googleapis.com/token with `requests` (timeout=(5,5)),
     and returns (access_token, expires_at_unix_seconds).

  2. _ManualTokenAuth is a tiny callable that gspread's session uses
     as a request hook — it adds the Authorization header to every
     outbound request. If the cached token is near expiry, it
     transparently refreshes by calling (1) again.

  3. _get_client() builds a gspread.Client with our manual auth hook
     wired in, and applies set_timeout((10,10)) for all sheet RPCs.

Public API is UNCHANGED. app.py does not need any modifications.

----------------------------------------------------------------------
v3.6.4 — added SIGALRM safety net in app.py (hard wall-clock timeout)
v3.6.3 — explicit OAuth refresh + monkey-patched session.request timeout
v3.6.2 — scoped socket timeout via context manager (didn't help)
v3.6.1 — global socket.setdefaulttimeout (broke other routes)
v3.6.0 — initial gspread integration; hung at 30s on Render
----------------------------------------------------------------------

ARCHITECTURE NOTES (unchanged from v3.6.0)
----------------------------------------------------------------------
Why gspread:
  - Thin, well-maintained wrapper over the raw Google Sheets API
  - Handles batching, range parsing, error mapping
  - Public API stays identical even though we replaced auth internals

Failure-mode design:
  - Credentials loaded once at module import time and cached. If env var
    is missing/malformed, ALL functions raise RuntimeError with clear msg.
  - Sheet open is also cached. If un-shared with the SA mid-runtime, the
    next call surfaces the permission error.
  - All HTTP calls have explicit timeouts. Anything slower errors clean.

Public API (used by app.py — unchanged across versions):
  - health_check()         -> quick connection test (returns dict)
  - read_car_prices()      -> returns car_prices tab as list of dicts
  - write_price_update()   -> updates one row's ex_showroom_price + notes
  - find_row()             -> look up a (make,model,variant,fuel) row
  - get_service_account_email() -> email str (never raises)
  - get_sheet_metadata()   -> sheet title + tab names
  - reset_caches()         -> force re-auth on next call
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

# OAuth2 token endpoint. The same one whose 270ms response we verified
# in /admin/diag-egress.
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

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

# Hard timeouts. Gunicorn's default worker timeout is 30s; we cap our
# individual calls at 10s so we always error cleanly before gunicorn
# kills us. The token-mint call uses (5,5) since it's a single fast POST.
_GOOGLE_API_TIMEOUT_SECONDS = 10
_TOKEN_MINT_CONNECT_TIMEOUT = 5
_TOKEN_MINT_READ_TIMEOUT = 5


# ============================================================
# MODULE-LEVEL CACHES (thread-safe)
# ============================================================

# Parsed service-account JSON dict.
_sa_info: Optional[Dict[str, Any]] = None

# Cached OAuth2 access token + its unix-seconds expiry.
_access_token: Optional[str] = None
_access_token_expires_at: Optional[float] = None

# gspread client + spreadsheet.
_gc_client = None
_spreadsheet = None

# Single lock guards all five caches above.
_lock = threading.Lock()


# ============================================================
# CREDENTIAL LOADING
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
# v3.6.5: MANUAL JWT-GRANT OAuth FLOW
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
            "It should be installed as a dependency of google-auth."
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
    Mint a fresh OAuth2 access token by:
      1. Building a signed JWT assertion (RS256, scopes baked in)
      2. POSTing it to oauth2.googleapis.com/token with grant_type=jwt-bearer
      3. Parsing the response

    /admin/diag-egress confirmed this exact endpoint responds in ~270ms.
    We use a (5,5) timeout so total bounded at ~10s worst case.
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
            timeout=(_TOKEN_MINT_CONNECT_TIMEOUT, _TOKEN_MINT_READ_TIMEOUT),
        )
    except requests.exceptions.Timeout as e:
        raise RuntimeError(
            f"Token mint timed out. Render may be unable to reach "
            f"oauth2.googleapis.com. Run /admin/diag-egress to diagnose. "
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
    Return a valid access token, minting a new one if cached one is
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
# v3.6.5: GSPREAD CLIENT WIRED UP TO MANUAL TOKEN
# ============================================================

class _ManualTokenAuth:
    """
    A small auth helper that gspread's HTTP session calls before every
    outbound request. We add the Authorization header with our manually-
    minted access token, transparently refreshing if it's near expiry.

    Implements the standard requests.auth.AuthBase callable interface.
    """

    def __call__(self, request):
        token = _get_access_token()
        request.headers["Authorization"] = f"Bearer {token}"
        return request


def _get_client():
    """
    Build (and cache) a gspread client wired up to our manual access
    token. v3.6.5: skips google-auth's broken creds.refresh() entirely.
    """
    global _gc_client

    # Check cache first under lock.
    with _lock:
        if _gc_client is not None:
            return _gc_client

    # Mint token OUTSIDE any lock — _get_access_token has its own locking.
    # This primes the cache and surfaces any auth error fast.
    _get_access_token()

    # Now build the client. Re-check cache under lock for race safety.
    with _lock:
        if _gc_client is not None:
            return _gc_client

        try:
            import gspread
            import requests
        except ImportError as e:
            raise RuntimeError(
                f"required library not available: {e}. "
                "Add gspread + requests to requirements.txt."
            )

        session = requests.Session()
        session.auth = _ManualTokenAuth()

        try:
            # gspread.Client(auth=None, session=...) - passing auth=None
            # tells gspread we're handling auth via the session's auth hook.
            _gc_client = gspread.Client(auth=None, session=session)
            _gc_client.set_timeout(
                (_GOOGLE_API_TIMEOUT_SECONDS, _GOOGLE_API_TIMEOUT_SECONDS)
            )
        except Exception as e:
            raise RuntimeError(f"Could not build gspread client: {e}")

        logger.info(
            "sheets_writer: gspread client built with manual JWT auth "
            "(timeout=%ds)",
            _GOOGLE_API_TIMEOUT_SECONDS,
        )
        return _gc_client


def _get_spreadsheet():
    """
    Open (and cache) the AutoKnowMus Price Data spreadsheet.
    """
    global _spreadsheet

    # Get client OUTSIDE our lock since _get_client has its own locking.
    client = _get_client()

    with _lock:
        if _spreadsheet is not None:
            return _spreadsheet

        try:
            _spreadsheet = client.open_by_key(SHEET_ID)
        except Exception as e:
            err_str = str(e).lower()
            if "permission" in err_str or "denied" in err_str or "403" in err_str:
                raise RuntimeError(
                    f"Permission denied opening sheet {SHEET_ID}. "
                    f"Share the sheet as Editor with "
                    f"{get_service_account_email()}. Original error: {e}"
                )
            if "not found" in err_str or "404" in err_str:
                raise RuntimeError(
                    f"Spreadsheet {SHEET_ID} not found. "
                    f"Original error: {e}"
                )
            raise RuntimeError(f"Could not open spreadsheet {SHEET_ID}: {e}")

        return _spreadsheet


def _get_worksheet(tab_name: str):
    """Open a specific tab inside the spreadsheet."""
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
                f"Tab '{tab_name}' not found. Available tabs: {available}"
            )
        raise RuntimeError(f"Could not open tab '{tab_name}': {e}")


# ============================================================
# PUBLIC API (UNCHANGED ACROSS VERSIONS)
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
    Quick end-to-end test: load credentials, mint OAuth token, open sheet,
    read one cell.
    """
    metadata = get_sheet_metadata()

    if TAB_CAR_PRICES not in metadata["tabs"]:
        raise RuntimeError(
            f"Tab '{TAB_CAR_PRICES}' not found. Available: {metadata['tabs']}."
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
    Read the entire car_prices tab and return as list of dicts.
    Each dict has CAR_PRICES_COLUMNS keys plus _row_number (1-indexed).
    Header row excluded.
    """
    ws = _get_worksheet(TAB_CAR_PRICES)
    try:
        all_values = ws.get_all_values()
    except Exception as e:
        raise RuntimeError(f"Could not read car_prices tab: {e}")

    if not all_values:
        return []

    header = all_values[0]
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

    ws = _get_worksheet(TAB_CAR_PRICES)
    try:
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
    Force-clear module-level caches.
    """
    global _sa_info, _access_token, _access_token_expires_at
    global _gc_client, _spreadsheet
    with _lock:
        _sa_info = None
        _access_token = None
        _access_token_expires_at = None
        _gc_client = None
        _spreadsheet = None
    logger.info("sheets_writer: caches reset; next call will re-authenticate")
