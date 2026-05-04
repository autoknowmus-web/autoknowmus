"""
price_scraper.py — CarWale ex-showroom price scraper for AutoKnowMus
=====================================================================

VERSION: v2.1 (03-May-2026)

CHANGELOG:
    v2.1 — URL builder fix.
        Sheet stores slugs as `{make-slug}/{model-slug}` (e.g.
        "maruti-suzuki/swift"). CarWale's actual URL convention is
        `{make-slug}-cars/{model-slug}/` (e.g. "maruti-suzuki-cars/swift/").
        v2.0 hit /maruti-suzuki/swift/ → 404 across the board.
        v2.1 introduces _slug_to_url_path() which appends "-cars" to the
        make portion at URL-build time. Sheet untouched. Manual-fix slugs
        (land-rover/freelander-2, toyota/landcruiserprado) work the same
        way — verified universal across makes including Land Rover, Lexus,
        Mercedes-Benz, Rolls-Royce, BMW.

    v2.0 — Rewritten parser. v1.0's BeautifulSoup CSS selectors didn't
        work against CarWale's React-rendered DOM. v2.0 extracts
        window.__INITIAL_STATE__ JSON blob and walks balanced braces to
        get the variant list. Public API shape unchanged.

WHAT THIS MODULE DOES:
    Public function `fetch_price(make, model, variant, fuel)` returns
    a dict with the ex-showroom price for a single (make, model, variant,
    fuel) combo, scraped live from carwale.com.

PUBLIC API SHAPE (unchanged from v2.0 — app.py's /admin/test-scraper
route does NOT need any changes):

    {
      "ok": True,
      "status": "found" | "found_multiple" | "found_fuzzy"
                | "not_found" | "not_found_fuel" | "no_slug"
                | "error",
      "make": str,
      "model": str,
      "variant": str,
      "fuel": str,
      "ex_showroom_inr": int | None,
      "matched_variant": str | None,        # full versionName from CarWale
      "url": str | None,                     # CarWale URL we hit
      "scraped_at": ISO timestamp,
      "candidates_considered": int,          # how many variants we matched
      "all_variants": list[str] | None,      # for debug — full list when matching
      "error": str | None,                   # human-readable error
    }

HIGH-LEVEL FLOW:
    1. Read the model_slugs tab to look up the carwale_slug for
       (make, model). Cached in-memory for SLUG_CACHE_TTL.
    2. Build URL via _slug_to_url_path: "maruti-suzuki/swift"
       -> "https://www.carwale.com/maruti-suzuki-cars/swift/"
    3. Rate-limited HTTP GET (User-Agent spoofed, retries on 429/503).
    4. Find `window.__INITIAL_STATE__ = {...};` inside the response.
    5. Walk balanced braces to extract the JSON, parse with json.loads.
    6. Pull `modelPage.versions` — list of variant dicts with
       priceOverview.exShowRoomPrice (clean integer rupees).
    7. Match user's variant: normalize names (strip fuel/transmission/
       trim modifiers), filter by fuel, return cheapest match (because
       a user typing "VXI Petrol" usually means the base VXI Petrol,
       not VXI (O) or VXI Petrol AMT).

DEPENDENCIES:
    - requests (already in requirements.txt)
    - stdlib: json, re, time, threading, datetime, random, logging
"""

from __future__ import annotations

import json
import logging
import random
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# Local — model_slugs tab lookups
from sheets_writer import read_model_slugs

# ============================================================
# CONFIG
# ============================================================

CARWALE_BASE = "https://www.carwale.com"

# Rate-limit knobs
MIN_REQ_INTERVAL_SECS = 2.0
JITTER_SECS = 0.5
MAX_RETRIES = 4
BACKOFF_BASE_SECS = 1.0  # 1, 2, 4, 8

# HTTP timeouts
CONNECT_TIMEOUT_SECS = 8.0
READ_TIMEOUT_SECS = 20.0

# Slug cache (in-process, single-worker — Render free tier)
SLUG_CACHE_TTL_SECS = 3600  # 1 hour

# Browser-like User-Agent — CarWale's CDN is sensitive to bot UAs
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    # v2.7: Explicitly decline Brotli. CarWale's CDN sends `Content-Encoding: br`
    # if we don't, and `requests` doesn't auto-decompress Brotli — we'd get
    # garbage bytes back where __INITIAL_STATE__ should be. Forcing gzip-only
    # makes the response decompressable by the stdlib path. Verified via
    # /admin/diag-scraper-fetch which showed `br`-encoded responses producing
    # zero needle matches for `__INITIAL_STATE__`, `exShowRoomPrice`, etc.
    "Accept-Encoding": "gzip, deflate, identity;q=0.5, *;q=0",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# Tokens to strip when comparing variant names. Lowercased.
VARIANT_NOISE_TOKENS = (
    "petrol", "diesel", "cng", "electric", "hybrid",
    "phev", "hev", "bev", "ev",
    "manual", "automatic", "amt", "cvt", "ivt", "dct", "dsg",
    "tiptronic", "stronic", "torque converter", "tc",
    "dual tone", "dual-tone", "dualtone",
)

logger = logging.getLogger("price_scraper")
logger.setLevel(logging.INFO)


# ============================================================
# RATE LIMITER (process-global, thread-safe)
# ============================================================

_rate_lock = threading.Lock()
_last_request_at = 0.0


def _wait_for_rate_limit() -> None:
    """Block until we're at least MIN_REQ_INTERVAL_SECS (+jitter) after the
    last outbound request. Thread-safe."""
    global _last_request_at
    with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_request_at
        wait_floor = MIN_REQ_INTERVAL_SECS + random.uniform(0.0, JITTER_SECS)
        if elapsed < wait_floor:
            time.sleep(wait_floor - elapsed)
        _last_request_at = time.monotonic()


# ============================================================
# SLUG CACHE
# ============================================================

_slug_cache: Dict[Tuple[str, str], str] = {}
_slug_cache_built_at: float = 0.0
_slug_cache_lock = threading.Lock()


def _normalize_make_model(make: str, model: str) -> Tuple[str, str]:
    """Trim + collapse internal whitespace. Don't touch case — sheet stores
    canonical-cased names like 'Maruti Suzuki' / 'Swift'."""
    return " ".join(make.split()).strip(), " ".join(model.split()).strip()


def _refresh_slug_cache() -> None:
    """Rebuild slug cache from sheet's model_slugs tab. Safe to call on
    miss — read_model_slugs returns [] if the tab doesn't exist."""
    global _slug_cache, _slug_cache_built_at
    with _slug_cache_lock:
        try:
            rows = read_model_slugs()
        except Exception as e:
            logger.warning("slug cache refresh failed: %s", e)
            return
        new_cache: Dict[Tuple[str, str], str] = {}
        for row in rows:
            mk = (row.get("make") or "").strip()
            md = (row.get("model") or "").strip()
            slug = (row.get("carwale_slug") or "").strip()
            if not mk or not md or not slug:
                continue
            if slug == "NEEDS_MANUAL_FIX":
                continue
            new_cache[(mk, md)] = slug
        _slug_cache = new_cache
        _slug_cache_built_at = time.monotonic()
        logger.info("slug cache refreshed: %d entries", len(_slug_cache))


def _get_slug(make: str, model: str) -> Optional[str]:
    """Look up CarWale slug for (make, model). Refreshes the cache if it's
    stale or empty. Returns None if not found / NEEDS_MANUAL_FIX."""
    mk, md = _normalize_make_model(make, model)
    age = time.monotonic() - _slug_cache_built_at
    if not _slug_cache or age > SLUG_CACHE_TTL_SECS:
        _refresh_slug_cache()
    return _slug_cache.get((mk, md))


# ============================================================
# v2.1: SLUG -> URL PATH
# ============================================================

def _slug_to_url_path(slug: str) -> str:
    """Convert a sheet slug to the CarWale URL path segment.

    The model_slugs sheet stores slugs in the form '{make-slug}/{model-slug}'
    (e.g. 'maruti-suzuki/swift', 'land-rover/freelander-2'). CarWale's URL
    convention adds '-cars' to the make portion:
        'maruti-suzuki/swift'      -> 'maruti-suzuki-cars/swift'
        'land-rover/freelander-2'  -> 'land-rover-cars/freelander-2'
        'toyota/landcruiserprado'  -> 'toyota-cars/landcruiserprado'

    Verified universal: land-rover-cars, lexus-cars, mercedes-benz-cars,
    rolls-royce-cars, bmw-cars all resolve on carwale.com.

    Defensive: if the slug already contains '-cars/' we leave it alone
    (so a future hand-edit in the sheet won't double-stamp).
    Defensive: if the slug has no '/' (unexpected shape), fall back to
    appending '-cars/' so we still produce a syntactically valid URL —
    this will likely 404 but the error will be visible and traceable.
    """
    s = (slug or "").strip().strip("/")
    if not s:
        return ""
    # Already has -cars/ — leave alone (caller hand-edited the sheet)
    if "-cars/" in s:
        return s
    if "/" not in s:
        # Unexpected shape; best-effort fallback
        return f"{s}-cars"
    make_part, _, rest = s.partition("/")
    return f"{make_part}-cars/{rest}"


def _build_url(slug: str) -> str:
    """Public URL for a given sheet slug. Centralized so fetch_price and
    list_variants stay in lockstep."""
    return f"{CARWALE_BASE}/{_slug_to_url_path(slug)}/"


# ============================================================
# HTTP FETCH WITH RETRY
# ============================================================

class FetchError(Exception):
    """Raised when we can't fetch a page after retries."""


def _http_get(url: str) -> str:
    """GET a URL with rate limiting, retries on 429/503, exponential backoff.
    Returns the HTML body as a str. Raises FetchError on terminal failure."""
    last_err: Optional[str] = None
    for attempt in range(MAX_RETRIES):
        _wait_for_rate_limit()
        try:
            resp = requests.get(
                url,
                headers=DEFAULT_HEADERS,
                timeout=(CONNECT_TIMEOUT_SECS, READ_TIMEOUT_SECS),
                allow_redirects=True,
            )
        except requests.exceptions.Timeout as e:
            last_err = f"timeout: {e}"
            wait = BACKOFF_BASE_SECS * (2 ** attempt)
            logger.warning("[%s] timeout (attempt %d/%d), backing off %.1fs",
                          url, attempt + 1, MAX_RETRIES, wait)
            time.sleep(wait)
            continue
        except requests.exceptions.RequestException as e:
            last_err = f"request_exception: {e}"
            wait = BACKOFF_BASE_SECS * (2 ** attempt)
            logger.warning("[%s] request error (attempt %d/%d): %s",
                          url, attempt + 1, MAX_RETRIES, e)
            time.sleep(wait)
            continue

        # Got a response. Check status.
        if resp.status_code in (429, 503):
            last_err = f"http_{resp.status_code}"
            wait = BACKOFF_BASE_SECS * (2 ** attempt)
            logger.warning("[%s] HTTP %d (attempt %d/%d), backing off %.1fs",
                          url, resp.status_code, attempt + 1, MAX_RETRIES, wait)
            time.sleep(wait)
            continue
        if resp.status_code == 404:
            raise FetchError(f"HTTP 404 — page not found: {url}")
        if resp.status_code >= 400:
            raise FetchError(f"HTTP {resp.status_code} for {url}")
        # 2xx — decompress if needed, then return body.
        # v2.8: CarWale's CloudFront edge sends `Content-Encoding: br` even
        # when we ask for gzip-only (v2.7 Accept-Encoding header is ignored).
        # `requests` doesn't auto-decompress Brotli, so resp.text returned
        # garbage bytes. Plan B: explicitly decompress Brotli on our side.
        # The Brotli package is in requirements.txt as of v2.8.
        encoding = (resp.headers.get("Content-Encoding") or "").lower()
        if encoding == "br":
            try:
                import brotli
                decoded = brotli.decompress(resp.content).decode("utf-8", errors="replace")
                logger.info("[%s] Brotli decoded: %d compressed bytes -> %d chars",
                            url, len(resp.content), len(decoded))
                return decoded
            except Exception as e:
                logger.warning("[%s] Brotli decode failed: %s", url, e)
                # Fall through to resp.text — likely garbage but better than crash
        return resp.text
    raise FetchError(f"max retries exhausted ({MAX_RETRIES}); last_err={last_err}")

# ============================================================
# JSON EXTRACTION FROM CARWALE HTML
# ============================================================

INITIAL_STATE_NEEDLE = "__INITIAL_STATE__"


def _extract_initial_state(html: str) -> Dict[str, Any]:
    """Locate `window.__INITIAL_STATE__ = { ... };` in the HTML and return
    the parsed dict. Walks balanced braces (string-aware) to find the JSON
    end — safer than regex for nested objects.
    Raises ValueError on any failure."""
    idx = html.find(INITIAL_STATE_NEEDLE)
    if idx < 0:
        raise ValueError(
            f"'{INITIAL_STATE_NEEDLE}' not found in HTML "
            f"(page may be a redirect, error page, or layout changed)"
        )
    eq_idx = html.find("=", idx)
    if eq_idx < 0:
        raise ValueError(f"no '=' after {INITIAL_STATE_NEEDLE}")
    # Skip whitespace after '='
    i = eq_idx + 1
    while i < len(html) and html[i] in " \t\n\r":
        i += 1
    if i >= len(html) or html[i] != "{":
        raise ValueError(f"expected '{{' after = (got {html[i:i+10]!r})")

    # Balanced-brace walk, string-aware so braces inside string literals
    # don't count toward the depth counter.
    depth = 0
    in_string = False
    escape = False
    start = i
    for j in range(i, len(html)):
        ch = html[j]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                blob = html[start:j + 1]
                try:
                    return json.loads(blob)
                except json.JSONDecodeError as e:
                    raise ValueError(f"JSON parse failed: {e}") from e
    raise ValueError("unterminated JSON — never reached depth 0")


# ============================================================
# VARIANT MATCHING
# ============================================================

_paren_re = re.compile(r"\([^)]*\)")
_ws_re = re.compile(r"\s+")


def _normalize_variant_name(name: str) -> str:
    """Strip fuel/transmission/decorative tokens to get just the trim name.

    Examples:
        "VXi Petrol Manual"             -> "vxi"
        "VXi (O) Petrol Automatic"      -> "vxi"
        "ZXi Plus Petrol Manual Dual Tone" -> "zxi plus"
        "S(O) Diesel AMT"               -> "s"

    The cleaned form is what we compare against the user's input."""
    s = (name or "").lower().strip()
    # Drop parenthetical groups e.g. "(O)", "(AMT)", "(MT)"
    s = _paren_re.sub(" ", s)
    # Replace each known noise token with a space (whole-substring replace
    # is fine here — none of the tokens appear inside trim names like
    # "ZXi Plus" or "Sportz")
    for tok in VARIANT_NOISE_TOKENS:
        s = s.replace(tok, " ")
    # Collapse whitespace
    s = _ws_re.sub(" ", s).strip()
    # Trim hyphens / dashes left over from removals
    s = s.strip("- ")
    return s


def _get_fuel_from_version(v: Dict[str, Any]) -> str:
    """Extract human-readable fuel type from a version dict's specsSummary
    (which is more reliable than fuelTypeId — that field is sometimes 0
    even when fuel is set, as we observed on the Swift page)."""
    for spec in v.get("specsSummary") or []:
        if spec.get("itemName") == "Fuel Type":
            return (spec.get("value") or "").strip()
    # Fallback: parse from versionName ("VXi Petrol Manual" -> "Petrol")
    name = (v.get("versionName") or "").lower()
    for fuel in ("petrol", "diesel", "cng", "electric", "hybrid"):
        if fuel in name:
            return fuel.capitalize()
    return ""


def _parse_variants(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull a clean variant list from the parsed __INITIAL_STATE__ dict.
    Each item: {versionName, displayName, maskingName, fuel,
                 ex_showroom, on_road}.

    v2.6: CarWale's priceOverview now puts the actual ex-showroom price
    in the field 'price'. The field still named 'exShowRoomPrice' is 0
    in the JSON we get from server-side scrapers (it appears to only
    populate in some browser-served variants). 'onRoadPrice' is also
    0 server-side, so we don't expose on-road in the output anymore —
    only ex-showroom, which is what AutoKnowMus consumes anyway.
    Tries 'price' first, falls back to 'exShowRoomPrice' for safety.
    """
    versions = (state.get("modelPage") or {}).get("versions") or []
    out: List[Dict[str, Any]] = []
    for v in versions:
        po = v.get("priceOverview") or {}
        # v2.6: 'price' is the field with the real number; 'exShowRoomPrice'
        # is a legacy field that's now 0. Try 'price' first.
        ex = po.get("price") or po.get("exShowRoomPrice") or 0
        if not isinstance(ex, (int, float)) or ex <= 0:
            # Skip variants with no usable price (e.g. discontinued, "coming soon")
            continue
        out.append({
            "versionName": (v.get("versionName") or "").strip(),
            "displayName": (v.get("displayName") or "").strip(),
            "maskingName": (v.get("versionMaskingName") or "").strip(),
            "fuel": _get_fuel_from_version(v),
            "ex_showroom": int(ex),
            "on_road": int(po.get("onRoadPrice") or 0),
        })
    return out


def _match_variant(
    variants: List[Dict[str, Any]],
    user_variant: str,
    user_fuel: str,
) -> Tuple[str, Optional[Dict[str, Any]], List[str]]:
    """Find the user's variant in the parsed variant list.

    Strategy:
        1. Normalize user_variant ("VXI" -> "vxi", "ZXi Plus" -> "zxi plus").
        2. Filter variants by fuel match (case-insensitive).
        3. Find variants with the SAME normalized name. Cheapest wins
           (the user's "VXI Petrol" is the base VXI Petrol — they didn't
           ask for the AMT or the Optional pack).
        4. If no exact match, try prefix match.
        5. Token-overlap fallback.
        6. Return (status, best_variant_dict_or_None, candidates_list).

    Returns:
        ('found', dict, [name])               — exactly 1 candidate
        ('found_multiple', dict, [names])     — multiple matches, picked cheapest
        ('found_fuzzy', dict, [names])        — prefix/token fallback fired
        ('not_found_fuel', None, [all names]) — fuel filter rejected everything
        ('not_found', None, [fuel-matched names]) — fuel ok, name didn't match
    """
    user_norm = _normalize_variant_name(user_variant)
    if not user_norm:
        return ("not_found", None, [v["versionName"] for v in variants])

    # 2. Filter by fuel
    fuel_norm = (user_fuel or "").strip().lower()
    if fuel_norm:
        fuel_filtered = [v for v in variants if v["fuel"].lower() == fuel_norm]
        if not fuel_filtered:
            return ("not_found_fuel", None, [v["versionName"] for v in variants])
    else:
        fuel_filtered = list(variants)

    # 3. Exact match on normalized name
    exact = [v for v in fuel_filtered if _normalize_variant_name(v["versionName"]) == user_norm]
    if exact:
        best = min(exact, key=lambda v: v["ex_showroom"])
        status = "found" if len(exact) == 1 else "found_multiple"
        return (status, best, [v["versionName"] for v in exact])

    # 4. Prefix fallback
    starts = [v for v in fuel_filtered if _normalize_variant_name(v["versionName"]).startswith(user_norm)]
    if starts:
        best = min(starts, key=lambda v: v["ex_showroom"])
        return ("found_fuzzy", best, [v["versionName"] for v in starts])

    # 5. Token-overlap fallback
    user_tokens = set(user_norm.split())
    if user_tokens:
        token_match = []
        for v in fuel_filtered:
            v_tokens = set(_normalize_variant_name(v["versionName"]).split())
            if user_tokens & v_tokens:
                token_match.append(v)
        if token_match:
            best = min(token_match, key=lambda v: v["ex_showroom"])
            return ("found_fuzzy", best, [v["versionName"] for v in token_match])

    return ("not_found", None, [v["versionName"] for v in fuel_filtered])


# ============================================================
# PUBLIC API
# ============================================================

def fetch_price(
    make: str,
    model: str,
    variant: str,
    fuel: str,
) -> Dict[str, Any]:
    """Scrape the ex-showroom price for a single variant from CarWale.

    All output statuses (see module docstring) are reported as JSON-safe
    dicts. Never raises — every failure mode is encoded in the dict.

    Args:
        make: e.g. "Maruti Suzuki"
        model: e.g. "Swift"
        variant: e.g. "VXI" or "ZXi Plus"
        fuel: e.g. "Petrol" / "Diesel" / "CNG" / "Electric"

    Returns:
        Dict matching the public API shape in the module docstring.
    """
    started_at = datetime.now(timezone.utc).isoformat()

    result: Dict[str, Any] = {
        "ok": False,
        "status": "error",
        "make": make,
        "model": model,
        "variant": variant,
        "fuel": fuel,
        "ex_showroom_inr": None,
        "matched_variant": None,
        "url": None,
        "scraped_at": started_at,
        "candidates_considered": 0,
        "all_variants": None,
        "error": None,
    }

    # 1. Slug lookup
    try:
        slug = _get_slug(make, model)
    except Exception as e:
        result["error"] = f"slug_lookup_failed: {e}"
        return result
    if not slug:
        result["status"] = "no_slug"
        result["error"] = (
            f"No carwale_slug found in model_slugs tab for "
            f"({make!r}, {model!r}). Populate the row or run populate_slugs.py."
        )
        return result

    # 2. Build URL — v2.1: routed through _slug_to_url_path() so
    # 'maruti-suzuki/swift' becomes '.../maruti-suzuki-cars/swift/'.
    url = _build_url(slug)
    result["url"] = url

    # 3. Fetch HTML
    try:
        html = _http_get(url)
    except FetchError as e:
        result["status"] = "error"
        result["error"] = f"fetch_failed: {e}"
        return result
    except Exception as e:
        result["status"] = "error"
        result["error"] = f"fetch_exception: {e.__class__.__name__}: {e}"
        return result

    # 4. Extract __INITIAL_STATE__ JSON
    try:
        state = _extract_initial_state(html)
    except ValueError as e:
        result["status"] = "error"
        result["error"] = (
            f"Could not extract __INITIAL_STATE__ from {url} ({e}). "
            "CarWale layout may have changed; the parser needs updating."
        )
        return result

    # 5. Parse variant list
    variants = _parse_variants(state)
    if not variants:
        try:
            top_keys = sorted(state.keys()) if isinstance(state, dict) else []
            mp = state.get("modelPage") if isinstance(state, dict) else None
            mp_keys = sorted(mp.keys()) if isinstance(mp, dict) else []
            mp_versions = mp.get("versions") if isinstance(mp, dict) else None
            mp_versions_type = type(mp_versions).__name__
            mp_versions_len = len(mp_versions or []) if isinstance(mp_versions, list) else 0
            # v2.4 diag: dump first version's keys + sample fields so we can see
            # the actual schema (price fields might be renamed or differently nested).
            first_v_keys = []
            first_v_sample = {}
            if isinstance(mp_versions, list) and mp_versions:
                v0 = mp_versions[0]
                if isinstance(v0, dict):
                    first_v_keys = sorted(v0.keys())
                    # Sample a few likely-relevant fields
                    for k in ("versionName", "displayName", "versionMaskingName",
                              "priceOverview", "price", "exShowRoomPrice",
                              "onRoadPrice", "priceData", "fuelType",
                              "fuelTypeId", "specsSummary"):
                        if k in v0:
                            v_val = v0[k]
                            # v2.5: For priceOverview specifically, dump the FULL
                            # contents (not just keys) so we can see if exShowRoomPrice
                            # is actually a number or "Available on request" / 0 / None.
                            if k == "priceOverview" and isinstance(v_val, dict):
                                first_v_sample[k] = dict(v_val)  # full copy
                            elif isinstance(v_val, dict):
                                first_v_sample[k] = {"_keys": sorted(v_val.keys())[:15]}
                            elif isinstance(v_val, list):
                                first_v_sample[k] = {"_listlen": len(v_val),
                                                     "_first": v_val[0] if v_val else None}
                            else:
                                first_v_sample[k] = v_val
                # Also sample the SECOND variant's priceOverview to confirm the
                # pattern — first variant might be a "default" with no price set.
                second_v_priceOverview = None
                if isinstance(mp_versions, list) and len(mp_versions) >= 2:
                    v1 = mp_versions[1]
                    if isinstance(v1, dict):
                        po1 = v1.get("priceOverview")
                        if isinstance(po1, dict):
                            second_v_priceOverview = {
                                "versionName": v1.get("versionName"),
                                "priceOverview": dict(po1),
                            }
            logger.warning(
                "[parse-fail] url=%s | top_keys=%s | modelPage.keys=%s | "
                "versions type=%s len=%d | first_version_keys=%s | "
                "first_version_sample=%s | second_version_priceOverview=%s",
                url, top_keys[:30], mp_keys[:30], mp_versions_type,
                mp_versions_len, first_v_keys, first_v_sample,
                second_v_priceOverview
            )
        except Exception as _diag_e:
            logger.warning("[parse-fail] diag log failed: %s", _diag_e)
        result["status"] = "error"
        result["error"] = (
            f"Parsed 0 variants from {url}. The page may be a discontinued-model "
            "stub, a redirect, or CarWale layout may have changed."
        )
        return result

    # 6. Match user's request
    status, best, candidates = _match_variant(variants, variant, fuel)
    result["all_variants"] = [v["versionName"] for v in variants]
    result["candidates_considered"] = len(candidates)

    if best is None:
        result["status"] = status
        if status == "not_found_fuel":
            result["error"] = (
                f"Fuel {fuel!r} not available for {make} {model}. "
                f"Available fuels: {sorted({v['fuel'] for v in variants if v['fuel']})}"
            )
        else:
            result["error"] = (
                f"Variant {variant!r} (fuel {fuel!r}) not found among "
                f"{len(candidates)} fuel-matched variants on {url}."
            )
        return result

    # 7. Found a match
    result["ok"] = True
    result["status"] = status
    result["ex_showroom_inr"] = best["ex_showroom"]
    result["matched_variant"] = best["versionName"]
    return result


# ============================================================
# DIAGNOSTIC HELPERS
# ============================================================

def list_variants(make: str, model: str) -> Dict[str, Any]:
    """List all variants of a model from CarWale. Useful for debugging
    when fetch_price returns 'not_found' — caller can see what's actually
    on the page."""
    started_at = datetime.now(timezone.utc).isoformat()
    result: Dict[str, Any] = {
        "ok": False,
        "make": make,
        "model": model,
        "url": None,
        "variants": None,
        "scraped_at": started_at,
        "error": None,
    }
    slug = _get_slug(make, model)
    if not slug:
        result["error"] = f"no slug for ({make!r}, {model!r})"
        return result
    # v2.1: same URL builder as fetch_price so both paths stay in lockstep.
    url = _build_url(slug)
    result["url"] = url
    try:
        html = _http_get(url)
        state = _extract_initial_state(html)
        variants = _parse_variants(state)
    except Exception as e:
        result["error"] = f"{e.__class__.__name__}: {e}"
        return result
    result["ok"] = True
    result["variants"] = [
        {
            "versionName": v["versionName"],
            "fuel": v["fuel"],
            "ex_showroom_inr": v["ex_showroom"],
            "on_road_inr": v["on_road"],
        }
        for v in variants
    ]
    return result


# ============================================================
# Module-level smoke test
# ============================================================

if __name__ == "__main__":
    import pprint
    logging.basicConfig(level=logging.INFO)
    print("=" * 60)
    print("price_scraper.py v2.1 smoke test")
    print("=" * 60)

    # Quick unit-style check on _slug_to_url_path before hitting the network
    print("\n--- _slug_to_url_path unit checks ---")
    cases = [
        ("maruti-suzuki/swift",       "maruti-suzuki-cars/swift"),
        ("hyundai/creta",              "hyundai-cars/creta"),
        ("land-rover/freelander-2",    "land-rover-cars/freelander-2"),
        ("toyota/landcruiserprado",    "toyota-cars/landcruiserprado"),
        ("mercedes-benz/c-class",      "mercedes-benz-cars/c-class"),
        # Already has -cars/ → leave alone
        ("bmw-cars/x1",                "bmw-cars/x1"),
        # No slash → fallback
        ("audi",                       "audi-cars"),
        # Empty
        ("",                           ""),
    ]
    for inp, expected in cases:
        got = _slug_to_url_path(inp)
        ok = "✓" if got == expected else "✗"
        print(f"  {ok} {inp!r:35s} -> {got!r:40s} (expected {expected!r})")

    test_cases = [
        ("Maruti Suzuki", "Swift", "VXI", "Petrol"),
        ("Maruti Suzuki", "Swift", "LXI", "Petrol"),
        ("Maruti Suzuki", "Swift", "VXI", "CNG"),
        ("Hyundai", "Creta", "E", "Petrol"),
    ]
    for make, model, variant, fuel in test_cases:
        print(f"\n--- {make} {model} {variant} {fuel} ---")
        r = fetch_price(make, model, variant, fuel)
        if r.get("all_variants") and len(r["all_variants"]) > 5:
            r["all_variants"] = r["all_variants"][:5] + [f"... +{len(r['all_variants']) - 5} more"]
        pprint.pprint(r)
