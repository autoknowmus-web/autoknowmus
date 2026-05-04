"""
price_scraper.py — CarWale ex-showroom price scraper for AutoKnowMus
=====================================================================

VERSION: v2.9.1 (04-May-2026)

CHANGELOG:
    v2.9 — Per-variant URL fetching.
        CarWale's model overview page (e.g. /maruti-suzuki-cars/ertiga/)
        used to expose all variant prices in __INITIAL_STATE__. As of
        04-May-2026 those prices come back as 0 server-side; only the
        per-variant pages (/maruti-suzuki-cars/ertiga/lxi/) populate the
        actual price.
        Diagnosed via /admin/diag-scraper-fetch + needle counts which
        showed Content-Encoding=gzip, body fully decompressed, but
        priceOverview.price=0 and exShowRoomPrice=0 across all versions.
        v2.9 strategy:
          1. Fetch model overview page → extract variant index
             (list of {name, url, fuel, masking}) using either JSON
             (if __INITIAL_STATE__.modelPage.versions populated) or HTML
             regex over href patterns as fallback.
          2. Match user's requested variant against that index by
             normalized name + fuel.
          3. Fetch the matched variant's specific URL → extract its
             ex-showroom price using JSON (modelPage.version /
             versionPage / single-element versions list) or HTML regex
             over "Ex-Showroom Price ... Rs. X,XX,XXX" as fallback.
        Cost: 2 HTTP requests per fetch_price() instead of 1. With
        rate-limit at 2s/request that doubles per-variant scrape time
        but produces accurate prices for current CarWale layout.
        Old _parse_variants() and the in-fetch_price() match path are
        retained as a legacy fallback when the variant index returns
        non-empty AND each variant in it already has a price (older
        layout still serving prices in modelPage.versions).

    v2.8.1 — Diagnostic. Log Content-Encoding + body sample on every
        successful fetch so we can see what CarWale actually sends.
        This is what surfaced the per-variant-page issue: the model
        overview page came back as gzip-decoded HTML cleanly, with
        __INITIAL_STATE__ present, but priceOverview values were 0.

    v2.8 — Brotli decompression on our side. Kept as a safety net even
        though current edges return gzip, because CarWale's CloudFront
        decision is per-edge and may switch back. Requires `Brotli` in
        requirements.txt.

    v2.7 — Decline Brotli via Accept-Encoding. Made some edges fall
        back to gzip (which `requests` auto-decompresses). Did not
        cover all edges, so v2.8 added explicit Brotli decode.

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

PUBLIC API SHAPE (unchanged across versions — app.py's
/admin/test-scraper and /admin/price-tools/* routes do NOT need any
changes):

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
      "url": str | None,                     # CarWale URL we hit (the variant page)
      "scraped_at": ISO timestamp,
      "candidates_considered": int,          # how many variants we matched
      "all_variants": list[str] | None,      # for debug — full list when matching
      "error": str | None,                   # human-readable error
    }

DEPENDENCIES:
    - requests (in requirements.txt)
    - Brotli (in requirements.txt as of v2.8)
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
    # makes the response decompressable by the stdlib path. v2.8 also has a
    # Brotli decode safety net for edges that ignore this preference.
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
    convention adds '-cars' to the make portion.
    """
    s = (slug or "").strip().strip("/")
    if not s:
        return ""
    if "-cars/" in s:
        return s
    if "/" not in s:
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
        encoding = (resp.headers.get("Content-Encoding") or "").lower()
        logger.info("[%s] HTTP %d, Content-Encoding=%r, body_len=%d, first_bytes=%r",
                    url, resp.status_code, encoding,
                    len(resp.content), resp.content[:50])
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
    end. Raises ValueError on any failure."""
    idx = html.find(INITIAL_STATE_NEEDLE)
    if idx < 0:
        raise ValueError(
            f"'{INITIAL_STATE_NEEDLE}' not found in HTML "
            f"(page may be a redirect, error page, or layout changed)"
        )
    eq_idx = html.find("=", idx)
    if eq_idx < 0:
        raise ValueError(f"no '=' after {INITIAL_STATE_NEEDLE}")
    i = eq_idx + 1
    while i < len(html) and html[i] in " \t\n\r":
        i += 1
    if i >= len(html) or html[i] != "{":
        raise ValueError(f"expected '{{' after = (got {html[i:i+10]!r})")

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
# VARIANT NAME NORMALIZATION + FUEL EXTRACTION
# ============================================================

_paren_re = re.compile(r"\([^)]*\)")
_ws_re = re.compile(r"\s+")


def _normalize_variant_name(name: str) -> str:
    """Strip fuel/transmission/decorative tokens to get just the trim name."""
    s = (name or "").lower().strip()
    s = _paren_re.sub(" ", s)
    for tok in VARIANT_NOISE_TOKENS:
        s = s.replace(tok, " ")
    s = _ws_re.sub(" ", s).strip()
    s = s.strip("- ")
    return s


def _get_fuel_from_version(v: Dict[str, Any]) -> str:
    """Extract human-readable fuel type from a version dict's specsSummary
    (more reliable than fuelTypeId — that field is sometimes 0 even when
    fuel is set, as we observed on the Swift page)."""
    for spec in v.get("specsSummary") or []:
        if spec.get("itemName") == "Fuel Type":
            return (spec.get("value") or "").strip()
    name = (v.get("versionName") or "").lower()
    for fuel in ("petrol", "diesel", "cng", "electric", "hybrid"):
        if fuel in name:
            return fuel.capitalize()
    return ""


# ============================================================
# v2.9: Per-variant URL discovery
#
# CarWale's model overview page (e.g. /maruti-suzuki-cars/ertiga/) used to
# include the full variant list with prices in __INITIAL_STATE__. As of
# 04-May-2026 the prices on that page are no longer populated server-side
# (they appear only on per-variant pages like /maruti-suzuki-cars/ertiga/lxi/).
#
# v2.9 strategy:
#   1. Fetch the model overview page → extract list of {name, url, fuel,
#      masking} for each variant via _extract_variant_index().
#   2. Match user's requested variant against that list (by name + fuel).
#   3. Fetch the specific matched variant URL → extract ex-showroom price
#      via _fetch_single_variant_price().
#
# Two HTTP requests per fetch_price() instead of one. Rate-limited as usual.
# ============================================================

def _build_variant_href_regex(slug: str) -> Optional[re.Pattern]:
    """Compile a regex that matches `href="/{make-cars}/{model}/{variant-slug}/"`
    given a sheet slug like 'maruti-suzuki/ertiga'. Returns None on bad input."""
    if not slug or "/" not in slug:
        return None
    make_part, _, model_part = slug.strip("/").partition("/")
    if not make_part or not model_part:
        return None
    make_re = re.escape(make_part)
    model_re = re.escape(model_part)
    pattern = (
        r'href="(/' + make_re + r'-cars/' + model_re +
        r'/([a-z0-9][a-z0-9\-]*)/?)"'
    )
    return re.compile(pattern, re.IGNORECASE)


def _extract_variant_index(html: str, slug: str) -> List[Dict[str, str]]:
    """Extract the list of variants from a model overview page response.

    Returns: [{"name": "LXi", "url": "https://.../lxi/", "fuel": "Petrol",
               "masking": "lxi"}, ...]

    Tries two strategies:
      1. JSON: parse __INITIAL_STATE__, look for modelPage.versions with
         versionMaskingName / versionName fields.
      2. HTML regex fallback: scan for href links matching the variant URL
         pattern.
    """
    out: List[Dict[str, str]] = []

    # Strategy 1 — JSON path.
    try:
        state = _extract_initial_state(html)
        versions = (state.get("modelPage") or {}).get("versions") or []
        for v in versions:
            if not isinstance(v, dict):
                continue
            name = (v.get("versionName") or v.get("displayName") or "").strip()
            masking = (v.get("versionMaskingName") or "").strip()
            if masking and slug:
                variant_url = f"{CARWALE_BASE}/{_slug_to_url_path(slug)}/{masking}/"
            else:
                variant_url = ""
            fuel = _get_fuel_from_version(v)
            if name:
                out.append({
                    "name": name,
                    "url": variant_url,
                    "fuel": fuel,
                    "masking": masking,
                })
        if out:
            logger.info("variant_index: JSON path found %d variants", len(out))
            return out
    except Exception as e:
        logger.info("variant_index: JSON path failed (%s), trying HTML regex", e)

    # Strategy 2 — HTML regex fallback.
    pattern = _build_variant_href_regex(slug)
    if not pattern:
        logger.warning("variant_index: bad slug %r, can't build regex", slug)
        return out
    seen_paths = set()
    SKIP = {"photos", "specifications", "specs", "colours", "colors",
            "user-reviews", "reviews", "news", "videos", "compare",
            "offers", "ex-showroom-price", "on-road-price", "mileage",
            "interior", "exterior", "features", "variants"}
    for match in pattern.finditer(html):
        path = match.group(1)
        masking = match.group(2)
        if path in seen_paths:
            continue
        if masking.lower() in SKIP:
            continue
        seen_paths.add(path)
        display_name = masking.replace("-", " ").title()
        out.append({
            "name": display_name,
            "url": f"{CARWALE_BASE}{path}",
            "fuel": "",   # unknown until we fetch the variant page
            "masking": masking,
        })
    logger.info("variant_index: HTML regex path found %d variants from %d hrefs",
                len(out), len(seen_paths))
    return out


def _fetch_single_variant_price(variant_url: str) -> Dict[str, Any]:
    """Fetch a single variant page and extract its ex-showroom price + canonical name.

    Returns a dict:
        {
          "ok": True/False,
          "price": int | None,
          "version_name": str | None,
          "fuel": str | None,
          "url": str,
          "error": str | None,
        }

    Tries JSON first (modelPage.version / versionPage / single-element
    versions list) then HTML regex over "Ex-Showroom Price ... Rs. X,XX,XXX".
    Never raises.
    """
    out: Dict[str, Any] = {
        "ok": False,
        "price": None,
        "version_name": None,
        "fuel": None,
        "url": variant_url,
        "error": None,
    }
    if not variant_url:
        out["error"] = "empty variant_url"
        return out

    try:
        html = _http_get(variant_url)
    except FetchError as e:
        out["error"] = f"fetch_failed: {e}"
        return out
    except Exception as e:
        out["error"] = f"fetch_exception: {e.__class__.__name__}: {e}"
        return out

    # v2.9.1 diag: count key needles + dump a window around "Ex-Showroom"
    # so we can see what the variant page actually contains. Once the
    # extraction is fixed, this block can be removed.
    try:
        needles = {
            "__INITIAL_STATE__": html.count("__INITIAL_STATE__"),
            "Ex-Showroom": html.count("Ex-Showroom"),
            "ex-showroom": html.count("ex-showroom"),
            "exShowRoomPrice": html.count("exShowRoomPrice"),
            "priceOverview": html.count("priceOverview"),
            "versionPage": html.count("versionPage"),
            "modelPage": html.count("modelPage"),
            "version\":": html.count("version\":"),
            "Lakh": html.count("Lakh"),
            "Rs.": html.count("Rs."),
        }
        idx = html.find("Ex-Showroom")
        if idx < 0:
            idx = html.find("ex-showroom")
        snippet = ""
        if idx >= 0:
            snippet = html[max(0, idx - 50):idx + 250]
        logger.info("[%s] variant_page needles=%s, snippet=%r",
                    variant_url, needles, snippet)
    except Exception as _diag_e:
        logger.warning("[%s] variant page diag failed: %s", variant_url, _diag_e)

    # Strategy 1 — JSON path
    try:
        state = _extract_initial_state(html)
        version_data = None
        if isinstance(state, dict):
            mp = state.get("modelPage") or {}
            v = mp.get("version") if isinstance(mp, dict) else None
            if isinstance(v, dict):
                version_data = v
            elif isinstance(state.get("versionPage"), dict):
                version_data = state["versionPage"]
            elif isinstance(mp.get("versions"), list) and len(mp["versions"]) == 1:
                version_data = mp["versions"][0]

        if isinstance(version_data, dict):
            po = version_data.get("priceOverview") or {}
            price = po.get("price") or po.get("exShowRoomPrice") or 0
            if isinstance(price, (int, float)) and price > 0:
                out["ok"] = True
                out["price"] = int(price)
                out["version_name"] = (version_data.get("versionName") or
                                       version_data.get("displayName") or "").strip() or None
                out["fuel"] = _get_fuel_from_version(version_data) or None
                return out

        logger.info("variant_price: JSON path didn't find price, trying regex on %s", variant_url)
    except Exception as e:
        logger.info("variant_price: JSON path failed (%s) on %s, trying regex", e, variant_url)

    # Strategy 2 — HTML regex fallback.
    try:
        pattern = re.compile(
            r'Ex[\s\-]?Showroom\s*Price[^0-9]{0,200}?(?:Rs\.|₹|INR)\s*([0-9][0-9,]*)',
            re.IGNORECASE | re.DOTALL,
        )
        match = pattern.search(html)
        if match:
            price_str = match.group(1).replace(",", "").strip()
            price_int = int(price_str) if price_str.isdigit() else 0
            if price_int > 0:
                out["ok"] = True
                out["price"] = price_int
                return out
        out["error"] = "no_price_found"
    except Exception as e:
        out["error"] = f"regex_fallback_failed: {e.__class__.__name__}: {e}"
    return out


# ============================================================
# LEGACY HELPERS — _parse_variants kept for list_variants() debug helper.
# Not used by fetch_price() in v2.9 because CarWale no longer populates
# prices on the model overview page.
# ============================================================

def _parse_variants(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Legacy: pull a clean variant list from a parsed __INITIAL_STATE__ dict.
    Each item: {versionName, displayName, maskingName, fuel, ex_showroom, on_road}.
    Skips any variant where price is missing or 0."""
    versions = (state.get("modelPage") or {}).get("versions") or []
    out: List[Dict[str, Any]] = []
    for v in versions:
        po = v.get("priceOverview") or {}
        ex = po.get("price") or po.get("exShowRoomPrice") or 0
        if not isinstance(ex, (int, float)) or ex <= 0:
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


# ============================================================
# VARIANT MATCHING (used against the variant-index, not legacy parse)
# ============================================================

def _match_variant_index(
    variants: List[Dict[str, str]],
    user_variant: str,
    user_fuel: str,
) -> Tuple[str, Optional[Dict[str, str]], List[str]]:
    """Find the user's variant in the variant-index list.

    Each item in `variants` is from _extract_variant_index() and has
    keys: name, url, fuel, masking.

    Strategy:
        1. Filter by fuel if known (JSON path provides fuel; regex path
           leaves it empty so we skip the filter for those).
        2. Exact match on normalized name → return cheapest URL alphabetically
           if multiple match (we don't have prices yet at this stage).
        3. Prefix fallback.
        4. Token-overlap fallback.
        5. Match against masking slug as a last resort (helpful when name
           was synthesized from URL like "Lxi" vs user typed "LXi").

    Returns (status, best_variant_dict_or_None, candidates_names_list).
    """
    user_norm = _normalize_variant_name(user_variant)
    if not user_norm:
        return ("not_found", None, [v.get("name", "") for v in variants])

    fuel_norm = (user_fuel or "").strip().lower()
    # Only filter by fuel if BOTH user_fuel is provided AND at least one
    # variant in the index has a non-empty fuel. The HTML-regex path leaves
    # fuel="" so filtering would empty the list.
    any_have_fuel = any(v.get("fuel") for v in variants)
    if fuel_norm and any_have_fuel:
        fuel_filtered = [v for v in variants if (v.get("fuel") or "").lower() == fuel_norm]
        if not fuel_filtered:
            return ("not_found_fuel", None, [v.get("name", "") for v in variants])
    else:
        fuel_filtered = list(variants)

    # 1. Exact match on normalized name
    exact = [v for v in fuel_filtered
             if _normalize_variant_name(v.get("name", "")) == user_norm]
    if exact:
        # Stable choice when multiple match: alphabetical by masking
        exact.sort(key=lambda v: v.get("masking", ""))
        best = exact[0]
        status = "found" if len(exact) == 1 else "found_multiple"
        return (status, best, [v.get("name", "") for v in exact])

    # 2. Prefix fallback
    starts = [v for v in fuel_filtered
              if _normalize_variant_name(v.get("name", "")).startswith(user_norm)]
    if starts:
        starts.sort(key=lambda v: v.get("masking", ""))
        return ("found_fuzzy", starts[0], [v.get("name", "") for v in starts])

    # 3. Token-overlap fallback
    user_tokens = set(user_norm.split())
    if user_tokens:
        token_match = []
        for v in fuel_filtered:
            v_tokens = set(_normalize_variant_name(v.get("name", "")).split())
            if user_tokens & v_tokens:
                token_match.append(v)
        if token_match:
            token_match.sort(key=lambda v: v.get("masking", ""))
            return ("found_fuzzy", token_match[0], [v.get("name", "") for v in token_match])

    # 4. Match against masking slug (handles HTML-regex path where name is
    # synthesized from URL). user "LXi" -> normalized "lxi" -> match masking="lxi".
    slug_match = [v for v in fuel_filtered
                  if (v.get("masking") or "").lower() == user_norm.replace(" ", "-")]
    if slug_match:
        return ("found_fuzzy", slug_match[0], [v.get("name", "") for v in slug_match])

    return ("not_found", None, [v.get("name", "") for v in fuel_filtered])


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

    v2.9 flow:
      1. Slug lookup
      2. Fetch model overview page
      3. Extract variant index from overview page (JSON or HTML regex)
      4. Match user's variant against the index
      5. Fetch the matched variant's specific URL
      6. Extract its ex-showroom price (JSON or HTML regex)

    Never raises — every failure mode is encoded in the return dict.
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

    # 2. Build model overview URL + fetch
    overview_url = _build_url(slug)
    # Note: result["url"] is set later to the per-variant URL we actually
    # hit for the price. Keep overview_url separate so error messages can
    # mention either as appropriate.
    try:
        overview_html = _http_get(overview_url)
    except FetchError as e:
        result["status"] = "error"
        result["url"] = overview_url
        result["error"] = f"overview_fetch_failed: {e}"
        return result
    except Exception as e:
        result["status"] = "error"
        result["url"] = overview_url
        result["error"] = f"overview_fetch_exception: {e.__class__.__name__}: {e}"
        return result

    # 3. Extract variant index from overview page
    variant_index = _extract_variant_index(overview_html, slug)
    if not variant_index:
        result["status"] = "error"
        result["url"] = overview_url
        result["error"] = (
            f"No variants discovered on {overview_url}. The page may be a "
            "redirect, a discontinued-model stub, or CarWale layout may have "
            "changed (neither __INITIAL_STATE__ nor href regex found anything)."
        )
        return result

    # 4. Match user's request against the index
    match_status, matched, candidate_names = _match_variant_index(
        variant_index, variant, fuel
    )
    result["all_variants"] = [v.get("name", "") for v in variant_index]
    result["candidates_considered"] = len(candidate_names)

    if matched is None:
        result["status"] = match_status
        result["url"] = overview_url
        if match_status == "not_found_fuel":
            available_fuels = sorted({v.get("fuel", "") for v in variant_index
                                      if v.get("fuel")})
            result["error"] = (
                f"Fuel {fuel!r} not available for {make} {model}. "
                f"Available fuels: {available_fuels}"
            )
        else:
            result["error"] = (
                f"Variant {variant!r} (fuel {fuel!r}) not found among "
                f"{len(variant_index)} variants on {overview_url}."
            )
        return result

    # 5. Fetch the matched variant's page → get the actual price
    variant_url = matched.get("url") or ""
    if not variant_url:
        result["status"] = "error"
        result["url"] = overview_url
        result["error"] = (
            f"Matched variant {matched.get('name')!r} has no URL — slug "
            "construction failed (variant index produced empty url)."
        )
        return result

    result["url"] = variant_url
    price_data = _fetch_single_variant_price(variant_url)

    if not price_data.get("ok"):
        result["status"] = "error"
        result["error"] = (
            f"Matched variant {matched.get('name')!r} but failed to extract "
            f"price from {variant_url}: {price_data.get('error', 'unknown')}"
        )
        return result

    # 6. Success
    result["ok"] = True
    result["status"] = match_status
    result["ex_showroom_inr"] = int(price_data["price"])
    # Prefer the canonical version_name from the variant page (clean, has fuel/
    # transmission suffix). Fall back to the index name if regex path didn't
    # give us one.
    result["matched_variant"] = (price_data.get("version_name")
                                 or matched.get("name")
                                 or variant)
    return result


# ============================================================
# DIAGNOSTIC HELPERS
# ============================================================

def list_variants(make: str, model: str) -> Dict[str, Any]:
    """List all variants of a model from CarWale. Useful for debugging
    when fetch_price returns 'not_found' — caller can see what's actually
    on the page.

    v2.9: uses the variant index (not _parse_variants) so it works even
    when CarWale doesn't populate prices on the overview page. Note that
    this means ex_showroom_inr / on_road_inr are NOT included here — fetch
    each variant URL individually if you need prices.
    """
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
    url = _build_url(slug)
    result["url"] = url
    try:
        html = _http_get(url)
    except Exception as e:
        result["error"] = f"{e.__class__.__name__}: {e}"
        return result
    variant_index = _extract_variant_index(html, slug)
    result["ok"] = True
    result["variants"] = [
        {
            "name": v.get("name"),
            "fuel": v.get("fuel"),
            "url": v.get("url"),
            "masking": v.get("masking"),
        }
        for v in variant_index
    ]
    return result


# ============================================================
# Module-level smoke test
# ============================================================

if __name__ == "__main__":
    import pprint
    logging.basicConfig(level=logging.INFO)
    print("=" * 60)
    print("price_scraper.py v2.9 smoke test")
    print("=" * 60)

    print("\n--- _slug_to_url_path unit checks ---")
    cases = [
        ("maruti-suzuki/swift",       "maruti-suzuki-cars/swift"),
        ("hyundai/creta",              "hyundai-cars/creta"),
        ("land-rover/freelander-2",    "land-rover-cars/freelander-2"),
        ("toyota/landcruiserprado",    "toyota-cars/landcruiserprado"),
        ("mercedes-benz/c-class",      "mercedes-benz-cars/c-class"),
        ("bmw-cars/x1",                "bmw-cars/x1"),
        ("audi",                       "audi-cars"),
        ("",                           ""),
    ]
    for inp, expected in cases:
        got = _slug_to_url_path(inp)
        ok = "✓" if got == expected else "✗"
        print(f"  {ok} {inp!r:35s} -> {got!r:40s} (expected {expected!r})")

    test_cases = [
        ("Maruti Suzuki", "Ertiga", "LXi", "Petrol"),
        ("Maruti Suzuki", "Ertiga", "VXi", "CNG"),
        ("Maruti Suzuki", "Swift", "VXI", "Petrol"),
        ("Hyundai", "Creta", "E", "Petrol"),
    ]
    for make, model, variant, fuel in test_cases:
        print(f"\n--- {make} {model} {variant} {fuel} ---")
        r = fetch_price(make, model, variant, fuel)
        if r.get("all_variants") and len(r["all_variants"]) > 5:
            r["all_variants"] = r["all_variants"][:5] + [f"... +{len(r['all_variants']) - 5} more"]
        pprint.pprint(r)
