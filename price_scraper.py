"""
price_scraper.py — CarWale ex-showroom price scraper for AutoKnowMus
=====================================================================

VERSION: v3.0.2 (05-May-2026)

CHANGELOG:
    v3.0.2 — (this version)
        - JSON path corrected. The v3.0.1 diagnostic revealed that trim
          pages use state.trimPage.otherVersions[] + state.trimPage.versionDetail,
          NOT state.modelPage.versions[] (which is the overview-page shape).
          Each entry under trimPage.otherVersions[] has a priceOverview
          dict with exShowRoomPrice in exact rupees (Bangalore-pinned via
          our city cookies). versionDetail's priceOverview omits
          exShowRoomPrice (the page that "is for" a variant only renders
          on-road price for that one variant), but otherVersions[] from
          ANY trim page covers all the others. So:
            - Walking just 2 trim pages typically harvests every variant
              with full ex-showroom data, since each trim's otherVersions[]
              contains the ~8 sibling variants.
            - The early-exit memoization in fetch_price() (v3.0)
              already handles this — once we've matched the user's
              variant, we stop.
        - Removed the v3.0.1 diagnostic block (top_keys/interesting_paths
          dump) since we now know the JSON shape. The function is
          ~50 lines shorter.
        - Added _extract_version_record() helper as a single canonical
          place to convert a CarWale version dict to our priced-variant
          shape, used by both otherVersions[] and versionDetail paths.

    v3.0.1 — Trim filter prefix matching, retry budget reduction, JSON
        diagnostic.
        - Fixed trim-slug over-collection. Bangalore-pinned overview page
          links to ~10 suburb pages (price-in-nelamangala etc) per model
          which we naively treated as trims. Added NON_TRIM_PREFIXES
          ("price-in-", "ex-showroom-price-in-", "on-road-price-in-") and
          a _looks_like_non_trim() helper that combines exact + prefix
          matching. Trim count for Ertiga should drop from 15 to 4.
        - Reduced MAX_RETRIES from 4 to 2 to fit Gunicorn's 30s worker
          timeout. With v3.0's 4 retries x ~5 trims, single fetch_price()
          calls were exceeding 30s and getting SIGKILLed.
        - Added diagnostic to _harvest_versions_from_trim_page() that
          dumps top-level keys + paths containing "version"/"price" when
          the expected JSON shape is missing. Needed because v3.0 logs
          showed "no versions[] found" on every trim page even though
          __INITIAL_STATE__.exShowRoomPrice was confirmed present
          (priceOverview/exShowRoomPrice ~44 occurrences each in needles
          dumps). The JSON path is somewhere else in the structure;
          this diag will reveal where so v3.0.2 can navigate correctly.

    v3.0 — Bangalore cookies + trim-page parsing.
        Root cause of v2.9.x failures: CarWale stopped serving per-variant
        ex-showroom prices on /{make}-cars/{model}/{variant-slug}/ URLs.
        That URL pattern now redirects to /{make}-cars/{model}/{trim}/,
        which renders ALL variants under that trim with Bangalore-pinned
        ex-showroom prices in __INITIAL_STATE__ — but ONLY when the
        request includes CarWale's city cookies.
        Investigation steps:
          1. Manually inspected page source on /ertiga/vxi/ in a browser
             logged into Bangalore. Confirmed all variants present with
             prices like "Ex-Showroom Price Rs. 11,20,300".
          2. Inspected DevTools cookies. Found three cookies that pin
             location: _CustCityIdMaster=2 (Bangalore), _CustAreaId=6149,
             _CustAreaName=Whitefield.
          3. Verified by deleting those cookies + reloading: page
             switched to "Select City" prompt and per-variant prices
             disappeared. So those cookies ARE the city-pinning mechanism.
        v3.0 strategy:
          1. Module-level requests.Session with the Bangalore cookies set.
          2. Slug -> overview URL fetch -> extract list of TRIM slugs
             (lxi, vxi, zxi, zxi-plus) by scanning href patterns.
             Trim slugs are short (no "petrol"/"manual" tokens); variant
             masking slugs (vxi-petrol-automatic) are filtered out.
          3. For each trim, fetch /{make}-cars/{model}/{trim}/ which
             returns __INITIAL_STATE__.modelPage.versions WITH Bangalore
             ex-showroom prices populated.
          4. Memoize: if the first trim page already returns prices for
             the user's variant, we don't need to fetch other trims.
          5. Match user's (variant, fuel) against the accumulated
             versions list using the same name-normalization logic.
        Public API shape unchanged. app.py needs no changes.
        Cost: 1 overview + 1-N trim fetches. Typical model has 3-5 trims;
        with rate limiter at 2s, a worst case is ~12s per fetch_price().
        First trim hit usually has the answer for the user's specific
        variant, so most calls take ~4s.

    v2.9.1 — Diagnostic needles. Removed in v3.0.

    v2.9 — Per-variant URL fetching. CarWale's variant-slug URLs now
        redirect to trim pages, which broke this strategy entirely.
        Replaced in v3.0.

    v2.8.1 — Brotli safety net. Retained in v3.0.

    v2.7 — Decline Brotli via Accept-Encoding. Retained in v3.0.

    v2.1 — URL builder fix (slug -> -cars suffix). Retained verbatim
        in v3.0; the slug convention is unchanged.

    v2.0 — __INITIAL_STATE__ JSON parser. Retained in v3.0; trim pages
        also embed __INITIAL_STATE__, just at a different path.

WHAT THIS MODULE DOES:
    Public function `fetch_price(make, model, variant, fuel)` returns
    a dict with the ex-showroom price for a single (make, model, variant,
    fuel) combo, scraped live from carwale.com pinned to Bangalore.

PUBLIC API SHAPE (unchanged across versions):

    {
      "ok": True/False,
      "status": "found" | "found_multiple" | "found_fuzzy"
                | "not_found" | "not_found_fuel" | "no_slug"
                | "error",
      "make": str,
      "model": str,
      "variant": str,
      "fuel": str,
      "ex_showroom_inr": int | None,
      "matched_variant": str | None,
      "url": str | None,
      "scraped_at": ISO timestamp,
      "candidates_considered": int,
      "all_variants": list[str] | None,
      "error": str | None,
    }

DEPENDENCIES:
    - requests (in requirements.txt)
    - Brotli (in requirements.txt)
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

from sheets_writer import read_model_slugs

# ============================================================
# CONFIG
# ============================================================

CARWALE_BASE = "https://www.carwale.com"

# Rate-limit knobs
MIN_REQ_INTERVAL_SECS = 2.0
JITTER_SECS = 0.5
MAX_RETRIES = 2  # v3.0.1: reduced from 4 to fit gunicorn 30s timeout
BACKOFF_BASE_SECS = 1.0  # 1, 2, 4, 8

# HTTP timeouts
CONNECT_TIMEOUT_SECS = 8.0
READ_TIMEOUT_SECS = 20.0

# Slug cache (in-process, single-worker — Render free tier)
SLUG_CACHE_TTL_SECS = 3600  # 1 hour

# Bangalore-pinning cookies. Discovered by inspecting a live CarWale
# session in Chrome DevTools. CarWale uses these to pick which city's
# prices to render server-side. Without them the scraper would render
# whatever city CarWale geo-detects from Render's server IP (likely
# Mumbai) and we'd violate the "ex-showroom Bangalore" baseline rule.
CITY_COOKIES = {
    "_CustCityIdMaster": "2",       # 2 = Bangalore
    "_CustAreaId": "6149",          # 6149 = Whitefield (any Bangalore area works)
    "_CustAreaName": "Whitefield",
}

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

# Tokens that, when present in a URL slug, indicate the slug is a
# variant masking name (e.g. "vxi-petrol-automatic") rather than a
# trim slug (e.g. "vxi"). Used by _extract_trim_slugs.
VARIANT_MASKING_TOKENS = (
    "petrol", "diesel", "cng", "electric", "hybrid",
    "phev", "hev", "bev",
    "manual", "automatic", "amt", "cvt", "ivt", "dct",
)

# Path segments that aren't trim slugs at all (sub-pages of the model).
# Exact-match list for the common ones. Prefix-match logic in
# _looks_like_non_trim() catches the rest (e.g. price-in-{any-city}/{suburb}).
NON_TRIM_PATHS = {
    "photos", "images", "specifications", "specs", "colours", "colors",
    "user-reviews", "reviews", "expert-reviews", "news", "videos",
    "compare", "offers", "ex-showroom-price", "on-road-price", "mileage",
    "interior", "exterior", "features", "variants", "brochure",
    "service-cost", "service-costs", "dealer-showrooms", "dealers",
    "owner-reviews", "ratings", "specs-features", "fuel-types",
    "transmissions", "comparison", "alternatives", "competitors",
    "user-rating", "videos-reviews",
}

# Slug prefixes that always indicate a non-trim sub-page. CarWale uses
# /price-in-{any-city}/ for hundreds of city/suburb pages — impossible to
# enumerate exactly. Prefix-match catches them all.
NON_TRIM_PREFIXES = (
    "price-in-",
    "ex-showroom-price-in-",
    "on-road-price-in-",
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
# HTTP SESSION (singleton, with Bangalore cookies pre-set)
# ============================================================

_session_lock = threading.Lock()
_session: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    """Return the module-level requests.Session, creating it on first call.
    The session has the Bangalore-pinning cookies pre-set so every fetch
    via this session will be served Bangalore prices by CarWale."""
    global _session
    with _session_lock:
        if _session is None:
            s = requests.Session()
            s.headers.update(DEFAULT_HEADERS)
            for name, value in CITY_COOKIES.items():
                s.cookies.set(name, value, domain=".carwale.com", path="/")
            _session = s
            logger.info("price_scraper session created with Bangalore cookies "
                        "(_CustCityIdMaster=%s)", CITY_COOKIES["_CustCityIdMaster"])
        return _session


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
# SLUG -> URL PATH (v2.1 helper, unchanged)
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
    """Public URL for the model overview page."""
    return f"{CARWALE_BASE}/{_slug_to_url_path(slug)}/"


def _build_trim_url(slug: str, trim: str) -> str:
    """Public URL for a trim page, e.g. /maruti-suzuki-cars/ertiga/vxi/."""
    return f"{CARWALE_BASE}/{_slug_to_url_path(slug)}/{trim}/"


# ============================================================
# HTTP FETCH WITH RETRY
# ============================================================

class FetchError(Exception):
    """Raised when we can't fetch a page after retries."""


def _http_get(url: str) -> str:
    """GET a URL via the module session (Bangalore-pinned), with rate
    limiting and retries on 429/503. Returns the HTML body as a str.
    Raises FetchError on terminal failure."""
    session = _get_session()
    last_err: Optional[str] = None
    for attempt in range(MAX_RETRIES):
        _wait_for_rate_limit()
        try:
            resp = session.get(
                url,
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
    """Extract human-readable fuel type from a version dict's specsSummary."""
    for spec in v.get("specsSummary") or []:
        if spec.get("itemName") == "Fuel Type":
            return (spec.get("value") or "").strip()
    name = (v.get("versionName") or "").lower()
    for fuel in ("petrol", "diesel", "cng", "electric", "hybrid"):
        if fuel in name:
            return fuel.capitalize()
    return ""


# ============================================================
# v3.0: TRIM SLUG DISCOVERY FROM MODEL OVERVIEW PAGE
# ============================================================

def _build_trim_href_regex(slug: str) -> Optional[re.Pattern]:
    """Compile a regex that matches `href="/{make-cars}/{model}/{trim-slug}/"`
    given a sheet slug. Returns None on bad input."""
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


def _looks_like_variant_masking(candidate: str) -> bool:
    """Return True if the slug looks like a variant masking name
    (contains fuel/transmission tokens), False if it's a trim slug.

    Examples:
      "lxi"                  -> False (trim)
      "vxi"                  -> False (trim)
      "zxi-plus"             -> False (trim)
      "vxi-petrol-automatic" -> True  (variant masking)
      "lxi-cng-manual"       -> True  (variant masking)
    """
    parts = candidate.split("-")
    for part in parts:
        if part in VARIANT_MASKING_TOKENS:
            return True
    return False


def _looks_like_non_trim(candidate: str) -> bool:
    """Return True if the slug is a known non-trim sub-page (e.g. photos,
    specs, price-in-{anywhere}). Combines exact-match + prefix-match so
    we don't have to enumerate every possible suburb in NON_TRIM_PATHS."""
    if candidate in NON_TRIM_PATHS:
        return True
    for prefix in NON_TRIM_PREFIXES:
        if candidate.startswith(prefix):
            return True
    return False


def _extract_trim_slugs(html: str, slug: str) -> List[str]:
    """Extract the list of trim slugs from a model overview page.

    Strategy is HTML-href-scan (cheap, doesn't depend on JSON state).
    A slug counts as a "trim" if:
      - It's reached via a /{make}-cars/{model}/{slug}/ href on the page
      - It does NOT contain any fuel/transmission token (which would mark
        it as a variant masking slug, not a trim slug)
      - It is NOT one of the known non-trim sub-page paths
        (photos, specs, colours, price-in-*, etc.)

    Returns: ["lxi", "vxi", "zxi", "zxi-plus"] etc, deduplicated, in the
    order they first appear in the HTML.
    """
    pattern = _build_trim_href_regex(slug)
    if not pattern:
        logger.warning("trim_slugs: bad slug %r", slug)
        return []

    seen: List[str] = []
    seen_set = set()
    for match in pattern.finditer(html):
        candidate = match.group(2).lower().strip("-")
        if not candidate:
            continue
        if candidate in seen_set:
            continue
        if _looks_like_non_trim(candidate):
            continue
        if _looks_like_variant_masking(candidate):
            continue
        seen.append(candidate)
        seen_set.add(candidate)

    logger.info("trim_slugs: discovered %d trims for %s: %s",
                len(seen), slug, seen)
    return seen


# ============================================================
# v3.0: VERSION HARVESTING FROM TRIM PAGES
# ============================================================

def _extract_version_record(v: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract a single version dict (from either trimPage.otherVersions[] or
    trimPage.versionDetail) into our canonical priced-variant shape.

    Returns None if the entry is missing required fields or has no
    usable ex-showroom price.

    The CarWale JSON shape (confirmed via v3.0.1 diagnostics) is:
      {
        "versionName": "VXi Petrol Manual",
        "versionMaskingName": "vxi-petrol-manual",
        "priceOverview": {
          "exShowRoomPrice": 985300,   # int rupees, Bangalore-pinned
          "price": 1189857,            # int rupees, on-road
          "formattedPrice": "Rs. 11.90 Lakh",
          ...
        }
      }
    Only otherVersions[] entries reliably have exShowRoomPrice. The
    versionDetail entry omits exShowRoomPrice (only "price" / on-road
    is present) — so callers should prefer otherVersions[] data when
    a variant appears in both lists.
    """
    if not isinstance(v, dict):
        return None
    name = (v.get("versionName") or v.get("displayName") or "").strip()
    if not name:
        return None
    po = v.get("priceOverview")
    if not isinstance(po, dict):
        return None
    ex = po.get("exShowRoomPrice") or 0
    if not isinstance(ex, (int, float)) or ex <= 0:
        return None
    return {
        "name": name,
        "fuel": _get_fuel_from_version(v),
        "ex_showroom": int(ex),
        "masking": (v.get("versionMaskingName") or "").strip(),
    }


def _harvest_versions_from_trim_page(
    html: str,
    trim_url: str,
) -> List[Dict[str, Any]]:
    """Parse a trim page's __INITIAL_STATE__ and return all priced
    variants with Bangalore ex-showroom prices.

    Reads from state.trimPage.otherVersions[] (the primary source — these
    entries reliably contain exShowRoomPrice). state.trimPage.versionDetail
    is checked as a secondary source but its priceOverview omits
    exShowRoomPrice for the variant that the trim page is "for", so it
    typically yields nothing useful.

    Variants where exShowRoomPrice is 0 or missing are skipped silently —
    those entries are placeholders that this particular trim page can't
    price. They'll be priced by another trim page in the walk.
    """
    out: List[Dict[str, Any]] = []
    try:
        state = _extract_initial_state(html)
    except Exception as e:
        logger.info("[%s] trim_page __INITIAL_STATE__ unparseable: %s", trim_url, e)
        return out

    if not isinstance(state, dict):
        logger.info("[%s] trim_page: __INITIAL_STATE__ is not a dict", trim_url)
        return out

    trim_page = state.get("trimPage")
    if not isinstance(trim_page, dict):
        logger.info("[%s] trim_page: no trimPage key in __INITIAL_STATE__", trim_url)
        return out

    # Primary source: trimPage.otherVersions[]
    other_versions = trim_page.get("otherVersions")
    if isinstance(other_versions, list):
        for v in other_versions:
            rec = _extract_version_record(v)
            if rec:
                out.append(rec)

    # Secondary source: trimPage.versionDetail (singular). Usually omits
    # exShowRoomPrice but check anyway in case CarWale changes shape.
    version_detail = trim_page.get("versionDetail")
    if isinstance(version_detail, dict):
        rec = _extract_version_record(version_detail)
        if rec:
            out.append(rec)

    logger.info("[%s] trim_page: harvested %d priced variants", trim_url, len(out))
    return out


# ============================================================
# v3.0: VARIANT MATCHING (against the priced versions list)
# ============================================================

def _match_priced_variant(
    variants: List[Dict[str, Any]],
    user_variant: str,
    user_fuel: str,
) -> Tuple[str, Optional[Dict[str, Any]], List[str]]:
    """Find the user's variant in the priced versions list.

    Returns (status, best_variant_dict_or_None, candidate_names_for_debug).

    status: "found" | "found_multiple" | "found_fuzzy"
            | "not_found" | "not_found_fuel"
    """
    user_norm = _normalize_variant_name(user_variant)
    if not user_norm:
        return ("not_found", None, [v.get("name", "") for v in variants])

    fuel_norm = (user_fuel or "").strip().lower()
    if fuel_norm:
        fuel_filtered = [v for v in variants
                         if (v.get("fuel") or "").lower() == fuel_norm]
        if not fuel_filtered:
            return ("not_found_fuel", None,
                    [v.get("name", "") for v in variants])
    else:
        fuel_filtered = list(variants)

    # 1. Exact match on normalized name
    exact = [v for v in fuel_filtered
             if _normalize_variant_name(v.get("name", "")) == user_norm]
    if exact:
        # Stable choice when multiple match: lowest price first
        exact.sort(key=lambda v: v.get("ex_showroom", 0))
        best = exact[0]
        status = "found" if len(exact) == 1 else "found_multiple"
        return (status, best, [v.get("name", "") for v in exact])

    # 2. Prefix fallback
    starts = [v for v in fuel_filtered
              if _normalize_variant_name(v.get("name", "")).startswith(user_norm)]
    if starts:
        starts.sort(key=lambda v: v.get("ex_showroom", 0))
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
            token_match.sort(key=lambda v: v.get("ex_showroom", 0))
            return ("found_fuzzy", token_match[0],
                    [v.get("name", "") for v in token_match])

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
    """Scrape the ex-showroom price (Bangalore-pinned) for a single variant
    from CarWale.

    v3.0 flow:
      1. Slug lookup
      2. Fetch model overview page → extract trim slug list
      3. For each trim, fetch /{make}-cars/{model}/{trim}/ until either:
         (a) we've seen the user's variant in the harvested versions, OR
         (b) we've exhausted all trims
      4. Match user's (variant, fuel) against the accumulated versions list
      5. Return the matched variant's ex-showroom price

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

    # 2. Fetch model overview + extract trim slugs
    overview_url = _build_url(slug)
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

    trim_slugs = _extract_trim_slugs(overview_html, slug)
    if not trim_slugs:
        result["status"] = "error"
        result["url"] = overview_url
        result["error"] = (
            f"No trim slugs discovered on {overview_url}. The page may be a "
            "discontinued-model stub, redirect, or CarWale layout may have "
            "changed (no /{slug}/{trim}/ hrefs found)."
        )
        return result

    # 3. Walk trims, harvesting priced variants until we have enough
    all_priced: List[Dict[str, Any]] = []
    seen_keys: set = set()
    last_trim_url: Optional[str] = None

    for trim in trim_slugs:
        trim_url = _build_trim_url(slug, trim)
        last_trim_url = trim_url
        try:
            trim_html = _http_get(trim_url)
        except FetchError as e:
            logger.warning("[%s] trim fetch failed, skipping: %s", trim_url, e)
            continue
        except Exception as e:
            logger.warning("[%s] trim fetch exception, skipping: %s: %s",
                           trim_url, e.__class__.__name__, e)
            continue

        priced = _harvest_versions_from_trim_page(trim_html, trim_url)
        for p in priced:
            key = (p.get("name", "").lower(), (p.get("fuel") or "").lower())
            if key in seen_keys:
                continue
            seen_keys.add(key)
            all_priced.append(p)

        # Early-exit memoization: if the user's variant is already
        # in the accumulated priced list, stop fetching more trims.
        match_status, matched, _ = _match_priced_variant(all_priced, variant, fuel)
        if matched is not None:
            logger.info("[%s] early-exit after trim %r — found user's variant",
                        overview_url, trim)
            result["url"] = trim_url
            result["all_variants"] = [v.get("name", "") for v in all_priced]
            result["candidates_considered"] = len(all_priced)
            result["ok"] = True
            result["status"] = match_status
            result["ex_showroom_inr"] = int(matched["ex_showroom"])
            result["matched_variant"] = matched.get("name") or variant
            return result

    # 4. Final match attempt with the full priced list
    result["all_variants"] = [v.get("name", "") for v in all_priced]
    result["candidates_considered"] = len(all_priced)
    result["url"] = last_trim_url or overview_url

    if not all_priced:
        result["status"] = "error"
        result["error"] = (
            f"Walked {len(trim_slugs)} trims but found 0 priced variants. "
            f"Possible causes: city cookies not honored, trim pages have a "
            f"new __INITIAL_STATE__ shape, or model is discontinued."
        )
        return result

    match_status, matched, candidate_names = _match_priced_variant(
        all_priced, variant, fuel
    )

    if matched is None:
        result["status"] = match_status
        if match_status == "not_found_fuel":
            available_fuels = sorted({v.get("fuel", "") for v in all_priced
                                      if v.get("fuel")})
            result["error"] = (
                f"Fuel {fuel!r} not available for {make} {model}. "
                f"Available fuels: {available_fuels}"
            )
        else:
            result["error"] = (
                f"Variant {variant!r} (fuel {fuel!r}) not found among "
                f"{len(all_priced)} priced variants."
            )
        return result

    result["ok"] = True
    result["status"] = match_status
    result["ex_showroom_inr"] = int(matched["ex_showroom"])
    result["matched_variant"] = matched.get("name") or variant
    return result


# ============================================================
# DIAGNOSTIC HELPERS
# ============================================================

def list_variants(make: str, model: str) -> Dict[str, Any]:
    """List all variants of a model with prices, by walking every trim
    page. Useful for debugging when fetch_price returns 'not_found'.

    v3.0 flow: same as fetch_price's first 3 steps but accumulates ALL
    priced variants across trims (no early-exit).
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
    overview_url = _build_url(slug)
    result["url"] = overview_url
    try:
        overview_html = _http_get(overview_url)
    except Exception as e:
        result["error"] = f"{e.__class__.__name__}: {e}"
        return result

    trim_slugs = _extract_trim_slugs(overview_html, slug)
    if not trim_slugs:
        result["error"] = "no trim slugs on overview page"
        return result

    all_priced: List[Dict[str, Any]] = []
    seen_keys: set = set()
    for trim in trim_slugs:
        trim_url = _build_trim_url(slug, trim)
        try:
            trim_html = _http_get(trim_url)
        except Exception as e:
            logger.warning("list_variants: trim fetch failed for %s: %s", trim_url, e)
            continue
        priced = _harvest_versions_from_trim_page(trim_html, trim_url)
        for p in priced:
            key = (p.get("name", "").lower(), (p.get("fuel") or "").lower())
            if key in seen_keys:
                continue
            seen_keys.add(key)
            all_priced.append(p)

    result["ok"] = True
    result["variants"] = all_priced
    return result


# ============================================================
# Module-level smoke test
# ============================================================

if __name__ == "__main__":
    import pprint
    logging.basicConfig(level=logging.INFO)
    print("=" * 60)
    print("price_scraper.py v3.0 smoke test")
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
        ok = "OK" if got == expected else "FAIL"
        print(f"  [{ok}] {inp!r:35s} -> {got!r:40s} (expected {expected!r})")

    print("\n--- _looks_like_variant_masking unit checks ---")
    masking_cases = [
        ("lxi", False),
        ("vxi", False),
        ("zxi", False),
        ("zxi-plus", False),
        ("vxi-petrol-automatic", True),
        ("lxi-cng-manual", True),
        ("zxi-plus-petrol-manual", True),
        ("xline", False),
        ("alpha", False),
    ]
    for inp, expected in masking_cases:
        got = _looks_like_variant_masking(inp)
        ok = "OK" if got == expected else "FAIL"
        print(f"  [{ok}] {inp!r:30s} -> {got!s:6} (expected {expected!s})")

    print("\n--- live fetch_price tests (Bangalore-pinned) ---")
    test_cases = [
        ("Maruti Suzuki", "Ertiga", "LXi", "Petrol"),
        ("Maruti Suzuki", "Ertiga", "VXi", "CNG"),
        ("Maruti Suzuki", "Ertiga", "ZXi+", "Petrol"),
        ("Maruti Suzuki", "Swift", "VXI", "Petrol"),
        ("Hyundai", "Creta", "E", "Petrol"),
    ]
    for make, model, variant, fuel in test_cases:
        print(f"\n--- {make} {model} {variant} {fuel} ---")
        r = fetch_price(make, model, variant, fuel)
        if r.get("all_variants") and len(r["all_variants"]) > 5:
            r["all_variants"] = r["all_variants"][:5] + [f"... +{len(r['all_variants']) - 5} more"]
        pprint.pprint(r)
