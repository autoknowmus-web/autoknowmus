"""
price_scraper.py
----------------
AutoKnowMus — External price scraper for ex-showroom price intelligence.

Phase 1: CarWale only. Phase 2 (future): add CarDekho as cross-validator.

Given a (make, model, variant, fuel) tuple, this module returns one of:
  - 'found'     → scraped price + URL + scraped_at timestamp
  - 'ambiguous' → multiple matching variants found, top N candidates returned
  - 'not_found' → CarWale doesn't have this car (likely discontinued or naming drift)
  - 'no_slug'   → no carwale_slug registered for this make/model
  - 'error'     → scrape failed (rate-limit, captcha, network, parse error)

The slug for each (make, model) is read from the `model_slugs` tab in the
AutoKnowMus Price Data Google Sheet. Run populate_slugs.py once to fill
that tab automatically.

----------------------------------------------------------------------
ARCHITECTURE
----------------------------------------------------------------------
Public API:
  - fetch_price(make, model, variant, fuel) -> dict with status + payload
  - normalize_variant(s) -> str  (exposed for unit testing)
  - get_carwale_url(make, model) -> str | None

Internal flow for fetch_price():
  1. Look up the carwale_slug for (make, model). If missing → 'no_slug'.
  2. Construct the CarWale model page URL.
  3. Fetch with rate-limit (2s ± 0.5s jitter), retries with backoff.
  4. Parse the variant table from the HTML.
  5. Filter variants by fuel.
  6. Match the requested variant string against scraped variants:
     a. Try normalized match first (strip transmission/fuel tokens, lowercase)
     b. If no normalized match, try fuzzy match (difflib, threshold 0.85)
  7. Return one match → 'found'. Multiple matches → 'ambiguous'. None → 'not_found'.

Variant matching rationale:
  Indian car variants often differ by suffixes like AT/AMT/MT/CVT (transmission)
  and Petrol/Diesel/CNG (fuel). The fuel is filtered separately. The
  transmission is part of the variant string but admins might omit it in the
  sheet (e.g. "VXI" in sheet vs "VXI AT" on CarWale). Normalized matching
  strips these tokens for the primary comparison.

Rate limiting:
  Default 2s ± 0.5s jitter between requests. Tunable via SCRAPE_INTERVAL_SECONDS.
  Background cron jobs run at night so we don't need to be fast — we need to
  be invisible to CarWale's rate limiters.

Bot detection mitigation:
  - Browser-like User-Agent
  - Standard Accept / Accept-Language headers
  - Connection reuse via a module-level requests.Session
  - Exponential backoff on 503/429: 1s → 2s → 4s → 8s, max 4 retries
  - If all retries fail, return 'error' status (NOT 'not_found') so admin
    can distinguish "CarWale was unhappy" from "CarWale doesn't have this"

----------------------------------------------------------------------
DEPENDENCIES
----------------------------------------------------------------------
  - requests (already in requirements.txt)
  - beautifulsoup4 (NEW — add to requirements.txt: `beautifulsoup4==4.12.3`)
  - sheets_writer (already in repo)

If beautifulsoup4 is missing, the module raises ImportError on first call
with a helpful message.
"""

import os
import re
import time
import random
import logging
import threading
import difflib
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger(__name__)


# ============================================================
# CONFIGURATION
# ============================================================

CARWALE_BASE_URL = "https://www.carwale.com"

# Rate limiting — locked at 2s ± 0.5s jitter per user decision.
SCRAPE_INTERVAL_SECONDS = 2.0
SCRAPE_JITTER_SECONDS = 0.5

# Retry policy for transient HTTP failures (503/429/timeout).
MAX_RETRIES = 4
RETRY_BACKOFF_BASE = 1.0  # seconds; doubles each retry: 1, 2, 4, 8

# HTTP timeouts — bounded well below gunicorn's 30s worker timeout.
HTTP_CONNECT_TIMEOUT = 5
HTTP_READ_TIMEOUT = 15
HTTP_TIMEOUT = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)

# Variant matching — locked at 0.85 fuzzy threshold per user decision,
# but normalized comparison runs first.
FUZZY_MATCH_THRESHOLD = 0.85

# Tokens stripped during normalized variant comparison. These are filtered
# separately (fuel) or are non-discriminating suffixes (transmission).
TRANSMISSION_TOKENS = {"at", "amt", "mt", "cvt", "dct", "ivt", "tct"}
FUEL_TOKENS = {"petrol", "diesel", "cng", "hev", "phev", "bev", "electric", "hybrid"}

# Browser-like headers — minimum set to look like a real visitor.
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


# ============================================================
# MODULE-LEVEL STATE
# ============================================================

_session = None
_last_request_at: float = 0.0  # unix seconds; for rate limiting
_slug_cache: Optional[Dict[Tuple[str, str], str]] = None  # (make, model) -> slug
_slug_cache_loaded_at: float = 0.0
_lock = threading.Lock()

# Cache slugs for 1 hour. If admin updates the sheet, force a reset
# via reset_caches().
SLUG_CACHE_TTL_SECONDS = 3600


# ============================================================
# RATE LIMITING
# ============================================================

def _rate_limit_wait():
    """
    Sleep so successive scrape requests are spaced out by SCRAPE_INTERVAL ± jitter.
    Thread-safe so concurrent scrapes (if any) cooperate.
    """
    global _last_request_at
    with _lock:
        now = time.time()
        elapsed = now - _last_request_at
        target_interval = SCRAPE_INTERVAL_SECONDS + random.uniform(
            -SCRAPE_JITTER_SECONDS, SCRAPE_JITTER_SECONDS
        )
        if elapsed < target_interval:
            sleep_for = target_interval - elapsed
            logger.debug("price_scraper: rate-limit sleeping %.2fs", sleep_for)
            time.sleep(sleep_for)
        _last_request_at = time.time()


# ============================================================
# HTTP SESSION
# ============================================================

def _get_session():
    """Lazy-build a single requests.Session for connection reuse."""
    global _session
    with _lock:
        if _session is not None:
            return _session
        try:
            import requests
        except ImportError as e:
            raise RuntimeError(f"requests library not available: {e}.")
        _session = requests.Session()
        _session.headers.update(BROWSER_HEADERS)
        logger.info("price_scraper: built requests.Session with browser headers")
        return _session


def _http_get(url: str, op_label: str = "fetch") -> str:
    """
    GET a URL with rate limiting, retries, and exponential backoff.
    Returns response body as str. Raises RuntimeError on hard failure.
    """
    try:
        import requests
    except ImportError as e:
        raise RuntimeError(f"requests library not available: {e}.")

    session = _get_session()
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        _rate_limit_wait()

        start = time.time()
        try:
            resp = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True)
        except requests.exceptions.Timeout as e:
            elapsed = int((time.time() - start) * 1000)
            last_error = f"timeout after {elapsed}ms ({e})"
            logger.warning(
                "price_scraper: %s attempt %d/%d → %s",
                op_label, attempt, MAX_RETRIES, last_error,
            )
            if attempt < MAX_RETRIES:
                _backoff_sleep(attempt)
            continue
        except requests.exceptions.RequestException as e:
            last_error = f"network error: {e}"
            logger.warning(
                "price_scraper: %s attempt %d/%d → %s",
                op_label, attempt, MAX_RETRIES, last_error,
            )
            if attempt < MAX_RETRIES:
                _backoff_sleep(attempt)
            continue

        elapsed_ms = int((time.time() - start) * 1000)

        if resp.status_code == 200:
            logger.info(
                "price_scraper: %s OK in %dms (%d bytes)",
                op_label, elapsed_ms, len(resp.content),
            )
            return resp.text

        if resp.status_code == 404:
            logger.info(
                "price_scraper: %s → 404 (page does not exist) for %s",
                op_label, url,
            )
            raise _NotFoundError(f"404 from CarWale for {url}")

        if resp.status_code in (429, 503):
            last_error = f"HTTP {resp.status_code} (rate-limited or unavailable)"
            logger.warning(
                "price_scraper: %s attempt %d/%d → %s",
                op_label, attempt, MAX_RETRIES, last_error,
            )
            if attempt < MAX_RETRIES:
                _backoff_sleep(attempt)
            continue

        # Other 4xx/5xx: don't retry, bubble up.
        body_preview = (resp.text or "")[:200]
        raise RuntimeError(
            f"{op_label} returned HTTP {resp.status_code} for {url}. "
            f"Body preview: {body_preview}"
        )

    raise RuntimeError(
        f"{op_label} failed after {MAX_RETRIES} attempts. Last error: {last_error}"
    )


def _backoff_sleep(attempt: int):
    """Exponential backoff: 1s, 2s, 4s, 8s ..."""
    sleep_for = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
    logger.debug("price_scraper: backing off %.1fs before retry", sleep_for)
    time.sleep(sleep_for)


class _NotFoundError(Exception):
    """Internal-only: CarWale returned 404 for a URL we built."""
    pass


# ============================================================
# SLUG LOOKUP (from model_slugs tab)
# ============================================================

def _load_slug_cache() -> Dict[Tuple[str, str], str]:
    """
    Load the model_slugs tab from the Google Sheet into an in-memory cache.
    Cache lives for SLUG_CACHE_TTL_SECONDS (1 hour) before refresh.

    Returns dict keyed by (make_lower_stripped, model_lower_stripped) → slug.
    """
    global _slug_cache, _slug_cache_loaded_at

    with _lock:
        now = time.time()
        if (_slug_cache is not None
                and (now - _slug_cache_loaded_at) < SLUG_CACHE_TTL_SECONDS):
            return _slug_cache

    # Load outside the lock since it's a network call.
    try:
        import sheets_writer
    except ImportError as e:
        raise RuntimeError(
            f"Could not import sheets_writer: {e}. "
            "price_scraper depends on sheets_writer for slug lookup."
        )

    try:
        rows = sheets_writer.read_model_slugs()
    except Exception as e:
        raise RuntimeError(f"Failed to read model_slugs tab: {e}")

    cache: Dict[Tuple[str, str], str] = {}
    for r in rows:
        make = (r.get("make") or "").strip()
        model = (r.get("model") or "").strip()
        slug = (r.get("carwale_slug") or "").strip()
        if not make or not model or not slug:
            continue
        if slug == "NEEDS_MANUAL_FIX":
            continue
        key = (make.lower(), model.lower())
        cache[key] = slug

    with _lock:
        _slug_cache = cache
        _slug_cache_loaded_at = time.time()

    logger.info("price_scraper: loaded %d slugs into cache", len(cache))
    return cache


def _lookup_slug(make: str, model: str) -> Optional[str]:
    """Return the carwale_slug for (make, model), or None if not registered."""
    cache = _load_slug_cache()
    key = (make.strip().lower(), model.strip().lower())
    return cache.get(key)


# ============================================================
# URL CONSTRUCTION
# ============================================================

def get_carwale_url(make: str, model: str) -> Optional[str]:
    """
    Build the CarWale model page URL given a (make, model). Returns None
    if no slug is registered.

    Example: ('Maruti Suzuki', 'Swift') → 'https://www.carwale.com/maruti-suzuki-cars/swift/'

    The URL pattern is: {base}/{make-slug}-cars/{model-slug}/
    """
    slug = _lookup_slug(make, model)
    if not slug:
        return None
    # Slug format expected from sheet: 'maruti-suzuki/swift'
    # We split into make-slug and model-slug.
    if "/" not in slug:
        logger.warning(
            "price_scraper: invalid slug format for (%s, %s): %r "
            "(expected 'make-slug/model-slug')",
            make, model, slug,
        )
        return None
    make_slug, model_slug = slug.split("/", 1)
    return f"{CARWALE_BASE_URL}/{make_slug}-cars/{model_slug}/"


# ============================================================
# VARIANT NORMALIZATION & MATCHING
# ============================================================

# Tokens to expand before normalization. Order matters — longer first so
# we don't double-expand.
_VARIANT_EXPANSIONS = [
    ("+", " plus "),
]


def normalize_variant(s: str) -> str:
    """
    Normalize a variant string for comparison:
      1. Lowercase
      2. Expand '+' → 'plus'
      3. Strip transmission tokens (AT/AMT/MT/CVT/DCT/IVT/TCT)
      4. Strip fuel tokens (Petrol/Diesel/CNG/HEV/PHEV/BEV/Electric/Hybrid)
      5. Strip punctuation (keep alphanumeric and spaces)
      6. Collapse multiple spaces, strip ends

    Examples:
      'VXI AT'        → 'vxi'
      'VXI AT Petrol' → 'vxi'
      'ZXI+ DT'       → 'zxi plus dt'
      'Sigma 1.2L MT' → 'sigma 12l'
      'XZA Plus'      → 'xza plus'
    """
    if not s:
        return ""
    s = s.lower()

    for old, new in _VARIANT_EXPANSIONS:
        s = s.replace(old, new)

    # Tokenize on whitespace, drop transmission/fuel tokens.
    tokens = re.split(r"\s+", s)
    kept = []
    for tok in tokens:
        # Strip punctuation from the token before checking.
        clean_tok = re.sub(r"[^\w]+", "", tok)
        if not clean_tok:
            continue
        if clean_tok in TRANSMISSION_TOKENS:
            continue
        if clean_tok in FUEL_TOKENS:
            continue
        kept.append(clean_tok)

    return " ".join(kept).strip()


def _match_variants(target_variant: str,
                    candidates: List[Dict[str, Any]]
                    ) -> Tuple[str, List[Dict[str, Any]]]:
    """
    Match the target_variant against a list of scraped variant dicts.

    Each candidate dict has at least keys: 'variant', 'price', 'fuel'.
    (Already pre-filtered to the right fuel by the caller.)

    Strategy:
      1. Normalized exact match — if exactly one candidate's normalized
         variant equals the target's normalized variant → return it.
      2. Normalized exact match returning multiple → 'ambiguous'.
      3. Fuzzy match — compute difflib ratio against each candidate's
         RAW variant string. If best score ≥ FUZZY_MATCH_THRESHOLD AND
         no other candidate is within 0.05 of the best → return best.
         Else → 'ambiguous' with top 3 candidates.
      4. No fuzzy match clears threshold → 'not_found'.

    Returns (status, matched_candidates) where status is one of:
      'found', 'ambiguous', 'not_found'.
    """
    if not candidates:
        return ("not_found", [])

    target_norm = normalize_variant(target_variant)

    # Strategy 1: normalized match
    norm_matches = [
        c for c in candidates
        if normalize_variant(c.get("variant", "")) == target_norm
    ]
    if len(norm_matches) == 1:
        return ("found", norm_matches)
    if len(norm_matches) > 1:
        return ("ambiguous", norm_matches)

    # Strategy 2: fuzzy match (raw strings)
    scored = []
    for c in candidates:
        raw_v = c.get("variant", "")
        score = difflib.SequenceMatcher(
            None, target_variant.lower(), raw_v.lower()
        ).ratio()
        scored.append((score, c))
    scored.sort(key=lambda x: x[0], reverse=True)

    best_score = scored[0][0] if scored else 0.0
    if best_score < FUZZY_MATCH_THRESHOLD:
        # Even the best candidate is below threshold — return ambiguous
        # so admin can pick from the top 3.
        top3 = [c for _, c in scored[:3]]
        return ("not_found", top3)

    # Best score clears threshold. Check if it's clearly the best (no near-tie).
    if len(scored) == 1:
        return ("found", [scored[0][1]])
    second_best_score = scored[1][0]
    if (best_score - second_best_score) >= 0.05:
        return ("found", [scored[0][1]])

    # Near-tie → ambiguous.
    top3 = [c for _, c in scored[:3]]
    return ("ambiguous", top3)


# ============================================================
# CARWALE HTML PARSING
# ============================================================

def _parse_carwale_variants(html: str, model_url: str) -> List[Dict[str, Any]]:
    """
    Parse the CarWale model page HTML and extract a list of variant dicts.

    Each variant dict has:
      {
        'variant': str,        # e.g. 'VXI AT'
        'price': int,          # ex-showroom price in INR (e.g. 825000)
        'fuel': str,           # 'Petrol' / 'Diesel' / 'CNG' / etc.
        'transmission': str,   # 'Manual' / 'Automatic' (best effort)
        'url': str,            # variant deep-link if present, else model_url
      }

    CarWale's HTML structure has changed over time. As of 2026-05, the
    variant table is typically rendered server-side at
    /{make}-cars/{model}/ in a section labeled "Price List" with
    individual variant rows.

    NOTE: This parser uses CSS selectors based on observed CarWale HTML
    structure. If CarWale changes their layout, this function may need
    updates. We log warnings on parse failures so we know when to revisit.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError as e:
        raise RuntimeError(
            f"beautifulsoup4 not installed: {e}. "
            "Add `beautifulsoup4==4.12.3` to requirements.txt."
        )

    soup = BeautifulSoup(html, "html.parser")
    variants: List[Dict[str, Any]] = []

    # ----------------------------------------------------------
    # CarWale price-list parsing
    # ----------------------------------------------------------
    # As of May 2026, CarWale renders variants in a structure like:
    #   <div class="o-cpzZyB"> ... variant rows ...
    #   <a href="/.../variant-name/" class="o-...">
    #       <span>VXI AT</span>
    #       <span>Petrol · Manual · 22.4 kmpl</span>
    #       <span>Rs. 8.25 Lakh*</span>
    #   </a>
    #
    # We use multiple selector strategies and pick the first that yields
    # results. This makes us resilient to minor CSS class hash changes.
    # ----------------------------------------------------------

    # Strategy 1: Look for variant cards/rows by structural pattern —
    # any <a> or <div> containing both a price string and a variant name.
    # Prices on CarWale are formatted as "Rs. X.YY Lakh" or "Rs. X,XX,XXX".
    price_pattern = re.compile(
        r"(?:Rs\.?|₹)\s*([\d,]+(?:\.\d+)?)\s*(Lakh|Crore|cr|L)?",
        re.IGNORECASE,
    )

    # Look for all elements that look like variant rows: contain a price
    # string and at least one of the fuel keywords.
    fuel_keywords_re = re.compile(
        r"\b(petrol|diesel|cng|electric|hybrid|hev|phev|bev)\b",
        re.IGNORECASE,
    )

    # Find candidate variant containers — anything with both a price and
    # a fuel keyword in its text.
    candidate_blocks = []
    for elem in soup.find_all(["a", "div", "li", "tr"]):
        text = elem.get_text(" ", strip=True)
        if not text:
            continue
        if not price_pattern.search(text):
            continue
        if not fuel_keywords_re.search(text):
            continue
        # Avoid huge containers — prefer leaf-ish elements.
        if len(text) > 400:
            continue
        candidate_blocks.append((elem, text))

    if not candidate_blocks:
        logger.warning(
            "price_scraper: parser found no variant blocks on %s. "
            "CarWale layout may have changed.",
            model_url,
        )
        return []

    # Deduplicate: prefer smaller blocks (more leaf-like), drop blocks
    # that are ancestors of other candidates.
    candidate_blocks.sort(key=lambda x: len(x[1]))

    seen_signatures = set()
    for elem, text in candidate_blocks:
        # Build a "signature" from price + first fuel mention to dedupe.
        price_match = price_pattern.search(text)
        fuel_match = fuel_keywords_re.search(text)
        if not price_match or not fuel_match:
            continue

        price_str = price_match.group(1)
        price_unit = (price_match.group(2) or "").lower()
        price_inr = _parse_price_to_inr(price_str, price_unit)
        if price_inr is None or price_inr < 100000:  # sanity floor: <1L is suspect
            continue

        fuel_raw = fuel_match.group(1).lower()
        fuel = _normalize_fuel(fuel_raw)

        # Variant name: extract from the FIRST text token before any
        # separator like "·", "•", "|" or before the fuel/price string.
        variant_name = _extract_variant_name(text, fuel_raw, price_match.group(0))
        if not variant_name:
            continue

        signature = (variant_name.lower(), price_inr, fuel)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)

        # Variant deep link if this elem is an <a>
        href = ""
        if elem.name == "a" and elem.get("href"):
            href = elem.get("href")
            if href.startswith("/"):
                href = CARWALE_BASE_URL + href

        # Transmission detection — look for AT/AMT/MT/CVT in the text.
        transmission = _detect_transmission(text)

        variants.append({
            "variant": variant_name,
            "price": price_inr,
            "fuel": fuel,
            "transmission": transmission,
            "url": href or model_url,
        })

    logger.info(
        "price_scraper: parsed %d variants from %s",
        len(variants), model_url,
    )
    return variants


def _parse_price_to_inr(price_str: str, unit: str) -> Optional[int]:
    """
    Convert a CarWale-formatted price into INR rupees.

    '8.25', 'lakh'        → 825000
    '12,50,000', ''       → 1250000
    '1.2', 'crore'        → 12000000
    '8,25,000', 'lakh'    → 825000  (the unit is supplementary)
    """
    try:
        cleaned = price_str.replace(",", "")
        val = float(cleaned)
    except (ValueError, AttributeError):
        return None

    unit_l = (unit or "").lower()
    if unit_l in ("lakh", "l") and val < 10000:
        # "8.25 Lakh" → 8.25 * 100000
        return int(round(val * 100000))
    if unit_l in ("crore", "cr") and val < 1000:
        return int(round(val * 10000000))
    # No unit, or large absolute number with unit
    if val >= 100000:
        return int(val)
    # Sanity fallback: small number with no unit is suspect; treat as Lakh
    if val < 100:
        return int(round(val * 100000))
    return int(val)


def _normalize_fuel(raw: str) -> str:
    """Map a raw fuel string from CarWale to our standard fuel names."""
    raw_l = raw.lower().strip()
    if raw_l == "petrol":
        return "Petrol"
    if raw_l == "diesel":
        return "Diesel"
    if raw_l == "cng":
        return "CNG"
    if raw_l in ("electric", "bev"):
        return "BEV"
    if raw_l in ("hev", "hybrid"):
        return "HEV"
    if raw_l == "phev":
        return "PHEV"
    return raw_l.title()


def _detect_transmission(text: str) -> str:
    """Best-effort transmission detection from variant text."""
    text_l = text.lower()
    if re.search(r"\b(automatic|amt|cvt|dct|ivt|tct)\b", text_l):
        return "Automatic"
    if re.search(r"\bat\b", text_l):
        return "Automatic"
    if re.search(r"\b(manual|mt)\b", text_l):
        return "Manual"
    return ""


def _extract_variant_name(text: str, fuel_raw: str, price_token: str) -> str:
    """
    Pull the variant name out of a variant row's text.
    Strategy: take everything before the first occurrence of fuel or price.
    """
    # Find the earliest cut point.
    cut = len(text)
    fuel_pos = text.lower().find(fuel_raw.lower())
    if fuel_pos > 0 and fuel_pos < cut:
        cut = fuel_pos
    price_pos = text.find(price_token)
    if price_pos > 0 and price_pos < cut:
        cut = price_pos

    name = text[:cut].strip()
    # Strip trailing separators
    name = re.sub(r"[\s·•|,\-]+$", "", name).strip()
    # Strip leading "Variant:" labels if any
    name = re.sub(r"^variant\s*:\s*", "", name, flags=re.IGNORECASE)
    # Cap length
    if len(name) > 80:
        name = name[:80].rsplit(" ", 1)[0]
    return name


# ============================================================
# PUBLIC API
# ============================================================

def fetch_price(make: str, model: str, variant: str, fuel: str) -> Dict[str, Any]:
    """
    Fetch the latest ex-showroom price for a (make, model, variant, fuel).

    Returns a dict:
      {
        "status": "found" | "ambiguous" | "not_found" | "no_slug" | "error",
        "make": str, "model": str, "variant": str, "fuel": str,
        "url": str | None,           # CarWale model page URL
        "scraped_at": ISO timestamp,
        "match": {                   # only when status='found'
          "variant": str,
          "price": int,
          "fuel": str,
          "transmission": str,
          "url": str,
        },
        "candidates": [...],         # when status='ambiguous' or 'not_found'
        "error": str,                # when status='error'
      }

    Never raises — all failures are returned as status='error' with a
    human-readable message in 'error' key. This makes it safe to call
    from cron jobs without try/except wrapping.
    """
    result: Dict[str, Any] = {
        "status": "error",
        "make": make,
        "model": model,
        "variant": variant,
        "fuel": fuel,
        "url": None,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        # ---- 1. Look up slug
        url = get_carwale_url(make, model)
        if url is None:
            result["status"] = "no_slug"
            result["error"] = (
                f"No carwale_slug registered for ({make}, {model}). "
                "Run populate_slugs.py or add manually to model_slugs tab."
            )
            return result
        result["url"] = url

        # ---- 2. Fetch HTML
        try:
            html = _http_get(url, op_label=f"carwale[{make}/{model}]")
        except _NotFoundError:
            result["status"] = "not_found"
            result["error"] = f"CarWale returned 404 for {url} (model may be discontinued)."
            return result

        # ---- 3. Parse variants
        all_variants = _parse_carwale_variants(html, url)
        if not all_variants:
            result["status"] = "error"
            result["error"] = (
                f"Parsed 0 variants from {url}. CarWale layout may have "
                "changed; the parser needs updating."
            )
            return result

        # ---- 4. Filter by fuel
        target_fuel = _normalize_fuel(fuel)
        fuel_filtered = [v for v in all_variants if v["fuel"] == target_fuel]
        if not fuel_filtered:
            available_fuels = sorted(set(v["fuel"] for v in all_variants))
            result["status"] = "not_found"
            result["error"] = (
                f"No {target_fuel} variants found on {url}. "
                f"Available fuels: {available_fuels}."
            )
            result["candidates"] = all_variants[:5]
            return result

        # ---- 5. Match variant
        match_status, matched = _match_variants(variant, fuel_filtered)

        if match_status == "found":
            result["status"] = "found"
            result["match"] = matched[0]
            return result

        if match_status == "ambiguous":
            result["status"] = "ambiguous"
            result["candidates"] = matched
            result["error"] = (
                f"Multiple variants matched ({len(matched)}). "
                "Admin must pick the right one."
            )
            return result

        # not_found
        result["status"] = "not_found"
        result["candidates"] = matched  # top 3 nearest
        result["error"] = (
            f"No {target_fuel} variant matched '{variant}' on {url}. "
            f"Closest candidates returned for review."
        )
        return result

    except Exception as e:
        logger.exception("price_scraper: unexpected error in fetch_price")
        result["status"] = "error"
        result["error"] = f"{type(e).__name__}: {e}"
        return result


def reset_caches():
    """Force-clear caches. Call after admin updates model_slugs tab."""
    global _slug_cache, _slug_cache_loaded_at, _session
    with _lock:
        _slug_cache = None
        _slug_cache_loaded_at = 0.0
        _session = None
    logger.info("price_scraper: caches reset")
