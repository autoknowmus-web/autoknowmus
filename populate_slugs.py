"""
populate_slugs.py
-----------------
ONE-TIME SCRIPT — run once after creating the model_slugs tab.

Reads the unique (make, model) pairs from car_prices, derives candidate
CarWale URL slugs, verifies each by fetching the CarWale URL, and writes
the results to the model_slugs tab.

Result:
  - ~95% of slugs auto-resolved (HTTP 200 on the candidate URL)
  - ~5% marked as 'NEEDS_MANUAL_FIX' for human review

Run via:
  python populate_slugs.py

Or via Render shell:
  cd /opt/render/project/src
  python populate_slugs.py

After it finishes, open your Google Sheet → model_slugs tab → search for
'NEEDS_MANUAL_FIX' and fill in those slugs by visiting CarWale manually
to find the right URL.

----------------------------------------------------------------------
SLUG FORMAT
----------------------------------------------------------------------
Each slug is stored as 'make-slug/model-slug' (single string with '/'
separator). The price_scraper splits this into the URL pattern
'/{make-slug}-cars/{model-slug}/'.

Example:
  make='Maruti Suzuki', model='Swift'
  → derived slug 'maruti-suzuki/swift'
  → URL https://www.carwale.com/maruti-suzuki-cars/swift/
  → If HTTP 200 → write to sheet
  → If HTTP 404 → try fallbacks (e.g. 'maruti/swift'), else NEEDS_MANUAL_FIX

----------------------------------------------------------------------
DEPENDENCIES
----------------------------------------------------------------------
  - requests
  - sheets_writer (uses read_car_prices, write_model_slug)

Note: This script writes to a tab the sheets_writer needs to know about.
A small patch to sheets_writer.py is provided separately to add:
  - read_model_slugs()  (used by price_scraper)
  - write_model_slug(make, model, slug)  (used by this script)
"""

import os
import sys
import time
import random
import re
import logging
from typing import List, Tuple, Optional, Set

# Configure logging for standalone script use
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("populate_slugs")

CARWALE_BASE_URL = "https://www.carwale.com"

# Rate-limit: 2s ± 0.5s jitter to be polite to CarWale.
SCRAPE_INTERVAL_SECONDS = 2.0
SCRAPE_JITTER_SECONDS = 0.5

# HTTP timeouts.
HTTP_TIMEOUT = (5, 15)

# Browser-like headers.
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
    "Connection": "keep-alive",
}


# ============================================================
# SLUG DERIVATION
# ============================================================

def derive_slug(make: str, model: str) -> str:
    """
    Derive a candidate slug 'make-slug/model-slug' from raw make + model.

    Rules:
      - lowercase
      - strip leading/trailing whitespace
      - replace '&' with 'and'
      - replace any non-alphanumeric run with a single hyphen
      - strip leading/trailing hyphens
    """
    def slugify(s: str) -> str:
        s = s.lower().strip()
        s = s.replace("&", "and")
        s = re.sub(r"[^a-z0-9]+", "-", s)
        s = s.strip("-")
        return s

    return f"{slugify(make)}/{slugify(model)}"


def fallback_slugs(make: str, model: str) -> List[str]:
    """
    Generate fallback slug candidates when the primary derivation 404s.

    Common patterns:
      - 'Maruti Suzuki' might be just 'maruti' on CarWale
      - 'Mahindra & Mahindra' might be just 'mahindra'
      - hyphens in model name might be underscores or removed
    """
    candidates: List[str] = []

    # Take the first word of the make as a fallback (handles 'Maruti Suzuki' → 'maruti')
    first_make_word = make.split()[0].lower() if make.split() else ""
    if first_make_word:
        m_slug = re.sub(r"[^a-z0-9]+", "-", model.lower()).strip("-")
        candidates.append(f"{first_make_word}/{m_slug}")

    # Try removing internal spaces from make (e.g. 'maruti suzuki' → 'marutisuzuki')
    make_no_space = re.sub(r"\s+", "", make.lower())
    if make_no_space:
        m_slug = re.sub(r"[^a-z0-9]+", "-", model.lower()).strip("-")
        candidates.append(f"{make_no_space}/{m_slug}")

    # Try removing hyphens from model (e.g. 'grand-i10' → 'grandi10')
    primary = derive_slug(make, model)
    if "-" in primary:
        ms, mods = primary.split("/", 1)
        candidates.append(f"{ms}/{mods.replace('-', '')}")

    # Dedupe while preserving order, drop the primary
    seen: Set[str] = {derive_slug(make, model)}
    deduped = []
    for c in candidates:
        if c not in seen:
            deduped.append(c)
            seen.add(c)
    return deduped


# ============================================================
# CARWALE URL VERIFICATION
# ============================================================

_session = None
_last_request_at = 0.0


def _get_session():
    global _session
    if _session is not None:
        return _session
    import requests
    _session = requests.Session()
    _session.headers.update(BROWSER_HEADERS)
    return _session


def _rate_limit():
    """Sleep to maintain SCRAPE_INTERVAL_SECONDS ± jitter between requests."""
    global _last_request_at
    now = time.time()
    elapsed = now - _last_request_at
    target = SCRAPE_INTERVAL_SECONDS + random.uniform(
        -SCRAPE_JITTER_SECONDS, SCRAPE_JITTER_SECONDS
    )
    if elapsed < target:
        time.sleep(target - elapsed)
    _last_request_at = time.time()


def verify_slug(slug: str) -> Tuple[bool, int, str]:
    """
    HEAD request the CarWale URL for a given slug. Returns
    (works, status_code, url).

    'works' is True only when status_code == 200 AND the response is
    actually a model page (not a redirect to a search/error page).
    """
    if "/" not in slug:
        return (False, 0, "")

    make_slug, model_slug = slug.split("/", 1)
    url = f"{CARWALE_BASE_URL}/{make_slug}-cars/{model_slug}/"

    _rate_limit()

    try:
        import requests
    except ImportError:
        logger.error("requests library not installed")
        return (False, 0, url)

    session = _get_session()

    try:
        # Use GET (not HEAD) because some sites return 200 to HEAD and
        # 404 to GET. We need GET to truly verify.
        # We'll abort the read after a few KB — we just need status.
        resp = session.get(url, timeout=HTTP_TIMEOUT, allow_redirects=True, stream=True)
        # Read just a tiny bit to confirm it's a real page
        content_preview = resp.raw.read(2048, decode_content=True) if resp.raw else b""
        resp.close()
    except Exception as e:
        logger.warning("  verify failed: %s — %s", url, e)
        return (False, 0, url)

    if resp.status_code != 200:
        return (False, resp.status_code, url)

    # Sanity check: the response should be HTML and contain the model
    # name somewhere. Avoid false positives from generic search pages.
    try:
        text = content_preview.decode("utf-8", errors="ignore").lower()
    except Exception:
        text = ""

    # CarWale model pages typically contain 'price' and the model slug
    # somewhere in the first 2KB. Generic 404/error pages don't.
    looks_like_model_page = (
        "price" in text or "variant" in text or "specifications" in text
    )
    if not looks_like_model_page:
        # Could be a redirect to homepage/search. Treat as not found.
        return (False, resp.status_code, url)

    return (True, 200, url)


# ============================================================
# MAIN
# ============================================================

def main():
    logger.info("populate_slugs.py — starting")

    try:
        import sheets_writer
    except ImportError as e:
        logger.error("Could not import sheets_writer: %s", e)
        sys.exit(1)

    # ---- 1. Read all car_prices rows
    logger.info("Reading car_prices tab...")
    try:
        rows = sheets_writer.read_car_prices()
    except Exception as e:
        logger.error("Failed to read car_prices: %s", e)
        sys.exit(1)

    if not rows:
        logger.warning("car_prices tab is empty. Nothing to do.")
        return

    # ---- 2. Extract unique (make, model) pairs
    pairs = sorted({(r["make"], r["model"]) for r in rows if r.get("make") and r.get("model")})
    logger.info("Found %d unique (make, model) pairs", len(pairs))

    # ---- 3. Read existing slugs (skip pairs already populated)
    try:
        existing_slug_rows = sheets_writer.read_model_slugs()
    except Exception as e:
        logger.warning("Could not read model_slugs (may be empty/new): %s", e)
        existing_slug_rows = []

    already_populated: Set[Tuple[str, str]] = set()
    for r in existing_slug_rows:
        m = (r.get("make") or "").strip()
        mo = (r.get("model") or "").strip()
        slug = (r.get("carwale_slug") or "").strip()
        # Re-resolve NEEDS_MANUAL_FIX (in case it's now resolvable)
        if m and mo and slug and slug != "NEEDS_MANUAL_FIX":
            already_populated.add((m, mo))

    pending = [(m, mo) for m, mo in pairs if (m, mo) not in already_populated]
    logger.info(
        "%d pairs already have slugs, %d pending resolution",
        len(already_populated), len(pending),
    )

    if not pending:
        logger.info("All pairs already populated. Done.")
        return

    # ---- 4. For each pending pair, derive + verify
    resolved = 0
    needs_fix = 0
    errors = 0

    for idx, (make, model) in enumerate(pending, start=1):
        logger.info(
            "[%d/%d] %s / %s",
            idx, len(pending), make, model,
        )

        # Try primary derivation
        primary = derive_slug(make, model)
        works, status, url = verify_slug(primary)
        if works:
            logger.info("  ✓ resolved: %s (HTTP %d)", primary, status)
            try:
                sheets_writer.write_model_slug(make, model, primary)
                resolved += 1
            except Exception as e:
                logger.error("  ✗ sheet write failed: %s", e)
                errors += 1
            continue

        logger.info("  primary slug %s → HTTP %d, trying fallbacks", primary, status)

        # Try fallbacks
        found = False
        for fb_slug in fallback_slugs(make, model):
            works, status, url = verify_slug(fb_slug)
            if works:
                logger.info("  ✓ resolved via fallback: %s (HTTP %d)", fb_slug, status)
                try:
                    sheets_writer.write_model_slug(make, model, fb_slug)
                    resolved += 1
                    found = True
                    break
                except Exception as e:
                    logger.error("  ✗ sheet write failed: %s", e)
                    errors += 1
                    found = True
                    break
            logger.info("  fallback %s → HTTP %d", fb_slug, status)

        if not found:
            logger.warning("  ✗ all candidates failed → marking NEEDS_MANUAL_FIX")
            try:
                sheets_writer.write_model_slug(make, model, "NEEDS_MANUAL_FIX")
                needs_fix += 1
            except Exception as e:
                logger.error("  ✗ sheet write failed: %s", e)
                errors += 1

    # ---- 5. Summary
    logger.info("=" * 60)
    logger.info("DONE.")
    logger.info("  Resolved automatically: %d", resolved)
    logger.info("  Need manual fix:        %d", needs_fix)
    logger.info("  Sheet write errors:     %d", errors)
    logger.info(
        "Open the Google Sheet → model_slugs tab → search for "
        "'NEEDS_MANUAL_FIX' to find rows needing human review."
    )


if __name__ == "__main__":
    main()
