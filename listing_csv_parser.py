"""
listing_csv_parser.py
---------------------
AutoKnowMus — Listing Calibration Pipeline · Step 3 (CSV parser, CarWale v1).

PURPOSE
  Parse CSV uploads from Instant Data Scraper (run on CarWale Bangalore
  listings pages) into structured ParsedListingRow objects ready to insert
  into research_log with data_source='Listing Aggregator'.

  Skipped rows (parse failures, unknown makes/models, variant mismatches)
  are returned alongside as SkippedRow objects with a reason — drives the
  admin upload-detail UI and the calibration skip-list.

INPUT — Instant Data Scraper CSV columns (CarWale Bangalore listings page)
  Columns we use:
    o-C href  — listing URL with make-model slug
    o-o       — title: "{YYYY} {Make} {Model} {Variant} [bracket-suffix]"
    o-j1      — combined: "{kms} km  |  {fuel}  |  {locality}, {city}"
    o-j5      — price: "Rs. N Lakh", "Rs. N.N Lakh", or "Rs. N.N Crore"

  Other columns (image URLs, feature chips like "Panoramic Sunroof") are
  ignored. We never look at them.

PARSING STRATEGY (locked design decisions)

  1. URL slug is the primary source for (make, model).
     CarWale URLs have the form:
       https://www.carwale.com/used/bangalore/{make-slug}-{model-slug}/{id}/
     The slug uses hyphenated lowercase. We match against canonical
     make/model from car_data.py via longest-prefix match — e.g. for
     'mercedes-benz-e-class' we try 'mercedes-benz' (3 tokens won't match
     a known make), then 'mercedes' (also won't match), then we look for
     known multi-word makes in our catalog. Real implementation: try
     all known makes, longest-first, and consume that prefix.

  2. Title is the source for (year, raw_variant).
     Title format: "{YYYY} {make-and-model words} {variant words} [optional bracket]"
     We strip the year (always 4 digits at start), strip any [bracket] suffix
     that CarWale appends for facelift versions, strip the make+model words
     (now known from URL slug), and what remains is the raw variant string
     fed to variant_resolver.

  3. variant_resolver decides the row's fate.
     - auto_match → row included
     - needs_review → row included BUT flagged (admin can review later)
     - rejected → row dropped, logged to skipped list with reason

  4. Owner is NOT in CarWale grid view.
     CarWale shows owner only on the detail page — out of scope for v1.
     All listing entries land with owner=None. The calibration math
     averages this out across the dataset.

  5. Condition is NOT in CarWale grid either.
     Set to None. Calibration math handles None as 'Good' (default).

DATA QUALITY
  All listing entries get data_quality='medium' per spec section 6 —
  asking prices ≠ closed deals.

DESIGN NOTES
  - This module is CarWale-specific in v1. Phase 2 will add separate
    parsers for CarDekho/Spinny/Cars24/OLX — each marketplace has its
    own column structure. Don't try to generalize prematurely.
  - Pure function: same CSV → same output. No DB writes here. Caller
    (the admin upload route) decides what to do with the results.
  - Module never raises on a single bad row — bad rows go to skipped[]
    with reason. Only top-level CSV parse errors (missing columns,
    fundamentally broken file) raise.

PUBLIC API
  parse_carwale_csv(csv_bytes_or_text, default_state='KA', default_city='Bangalore')
      → ParseResult(parsed: List[ParsedListingRow], skipped: List[SkippedRow])

  ParsedListingRow — per-row output ready for research_log insertion.
  SkippedRow — row that didn't make it, with structured reason.

USAGE
  with open('carwale.csv', 'rb') as f:
      result = parse_carwale_csv(f.read())
  for row in result.parsed:
      # insert into research_log
      ...
  for skip in result.skipped:
      # log to listing_uploads.skipped_rows or display in admin UI
      ...
"""

import csv
import io
import re
from typing import List, NamedTuple, Optional, Tuple

import car_data
from variant_resolver import (
    resolve_variant,
    DECISION_AUTO_MATCH,
    DECISION_NEEDS_REVIEW,
    DECISION_REJECTED,
)


# ============================================================
# CONSTANTS — CarWale-specific (v1)
# ============================================================

# Required CSV column names. If any is missing from the CSV header, the
# parse aborts at the top level — caller gets a clear error message.
REQUIRED_COLUMNS = ('o-C href', 'o-o', 'o-j1', 'o-j5')

# Year acceptable range. Used cars older than 2010 are extreme outliers
# in our catalog; year > current+1 is a data error.
MIN_YEAR = 2010
MAX_YEAR_OFFSET = 1   # CURRENT_YEAR + this

# Mileage sanity bounds (km).
MIN_MILEAGE_KM = 0
MAX_MILEAGE_KM = 1_000_000

# Asking price sanity bounds (₹). Anything below 50,000 or above 10 Cr
# in the listing grid is a data error — not a real Bangalore listing.
MIN_PRICE_INR = 50_000
MAX_PRICE_INR = 100_000_000

# Listings always land with these defaults per spec section 6.
DEFAULT_DATA_QUALITY = 'medium'
DEFAULT_DATA_SOURCE = 'Listing Aggregator'

# CarWale URL pattern. We extract make-model slug from match group 2.
CARWALE_URL_PATTERN = re.compile(
    r'^https?://(?:www\.)?carwale\.com/used/([^/]+)/([^/]+)/[^/]+/?$',
    re.IGNORECASE,
)

# Skip reasons (returned in SkippedRow.reason for audit + admin UI display)
SKIP_MISSING_REQUIRED = 'missing_required_field'
SKIP_BAD_URL = 'unparseable_url'
SKIP_BAD_TITLE = 'unparseable_title'
SKIP_BAD_PRICE = 'unparseable_price'
SKIP_BAD_MILEAGE = 'unparseable_mileage'
SKIP_BAD_FUEL = 'missing_fuel'
SKIP_YEAR_OUT_OF_RANGE = 'year_out_of_range'
SKIP_PRICE_OUT_OF_RANGE = 'price_out_of_range'
SKIP_MILEAGE_OUT_OF_RANGE = 'mileage_out_of_range'
SKIP_MAKE_NOT_IN_CATALOG = 'make_not_in_catalog'
SKIP_MODEL_NOT_IN_CATALOG = 'model_not_in_catalog'
SKIP_VARIANT_REJECTED = 'variant_rejected_by_resolver'


# ============================================================
# RESULT TYPES
# ============================================================

class ParsedListingRow(NamedTuple):
    """A successfully parsed listing row, ready for research_log insertion."""
    listing_url:        str
    year:               int
    make:               str        # canonical (matches car_data.get_makes())
    model:              str        # canonical (matches car_data.get_models(make))
    variant:            str        # canonical (matches car_data.get_variants(make, model))
    fuel:               str        # 'Petrol' / 'Diesel' / 'CNG' / 'HEV' / 'PHEV' / 'BEV'
    asking_price:       int        # ₹
    mileage_km:         int
    locality:           Optional[str]   # e.g. "Yelahanka"
    city:               str             # always 'Bangalore' in v1
    state_code:         str             # always 'KA' in v1
    raw_title:          str             # for audit
    raw_variant:        str             # what the resolver matched FROM
    variant_confidence: int             # 0-100 from resolver
    needs_review:       bool            # True if resolver said needs_review
    data_quality:       str = DEFAULT_DATA_QUALITY
    data_source:        str = DEFAULT_DATA_SOURCE


class SkippedRow(NamedTuple):
    """A row we couldn't or wouldn't parse, with structured reason."""
    row_index:    int               # 1-based row number in the CSV
    reason:       str               # one of SKIP_* constants
    detail:       str               # human-readable specific cause
    raw_url:      Optional[str]
    raw_title:    Optional[str]


class ParseResult(NamedTuple):
    """Top-level outcome of parsing a CSV."""
    total_rows:     int
    parsed:         List[ParsedListingRow]
    skipped:        List[SkippedRow]


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _extract_url_slug(url: str) -> Optional[str]:
    """
    Extract the make-model slug from a CarWale listing URL.

    Example:
      https://www.carwale.com/used/bangalore/mercedes-benz-e-class/8mn0n12i/
      → 'mercedes-benz-e-class'

    Returns None if the URL doesn't match the expected pattern. We tolerate
    different city slugs (group 1) but require the structure to be intact.
    """
    if not url:
        return None
    m = CARWALE_URL_PATTERN.match(url.strip())
    if not m:
        return None
    return m.group(2).lower()


def _make_to_slug(make: str) -> str:
    """
    Convert a canonical car_data.py make name to its CarWale URL slug form.
    Example: 'Mercedes-Benz' → 'mercedes-benz', 'Maruti Suzuki' → 'maruti-suzuki'.
    """
    return make.lower().replace(' ', '-')


def _model_to_slug(model: str) -> str:
    """
    Convert a canonical model name to slug form.
    Example: 'Grand i10 Nios' → 'grand-i10-nios', 'S-Cross' → 's-cross',
             'C-Class' → 'c-class'.
    Note: model names already containing hyphens (S-Cross, C-Class) lowercase
    fine; multi-word models with spaces convert to hyphens.
    """
    return model.lower().replace(' ', '-')


def _resolve_make_model_from_slug(slug: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Match a CarWale URL slug to a canonical (make, model) pair from car_data.

    Strategy: iterate through all known makes (longest first, so multi-word
    makes like 'Mercedes-Benz' and 'Maruti Suzuki' are tried before single-
    word makes), and check if the slug starts with that make's slug form.
    If yes, the rest of the slug is the model — match against that make's
    known models, again longest-first.

    Returns (make, model) on success, or (None, None) / (make, None) on
    failure — caller decides what to do (skip vs flag).
    """
    if not slug:
        return (None, None)

    # Step 1: find the make. Sort makes by slug-length descending so multi-
    # token makes like 'mercedes-benz' beat 'mercedes' if we ever add a
    # standalone 'Mercedes' make in the future.
    candidate_makes = car_data.get_makes()
    make_slugs = sorted(
        ((_make_to_slug(m), m) for m in candidate_makes),
        key=lambda x: len(x[0]),
        reverse=True,
    )

    matched_make = None
    rest_after_make = None
    for make_slug, original_make in make_slugs:
        # Slug must START with make-slug followed by a hyphen (or be exactly
        # the make-slug, though that's unlikely since it'd mean no model).
        if slug == make_slug:
            matched_make = original_make
            rest_after_make = ''
            break
        prefix = make_slug + '-'
        if slug.startswith(prefix):
            matched_make = original_make
            rest_after_make = slug[len(prefix):]
            break

    if not matched_make:
        return (None, None)
    if not rest_after_make:
        # Slug had no model portion — shouldn't happen for real CarWale URLs.
        return (matched_make, None)

    # Step 2: find the model. Same longest-first strategy.
    candidate_models = car_data.get_models(matched_make)
    model_slugs = sorted(
        ((_model_to_slug(m), m) for m in candidate_models),
        key=lambda x: len(x[0]),
        reverse=True,
    )

    for model_slug, original_model in model_slugs:
        if rest_after_make == model_slug:
            return (matched_make, original_model)

    # No model match. We know the make but not the model — caller can
    # decide whether to skip (model_not_in_catalog) or to enqueue this for
    # admin review of the catalog.
    return (matched_make, None)


# Pre-compiled regex patterns for title parsing
TITLE_YEAR_PATTERN = re.compile(r'^\s*(\d{4})\s+(.*)$')
# Trailing "[...]" suffix CarWale appends for facelift versions
TITLE_BRACKET_SUFFIX_PATTERN = re.compile(r'\s*\[[^\]]*\]\s*$')


def _extract_variant_from_title(title: str, make: str, model: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Pull the year and the raw variant string out of a CarWale title.

    Title format: "{YYYY} {make_words} {model_words} {variant_words} [optional bracket]"
    Example:
      title="2022 Mercedes-Benz E-Class E 200 Exclusive [2021-2023]"
      make="Mercedes-Benz", model="E-Class"
      → year=2022, raw_variant="E 200 Exclusive"

    Returns (year, raw_variant) where either may be None if extraction fails.
    """
    if not title:
        return (None, None)

    s = title.strip()

    # Strip trailing [bracket]
    s = TITLE_BRACKET_SUFFIX_PATTERN.sub('', s).strip()

    # Pull off the leading 4-digit year
    m = TITLE_YEAR_PATTERN.match(s)
    if not m:
        return (None, None)
    year = int(m.group(1))
    after_year = m.group(2).strip()

    # Now strip make + model words from the front. We do this loosely — we
    # split into tokens and remove tokens that align with make + model
    # (case-insensitive). Make/model words may contain hyphens which we
    # normalize to spaces for the comparison.
    def _to_tokens(text: str) -> List[str]:
        return [t for t in re.split(r'[\s\-]+', text) if t]

    make_tokens = _to_tokens(make)
    model_tokens = _to_tokens(model)
    expected_tokens = [t.lower() for t in make_tokens + model_tokens]

    title_tokens = _to_tokens(after_year)
    title_tokens_lower = [t.lower() for t in title_tokens]

    # Remove the expected prefix tokens. If alignment fails (e.g. CarWale
    # uses a different model spelling), we fall back to removing only what
    # aligns and keep the rest as variant.
    consumed = 0
    for expected in expected_tokens:
        if consumed >= len(title_tokens_lower):
            break
        if title_tokens_lower[consumed] == expected:
            consumed += 1
        else:
            # Mismatch — stop consuming. Whatever's left becomes variant.
            break

    variant_tokens = title_tokens[consumed:]
    raw_variant = ' '.join(variant_tokens).strip()
    if not raw_variant:
        return (year, None)
    return (year, raw_variant)


# Mileage parser — handles "37,627 km", "1,07,000 km" (Indian commas), "37627 km"
MILEAGE_PATTERN = re.compile(r'([\d,]+)\s*km', re.IGNORECASE)


def _parse_mileage(s: str) -> Optional[int]:
    if not s:
        return None
    m = MILEAGE_PATTERN.search(s)
    if not m:
        return None
    digits = m.group(1).replace(',', '')
    try:
        return int(digits)
    except ValueError:
        return None


# Price parser — handles "Rs. 7.50 Lakh", "Rs. 1.4 Crore", "Rs. 13 Lakh"
PRICE_PATTERN = re.compile(
    r'rs\.?\s*([\d,]+(?:\.\d+)?)\s*(lakh|lac|cr(?:ore)?)',
    re.IGNORECASE,
)


def _parse_price(s: str) -> Optional[int]:
    """Returns ₹ as int. 'Rs. 7.50 Lakh' → 750000."""
    if not s:
        return None
    m = PRICE_PATTERN.search(s)
    if not m:
        return None
    num_str = m.group(1).replace(',', '')
    unit = m.group(2).lower()
    try:
        num = float(num_str)
    except ValueError:
        return None
    if unit.startswith('cr'):
        return int(round(num * 10_000_000))
    if unit.startswith('la'):  # 'lakh' or 'lac'
        return int(round(num * 100_000))
    return None


# Fuel normalization — CarWale uses these specific spellings
FUEL_NORMALIZE = {
    'petrol':         'Petrol',
    'diesel':         'Diesel',
    'cng':            'CNG',
    'hybrid':         'HEV',          # CarWale sometimes lists "Hybrid"
    'hybrid (electric + petrol)': 'HEV',
    'electric':       'BEV',
    'electric (bev)': 'BEV',
    'plug-in hybrid': 'PHEV',
    'phev':           'PHEV',
    'lpg':            'CNG',          # CarWale rare; map to CNG bucket
}


def _normalize_fuel(s: str) -> Optional[str]:
    if not s:
        return None
    return FUEL_NORMALIZE.get(s.strip().lower())


def _split_oj1(s: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    Split o-j1 like "37,627 km  |  Petrol  |  Yelahanka, Bangalore"
    into (mileage_str, fuel_str, locality, city).

    Returns Nones where parts are missing/malformed. Caller validates
    individual fields downstream.
    """
    if not s:
        return (None, None, None, None)
    parts = [p.strip() for p in s.split('|') if p.strip()]
    if len(parts) < 3:
        return (None, None, None, None)
    mileage_str = parts[0]
    fuel_str = parts[1]
    location_str = parts[2]
    # Locality, city — split on the first comma. If no comma, treat whole
    # thing as city and leave locality None.
    if ',' in location_str:
        locality, city = location_str.split(',', 1)
        return (mileage_str, fuel_str, locality.strip(), city.strip())
    return (mileage_str, fuel_str, None, location_str)


# ============================================================
# PUBLIC API
# ============================================================

def parse_carwale_csv(csv_bytes_or_text,
                      default_state: str = 'KA',
                      default_city: str = 'Bangalore') -> ParseResult:
    """
    Parse a CarWale Bangalore Instant Data Scraper CSV.

    Args:
      csv_bytes_or_text  CSV content as bytes (uploaded file) or str.
      default_state      Two-letter state code to attach to all rows (v1: 'KA').
      default_city       City name to attach when CarWale's o-j1 city
                         doesn't match (v1: 'Bangalore'). Most rows will
                         have city='Bangalore' inside o-j1 anyway.

    Returns:
      ParseResult(total_rows, parsed[], skipped[])

    Raises:
      ValueError if the CSV is missing required columns or is fundamentally
      malformed. Per-row failures are returned in skipped[], not raised.
    """
    # Normalize input → text. utf-8-sig strips BOM if present.
    if isinstance(csv_bytes_or_text, bytes):
        text = csv_bytes_or_text.decode('utf-8-sig')
    else:
        text = csv_bytes_or_text

    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    missing = [c for c in REQUIRED_COLUMNS if c not in fieldnames]
    if missing:
        raise ValueError(
            f"CSV missing required columns: {missing}. "
            f"Found columns: {fieldnames}. "
            "Make sure Instant Data Scraper picked the listing-card table "
            "(should include o-C href, o-o, o-j1, o-j5)."
        )

    parsed: List[ParsedListingRow] = []
    skipped: List[SkippedRow] = []
    total = 0

    for row_idx, row in enumerate(reader, start=1):
        total += 1
        url = (row.get('o-C href') or '').strip()
        title = (row.get('o-o') or '').strip()
        oj1 = (row.get('o-j1') or '').strip()
        price_str = (row.get('o-j5') or '').strip()

        # ---- Required field presence ----
        if not url or not title or not oj1 or not price_str:
            skipped.append(SkippedRow(
                row_index=row_idx,
                reason=SKIP_MISSING_REQUIRED,
                detail=f"url={'Y' if url else 'N'} title={'Y' if title else 'N'} "
                       f"oj1={'Y' if oj1 else 'N'} price={'Y' if price_str else 'N'}",
                raw_url=url or None,
                raw_title=title or None,
            ))
            continue

        # ---- URL slug → make/model ----
        slug = _extract_url_slug(url)
        if not slug:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_BAD_URL,
                detail=f"URL doesn't match CarWale pattern: {url[:80]}",
                raw_url=url, raw_title=title,
            ))
            continue

        make, model = _resolve_make_model_from_slug(slug)
        if not make:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_MAKE_NOT_IN_CATALOG,
                detail=f"slug='{slug}' has no matching make in catalog",
                raw_url=url, raw_title=title,
            ))
            continue
        if not model:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_MODEL_NOT_IN_CATALOG,
                detail=f"make='{make}' OK but model from slug '{slug}' not in catalog",
                raw_url=url, raw_title=title,
            ))
            continue

        # ---- Title → year + raw_variant ----
        year, raw_variant = _extract_variant_from_title(title, make, model)
        if not year:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_BAD_TITLE,
                detail=f"Could not extract year from title: {title}",
                raw_url=url, raw_title=title,
            ))
            continue
        current_year_plus = car_data.CURRENT_YEAR + MAX_YEAR_OFFSET
        if year < MIN_YEAR or year > current_year_plus:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_YEAR_OUT_OF_RANGE,
                detail=f"year={year}, valid range {MIN_YEAR}-{current_year_plus}",
                raw_url=url, raw_title=title,
            ))
            continue
        if not raw_variant:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_BAD_TITLE,
                detail=f"No variant text after stripping year+make+model: {title}",
                raw_url=url, raw_title=title,
            ))
            continue

        # ---- o-j1 → mileage + fuel + locality + city ----
        mileage_str, fuel_str, locality, city_in_listing = _split_oj1(oj1)
        if not mileage_str or not fuel_str:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_BAD_MILEAGE,
                detail=f"Could not split o-j1: {oj1}",
                raw_url=url, raw_title=title,
            ))
            continue

        mileage = _parse_mileage(mileage_str)
        if mileage is None:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_BAD_MILEAGE,
                detail=f"Unparseable mileage: {mileage_str}",
                raw_url=url, raw_title=title,
            ))
            continue
        if mileage < MIN_MILEAGE_KM or mileage > MAX_MILEAGE_KM:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_MILEAGE_OUT_OF_RANGE,
                detail=f"mileage={mileage} km out of range",
                raw_url=url, raw_title=title,
            ))
            continue

        fuel = _normalize_fuel(fuel_str)
        if not fuel:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_BAD_FUEL,
                detail=f"Unknown fuel: {fuel_str}",
                raw_url=url, raw_title=title,
            ))
            continue

        # ---- Price ----
        price = _parse_price(price_str)
        if price is None:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_BAD_PRICE,
                detail=f"Unparseable price: {price_str}",
                raw_url=url, raw_title=title,
            ))
            continue
        if price < MIN_PRICE_INR or price > MAX_PRICE_INR:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_PRICE_OUT_OF_RANGE,
                detail=f"price=₹{price:,} out of range",
                raw_url=url, raw_title=title,
            ))
            continue

        # ---- Variant resolver ----
        match = resolve_variant(make, model, raw_variant)
        if match.decision == DECISION_REJECTED:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_VARIANT_REJECTED,
                detail=f"raw_variant='{raw_variant}' → rejected (conf={match.confidence}, reason={match.reason})",
                raw_url=url, raw_title=title,
            ))
            continue

        # auto_match or needs_review → row is included
        canonical_variant = match.matched_variant
        needs_review = (match.decision == DECISION_NEEDS_REVIEW)

        # ---- All checks passed — emit the row ----
        parsed.append(ParsedListingRow(
            listing_url=url,
            year=year,
            make=make,
            model=model,
            variant=canonical_variant,
            fuel=fuel,
            asking_price=price,
            mileage_km=mileage,
            locality=locality,
            city=city_in_listing or default_city,
            state_code=default_state,
            raw_title=title,
            raw_variant=raw_variant,
            variant_confidence=match.confidence,
            needs_review=needs_review,
            data_quality=DEFAULT_DATA_QUALITY,
            data_source=DEFAULT_DATA_SOURCE,
        ))

    return ParseResult(total_rows=total, parsed=parsed, skipped=skipped)


# ============================================================
# DIAGNOSTICS — admin debugging only
# ============================================================

def summarize_parse_result(result: ParseResult) -> dict:
    """
    Group counts by skip reason for the admin upload-detail UI.
    Returns a plain dict — JSON-serializable.
    """
    by_reason = {}
    for s in result.skipped:
        by_reason[s.reason] = by_reason.get(s.reason, 0) + 1

    needs_review_count = sum(1 for p in result.parsed if p.needs_review)

    return {
        'total_rows':           result.total_rows,
        'parsed_count':         len(result.parsed),
        'parsed_auto_match':    len(result.parsed) - needs_review_count,
        'parsed_needs_review':  needs_review_count,
        'skipped_count':        len(result.skipped),
        'skipped_by_reason':    by_reason,
    }
