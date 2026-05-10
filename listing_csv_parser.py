"""
listing_csv_parser.py
---------------------
AutoKnowMus — Listing Calibration Pipeline · Step 3 (parser, v2 paste-aware).

PURPOSE
  Parse listings into structured ParsedListingRow objects ready to insert
  into research_log with data_source='Listing Aggregator'. Two entry points:

    parse_carwale_csv(csv_bytes_or_text, ...)
        v1 — Instant Data Scraper CSV uploads. UNTOUCHED in v2: same input
        contract, same output shape. The old admin CSV upload route keeps
        working without code changes.

    parse_carwale_paste(raw_text, ...)                  ← v2 NEW
        Paste extractor. User browses CarWale in their normal browser,
        Ctrl+A → Ctrl+C, pastes into AutoKnowMus admin. Server extracts
        listings via regex anchors on the repeating block structure.

  Skipped rows (parse failures, unknown makes/models, variant mismatches)
  are returned alongside as SkippedRow objects with a reason — drives the
  admin upload-detail UI and the calibration skip-list.

CARWALE PASTE BLOCK STRUCTURE (v2 anchor)
  Every real listing on a CarWale results page has this 3-line shape,
  with optional badge/feature lines around it:

      {YYYY} {Make} {Model} {Variant} [optional [bracket-suffix]]
      {nn,nnn} km  |  {Fuel}  |  {Locality}, {City}
      Rs. {N.NN} {Lakh|Crore}

  The first line and second line are tightly bound — second line MUST
  appear within ~10 lines of the first (between them, CarWale may inject
  "Featured", "Dealers Logo", "Excellent Condition", etc.).

  The price line typically follows immediately after the km|fuel|location
  line, but CarWale sometimes inserts "Rs. X.XX Lakh\nRs. Y.YY Lakh"
  (was/now strikethrough) — in that case we take the FIRST price as
  the current asking price.

CHROME REJECTION HEURISTICS (v2)
  These patterns mean "this is widget chrome, not a listing":
    - "Used X" / "Used X Cars" / "Avg. Price" headers
    - Brand names followed by "(NNN)" filter counts
    - Pagination ("Back / 1 / 2 / 3 / Next")
    - FAQ headings, footer text
    - Ad disclaimers ("AD")

  We don't try to reject these explicitly — instead, we ANCHOR on the
  repeating 3-line listing structure and ignore everything that doesn't
  fit. False-positive risk is near-zero because all three lines must
  match in sequence.

PARSING STRATEGY (locked)

  CSV path (v1, unchanged):
    1. URL slug → (make, model) via _resolve_make_model_from_slug
    2. Title → (year, raw_variant) via _extract_variant_from_title
    3. o-j1 → (mileage, fuel, locality, city) via _split_oj1
    4. o-j5 → price via _parse_price
    5. variant_resolver decides auto_match / needs_review / rejected

  Paste path (v2 new):
    1. Pre-clean: drop empty lines, normalize whitespace
    2. Sliding-window scan: find lines matching TITLE_LINE_PATTERN
       (starts with "YYYY ", at least 4 more words after the year)
    3. For each title-line candidate, look ahead up to 10 non-blank
       lines for KM_FUEL_LOC_PATTERN
    4. From the km/fuel/loc line, look ahead up to 6 non-blank lines
       for PRICE_PATTERN
    5. Resolve (make, model) by SLUG-FREE catalog walk: scan the title
       text for the longest known make+model prefix
    6. Same variant_resolver fuzzy match as CSV path
    7. Same skip-categorization, same ParsedListingRow output shape

ParsedListingRow GAINED ONE FIELD IN v2:
    locality: Optional[str]  — was already present in v1; kept identical
                                shape for downstream insert function
    (No schema change needed — locality column added in Migration 10.)

DATA QUALITY
  CSV path:    data_source='Listing Aggregator', marketplace='CarWale'
  Paste path:  data_source='Listing Aggregator', marketplace='CarWale'
               (paste extractor only knows CarWale layout in v2)
  Both:        data_source_type='asking_price' (haircut applies)
               data_quality_tier='aggregator_paste_listings' (65% ceiling)
               data_quality='medium' (legacy 3-bucket field, unchanged)

  These tags get applied by the INSERT function, NOT this parser.
  This parser only emits ParsedListingRow with extracted fields.

PUBLIC API (v2)
  parse_carwale_csv(csv_bytes_or_text, default_state='KA', default_city='Bangalore')
  parse_carwale_paste(raw_text, default_state='KA', default_city='Bangalore')
      → ParseResult(parsed: List[ParsedListingRow], skipped: List[SkippedRow])

  ParsedListingRow — per-row output ready for research_log insertion.
  SkippedRow — row that didn't make it, with structured reason.
  ParseResult — top-level outcome.

USAGE
  # CSV (admin file upload)
  result = parse_carwale_csv(uploaded_file.read())

  # Paste (admin textarea)
  result = parse_carwale_paste(textarea_value)

  # Both produce identical output shape — same downstream insert handles both.
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
# CONSTANTS — CarWale (v1 + v2)
# ============================================================

# CSV header columns (v1, unchanged)
REQUIRED_COLUMNS = ('o-C href', 'o-o', 'o-j1', 'o-j5')

# Year acceptable range. Used cars older than 2010 are extreme outliers
# in our catalog; year > current+1 is a data error.
MIN_YEAR = 2010
MAX_YEAR_OFFSET = 1   # CURRENT_YEAR + this

# Mileage sanity bounds (km).
MIN_MILEAGE_KM = 0
MAX_MILEAGE_KM = 1_000_000

# Asking price sanity bounds (₹). Below 50K or above 10 Cr in the listing
# grid is a data error — not a real Bangalore listing.
MIN_PRICE_INR = 50_000
MAX_PRICE_INR = 100_000_000

# Defaults applied to every row (v1 unchanged)
DEFAULT_DATA_QUALITY = 'medium'
DEFAULT_DATA_SOURCE = 'Listing Aggregator'

# CarWale URL pattern. v1 only.
CARWALE_URL_PATTERN = re.compile(
    r'^https?://(?:www\.)?carwale\.com/used/([^/]+)/([^/]+)/[^/]+/?$',
    re.IGNORECASE,
)

# v2: paste-mode block detection patterns
# A title line starts with a 4-digit year, then one or more words.
# Real titles always have ≥3 tokens after the year (Make + Model + Variant).
TITLE_LINE_PATTERN = re.compile(r'^\s*(\d{4})\s+([A-Za-z][\w\-\.\s\(\)\[\]\+/]+)$')

# v2: km|fuel|locality|city line — Pipe separators with optional whitespace.
# Example: "37,627 km  |  Petrol  |  Yelahanka, Bangalore"
KM_FUEL_LOC_PATTERN = re.compile(
    r'^\s*([\d,]+)\s*km\s*\|\s*([A-Za-z][A-Za-z\s\(\)\+\-]+?)\s*\|\s*(.+?)\s*$',
    re.IGNORECASE,
)

# v2: price line — "Rs. N.NN Lakh", "Rs N.NN Lakh", "Rs. N Crore", etc.
# Strict: must start with "Rs" (case-insensitive). Loose patterns inside
# tooltips/promos use different prefixes ("EMI at Rs.6,299" → not a listing
# price; we explicitly skip lines starting with "EMI").
LISTING_PRICE_PATTERN = re.compile(
    r'^\s*rs\.?\s*([\d,]+(?:\.\d+)?)\s*(lakh|lac|cr(?:ore)?)\s*$',
    re.IGNORECASE,
)

# v2: hard reject lines — these signal we're inside a chrome widget,
# not a listing. If we see one of these BETWEEN a title-line and a
# km|fuel|loc line, abandon that title-line candidate.
CHROME_REJECT_PATTERNS = [
    re.compile(r'^\s*used\s+[\w\s]+(?:cars?)\s*$', re.IGNORECASE),  # "Used Swift" / "Used Hyundai Cars"
    re.compile(r'^\s*\d+\s+used\s+', re.IGNORECASE),                # "181 Used Swift"
    re.compile(r'^\s*rs\s+\d+(?:\.\d+)?\s+lakh\s+avg', re.IGNORECASE),  # "Rs 5 Lakh Avg. Price"
    re.compile(r'^\s*\d+\s*\+\s*cars?\s*$', re.IGNORECASE),         # "540+ Cars"
    re.compile(r'^\s*back\s*$', re.IGNORECASE),                     # pagination
    re.compile(r'^\s*next\s*$', re.IGNORECASE),                     # pagination
]

# v2: pure noise lines we want to silently strip when sliding the window.
# These don't reject a listing block; they just get skipped during the
# look-ahead scan from title → km/fuel/loc.
NOISE_LINE_PATTERNS = [
    re.compile(r'^\s*$'),                                           # blank
    re.compile(r'^\s*ad\s*$', re.IGNORECASE),                       # "AD"
    re.compile(r'^\s*featured\s*$', re.IGNORECASE),
    re.compile(r'^\s*dealers?\s*logo\s*$', re.IGNORECASE),
    re.compile(r'^\s*key\s+(highlights?|features?)\s*$', re.IGNORECASE),
    re.compile(r'^\s*excellent\s+condition\s*$', re.IGNORECASE),
    re.compile(r'^\s*premium\s+variant\s*$', re.IGNORECASE),
    re.compile(r'^\s*certified(\s+car)?\s*$', re.IGNORECASE),
    re.compile(r'^\s*home\s+test\s+drive\s*$', re.IGNORECASE),
    re.compile(r'^\s*test\s+drive\s+available\s*$', re.IGNORECASE),
    re.compile(r'^\s*service\s+history\s+available\s*$', re.IGNORECASE),
    re.compile(r'^\s*finance\s+offers?\s*$', re.IGNORECASE),
    re.compile(r'^\s*quality\s+report\s*$', re.IGNORECASE),
    re.compile(r'^\s*direct\s+owner\s+car\s*$', re.IGNORECASE),
    re.compile(r'^\s*eligible\s+for\s+', re.IGNORECASE),
    re.compile(r'^\s*eligible\s+for\s+warranty\s*$', re.IGNORECASE),
    re.compile(r'^\s*\w+\s+warranty\s+included\s*$', re.IGNORECASE),
    re.compile(r'^\s*under\s+oem\s+warranty\s*$', re.IGNORECASE),
    re.compile(r'^\s*less\s+driven\s*$', re.IGNORECASE),
    re.compile(r'^\s*great\s+price\s*$', re.IGNORECASE),
    re.compile(r'^\s*make\s+offer\s*$', re.IGNORECASE),
    re.compile(r'^\s*emi\s+at\s*$', re.IGNORECASE),
    re.compile(r'^\s*emi\s+at\s+rs', re.IGNORECASE),
    re.compile(r'^\s*rs\.?\s*[\d,]+\s*(?:l|cr)?\s*$', re.IGNORECASE),  # bare EMI value "Rs.6,299"
    re.compile(r'^\s*panoramic\s+sunroof\s*$', re.IGNORECASE),
    re.compile(r'^\s*electrically\s+adjustable\s+sunroof\s*$', re.IGNORECASE),
    re.compile(r'^\s*chrome\s+finish\s+exhaust\s*$', re.IGNORECASE),
    re.compile(r'^\s*\d+\s*x\s*12v\s+power', re.IGNORECASE),
    re.compile(r'^\s*12v\s+power\s+outlet', re.IGNORECASE),
    re.compile(r'^\s*manual\s+air\s+conditioner', re.IGNORECASE),
    re.compile(r'^\s*automatic\s+climate\s+control', re.IGNORECASE),
    re.compile(r'^\s*lane\s+departure', re.IGNORECASE),
    re.compile(r'^\s*rear\s+middle', re.IGNORECASE),
    re.compile(r'^\s*body\s+coloured', re.IGNORECASE),
    re.compile(r'^\s*passenger\s+airbag', re.IGNORECASE),
    re.compile(r'^\s*manual\s+fuel', re.IGNORECASE),
    re.compile(r'^\s*cabin\s+lamp', re.IGNORECASE),
    re.compile(r'^\s*low\s+fuel', re.IGNORECASE),
    re.compile(r'^\s*aux\s+compatibility', re.IGNORECASE),
    re.compile(r'^\s*front\s*(?:&\s*rear\s*)?power\s+windows', re.IGNORECASE),
    re.compile(r'^\s*second\s+hand\s+', re.IGNORECASE),  # "Second Hand X in Bangalore" — image alt-text echo
]

# Window sizes for paste-mode scanning
TITLE_TO_KMFUEL_LOOKAHEAD = 12   # max non-noise lines from title to km|fuel|loc line
KMFUEL_TO_PRICE_LOOKAHEAD = 8    # max non-noise lines from km|fuel|loc to price


# Skip reasons (returned in SkippedRow.reason)
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
SKIP_INCOMPLETE_BLOCK = 'incomplete_paste_block'  # v2: title found but no km/price


# ============================================================
# RESULT TYPES — unchanged in v2 except for `locality`
# ============================================================

class ParsedListingRow(NamedTuple):
    """A successfully parsed listing row, ready for research_log insertion."""
    listing_url:        Optional[str]    # v2: optional (paste-mode rows have no URL)
    year:               int
    make:               str
    model:              str
    variant:            str
    fuel:               str
    asking_price:       int
    mileage_km:         int
    locality:           Optional[str]
    city:               str
    state_code:         str
    raw_title:          str
    raw_variant:        str
    variant_confidence: int
    needs_review:       bool
    data_quality:       str = DEFAULT_DATA_QUALITY
    data_source:        str = DEFAULT_DATA_SOURCE


class SkippedRow(NamedTuple):
    """A row we couldn't or wouldn't parse, with structured reason."""
    row_index:    int
    reason:       str
    detail:       str
    raw_url:      Optional[str]
    raw_title:    Optional[str]


class ParseResult(NamedTuple):
    """Top-level outcome of parsing."""
    total_rows:     int
    parsed:         List[ParsedListingRow]
    skipped:        List[SkippedRow]


# ============================================================
# INTERNAL HELPERS — shared by CSV and paste paths
# ============================================================

def _extract_url_slug(url: str) -> Optional[str]:
    """v1: extract make-model slug from CarWale URL. Used by CSV path only."""
    if not url:
        return None
    m = CARWALE_URL_PATTERN.match(url.strip())
    if not m:
        return None
    return m.group(2).lower()


def _make_to_slug(make: str) -> str:
    return make.lower().replace(' ', '-')


def _model_to_slug(model: str) -> str:
    return model.lower().replace(' ', '-')


def _resolve_make_model_from_slug(slug: str) -> Tuple[Optional[str], Optional[str]]:
    """v1: match URL slug to canonical (make, model). Used by CSV path only."""
    if not slug:
        return (None, None)

    candidate_makes = car_data.get_makes()
    make_slugs = sorted(
        ((_make_to_slug(m), m) for m in candidate_makes),
        key=lambda x: len(x[0]),
        reverse=True,
    )

    matched_make = None
    rest_after_make = None
    for make_slug, original_make in make_slugs:
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
        return (matched_make, None)

    candidate_models = car_data.get_models(matched_make)
    model_slugs = sorted(
        ((_model_to_slug(m), m) for m in candidate_models),
        key=lambda x: len(x[0]),
        reverse=True,
    )

    for model_slug, original_model in model_slugs:
        if rest_after_make == model_slug:
            return (matched_make, original_model)

    return (matched_make, None)


def _resolve_make_model_from_title_text(title_after_year: str) -> Tuple[Optional[str], Optional[str], str]:
    """
    v2 paste-mode helper: given the text AFTER the year was stripped from a
    title line (e.g. "Mercedes-Benz E-Class E 200 Exclusive [2021-2023]"),
    walk the catalog longest-make-first, longest-model-first, to peel off
    the (make, model) prefix.

    Returns (make, model, remaining_text) where remaining_text is what's
    left after the make+model prefix is consumed (i.e. the variant text).

    Returns (None, None, original_text) if no make in our catalog appears
    as a prefix.

    Hyphenation tolerance: CarWale sometimes writes "BMW 3-Series" and
    sometimes "BMW 3 Series". We normalize both halves of the comparison
    by stripping hyphens before matching.
    """
    if not title_after_year:
        return (None, None, '')

    # v2: normalize hyphens to spaces in the title for comparison purposes
    # (we keep the original around for the variant text remainder).
    normalized_title = title_after_year.replace('-', ' ').replace('  ', ' ').strip()
    normalized_title_lower = normalized_title.lower()

    candidate_makes = car_data.get_makes()
    # Try long makes first ("Mercedes-Benz", "Maruti Suzuki", "Land Rover",
    # "Aston Martin", "Rolls-Royce") so they beat single-word prefixes.
    make_candidates = sorted(
        candidate_makes,
        key=lambda m: len(m.replace('-', ' ').replace('  ', ' ')),
        reverse=True,
    )

    matched_make = None
    title_after_make_lower = ''
    chars_consumed = 0  # how many chars of the ORIGINAL we ate

    for make in make_candidates:
        make_norm = make.replace('-', ' ').replace('  ', ' ').strip().lower()
        if not make_norm:
            continue
        # Must match at the start, followed by a space (or end-of-string).
        if normalized_title_lower == make_norm:
            matched_make = make
            chars_consumed = len(title_after_year)
            title_after_make_lower = ''
            break
        prefix_with_space = make_norm + ' '
        if normalized_title_lower.startswith(prefix_with_space):
            matched_make = make
            # Find equivalent position in the ORIGINAL string. The original
            # may have hyphens where normalized has spaces, so we walk both
            # in lock-step to find where the make ends in the original.
            chars_consumed = _find_original_position(
                title_after_year, len(prefix_with_space)
            )
            title_after_make_lower = normalized_title_lower[len(prefix_with_space):].strip()
            break

    if not matched_make:
        return (None, None, title_after_year)

    if not title_after_make_lower:
        # Title was just "{year} {make}" with no model — almost never happens
        # for real listings. Treat as unresolved.
        return (matched_make, None, title_after_year[chars_consumed:].strip())

    # Now match model against catalog for this make
    candidate_models = car_data.get_models(matched_make)
    model_candidates = sorted(
        candidate_models,
        key=lambda m: len(m.replace('-', ' ').replace('  ', ' ')),
        reverse=True,
    )

    for model in model_candidates:
        model_norm = model.replace('-', ' ').replace('  ', ' ').strip().lower()
        if not model_norm:
            continue
        if title_after_make_lower == model_norm:
            # Title was exactly "{year} {make} {model}" — variant is empty
            return (matched_make, model, '')
        prefix_with_space = model_norm + ' '
        if title_after_make_lower.startswith(prefix_with_space):
            # Find where the model ends in the original (post-make) text.
            after_make_original = title_after_year[chars_consumed:]
            model_chars_consumed = _find_original_position(
                after_make_original, len(prefix_with_space)
            )
            variant_text = after_make_original[model_chars_consumed:].strip()
            return (matched_make, model, variant_text)

    # Make matched but model didn't
    return (matched_make, None, title_after_year[chars_consumed:].strip())


def _find_original_position(original: str, normalized_chars: int) -> int:
    """
    Helper for hyphen-tolerant matching. Given an original string that may
    contain hyphens and a number of CHARS we want to consume in the
    hyphen-normalized version, returns the number of chars to consume in
    the original.

    Walks both strings in lock-step: hyphens in original count as space
    in normalized.
    """
    orig_idx = 0
    norm_idx = 0
    while orig_idx < len(original) and norm_idx < normalized_chars:
        c = original[orig_idx]
        if c == '-':
            # Hyphen in original = space in normalized. Both advance 1.
            orig_idx += 1
            norm_idx += 1
        else:
            orig_idx += 1
            norm_idx += 1
    return orig_idx


# Pre-compiled regex patterns for title parsing (v1, used by both paths)
TITLE_YEAR_PATTERN = re.compile(r'^\s*(\d{4})\s+(.*)$')
TITLE_BRACKET_SUFFIX_PATTERN = re.compile(r'\s*\[[^\]]*\]\s*$')


def _strip_year_and_brackets(title: str) -> Tuple[Optional[int], Optional[str]]:
    """
    Common preprocessing for both CSV and paste paths.
    Strips trailing [bracket] and the leading 4-digit year.

    Returns (year, text_after_year). Either may be None on failure.
    Note: bracket suffix is stripped from text BEFORE returning so the
    caller doesn't need to deal with it.
    """
    if not title:
        return (None, None)
    s = title.strip()
    s = TITLE_BRACKET_SUFFIX_PATTERN.sub('', s).strip()
    m = TITLE_YEAR_PATTERN.match(s)
    if not m:
        return (None, None)
    return (int(m.group(1)), m.group(2).strip())


def _extract_variant_from_title(title: str, make: str, model: str) -> Tuple[Optional[int], Optional[str]]:
    """
    v1 CSV-path helper: pull year and raw_variant from a title GIVEN the
    make and model are already known (from the URL slug).

    For v2 paste-mode the make/model are NOT known yet — see
    _resolve_make_model_from_title_text() instead.
    """
    if not title:
        return (None, None)

    year, after_year = _strip_year_and_brackets(title)
    if year is None or after_year is None:
        return (None, None)

    # Token-aligned strip of make+model from the front
    def _to_tokens(text: str) -> List[str]:
        return [t for t in re.split(r'[\s\-]+', text) if t]

    make_tokens = _to_tokens(make)
    model_tokens = _to_tokens(model)
    expected_tokens = [t.lower() for t in make_tokens + model_tokens]

    title_tokens = _to_tokens(after_year)
    title_tokens_lower = [t.lower() for t in title_tokens]

    consumed = 0
    for expected in expected_tokens:
        if consumed >= len(title_tokens_lower):
            break
        if title_tokens_lower[consumed] == expected:
            consumed += 1
        else:
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


# Price parser — "Rs. 7.50 Lakh", "Rs. 1.4 Crore", "Rs. 13 Lakh"
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
    if unit.startswith('la'):
        return int(round(num * 100_000))
    return None


# Fuel normalization — CarWale spellings
FUEL_NORMALIZE = {
    'petrol':                       'Petrol',
    'diesel':                       'Diesel',
    'cng':                          'CNG',
    'hybrid':                       'HEV',
    'hybrid (electric + petrol)':   'HEV',
    'hybrid (electric+petrol)':     'HEV',
    'electric':                     'BEV',
    'electric (bev)':                'BEV',
    'plug-in hybrid':               'PHEV',
    'phev':                         'PHEV',
    'lpg':                          'CNG',
}


def _normalize_fuel(s: str) -> Optional[str]:
    if not s:
        return None
    return FUEL_NORMALIZE.get(s.strip().lower())


def _split_oj1(s: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    v1: split o-j1 like "37,627 km  |  Petrol  |  Yelahanka, Bangalore"
    into (mileage_str, fuel_str, locality, city).
    """
    if not s:
        return (None, None, None, None)
    parts = [p.strip() for p in s.split('|') if p.strip()]
    if len(parts) < 3:
        return (None, None, None, None)
    mileage_str = parts[0]
    fuel_str = parts[1]
    location_str = parts[2]
    if ',' in location_str:
        locality, city = location_str.split(',', 1)
        return (mileage_str, fuel_str, locality.strip(), city.strip())
    return (mileage_str, fuel_str, None, location_str)


# ============================================================
# v2: PASTE-MODE BLOCK SCANNER
# ============================================================

def _line_is_noise(line: str) -> bool:
    """Returns True if the line is feature-chip / chrome / EMI / etc."""
    for pattern in NOISE_LINE_PATTERNS:
        if pattern.match(line):
            return True
    return False


def _line_is_chrome_reject(line: str) -> bool:
    """Returns True if the line indicates we're inside a widget (not a real listing block)."""
    for pattern in CHROME_REJECT_PATTERNS:
        if pattern.match(line):
            return True
    return False


class _PasteBlock(NamedTuple):
    """Internal: a candidate listing block found in pasted text."""
    title:        str         # raw title line
    kmfuelloc:    str         # raw "X km | Fuel | Locality, City" line
    price_line:   str         # raw "Rs. X Lakh" line
    title_lineno: int         # 1-indexed source-line number for skip-list audit


def _find_paste_blocks(raw_text: str) -> List[_PasteBlock]:
    """
    v2: Scan raw pasted text and return a list of candidate listing blocks.

    Strategy (sliding window):
      1. Walk lines top-to-bottom.
      2. When we see a TITLE_LINE_PATTERN match, treat it as a candidate.
      3. From that line, look ahead up to TITLE_TO_KMFUEL_LOOKAHEAD non-noise
         lines for a KM_FUEL_LOC_PATTERN match. Hard-reject if we hit a
         chrome line or another title line first.
      4. From the km/fuel/loc line, look ahead up to KMFUEL_TO_PRICE_LOOKAHEAD
         non-noise lines for a LISTING_PRICE_PATTERN match. Hit chrome → abandon.
      5. If all 3 lines found, emit a _PasteBlock and continue scanning AFTER
         the price line (no overlap).
      6. If title line had no matching km/fuel/loc within window, skip it
         and continue from the next line (it was probably a heading or
         widget element that happened to start with "{year}").

    Note: we INTENTIONALLY don't emit blocks that have a title but no
    km/fuel/loc — those are added to the skipped[] list by the caller
    via the SKIP_INCOMPLETE_BLOCK reason.
    """
    blocks: List[_PasteBlock] = []
    lines = raw_text.split('\n')
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i].rstrip()

        # Look for a title-line candidate
        title_match = TITLE_LINE_PATTERN.match(line)
        if not title_match:
            i += 1
            continue

        # Sanity: the year must be plausible.
        try:
            year = int(title_match.group(1))
        except ValueError:
            i += 1
            continue
        # Block-finder is permissive on year range — final validation is done
        # downstream (MIN_YEAR / current_year+1). But we still need to filter
        # obvious garbage (e.g. "12345 something" matching the regex).
        if year < 1990 or year > 2100:
            i += 1
            continue

        title_line = line
        title_lineno = i + 1

        # Look ahead for km/fuel/loc line
        kmfuelloc_line = None
        kmfuelloc_idx = None
        for j in range(i + 1, min(n, i + 1 + 50)):  # hard cap so chrome can't run forever
            scan = lines[j].rstrip()

            # Skip noise lines without consuming the lookahead budget
            if _line_is_noise(scan):
                continue

            # Bail if we hit obvious chrome
            if _line_is_chrome_reject(scan):
                break

            # Bail if we hit another title line — that means current title
            # had no km/fuel/loc partner
            if TITLE_LINE_PATTERN.match(scan):
                break

            # Try to match km/fuel/loc
            if KM_FUEL_LOC_PATTERN.match(scan):
                kmfuelloc_line = scan
                kmfuelloc_idx = j
                break

            # Otherwise count this as a "non-noise non-match" line and
            # decrement our patience budget
            # Implemented via the index range cap (50 lines max above).

        if kmfuelloc_line is None:
            # Title had no matching km/fuel/loc in window. Skip this title;
            # don't backtrack — most "stray title" cases are widget headings.
            i += 1
            continue

        # Look ahead for price line from km/fuel/loc
        price_line = None
        price_idx = None
        for j in range(kmfuelloc_idx + 1, min(n, kmfuelloc_idx + 1 + 30)):
            scan = lines[j].rstrip()

            if _line_is_noise(scan):
                continue
            if _line_is_chrome_reject(scan):
                break
            if TITLE_LINE_PATTERN.match(scan):
                break

            if LISTING_PRICE_PATTERN.match(scan):
                price_line = scan
                price_idx = j
                break

        if price_line is None:
            # Title + km/fuel/loc but no price → not a listing block. Skip.
            i += 1
            continue

        # All three lines found — emit the block
        blocks.append(_PasteBlock(
            title=title_line,
            kmfuelloc=kmfuelloc_line,
            price_line=price_line,
            title_lineno=title_lineno,
        ))
        # Advance past the price line so we don't double-count
        i = price_idx + 1

    return blocks


# ============================================================
# PUBLIC API — parse_carwale_csv (v1, UNCHANGED)
# ============================================================

def parse_carwale_csv(csv_bytes_or_text,
                      default_state: str = 'KA',
                      default_city: str = 'Bangalore') -> ParseResult:
    """
    v1 — Parse a CarWale Bangalore Instant Data Scraper CSV.
    UNCHANGED in v2. Existing admin_listing_calibration_upload route keeps
    working without modification.

    See module docstring for full contract. Returns ParseResult.
    Raises ValueError on top-level CSV parse errors.
    """
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

        if not url or not title or not oj1 or not price_str:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_MISSING_REQUIRED,
                detail=f"url={'Y' if url else 'N'} title={'Y' if title else 'N'} "
                       f"oj1={'Y' if oj1 else 'N'} price={'Y' if price_str else 'N'}",
                raw_url=url or None, raw_title=title or None,
            ))
            continue

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

        match = resolve_variant(make, model, raw_variant)
        if match.decision == DECISION_REJECTED:
            skipped.append(SkippedRow(
                row_index=row_idx, reason=SKIP_VARIANT_REJECTED,
                detail=f"raw_variant='{raw_variant}' → rejected (conf={match.confidence}, reason={match.reason})",
                raw_url=url, raw_title=title,
            ))
            continue

        canonical_variant = match.matched_variant
        needs_review = (match.decision == DECISION_NEEDS_REVIEW)

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
# PUBLIC API — parse_carwale_paste (v2 NEW)
# ============================================================

def parse_carwale_paste(raw_text: str,
                        default_state: str = 'KA',
                        default_city: str = 'Bangalore') -> ParseResult:
    """
    v2 — Parse browser-pasted CarWale results page text.

    Args:
      raw_text       The full page text the admin pasted (Ctrl+A → Ctrl+C
                     from a CarWale "Used Cars in {city}" page).
      default_state  State to attach to all rows (v2: 'KA').
      default_city   City fallback if a listing doesn't include one.

    Returns:
      ParseResult(total_rows, parsed[], skipped[])

    The total_rows count is the number of CANDIDATE BLOCKS found (i.e.,
    title+km/fuel/loc+price triples). Rows that fail downstream validation
    (year range, variant resolver, etc.) go to skipped[]. Title lines that
    didn't have matching km/fuel/loc partners are NOT counted (they're
    treated as page chrome, not failed listings).

    Never raises on a single bad block — bad blocks go to skipped[] with
    a reason. Caller handles empty-text edge case.
    """
    if not raw_text or not raw_text.strip():
        return ParseResult(total_rows=0, parsed=[], skipped=[])

    blocks = _find_paste_blocks(raw_text)

    parsed: List[ParsedListingRow] = []
    skipped: List[SkippedRow] = []
    total = len(blocks)

    for block_idx, block in enumerate(blocks, start=1):
        # row_index reflects the source-line number of the title for audit
        row_index = block.title_lineno

        # ---- 1. Strip year + bracket from title ----
        year, after_year = _strip_year_and_brackets(block.title)
        if year is None or not after_year:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_BAD_TITLE,
                detail=f"Could not extract year from title: {block.title}",
                raw_url=None, raw_title=block.title,
            ))
            continue

        current_year_plus = car_data.CURRENT_YEAR + MAX_YEAR_OFFSET
        if year < MIN_YEAR or year > current_year_plus:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_YEAR_OUT_OF_RANGE,
                detail=f"year={year}, valid range {MIN_YEAR}-{current_year_plus}",
                raw_url=None, raw_title=block.title,
            ))
            continue

        # ---- 2. Resolve make + model from title text (no URL slug) ----
        make, model, raw_variant = _resolve_make_model_from_title_text(after_year)
        if not make:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_MAKE_NOT_IN_CATALOG,
                detail=f"No matching make in catalog for title text: {after_year[:80]}",
                raw_url=None, raw_title=block.title,
            ))
            continue
        if not model:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_MODEL_NOT_IN_CATALOG,
                detail=f"make='{make}' OK but model not in catalog: {after_year[:80]}",
                raw_url=None, raw_title=block.title,
            ))
            continue
        if not raw_variant:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_BAD_TITLE,
                detail=f"No variant text after stripping year+make+model: {block.title}",
                raw_url=None, raw_title=block.title,
            ))
            continue

        # ---- 3. Parse km/fuel/locality/city from second line ----
        kmf_match = KM_FUEL_LOC_PATTERN.match(block.kmfuelloc)
        if not kmf_match:
            # This shouldn't happen — block-finder already validated this regex.
            # Defensive guard against future regex drift.
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_BAD_MILEAGE,
                detail=f"km/fuel/loc line failed re-match: {block.kmfuelloc}",
                raw_url=None, raw_title=block.title,
            ))
            continue

        mileage_str = kmf_match.group(1)
        fuel_str = kmf_match.group(2).strip()
        location_str = kmf_match.group(3).strip()

        mileage = _parse_mileage(mileage_str + ' km')
        if mileage is None:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_BAD_MILEAGE,
                detail=f"Unparseable mileage: {mileage_str}",
                raw_url=None, raw_title=block.title,
            ))
            continue
        if mileage < MIN_MILEAGE_KM or mileage > MAX_MILEAGE_KM:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_MILEAGE_OUT_OF_RANGE,
                detail=f"mileage={mileage} km out of range",
                raw_url=None, raw_title=block.title,
            ))
            continue

        fuel = _normalize_fuel(fuel_str)
        if not fuel:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_BAD_FUEL,
                detail=f"Unknown fuel: {fuel_str}",
                raw_url=None, raw_title=block.title,
            ))
            continue

        # Split locality, city
        if ',' in location_str:
            locality, city_in_listing = location_str.split(',', 1)
            locality = locality.strip()
            city_in_listing = city_in_listing.strip()
        else:
            locality = None
            city_in_listing = location_str

        # ---- 4. Parse price from third line ----
        price = _parse_price(block.price_line)
        if price is None:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_BAD_PRICE,
                detail=f"Unparseable price: {block.price_line}",
                raw_url=None, raw_title=block.title,
            ))
            continue
        if price < MIN_PRICE_INR or price > MAX_PRICE_INR:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_PRICE_OUT_OF_RANGE,
                detail=f"price=₹{price:,} out of range",
                raw_url=None, raw_title=block.title,
            ))
            continue

        # ---- 5. Variant resolver ----
        match = resolve_variant(make, model, raw_variant)
        if match.decision == DECISION_REJECTED:
            skipped.append(SkippedRow(
                row_index=row_index, reason=SKIP_VARIANT_REJECTED,
                detail=f"raw_variant='{raw_variant}' → rejected (conf={match.confidence}, reason={match.reason})",
                raw_url=None, raw_title=block.title,
            ))
            continue

        canonical_variant = match.matched_variant
        needs_review = (match.decision == DECISION_NEEDS_REVIEW)

        # ---- 6. Emit the row ----
        parsed.append(ParsedListingRow(
            listing_url=None,                       # paste-mode rows have no URL
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
            raw_title=block.title,
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
