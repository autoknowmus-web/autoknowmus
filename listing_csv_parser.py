"""
listing_csv_parser.py
---------------------
AutoKnowMus — Listing Calibration Pipeline · Step 3 (parser, v2.1 paste-aware).

PURPOSE
  Parse listings into structured ParsedListingRow objects ready to insert
  into research_log with data_source='Listing Aggregator'. Two entry points:

    parse_carwale_csv(csv_bytes_or_text, ...)
        v1 — Instant Data Scraper CSV uploads. UNTOUCHED in v2/v2.1: same
        input contract, same output shape. The CSV path is URL-keyed and
        dedups downstream in _insert_listings_to_research_log() via
        listing_url, so it doesn't need parser-level dedup.

    parse_carwale_paste(raw_text, ...)
        Paste extractor. User browses CarWale in their normal browser,
        Ctrl+A → Ctrl+C, pastes into AutoKnowMus admin. Server extracts
        listings via regex anchors on the repeating block structure.

  Skipped rows (parse failures, unknown makes/models, variant mismatches)
  are returned alongside as SkippedRow objects with a reason — drives the
  admin upload-detail UI and the calibration skip-list.

═════════════════════════════════════════════════════════════
v2.1 PARSER-LEVEL FINGERPRINT DEDUPLICATION (NEW)
═════════════════════════════════════════════════════════════
PROBLEM SOLVED:
  CarWale results pages render the SAME listing in multiple widgets on
  one page — main listing grid, "Popular Cars in Bangalore" carousel,
  "Recently Viewed" widget, "Featured Cars" panel, and so on. Each widget
  contains the listing's 3-line shape (title + km|fuel|loc + price), so
  the block-finder correctly emits each occurrence as a separate
  ParsedListingRow.

  Downstream, the insert function dedups by listing_url — but paste-mode
  rows have NO URL (CarWale doesn't expose the listing slug in the
  rendered text). So URL-keyed dedup is a no-op for paste mode.

  Result: a single paste of one CarWale page could write 6-8 copies of
  the same listing into research_log, polluting calibration math.

FIX (v2.1):
  After the main parse loop in parse_carwale_paste(), we now run an
  intra-batch fingerprint deduplication step. Fingerprint:

      (year, make, model, variant, fuel, mileage_km, asking_price)

  Locality and city are NOT part of the fingerprint — the same listing
  shown in different widgets sometimes has slightly different locality
  rendering (e.g. "Koramangala 6th block" vs "Koramangala"). Year+spec+
  price+mileage is sufficient to identify "same listing".

  First occurrence wins. Duplicates get demoted to SkippedRow entries
  with reason='intra_paste_fingerprint_dupe' so the admin skip-log
  surfaces what happened. This keeps the parser honest about what was
  rejected and why.

  The CSV path (parse_carwale_csv) is unchanged — its URL-keyed dedup
  downstream is sufficient, and CSV rows always have URLs.

═════════════════════════════════════════════════════════════

CARWALE PASTE BLOCK STRUCTURE (v2 anchor, unchanged in v2.1)
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

CHROME REJECTION HEURISTICS (v2, unchanged in v2.1)
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

  Paste path (v2 + v2.1 dedup step):
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
    8. v2.1 NEW: Run fingerprint dedup on the parsed[] list. Dupes
       get demoted to skipped[] with reason='intra_paste_fingerprint_dupe'.

ParsedListingRow GAINED ONE FIELD IN v2:
    locality: Optional[str]  — was already present in v1; kept identical
                                shape for downstream insert function
    (No schema change needed — locality column added in Migration 10.)

DATA QUALITY (unchanged)
  CSV path:    data_source='Listing Aggregator', marketplace='CarWale'
  Paste path:  data_source='Listing Aggregator', marketplace='CarWale'
               (paste extractor only knows CarWale layout in v2)
  Both:        data_source_type='asking_price' (haircut applies)
               data_quality_tier='aggregator_paste_listings' (65% ceiling)
               data_quality='medium' (legacy 3-bucket field, unchanged)

  These tags get applied by the INSERT function, NOT this parser.
  This parser only emits ParsedListingRow with extracted fields.

PUBLIC API (v2.1 — UNCHANGED from v2)
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
# CONSTANTS — CarWale (v1 + v2, unchanged in v2.1)
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
TITLE_LINE_PATTERN = re.compile(r'^\s*(\d{4})\s+([A-Za-z][\w\-\.\s\(\)\[\]\+/]+)$')

KM_FUEL_LOC_PATTERN = re.compile(
    r'^\s*([\d,]+)\s*km\s*\|\s*([A-Za-z][A-Za-z\s\(\)\+\-]+?)\s*\|\s*(.+?)\s*$',
    re.IGNORECASE,
)

LISTING_PRICE_PATTERN = re.compile(
    r'^\s*rs\.?\s*([\d,]+(?:\.\d+)?)\s*(lakh|lac|cr(?:ore)?)\s*$',
    re.IGNORECASE,
)

CHROME_REJECT_PATTERNS = [
    re.compile(r'^\s*used\s+[\w\s]+(?:cars?)\s*$', re.IGNORECASE),
    re.compile(r'^\s*\d+\s+used\s+', re.IGNORECASE),
    re.compile(r'^\s*rs\s+\d+(?:\.\d+)?\s+lakh\s+avg', re.IGNORECASE),
    re.compile(r'^\s*\d+\s*\+\s*cars?\s*$', re.IGNORECASE),
    re.compile(r'^\s*back\s*$', re.IGNORECASE),
    re.compile(r'^\s*next\s*$', re.IGNORECASE),
]

NOISE_LINE_PATTERNS = [
    re.compile(r'^\s*$'),
    re.compile(r'^\s*ad\s*$', re.IGNORECASE),
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
    re.compile(r'^\s*rs\.?\s*[\d,]+\s*(?:l|cr)?\s*$', re.IGNORECASE),
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
    re.compile(r'^\s*second\s+hand\s+', re.IGNORECASE),
]

# Window sizes for paste-mode scanning
TITLE_TO_KMFUEL_LOOKAHEAD = 12
KMFUEL_TO_PRICE_LOOKAHEAD = 8


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
SKIP_INCOMPLETE_BLOCK = 'incomplete_paste_block'

# v2.1 NEW: emitted by _deduplicate_paste_rows() when a parsed row's
# fingerprint matches an earlier row in the same paste.
SKIP_INTRA_PASTE_FINGERPRINT_DUPE = 'intra_paste_fingerprint_dupe'


# ============================================================
# RESULT TYPES — unchanged in v2 / v2.1
# ============================================================

class ParsedListingRow(NamedTuple):
    """A successfully parsed listing row, ready for research_log insertion."""
    listing_url:        Optional[str]
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
# v2.1 NEW: Parser-level fingerprint dedup
# ------------------------------------------------------------
# CarWale renders the same listing in multiple widgets on one page.
# The block-finder correctly emits each occurrence — we dedup HERE so
# downstream gets clean data.
# ============================================================

def _fingerprint_for_paste_row(row: ParsedListingRow) -> Tuple:
    """
    Build a stable fingerprint identifying a unique listing.

    Locality and city are intentionally EXCLUDED — the same listing
    rendered in different widgets sometimes has slightly different
    locality text (e.g. "Koramangala 6th block" vs "Koramangala").
    Year + spec + price + mileage is sufficient to identify
    "same listing on this page".

    Returns a hashable tuple suitable for use as a set/dict key.
    """
    return (
        row.year,
        (row.make or '').strip(),
        (row.model or '').strip(),
        (row.variant or '').strip(),
        (row.fuel or '').strip(),
        row.mileage_km,
        row.asking_price,
    )


def _deduplicate_paste_rows(
    parsed: List[ParsedListingRow],
    skipped: List[SkippedRow],
) -> Tuple[List[ParsedListingRow], List[SkippedRow]]:
    """
    Walk the parsed list in order. First occurrence of each fingerprint
    wins. Duplicates are removed from parsed[] and appended to skipped[]
    with reason=SKIP_INTRA_PASTE_FINGERPRINT_DUPE so the admin skip-log
    shows what got deduped and why.

    Order is preserved for kept rows so the admin preview table renders
    the same sequence the parser produced (matches expectations from the
    page scroll order).
    """
    seen = set()
    kept: List[ParsedListingRow] = []
    new_skipped: List[SkippedRow] = list(skipped)  # copy; don't mutate input

    for row in parsed:
        fp = _fingerprint_for_paste_row(row)
        if fp in seen:
            new_skipped.append(SkippedRow(
                row_index=0,  # no meaningful source line for dedup'd rows
                reason=SKIP_INTRA_PASTE_FINGERPRINT_DUPE,
                detail=(
                    f"Duplicate of earlier listing in this paste: "
                    f"{row.year} {row.make} {row.model} {row.variant} "
                    f"· {row.mileage_km:,} km · ₹{row.asking_price:,}"
                ),
                raw_url=row.listing_url,
                raw_title=row.raw_title,
            ))
            continue
        seen.add(fp)
        kept.append(row)

    return kept, new_skipped


# ============================================================
# INTERNAL HELPERS — shared by CSV and paste paths
# (unchanged from v2)
# ============================================================

def _extract_url_slug(url: str) -> Optional[str]:
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
    if not title_after_year:
        return (None, None, '')

    normalized_title = title_after_year.replace('-', ' ').replace('  ', ' ').strip()
    normalized_title_lower = normalized_title.lower()

    candidate_makes = car_data.get_makes()
    make_candidates = sorted(
        candidate_makes,
        key=lambda m: len(m.replace('-', ' ').replace('  ', ' ')),
        reverse=True,
    )

    matched_make = None
    title_after_make_lower = ''
    chars_consumed = 0

    for make in make_candidates:
        make_norm = make.replace('-', ' ').replace('  ', ' ').strip().lower()
        if not make_norm:
            continue
        if normalized_title_lower == make_norm:
            matched_make = make
            chars_consumed = len(title_after_year)
            title_after_make_lower = ''
            break
        prefix_with_space = make_norm + ' '
        if normalized_title_lower.startswith(prefix_with_space):
            matched_make = make
            chars_consumed = _find_original_position(
                title_after_year, len(prefix_with_space)
            )
            title_after_make_lower = normalized_title_lower[len(prefix_with_space):].strip()
            break

    if not matched_make:
        return (None, None, title_after_year)

    if not title_after_make_lower:
        return (matched_make, None, title_after_year[chars_consumed:].strip())

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
            return (matched_make, model, '')
        prefix_with_space = model_norm + ' '
        if title_after_make_lower.startswith(prefix_with_space):
            after_make_original = title_after_year[chars_consumed:]
            model_chars_consumed = _find_original_position(
                after_make_original, len(prefix_with_space)
            )
            variant_text = after_make_original[model_chars_consumed:].strip()
            return (matched_make, model, variant_text)

    return (matched_make, None, title_after_year[chars_consumed:].strip())


def _find_original_position(original: str, normalized_chars: int) -> int:
    orig_idx = 0
    norm_idx = 0
    while orig_idx < len(original) and norm_idx < normalized_chars:
        c = original[orig_idx]
        if c == '-':
            orig_idx += 1
            norm_idx += 1
        else:
            orig_idx += 1
            norm_idx += 1
    return orig_idx


TITLE_YEAR_PATTERN = re.compile(r'^\s*(\d{4})\s+(.*)$')
TITLE_BRACKET_SUFFIX_PATTERN = re.compile(r'\s*\[[^\]]*\]\s*$')


def _strip_year_and_brackets(title: str) -> Tuple[Optional[int], Optional[str]]:
    if not title:
        return (None, None)
    s = title.strip()
    s = TITLE_BRACKET_SUFFIX_PATTERN.sub('', s).strip()
    m = TITLE_YEAR_PATTERN.match(s)
    if not m:
        return (None, None)
    return (int(m.group(1)), m.group(2).strip())


def _extract_variant_from_title(title: str, make: str, model: str) -> Tuple[Optional[int], Optional[str]]:
    if not title:
        return (None, None)

    year, after_year = _strip_year_and_brackets(title)
    if year is None or after_year is None:
        return (None, None)

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


PRICE_PATTERN = re.compile(
    r'rs\.?\s*([\d,]+(?:\.\d+)?)\s*(lakh|lac|cr(?:ore)?)',
    re.IGNORECASE,
)


def _parse_price(s: str) -> Optional[int]:
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
# v2: PASTE-MODE BLOCK SCANNER (unchanged in v2.1)
# ============================================================

def _line_is_noise(line: str) -> bool:
    for pattern in NOISE_LINE_PATTERNS:
        if pattern.match(line):
            return True
    return False


def _line_is_chrome_reject(line: str) -> bool:
    for pattern in CHROME_REJECT_PATTERNS:
        if pattern.match(line):
            return True
    return False


class _PasteBlock(NamedTuple):
    title:        str
    kmfuelloc:    str
    price_line:   str
    title_lineno: int


def _find_paste_blocks(raw_text: str) -> List[_PasteBlock]:
    blocks: List[_PasteBlock] = []
    lines = raw_text.split('\n')
    i = 0
    n = len(lines)

    while i < n:
        line = lines[i].rstrip()

        title_match = TITLE_LINE_PATTERN.match(line)
        if not title_match:
            i += 1
            continue

        try:
            year = int(title_match.group(1))
        except ValueError:
            i += 1
            continue
        if year < 1990 or year > 2100:
            i += 1
            continue

        title_line = line
        title_lineno = i + 1

        kmfuelloc_line = None
        kmfuelloc_idx = None
        for j in range(i + 1, min(n, i + 1 + 50)):
            scan = lines[j].rstrip()

            if _line_is_noise(scan):
                continue
            if _line_is_chrome_reject(scan):
                break
            if TITLE_LINE_PATTERN.match(scan):
                break

            if KM_FUEL_LOC_PATTERN.match(scan):
                kmfuelloc_line = scan
                kmfuelloc_idx = j
                break

        if kmfuelloc_line is None:
            i += 1
            continue

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
            i += 1
            continue

        blocks.append(_PasteBlock(
            title=title_line,
            kmfuelloc=kmfuelloc_line,
            price_line=price_line,
            title_lineno=title_lineno,
        ))
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
    UNCHANGED in v2 and v2.1. CSV path is URL-keyed and dedups downstream
    via listing_url, so no parser-level dedup is needed here.

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
# PUBLIC API — parse_carwale_paste (v2.1: dedup added at end)
# ============================================================

def parse_carwale_paste(raw_text: str,
                        default_state: str = 'KA',
                        default_city: str = 'Bangalore') -> ParseResult:
    """
    v2.1 — Parse browser-pasted CarWale results page text.

    Args:
      raw_text       The full page text the admin pasted (Ctrl+A → Ctrl+C
                     from a CarWale "Used Cars in {city}" page).
      default_state  State to attach to all rows (v2: 'KA').
      default_city   City fallback if a listing doesn't include one.

    Returns:
      ParseResult(total_rows, parsed[], skipped[])

    v2.1 BEHAVIOR CHANGE (vs v2):
      After the main parse loop produces parsed[], we run an intra-batch
      fingerprint dedup step. Duplicates (same year+make+model+variant+
      fuel+mileage+price) get demoted from parsed[] to skipped[] with
      reason='intra_paste_fingerprint_dupe'. First occurrence wins.

      The total_rows count is unchanged (it reflects what the block-finder
      saw, NOT what the dedup produced) — so the admin still sees an
      accurate picture of how many candidate blocks the parser found.

      The CSV path (parse_carwale_csv) is UNCHANGED — its URL-keyed dedup
      downstream is sufficient.

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

        # ---- 2. Resolve make + model from title text ----
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

        # ---- 3. Parse km/fuel/locality/city ----
        kmf_match = KM_FUEL_LOC_PATTERN.match(block.kmfuelloc)
        if not kmf_match:
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

        if ',' in location_str:
            locality, city_in_listing = location_str.split(',', 1)
            locality = locality.strip()
            city_in_listing = city_in_listing.strip()
        else:
            locality = None
            city_in_listing = location_str

        # ---- 4. Parse price ----
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
            listing_url=None,
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

    # ============================================================
    # v2.1 NEW: Intra-batch fingerprint dedup
    # ------------------------------------------------------------
    # CarWale renders the same listing in multiple widgets per page
    # (main grid, popular cars carousel, featured panel, etc.).
    # The block-finder correctly emits each occurrence — we dedup
    # here so downstream gets clean data. First occurrence wins.
    # Duplicates become SkippedRow entries with a clear reason so
    # the admin sees what happened in the skip log.
    # ============================================================
    parsed, skipped = _deduplicate_paste_rows(parsed, skipped)

    return ParseResult(total_rows=total, parsed=parsed, skipped=skipped)


# ============================================================
# DIAGNOSTICS — admin debugging only
# ============================================================

def summarize_parse_result(result: ParseResult) -> dict:
    """
    Group counts by skip reason for the admin upload-detail UI.
    Returns a plain dict — JSON-serializable.

    v2.1: The new SKIP_INTRA_PASTE_FINGERPRINT_DUPE reason will appear
    here when paste-mode duplicates are surfaced. No code change needed
    in this function — it groups by .reason which now includes the new
    constant naturally.
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
