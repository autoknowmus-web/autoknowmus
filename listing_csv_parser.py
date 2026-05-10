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
DEFAULT_DATA_QUALITY =
