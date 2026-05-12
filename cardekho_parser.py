"""
cardekho_parser.py
------------------
AutoKnowMus — CarDekho paste-extract parser.

WHAT THIS DOES:
  Takes raw text pasted from CarDekho's "Cars with prices in India"
  filtered search page (with one or more model accordions expanded)
  and extracts structured rows: (make, model, variant, fuel, cc, price_inr).

  CarDekho text format (Bangalore-selected) — each model section looks like:

    Tata Sierra
    Tata Sierra                              <- duplicate brand-model line
    4.7293 Reviews
    Rs.11.49 - 21.29 Lakh*Get On-Road Price
    *Ex-Showroom Price in Bangalore
    1498 cc5 seater
    View May Offers
    24Variants Matching Your Search Criteria
    Tata Sierra Adventure DCA (Petrol)Rs.16.79 Lakh*, 1498 cc
    Tata Sierra Adventure (Diesel)Rs.16.49 Lakh*, 1497 cc
    ...
    <next model section starts here>

  Key parsing rules:
    - Variant rows match: "<full variant name with make+model prefix> (<fuel>)Rs.<X.XX> Lakh*, <cc> cc"
    - <make> and <model> are derived from the active model header, NOT
      from the variant line prefix (which we strip)
    - Collapsed model sections show the count but no variant rows —
      these get parsed as 0 variants and surfaced as a warning
    - Fuel comes from parentheses: (Petrol), (Diesel), (CNG), (Electric), (Hybrid)
    - Some variant rows include kmpl mileage; we ignore it for now (anchor
      table only stores price, not mileage)

WHAT THIS DOESN'T DO:
  - Does NOT touch the database
  - Does NOT touch the car_prices Google Sheet
  - Does NOT match against existing variants (that's done in app.py at preview time)
  - Does NOT validate prices against sanity bounds (preview UI shows ratio,
    admin decides)
  - Does NOT compute Delhi→Bangalore conversion (CarDekho with Bangalore
    selected already gives Bangalore prices)

DESIGN DECISIONS:
  - Pure function module — no state, no I/O, fully testable
  - Tolerant parser: skips lines it doesn't recognize rather than failing hard
  - Returns structured dataclass-like dicts that are JSON-serializable
  - Surfaces warnings (collapsed sections, unparseable lines) alongside
    successful parses so the preview UI can show them

USAGE:
  from cardekho_parser import parse_cardekho_paste

  result = parse_cardekho_paste(raw_pasted_text)
  # result = {
  #     "ok": True,
  #     "models_found": 21,
  #     "variants_found": 187,
  #     "rows": [
  #         {
  #             "make": "Tata",
  #             "model": "Sierra",
  #             "variant": "Adventure DCA",
  #             "fuel": "Petrol",
  #             "cc": 1498,
  #             "price_inr": 1679000,
  #             "raw_price_text": "Rs.16.79 Lakh",
  #             "source_line": "Tata Sierra Adventure DCA (Petrol)Rs.16.79 Lakh*, 1498 cc"
  #         },
  #         ...
  #     ],
  #     "warnings": [
  #         {
  #             "type": "collapsed_section",
  #             "make": "Tata", "model": "Nexon",
  #             "expected_count": 66,
  #             "message": "Model section shows 66 variants but no variant rows found — expand on CarDekho before pasting."
  #         },
  #         ...
  #     ],
  #     "summary": "21 models, 187 variants parsed. 4 models collapsed (skipped)."
  # }
"""

import re
from typing import Dict, List, Optional, Tuple


# ============================================================
# CONSTANTS
# ============================================================

# Fuel labels CarDekho uses inside parentheses on variant lines.
# Map CarDekho's display labels to AutoKnowMus's internal FUEL_ORDER values.
# AutoKnowMus locked fuel set (per memory rule #9 / handoff rule 8):
#   Petrol, Diesel, CNG, HEV, PHEV, BEV
#
# CarDekho writes:
#   (Petrol)   -> Petrol
#   (Diesel)   -> Diesel
#   (CNG)      -> CNG
#   (Electric) -> BEV
#   (Hybrid)   -> HEV   (CarDekho doesn't distinguish HEV vs PHEV in the list view)
CARDEKHO_FUEL_MAP = {
    "Petrol":   "Petrol",
    "Diesel":   "Diesel",
    "CNG":      "CNG",
    "Electric": "BEV",
    "Hybrid":   "HEV",
}

# Patterns to detect lines that announce a new model section.
# Examples we want to recognize as model section headers:
#   "Tata Sierra"  followed by  "Tata Sierra" (duplicate) and  "4.7293 Reviews"
#   "Maruti Suzuki e Vitara"
#   "Maruti Suzuki Grand Vitara"
#
# We match the "<Make Model>\n<Make Model>\n<rating>Reviews" triplet OR
# the price-range marker line "Rs.X - Y Lakh*Get On-Road Price"
# Combined with "Variants Matching Your Search Criteria" sentinel.
#
# We anchor primarily on the "N Variants Matching Your Search Criteria" line:
# the make+model is whatever appeared on the lines just above this sentinel.

VARIANT_COUNT_PATTERN = re.compile(
    r'^(\d+)Variants?\s+Matching\s+Your\s+Search\s+Criteria\s*$',
    re.IGNORECASE,
)

# Variant line pattern. Examples it must match:
#   "Tata Sierra Adventure DCA (Petrol)Rs.16.79 Lakh*, 1498 cc"
#   "Tata Sierra Accomplished Plus AT (Diesel)Rs.21.29 Lakh*, 1497 cc"
#   "Maruti Suzuki Brezza Zxi Plus AT (Petrol)Rs.12.86 Lakh*, 1462 cc, 19.8 kmpl"
#   "Maruti Suzuki e Vitara <variant> (Electric)Rs.15.99 Lakh*, ..."
#
# Capture groups:
#   1: full prefixed variant name with fuel paren  (we strip later)
#   2: fuel inside parens
#   3: price (e.g. "16.79")
#   4: cc (e.g. "1498")
#
# Lakh* is the unit marker. We require "Lakh*," to anchor confidently
# (CarDekho's variant rows always end the price with "Lakh*," followed by cc).
VARIANT_LINE_PATTERN = re.compile(
    r'^(?P<full_name>.+?)\s*\((?P<fuel>Petrol|Diesel|CNG|Electric|Hybrid)\)'
    r'Rs\.\s*(?P<price>\d+(?:\.\d+)?)\s*Lakh\*?,\s*'
    r'(?P<cc>\d+)\s*cc',
    re.IGNORECASE,
)

# Price-range header line ("Rs.X - Y Lakh*Get On-Road Price") — used as
# secondary signal that we're inside a model section. We don't extract
# values from it (those are aggregate ranges, not per-variant prices).
PRICE_RANGE_PATTERN = re.compile(
    r'^Rs\.\s*\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*Lakh\*?\s*Get\s+On-Road\s+Price',
    re.IGNORECASE,
)

# Lines we explicitly skip as noise.
NOISE_LINE_PATTERNS = [
    re.compile(r'^\d+(?:\.\d+)?\d+\s+Reviews?\s*$'),           # "4.7293 Reviews"
    re.compile(r'^View\s+(May|June|July|August|September|October|November|December|January|February|March|April)\s+Offers\s*$', re.IGNORECASE),
    re.compile(r'^\*Ex-Showroom\s+Price', re.IGNORECASE),
    re.compile(r'^Get\s+On-Road\s+Price\s*$', re.IGNORECASE),
    re.compile(r'^Ad\s*$'),
    re.compile(r'^Sort\s+by\s*:?\s*$', re.IGNORECASE),
    re.compile(r'^\s*$'),  # blank lines
]


# ============================================================
# DATA STRUCTURES
# ============================================================

def _empty_result() -> Dict:
    return {
        "ok": True,
        "models_found": 0,
        "variants_found": 0,
        "rows": [],
        "warnings": [],
        "summary": "",
    }


# ============================================================
# CORE PARSER
# ============================================================

def parse_cardekho_paste(raw_text: str) -> Dict:
    """
    Parse a CarDekho 'Cars with prices' page paste into structured variant rows.

    Args:
        raw_text: Full text content copied from a CarDekho filtered-search
                  page where one or more model accordions have been expanded.

    Returns:
        Dict with keys:
            ok            (bool)  — always True unless input is unusable
            models_found  (int)   — number of distinct model sections seen
            variants_found(int)   — number of variant rows successfully parsed
            rows          (list)  — list of variant dicts (see below)
            warnings      (list)  — list of warning dicts (collapsed sections,
                                    unparseable lines, etc.)
            summary       (str)   — one-line human-readable summary

        Each row dict has:
            make, model, variant, fuel, cc, price_inr,
            raw_price_text, source_line
    """
    result = _empty_result()

    if not raw_text or not isinstance(raw_text, str):
        result["ok"] = False
        result["summary"] = "Empty or invalid input."
        return result

    # Normalize line endings; CarDekho copy-paste sometimes has \r\n
    lines = [ln.rstrip() for ln in raw_text.replace('\r\n', '\n').split('\n')]

    # State machine: walk top-to-bottom, tracking the active model header.
    # The pattern across the page is:
    #     <make+model line>           <- e.g. "Tata Sierra"
    #     <make+model line again>     <- duplicate (CarDekho HTML artifact)
    #     <rating>Reviews
    #     Rs.X - Y Lakh*Get On-Road Price
    #     *Ex-Showroom Price in Bangalore
    #     <specs line e.g. "1498 cc5 seater">
    #     View <Month> Offers
    #     NVariants Matching Your Search Criteria
    #     <variant rows>
    #     ...
    #
    # The "N Variants Matching..." line is our anchor — we know the make+model
    # by looking backward from there, and we know how many variants to expect.

    current_make: Optional[str] = None
    current_model: Optional[str] = None
    expected_variant_count: Optional[int] = None
    variants_seen_in_current_section: int = 0

    # Track the last 6 non-noise lines to backtrack from "N Variants" sentinel
    recent_lines_buffer: List[str] = []
    BUFFER_SIZE = 6

    # Track which models we've already announced so we can detect collapsed ones
    models_announced: List[Tuple[str, str, int]] = []  # (make, model, expected)

    for raw_line in lines:
        line = raw_line.strip()

        # 1) Skip pure noise lines (but keep them out of the buffer)
        if _is_noise_line(line):
            continue

        # 2) Did we just hit "N Variants Matching Your Search Criteria"?
        m = VARIANT_COUNT_PATTERN.match(line)
        if m:
            # Close out the previous section's collapsed-check, if any
            if current_make and current_model and expected_variant_count is not None:
                _emit_collapsed_warning_if_needed(
                    result,
                    current_make,
                    current_model,
                    expected_variant_count,
                    variants_seen_in_current_section,
                )

            # Begin new section. Look backward in buffer for the make+model.
            expected_variant_count = int(m.group(1))
            make_model = _extract_make_model_from_buffer(recent_lines_buffer)
            if make_model:
                current_make, current_model = make_model
                models_announced.append((current_make, current_model, expected_variant_count))
                result["models_found"] += 1
            else:
                # Couldn't determine the active model — warn and keep going
                result["warnings"].append({
                    "type": "model_header_missing",
                    "message": (
                        f"Found 'N Variants Matching' sentinel but couldn't "
                        f"identify the preceding make+model line. "
                        f"Section will be skipped. Expected {expected_variant_count} variants."
                    ),
                })
                current_make = None
                current_model = None

            variants_seen_in_current_section = 0
            recent_lines_buffer = []
            continue

        # 3) Try to parse this line as a variant row
        if current_make and current_model:
            vm = VARIANT_LINE_PATTERN.match(line)
            if vm:
                row = _build_variant_row(
                    make=current_make,
                    model=current_model,
                    full_name=vm.group("full_name"),
                    fuel_raw=vm.group("fuel"),
                    price_str=vm.group("price"),
                    cc_str=vm.group("cc"),
                    source_line=line,
                )
                if row is not None:
                    result["rows"].append(row)
                    result["variants_found"] += 1
                    variants_seen_in_current_section += 1
                continue

        # 4) Otherwise, keep this line in the recent-buffer for the next
        #    sentinel-based make+model lookup
        if line:
            recent_lines_buffer.append(line)
            if len(recent_lines_buffer) > BUFFER_SIZE:
                recent_lines_buffer.pop(0)

    # 5) After the loop ends, check the last section for collapsed state
    if current_make and current_model and expected_variant_count is not None:
        _emit_collapsed_warning_if_needed(
            result,
            current_make,
            current_model,
            expected_variant_count,
            variants_seen_in_current_section,
        )

    # 6) Build summary text
    collapsed_count = sum(1 for w in result["warnings"] if w["type"] == "collapsed_section")
    parts = [
        f"{result['models_found']} model{'' if result['models_found'] == 1 else 's'}",
        f"{result['variants_found']} variant{'' if result['variants_found'] == 1 else 's'} parsed",
    ]
    if collapsed_count > 0:
        parts.append(
            f"{collapsed_count} model{'' if collapsed_count == 1 else 's'} collapsed (skipped)"
        )
    result["summary"] = ". ".join(parts) + "."

    return result


# ============================================================
# HELPERS
# ============================================================

def _is_noise_line(line: str) -> bool:
    """True if the line is recognized noise we want to ignore."""
    if not line:
        return True
    for pat in NOISE_LINE_PATTERNS:
        if pat.match(line):
            return True
    # Also skip the price-range "header" line — it has no per-variant info
    if PRICE_RANGE_PATTERN.match(line):
        return True
    return False


def _extract_make_model_from_buffer(buf: List[str]) -> Optional[Tuple[str, str]]:
    """
    Walk backward through the recent-lines buffer to find the most-recent
    "<Make> <Model>" header line.

    CarDekho's pattern is to repeat the model name twice before the rating:
      "Tata Sierra"
      "Tata Sierra"      <- duplicate
      "4.7293 Reviews"

    We look for the duplicated pair as the strongest signal. If we can't
    find a duplicated pair, we fall back to taking the line just before
    the Reviews/rating line.

    Returns (make, model) or None if we can't find one.
    """
    if not buf:
        return None

    # Try to find a duplicated consecutive line (the make+model pattern).
    # Walk from most recent backward to oldest.
    n = len(buf)
    for i in range(n - 1, 0, -1):
        cur = buf[i].strip()
        prev = buf[i - 1].strip()
        if cur and cur == prev and not cur[0].isdigit():
            # Found duplicate — split into make + model
            return _split_make_model(cur)

    # Fallback: take the most-recent non-rating, non-spec line
    # (skip lines like "1498 cc5 seater" or "4.7293 Reviews")
    for line in reversed(buf):
        line = line.strip()
        if not line:
            continue
        if re.match(r'^\d+\.?\d*\d+\s*Reviews?', line, re.IGNORECASE):
            continue
        if re.match(r'^\d+\s*cc', line):
            continue
        if re.match(r'^\d+\s*seater', line, re.IGNORECASE):
            continue
        if PRICE_RANGE_PATTERN.match(line):
            continue
        # Reasonable candidate for a make+model line
        if not line[0].isdigit():
            return _split_make_model(line)

    return None


def _split_make_model(text: str) -> Optional[Tuple[str, str]]:
    """
    Split a "<Make> <Model>" string into (make, model).

    Handles multi-word makes:
      "Tata Sierra"          -> ("Tata", "Sierra")
      "Maruti Suzuki Brezza" -> ("Maruti Suzuki", "Brezza")
      "Maruti Suzuki e Vitara" -> ("Maruti Suzuki", "e Vitara")
      "Maruti Suzuki Grand Vitara" -> ("Maruti Suzuki", "Grand Vitara")
      "Land Rover Defender"  -> ("Land Rover", "Defender")
      "Aston Martin Vantage" -> ("Aston Martin", "Vantage")
      "Rolls-Royce Ghost"    -> ("Rolls-Royce", "Ghost")
      "Mercedes-Benz E-Class" -> ("Mercedes-Benz", "E-Class")

    Strategy: match against the known list of multi-word makes first, then
    fall back to "first token is make, rest is model" for single-word makes.
    """
    if not text:
        return None
    text = text.strip()

    # Known multi-word makes (extend this list as your catalog grows).
    # Order: longest first so "Mercedes-Benz" wins over "Mercedes" etc.
    MULTI_WORD_MAKES = [
        "Aston Martin",
        "Land Rover",
        "Rolls-Royce",
        "Mercedes-Benz",
        "Maruti Suzuki",
        "Vayve Mobility",
        "Strom Motors",
    ]

    for make in MULTI_WORD_MAKES:
        if text.startswith(make + " "):
            model = text[len(make):].strip()
            if model:
                return (make, model)

    # Fallback: single-word make
    parts = text.split(None, 1)
    if len(parts) == 2:
        return (parts[0], parts[1])

    return None


def _build_variant_row(
    make: str,
    model: str,
    full_name: str,
    fuel_raw: str,
    price_str: str,
    cc_str: str,
    source_line: str,
) -> Optional[Dict]:
    """
    Build a single variant row dict.

    full_name is the prefixed variant name including make+model:
        "Tata Sierra Adventure DCA"
    We strip the make+model prefix to get just the variant ("Adventure DCA").
    """
    # 1) Strip the make+model prefix from full_name
    prefix = f"{make} {model}"
    full_name_clean = full_name.strip()
    if full_name_clean.startswith(prefix):
        variant = full_name_clean[len(prefix):].strip()
    else:
        # The variant line's prefix doesn't match the active model — sometimes
        # CarDekho omits one or the other. Be tolerant: just strip the make.
        if full_name_clean.startswith(make + " "):
            variant = full_name_clean[len(make):].strip()
        else:
            variant = full_name_clean

    if not variant:
        # Edge case: variant name is literally empty after stripping
        return None

    # 2) Map fuel from CarDekho label to AutoKnowMus internal value
    fuel = CARDEKHO_FUEL_MAP.get(fuel_raw.strip(), fuel_raw.strip())

    # 3) Convert price from Lakhs to INR integer
    try:
        price_lakh = float(price_str)
        price_inr = int(round(price_lakh * 100000))
    except (TypeError, ValueError):
        return None
    if price_inr <= 0:
        return None

    # 4) cc as int
    try:
        cc = int(cc_str)
    except (TypeError, ValueError):
        cc = None

    return {
        "make": make,
        "model": model,
        "variant": variant,
        "fuel": fuel,
        "cc": cc,
        "price_inr": price_inr,
        "raw_price_text": f"Rs.{price_str} Lakh",
        "source_line": source_line,
    }


def _emit_collapsed_warning_if_needed(
    result: Dict,
    make: str,
    model: str,
    expected: int,
    seen: int,
) -> None:
    """If a model section had 0 variant rows despite an N>0 sentinel, warn."""
    if expected > 0 and seen == 0:
        result["warnings"].append({
            "type": "collapsed_section",
            "make": make,
            "model": model,
            "expected_c
