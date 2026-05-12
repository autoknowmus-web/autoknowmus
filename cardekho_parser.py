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
  - Does NOT compute Delhi to Bangalore conversion (CarDekho with Bangalore
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
"""

import re
from typing import Dict, List, Optional, Tuple


# ============================================================
# CONSTANTS
# ============================================================

# Fuel labels CarDekho uses inside parentheses on variant lines.
# Map CarDekho's display labels to AutoKnowMus's internal FUEL_ORDER values.
# AutoKnowMus locked fuel set: Petrol, Diesel, CNG, HEV, PHEV, BEV
CARDEKHO_FUEL_MAP = {
    "Petrol":   "Petrol",
    "Diesel":   "Diesel",
    "CNG":      "CNG",
    "Electric": "BEV",
    "Hybrid":   "HEV",
}

# Sentinel: "N Variants Matching Your Search Criteria"
VARIANT_COUNT_PATTERN = re.compile(
    r'^(\d+)Variants?\s+Matching\s+Your\s+Search\s+Criteria\s*$',
    re.IGNORECASE,
)

# Variant line pattern. Examples it must match:
#   "Tata Sierra Adventure DCA (Petrol)Rs.16.79 Lakh*, 1498 cc"
#   "Tata Sierra Accomplished Plus AT (Diesel)Rs.21.29 Lakh*, 1497 cc"
VARIANT_LINE_PATTERN = re.compile(
    r'^(?P<full_name>.+?)\s*\((?P<fuel>Petrol|Diesel|CNG|Electric|Hybrid)\)'
    r'Rs\.\s*(?P<price>\d+(?:\.\d+)?)\s*Lakh\*?,\s*'
    r'(?P<cc>\d+)\s*cc',
    re.IGNORECASE,
)

# Price-range header (skip)
PRICE_RANGE_PATTERN = re.compile(
    r'^Rs\.\s*\d+(?:\.\d+)?\s*-\s*\d+(?:\.\d+)?\s*Lakh\*?\s*Get\s+On-Road\s+Price',
    re.IGNORECASE,
)

# Lines we explicitly skip as noise.
NOISE_LINE_PATTERNS = [
    re.compile(r'^\d+(?:\.\d+)?\d+\s+Reviews?\s*$'),
    re.compile(r'^View\s+(May|June|July|August|September|October|November|December|January|February|March|April)\s+Offers\s*$', re.IGNORECASE),
    re.compile(r'^\*Ex-Showroom\s+Price', re.IGNORECASE),
    re.compile(r'^Get\s+On-Road\s+Price\s*$', re.IGNORECASE),
    re.compile(r'^Ad\s*$'),
    re.compile(r'^Sort\s+by\s*:?\s*$', re.IGNORECASE),
    re.compile(r'^\s*$'),
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
    """
    result = _empty_result()

    if not raw_text or not isinstance(raw_text, str):
        result["ok"] = False
        result["summary"] = "Empty or invalid input."
        return result

    # Normalize line endings
    lines = [ln.rstrip() for ln in raw_text.replace('\r\n', '\n').split('\n')]

    current_make: Optional[str] = None
    current_model: Optional[str] = None
    expected_variant_count: Optional[int] = None
    variants_seen_in_current_section: int = 0

    recent_lines_buffer: List[str] = []
    BUFFER_SIZE = 6

    models_announced: List[Tuple[str, str, int]] = []

    for raw_line in lines:
        line = raw_line.strip()

        # 1) Skip noise
        if _is_noise_line(line):
            continue

        # 2) Did we just hit "N Variants Matching Your Search Criteria"?
        m = VARIANT_COUNT_PATTERN.match(line)
        if m:
            # Close out previous section's collapsed-check
            if current_make and current_model and expected_variant_count is not None:
                _emit_collapsed_warning_if_needed(
                    result,
                    current_make,
                    current_model,
                    expected_variant_count,
                    variants_seen_in_current_section,
                )

            # Begin new section
            expected_variant_count = int(m.group(1))
            make_model = _extract_make_model_from_buffer(recent_lines_buffer)
            if make_model:
                current_make, current_model = make_model
                models_announced.append((current_make, current_model, expected_variant_count))
                result["models_found"] += 1
            else:
                result["warnings"].append({
                    "type": "model_header_missing",
                    "message": (
                        "Found 'N Variants Matching' sentinel but couldn't "
                        "identify the preceding make+model line. "
                        "Section will be skipped. Expected "
                        + str(expected_variant_count) + " variants."
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

        # 4) Otherwise, keep this line in the recent-buffer
        if line:
            recent_lines_buffer.append(line)
            if len(recent_lines_buffer) > BUFFER_SIZE:
                recent_lines_buffer.pop(0)

    # 5) After loop, check last section
    if current_make and current_model and expected_variant_count is not None:
        _emit_collapsed_warning_if_needed(
            result,
            current_make,
            current_model,
            expected_variant_count,
            variants_seen_in_current_section,
        )

    # 6) Build summary
    collapsed_count = sum(1 for w in result["warnings"] if w["type"] == "collapsed_section")
    parts = [
        str(result["models_found"]) + (" model" if result["models_found"] == 1 else " models"),
        str(result["variants_found"]) + (" variant" if result["variants_found"] == 1 else " variants") + " parsed",
    ]
    if collapsed_count > 0:
        parts.append(
            str(collapsed_count) + (" model" if collapsed_count == 1 else " models") + " collapsed (skipped)"
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
    if PRICE_RANGE_PATTERN.match(line):
        return True
    return False


def _extract_make_model_from_buffer(buf: List[str]) -> Optional[Tuple[str, str]]:
    """
    Walk backward through buffer to find the most-recent
    duplicated make+model header line.
    """
    if not buf:
        return None

    n = len(buf)
    for i in range(n - 1, 0, -1):
        cur = buf[i].strip()
        prev = buf[i - 1].strip()
        if cur and cur == prev and not cur[0].isdigit():
            return _split_make_model(cur)

    # Fallback: most-recent non-rating, non-spec line
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
        if not line[0].isdigit():
            return _split_make_model(line)

    return None


def _split_make_model(text: str) -> Optional[Tuple[str, str]]:
    """
    Split a "<Make> <Model>" string into (make, model).

    Handles multi-word makes:
      "Tata Sierra"          -> ("Tata", "Sierra")
      "Maruti Suzuki Brezza" -> ("Maruti Suzuki", "Brezza")
    """
    if not text:
        return None
    text = text.strip()

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
    Strips the make+model prefix from full_name.
    """
    # Strip make+model prefix
    prefix = make + " " + model
    full_name_clean = full_name.strip()
    if full_name_clean.startswith(prefix):
        variant = full_name_clean[len(prefix):].strip()
    else:
        if full_name_clean.startswith(make + " "):
            variant = full_name_clean[len(make):].strip()
        else:
            variant = full_name_clean

    if not variant:
        return None

    # Map fuel
    fuel = CARDEKHO_FUEL_MAP.get(fuel_raw.strip(), fuel_raw.strip())

    # Convert price Lakhs to INR int
    try:
        price_lakh = float(price_str)
        price_inr = int(round(price_lakh * 100000))
    except (TypeError, ValueError):
        return None
    if price_inr <= 0:
        return None

    # CC as int
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
        "raw_price_text": "Rs." + price_str + " Lakh",
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
            "expected_count": expected,
            "message": (
                make + " " + model + ": section claims "
                + str(expected) + " variants but no variant rows "
                "were found. The accordion was probably collapsed on "
                "CarDekho - expand it and re-paste to include this model."
            ),
        })
    elif expected > 0 and seen < expected:
        result["warnings"].append({
            "type": "partial_section",
            "make": make,
            "model": model,
            "expected_count": expected,
            "seen_count": seen,
            "message": (
                make + " " + model + ": section claims "
                + str(expected) + " variants but only "
                + str(seen) + " were parsed. Some rows may have been malformed."
            ),
        })


# ============================================================
# CLI ENTRYPOINT (for ad-hoc testing - not used by Flask)
# ============================================================
if __name__ == "__main__":
    import sys
    import json
    if len(sys.argv) > 1:
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            raw = f.read()
    else:
        raw = sys.stdin.read()
    parsed = parse_cardekho_paste(raw)
    print(json.dumps(parsed, indent=2))
    print("\n[summary] " + parsed["summary"])
