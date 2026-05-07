"""
variant_resolver.py
-------------------
AutoKnowMus — Variant Resolver Helper for Listing Calibration Pipeline.

PURPOSE
  Marketplace listings (and forum posts) use messy, inconsistent variant
  strings. The pricing engine's catalog (car_data.py) uses canonical OEM
  spec sheet names. Without a fuzzy resolver, listings silently skip when
  variant strings don't match exactly — which is the same failure mode we
  saw with forum data harvesting (e.g. "300d" → engine returns None →
  entry dropped silently).

  This module bridges the gap. Given a (make, model, raw_variant) triple,
  it normalizes the raw string, applies an alias dictionary for known
  patterns, then fuzzy-matches against car_data.get_variants(make, model)
  using rapidfuzz. Returns a confidence-scored match.

CONFIDENCE TIERS (locked per spec section 3)
  ≥ 80    auto_match     — silent, log nothing special
  50-79   needs_review   — flag for admin review in listing upload UI
  < 50    rejected       — drop row, append to skip-list

PIPELINE
  raw_variant
      ↓ _normalize(): lowercase, strip, collapse whitespace, remove punctuation
      ↓ _apply_aliases(): word-order swaps, parens-vs-dashes, common typos
      ↓ get_variants(make, model): pull candidate list from catalog
      ↓ rapidfuzz.process.extractOne(scorer=token_set_ratio): best match
  → ResolverMatch(matched_variant, confidence, decision, reason)

DESIGN NOTES
  - Pure function: same input always produces the same output. No I/O,
    no global state mutation. Catalog is read via car_data.get_variants
    which is already thread-safe.
  - rapidfuzz.fuzz.token_set_ratio chosen because it handles word-order
    differences ("AT VX" vs "VX AT") and punctuation differences ("VX-O"
    vs "VX(O)") well in benchmarks against forum-style variant strings.
  - Alias dictionary is intentionally small in v1. It will grow as we
    discover real-world miss cases via the listing upload skip-list.
    Edit ALIAS_RULES to add new patterns — order matters (first match wins).
  - This module never prints/logs/raises on miss — caller decides what
    to do with each ResolverMatch.

PUBLIC API
  resolve_variant(make, model, raw_variant) → ResolverMatch
  ResolverMatch — NamedTuple with fields:
      matched_variant: Optional[str]
      confidence:      int  (0-100)
      decision:        str  ('auto_match' | 'needs_review' | 'rejected')
      reason:          str  (human-readable explanation)
      candidate_pool:  List[str]  (variants we tried to match against)

USAGE
  from variant_resolver import resolve_variant
  match = resolve_variant("Honda", "City", "VX CVT i-VTEC")
  if match.decision == "auto_match":
      use match.matched_variant
  elif match.decision == "needs_review":
      flag for admin
  else:
      skip + log to skip-list
"""

import re
from typing import List, NamedTuple, Optional, Tuple

from rapidfuzz import fuzz, process

import car_data


# ============================================================
# CONSTANTS
# ============================================================

CONFIDENCE_AUTO_MATCH = 80
CONFIDENCE_NEEDS_REVIEW = 50

# Result decisions
DECISION_AUTO_MATCH = "auto_match"
DECISION_NEEDS_REVIEW = "needs_review"
DECISION_REJECTED = "rejected"

# Internal reason codes (also returned in ResolverMatch.reason for audit)
REASON_EXACT_MATCH = "exact_match"
REASON_ALIAS_HIT = "alias_match"
REASON_FUZZY_HIGH = "fuzzy_high"
REASON_FUZZY_MID = "fuzzy_mid"
REASON_FUZZY_LOW = "fuzzy_low"
REASON_NO_CANDIDATES = "no_candidates"
REASON_EMPTY_INPUT = "empty_input"


# ============================================================
# RESULT TYPE
# ============================================================

class ResolverMatch(NamedTuple):
    """
    Outcome of a single variant resolution attempt.

    matched_variant   The catalog variant we matched to (None if rejected).
    confidence        0-100 score from rapidfuzz, or 100 for exact match.
    decision          One of: 'auto_match', 'needs_review', 'rejected'.
    reason            Why the resolver landed where it did. Useful for
                      debugging and for the admin UI's review queue.
    candidate_pool    The full list of variants we tried to match against.
                      Empty if make/model not in catalog.
    """
    matched_variant: Optional[str]
    confidence: int
    decision: str
    reason: str
    candidate_pool: List[str]


# ============================================================
# ALIAS DICTIONARY
# ============================================================
# Each rule is (compiled_regex, replacement). Applied in order, first match
# wins. Patterns are tried AFTER normalization (lowercase, collapsed
# whitespace, punctuation stripped) so all rules work on cleaned strings.
#
# Add new rules here as we discover them via the listing upload skip-list.
# Keep rules narrowly scoped — over-aggressive aliasing will mismatch.
#
# Examples covered:
#   "C 220d"      → "C 220 d"            (digit-letter glue, Mercedes diesel)
#   "300d"        → "C 220 d"            (Mercedes informal — handled by fuzzy, not alias)
#   "VX-O"        → "VX O"               (parens/dash/space normalization,
#                                          done in _normalize, not here)
#   "AT VX"       → "VX AT"              (word-order swap, handled by token_set_ratio)
#   "VXi+"        → "VXi+"               (kept as-is; rapidfuzz handles VXi vs VXi+)
#   "VXIO"        → "VXi O"              (Maruti option-pack glued spelling)

ALIAS_RULES: List[Tuple[re.Pattern, str]] = [
    # Mercedes-style letter-prefix + digits + optional 'd' suffix, smushed.
    # Handles: "C220d" → "c 220 d", "C 220d" → "c 220 d", "E350" → "e 350"
    # Designed to be safe: only fires when a single letter is glued directly
    # to a 3-digit number, optionally followed by 'd'. This pattern matches
    # German-marque diesel/petrol naming and almost nothing else.
    (re.compile(r'\b([a-z])(\d{3})(\s*d)?\b'),
     lambda m: f"{m.group(1)} {m.group(2)}" + (" d" if m.group(3) else "")),

    # Maruti option-pack glued spelling ("VXIO" → "VXi O", "VXI+O" → "VXi+ O")
    (re.compile(r'\bvxi\+?o\b'), 'vxi+ o'),
    (re.compile(r'\bvxio\b'), 'vxi o'),

    # i-VTEC / i-DTEC marketing suffixes — drop them, they don't disambiguate
    # variants (every Honda Petrol is i-VTEC, every Honda Diesel is i-DTEC)
    (re.compile(r'\s*i[\s\-]?(vtec|dtec)\b'), ''),

    # Common transmission suffix glue ("VXcvt" → "VX CVT")
    (re.compile(r'\b(vx|zx|sx|gxz?|asta)\s*(cvt|amt|mt|at)\b'),
     lambda m: f"{m.group(1)} {m.group(2).upper()}"),

    # Drop trailing "(o)" pattern variants — already normalized to space-form
    # by _normalize but be defensive
    (re.compile(r'\s+\(\s*o\s*\)\s*'), ' o'),

    # Normalize "GT TSI" / "GT-TSI" / "GTTSI" to "GT TSI"
    (re.compile(r'\bgt[\s\-]*tsi\b'), 'gt tsi'),
]


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _normalize(s: str) -> str:
    """
    Lowercase, strip, collapse whitespace, remove most punctuation.

    Keeps: letters, digits, plus sign (+ matters for VXi+), space.
    Removes: parentheses, dashes, dots, slashes (turned into space).

    Examples:
      "  VX-O "     → "vx o"
      "VXi+(O)"     → "vxi+ o"
      "GT TSI Highline"  → "gt tsi highline"
      "C  220d"     → "c 220d"   (single-digit-letter glue handled by alias)
    """
    if not s:
        return ""
    # Lowercase + strip leading/trailing whitespace
    s = s.lower().strip()
    # Replace common separators with space
    s = re.sub(r'[\-\(\)\[\]\.\/]', ' ', s)
    # Collapse multiple spaces
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _apply_aliases(s: str) -> str:
    """Apply ALIAS_RULES in order. First match wins per rule (search not match)."""
    for pattern, replacement in ALIAS_RULES:
        s = pattern.sub(replacement, s)
    # Re-collapse whitespace in case any rule introduced extras
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _normalize_for_match(s: str) -> str:
    """The full pre-match pipeline: normalize → apply aliases."""
    return _apply_aliases(_normalize(s))


def _exact_match_lookup(normalized_raw: str,
                        candidates: List[str]) -> Optional[str]:
    """
    Check if the normalized raw string matches any candidate (after candidate
    is also normalized). Returns the original-cased candidate on hit.
    """
    for c in candidates:
        if _normalize_for_match(c) == normalized_raw:
            return c
    return None


# ============================================================
# PUBLIC API
# ============================================================

def resolve_variant(make: str, model: str, raw_variant: str) -> ResolverMatch:
    """
    Resolve a messy variant string to a catalog variant.

    Args:
      make           Catalog make ("Honda", "Mercedes-Benz", etc.)
      model          Catalog model ("City", "C-Class", etc.)
      raw_variant    Whatever the marketplace/forum gave us.

    Returns:
      ResolverMatch — see class docstring above.

    Behavior:
      - Empty raw_variant → rejected (REASON_EMPTY_INPUT)
      - make/model not in catalog → rejected (REASON_NO_CANDIDATES)
      - Normalized raw matches a normalized candidate exactly →
            auto_match @ 100, REASON_EXACT_MATCH
      - Alias-rewrite changes the string AND result matches exactly →
            auto_match @ 100, REASON_ALIAS_HIT
      - Otherwise rapidfuzz token_set_ratio against all candidates:
            ≥ 80 → auto_match,    REASON_FUZZY_HIGH
            ≥ 50 → needs_review,  REASON_FUZZY_MID
            < 50 → rejected,      REASON_FUZZY_LOW
    """
    candidates = car_data.get_variants(make, model)

    # Edge case: empty input
    if not raw_variant or not raw_variant.strip():
        return ResolverMatch(
            matched_variant=None,
            confidence=0,
            decision=DECISION_REJECTED,
            reason=REASON_EMPTY_INPUT,
            candidate_pool=candidates,
        )

    # Edge case: make/model not in catalog
    if not candidates:
        return ResolverMatch(
            matched_variant=None,
            confidence=0,
            decision=DECISION_REJECTED,
            reason=REASON_NO_CANDIDATES,
            candidate_pool=[],
        )

    normalized_raw_only = _normalize(raw_variant)
    normalized_with_aliases = _apply_aliases(normalized_raw_only)

    # Tier 1: exact match after pure normalization (no alias rewrite needed)
    exact = _exact_match_lookup(normalized_raw_only, candidates)
    if exact:
        return ResolverMatch(
            matched_variant=exact,
            confidence=100,
            decision=DECISION_AUTO_MATCH,
            reason=REASON_EXACT_MATCH,
            candidate_pool=candidates,
        )

    # Tier 2: exact match only after alias rewrite
    if normalized_with_aliases != normalized_raw_only:
        alias_exact = _exact_match_lookup(normalized_with_aliases, candidates)
        if alias_exact:
            return ResolverMatch(
                matched_variant=alias_exact,
                confidence=100,
                decision=DECISION_AUTO_MATCH,
                reason=REASON_ALIAS_HIT,
                candidate_pool=candidates,
            )

    # Tier 3: fuzzy match. Build a parallel list of normalized candidates
    # so rapidfuzz compares apples to apples; we pick the original-cased
    # candidate from the same index for the return value.
    normalized_candidates = [_normalize_for_match(c) for c in candidates]
    best = process.extractOne(
        normalized_with_aliases,
        normalized_candidates,
        scorer=fuzz.token_set_ratio,
    )
    # rapidfuzz returns (choice, score, index) for list inputs.
    if not best:
        return ResolverMatch(
            matched_variant=None,
            confidence=0,
            decision=DECISION_REJECTED,
            reason=REASON_FUZZY_LOW,
            candidate_pool=candidates,
        )

    _, score, idx = best
    score_int = int(round(score))
    matched_original = candidates[idx]

    if score_int >= CONFIDENCE_AUTO_MATCH:
        return ResolverMatch(
            matched_variant=matched_original,
            confidence=score_int,
            decision=DECISION_AUTO_MATCH,
            reason=REASON_FUZZY_HIGH,
            candidate_pool=candidates,
        )
    if score_int >= CONFIDENCE_NEEDS_REVIEW:
        return ResolverMatch(
            matched_variant=matched_original,
            confidence=score_int,
            decision=DECISION_NEEDS_REVIEW,
            reason=REASON_FUZZY_MID,
            candidate_pool=candidates,
        )

    return ResolverMatch(
        matched_variant=None,
        confidence=score_int,
        decision=DECISION_REJECTED,
        reason=REASON_FUZZY_LOW,
        candidate_pool=candidates,
    )


# ============================================================
# DIAGNOSTICS — for ad-hoc admin debugging only, not used in production
# ============================================================

def explain_resolution(make: str, model: str, raw_variant: str) -> dict:
    """
    Returns a dict with every intermediate value of the resolution pipeline.
    Useful when investigating why a specific listing was rejected. Not used
    in production code paths — purely an admin debugging tool.
    """
    norm_only = _normalize(raw_variant)
    norm_aliased = _apply_aliases(norm_only)
    candidates = car_data.get_variants(make, model)
    normalized_candidates = [_normalize_for_match(c) for c in candidates]
    match = resolve_variant(make, model, raw_variant)
    return {
        "input": {"make": make, "model": model, "raw_variant": raw_variant},
        "normalized": norm_only,
        "after_aliases": norm_aliased,
        "alias_changed_string": norm_only != norm_aliased,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "normalized_candidates": normalized_candidates,
        "result": {
            "matched_variant": match.matched_variant,
            "confidence": match.confidence,
            "decision": match.decision,
            "reason": match.reason,
        },
    }
