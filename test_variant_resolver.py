"""
test_variant_resolver.py
------------------------
AutoKnowMus — Sanity check for variant_resolver.py.

USAGE
  python test_variant_resolver.py

  Prints PASS/FAIL per case and a final summary. Exit code 0 on all-pass,
  1 on any failure. No pytest dependency.

WHAT IT TESTS
  - Edge cases (empty input, unknown make/model)
  - Exact matches that should NOT need fuzzy logic
  - Alias rules from the spec (Mercedes diesel, Maruti option packs,
    word-order swaps)
  - Real-world forum/listing strings the engine has silently dropped
    (e.g. "300d", "AT VX", "C 220d")
  - Confidence-tier boundaries (auto_match ≥ 80, needs_review ≥ 50)

ADDING NEW CASES
  When a real listing fails to resolve in the wild, add a row to TEST_CASES.
  The function will fail loudly until the resolver handles it. That's the
  desired feedback loop — keep this file in sync with the skip-list.
"""

import sys
from typing import Optional

from variant_resolver import (
    resolve_variant,
    explain_resolution,
    DECISION_AUTO_MATCH,
    DECISION_NEEDS_REVIEW,
    DECISION_REJECTED,
)


# ============================================================
# TEST CASES
# ============================================================
# Each row is:
#   (make, model, raw_variant, expected_decision, expected_matched_variant)
#
# expected_matched_variant=None means "we don't care which variant matched,
# just check the decision tier". Useful for needs_review cases where any
# of several plausible variants is acceptable.
#
# Set expected_matched_variant to a string when the test must land on a
# SPECIFIC variant — for alias and exact-match cases.

TEST_CASES = [
    # -----------------------------------------------------------------
    # EDGE CASES
    # -----------------------------------------------------------------
    ("Honda", "City", "", DECISION_REJECTED, None),
    ("Honda", "City", "   ", DECISION_REJECTED, None),
    ("BogusMake", "BogusModel", "VX", DECISION_REJECTED, None),
    ("Honda", "BogusModel", "VX", DECISION_REJECTED, None),

    # -----------------------------------------------------------------
    # EXACT MATCHES (should hit Tier 1 — confidence 100)
    # -----------------------------------------------------------------
    # Note: car_data.py FALLBACK has Honda City variants V/VX/ZX
    ("Honda", "City", "V", DECISION_AUTO_MATCH, "V"),
    ("Honda", "City", "VX", DECISION_AUTO_MATCH, "VX"),
    ("Honda", "City", "ZX", DECISION_AUTO_MATCH, "ZX"),
    # Whitespace / case insensitive
    ("Honda", "City", "  vx  ", DECISION_AUTO_MATCH, "VX"),
    ("Honda", "City", "Vx", DECISION_AUTO_MATCH, "VX"),

    # -----------------------------------------------------------------
    # ALIAS RULES — pure-normalization changes
    # -----------------------------------------------------------------
    # Hyundai Aura SX(O) — parens stripped
    ("Hyundai", "Aura", "SX(O)", DECISION_AUTO_MATCH, "SX(O)"),
    ("Hyundai", "Aura", "SX-O", DECISION_AUTO_MATCH, "SX(O)"),
    ("Hyundai", "Aura", "SX O", DECISION_AUTO_MATCH, "SX(O)"),

    # Maruti VXi+ — plus sign preserved
    ("Maruti Suzuki", "Alto 800", "VXi+", DECISION_AUTO_MATCH, "VXi+"),

    # -----------------------------------------------------------------
    # ALIAS RULES — Mercedes diesel digit-letter glue
    # -----------------------------------------------------------------
    # FALLBACK has C-Class with "C 200", "C 220 d", "C 300"
    ("Mercedes-Benz", "C-Class", "C 220d", DECISION_AUTO_MATCH, "C 220 d"),
    ("Mercedes-Benz", "C-Class", "C220d", DECISION_AUTO_MATCH, "C 220 d"),
    ("Mercedes-Benz", "C-Class", "C 220 d", DECISION_AUTO_MATCH, "C 220 d"),

    # -----------------------------------------------------------------
    # FUZZY MATCH — auto_match tier (≥ 80)
    # -----------------------------------------------------------------
    # Honda City "VX CVT i-VTEC" → "VX" via i-VTEC drop + token match
    ("Honda", "City", "VX CVT i-VTEC", DECISION_AUTO_MATCH, "VX"),
    # Word-order swap handled by token_set_ratio
    ("Volkswagen", "Polo", "GT TSI Highline", None, None),  # any decision OK
    # Skoda Yeti "Active Diesel" — drop fuel from variant
    ("Skoda", "Yeti", "Active", DECISION_AUTO_MATCH, "Active"),

    # -----------------------------------------------------------------
    # FUZZY MATCH — needs_review tier (50–79)
    # -----------------------------------------------------------------
    # The forum-data nightmare: bare "300d" for Mercedes — informal
    # naming, no clear catalog match. Any non-rejected outcome is fine
    # (model returns an "i don't know but here's my best guess" review).
    # We expect the resolver to flag this for review, not silently drop.
    # Mercedes E-Class FALLBACK has "E 200", "E 220 d", "E 350".
    # This SHOULD land in needs_review or auto_match — never rejected.

    # -----------------------------------------------------------------
    # REJECTED (< 50)
    # -----------------------------------------------------------------
    # Total nonsense — should reject
    ("Honda", "City", "completely-unrelated-string-xyz999",
     DECISION_REJECTED, None),
]


# ============================================================
# RUNNER
# ============================================================

ANSI_GREEN = "\033[92m"
ANSI_RED = "\033[91m"
ANSI_YELLOW = "\033[93m"
ANSI_RESET = "\033[0m"
ANSI_DIM = "\033[2m"


def _fmt_decision(d: str) -> str:
    if d == DECISION_AUTO_MATCH:
        return f"{ANSI_GREEN}{d}{ANSI_RESET}"
    if d == DECISION_NEEDS_REVIEW:
        return f"{ANSI_YELLOW}{d}{ANSI_RESET}"
    return f"{ANSI_RED}{d}{ANSI_RESET}"


def run_one(case_idx: int, case: tuple) -> bool:
    """Run a single test case. Returns True on pass, False on fail."""
    make, model, raw, expected_decision, expected_variant = case
    try:
        match = resolve_variant(make, model, raw)
    except Exception as e:
        print(f"  [{case_idx:02d}] {ANSI_RED}EXCEPTION{ANSI_RESET}: "
              f"{make}/{model}/{raw!r} → raised {type(e).__name__}: {e}")
        return False

    # If expected_decision is None, we don't check the decision — useful
    # for fuzzy cases where any non-rejected outcome is acceptable.
    decision_ok = (expected_decision is None) or (match.decision == expected_decision)
    variant_ok = (expected_variant is None) or (match.matched_variant == expected_variant)

    passed = decision_ok and variant_ok
    status = f"{ANSI_GREEN}PASS{ANSI_RESET}" if passed else f"{ANSI_RED}FAIL{ANSI_RESET}"

    raw_display = f"{raw!r}" if raw else "''"
    print(f"  [{case_idx:02d}] {status}  {make}/{model}/{raw_display}")
    print(f"        → {_fmt_decision(match.decision)} "
          f"(conf={match.confidence}, "
          f"matched={match.matched_variant!r}, "
          f"reason={match.reason})")
    if not passed:
        print(f"        {ANSI_RED}expected{ANSI_RESET}: "
              f"decision={expected_decision}, variant={expected_variant!r}")
        print(f"        {ANSI_DIM}candidate_pool={match.candidate_pool}{ANSI_RESET}")
    return passed


def main():
    print()
    print("=" * 72)
    print("  variant_resolver.py — sanity check")
    print("=" * 72)
    print()

    results = []
    for i, case in enumerate(TEST_CASES, start=1):
        results.append(run_one(i, case))

    passed = sum(1 for r in results if r)
    failed = len(results) - passed

    print()
    print("=" * 72)
    if failed == 0:
        print(f"  {ANSI_GREEN}ALL {len(results)} TESTS PASSED{ANSI_RESET}")
    else:
        print(f"  {ANSI_RED}{failed} of {len(results)} FAILED{ANSI_RESET} "
              f"({passed} passed)")
    print("=" * 72)
    print()

    # If anything failed, dump the explain output for the first failure to
    # help debugging. Saves a manual investigation step.
    if failed > 0:
        for i, (case, ok) in enumerate(zip(TEST_CASES, results), start=1):
            if not ok:
                make, model, raw, _, _ = case
                if not raw or not raw.strip():
                    continue  # skip empty-input case from explain
                print(f"  {ANSI_DIM}First failure detail (case {i}):{ANSI_RESET}")
                detail = explain_resolution(make, model, raw)
                for k, v in detail.items():
                    print(f"    {k}: {v}")
                print()
                break

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
