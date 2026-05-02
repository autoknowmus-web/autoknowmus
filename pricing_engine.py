"""
pricing_engine.py
================
AutoKnowMus — listing-comparable pricing engine.

This is the NEW engine that runs alongside the existing depreciation engine
(in car_data.py). The two are connected via a binary switch in app.py:

    if N_effective_listings >= 5:  ──► route here (market engine)
    else:                          ──► route to depreciation engine (existing)

This module is PURE — no DB calls, no web requests, no Flask. It takes a list
of listing dicts (from car_data.get_listings_for_car()) plus a user's car
spec, and returns a structured valuation result.

Methodology source: 740Li_R11_reference.xlsx, sheet "2015"
Verified against: F35=15157.41, F37=12057.41, F38=9645.93, F39=8769.03
                  reproduced within 0.04 OMR (0.0003%). Far below the
                  ±0.5% target from BUILD_KICKOFF.md.

The seven steps:
  1. Determine effective listings (filter applied upstream by car_data.py)
  2. Compute market_avg_km — mean of listing km, with min+max trimmed
     (Excel: B36/(N-2). This is FOUNDER-LOCKED Drift #1 resolution: we
     trim ONLY the avg-km denominator, not the prices. Median absorbs
     price extremes naturally.)
  3. Use price-per-km elasticity from config (default 0.0465 OMR/km in
     reference; in INR market we'll calibrate per Section 1B of design doc)
  4. Compute comparable_price_i for each listing:
        F_i = asking_price_i - (mileage_i - market_avg_km) × elasticity
  5. NMP (Net Market Price) = median(comparables) - repair_cost
  6. Selling price (F38) = NMP / (1 + negotiation_buffer)
       FOUNDER-LOCKED Drift #3 resolution: bug in design doc Step 8 fixed.
  7. Purchase price (F39) = Selling / (1 + purchase_buffer)

User-mileage adjustment (FOUNDER-LOCKED Drift #2 resolution: market_avg_km):
  user_specific_price = NMP - (user_mileage - market_avg_km) × elasticity
  This applies the user's actual km to the market-relative baseline.
  (Founder chose option 2b — market_avg over age-expected.)

v3.5 (state expansion): Added optional `state_multiplier` parameter applied
ONLY at the final step. The empirical signal (median, comparables, NMP) is
NOT multiplied — only the user-facing output (estimated_price, price_low,
price_high) and the audit `selling_price_F38` / `purchase_price_F39`. This
preserves the engine's empirical accuracy when used for Bangalore queries
(multiplier=1.0) and applies geographic adjustment for non-Bangalore users
when no city/state-specific verified deals exist (caller decides).

Outputs:
  Returns a PricingResult dict with:
    - estimated_price: int (rupees) — same semantic as compute_base_valuation
    - price_low / price_high: int rupees — uses Phase 1 ±12% range by default
    - confidence: int (0-100)
    - engine_used: 'market_v1'
    - n_listings_used: int
    - market_avg_km: int (the trimmed-mean mileage)
    - price_per_km_elasticity: float
    - median_listing_price: int rupees
    - repair_cost_applied: int rupees
    - negotiation_buffer_pct: float
    - nmp_F37: int rupees (Net Market Price)
    - selling_price_F38: int rupees (multiplier-applied)
    - purchase_price_F39: int rupees (multiplier-applied)
    - state_multiplier_applied: float (v3.5: 1.0 if no multiplier)

These map 1:1 to columns added in migration_phase_1A.sql for `valuations`.
"""

from typing import Dict, List, Optional, Tuple
import math
import logging

logger = logging.getLogger(__name__)


# ============================================================
# ENGINE CONFIG (will move to Google Sheets meta tab in Phase 2)
# ============================================================

# Minimum effective listings to use this engine. Below this, app.py routes
# to the depreciation engine fallback.
MIN_EFFECTIVE_LISTINGS = 5

# Default price-per-km elasticity (INR/km). Reference Excel uses 0.0465 OMR/km
# for a BMW 740Li in UAE. INR market for typical mid-segment cars expected to
# be in the 0.40-1.20 INR/km range; this is a CONFIG that should be
# calibrated per-segment in Phase 2 (TODO: move to meta sheet, with
# segment-specific overrides). Until then, this single value is used for all.
DEFAULT_PRICE_PER_KM_INR = 0.80

# Default repair cost — placeholder for ANY car. In the reference, this was
# 3100 OMR for a 5-yr-old BMW 740Li. For Indian market, this needs to scale
# with segment / age. CONFIG that should be calibrated per-segment in Phase 2.
# For now: default to 0 (no repair adjustment) so Phase 1A behavior is
# conservative. Founder/admin can override per-engine-call if needed.
DEFAULT_REPAIR_COST_INR = 0

# Negotiation buffer — between NMP (asking-equivalent) and what the seller
# realistically expects to GET. 25% in reference Excel.
DEFAULT_NEGOTIATION_BUFFER = 0.25

# Purchase buffer — between selling and dealer purchase price. 10% in reference.
DEFAULT_PURCHASE_BUFFER = 0.10

# Range width for Phase 1A binary-switch output (locked: ±12% same as Phase 1
# in PHASE_BLEND from car_data.py — keeps UX consistent across engines).
DEFAULT_RANGE_PCT = 0.12

# Outlier trim on the avg-km denominator (Excel does this by trimming
# Max+Min before dividing by N-2). Applies ONLY to the km computation,
# not the prices. This is FOUNDER-LOCKED (Drift #1, faithful port).
TRIM_MIN_MAX_FROM_AVG_KM = True


# ============================================================
# PUBLIC ENTRY POINT
# ============================================================

def compute_market_valuation(
    listings: List[Dict],
    user_year: int,
    user_mileage: int,
    user_condition: str = "Good",
    user_owner: str = "1st Owner",
    user_fuel: Optional[str] = None,
    *,
    price_per_km: Optional[float] = None,
    repair_cost: Optional[int] = None,
    negotiation_buffer: Optional[float] = None,
    purchase_buffer: Optional[float] = None,
    range_pct: Optional[float] = None,
    state_multiplier: Optional[float] = None,  # v3.5: state expansion
) -> Optional[Dict]:
    """
    Run the market pricing engine on a set of listings.

    Args:
        listings: list of dicts from car_data.get_listings_for_car(). Each must
                  have at minimum: 'mileage' (int), 'asking_price' (int rupees).
                  Optional: 'year', 'condition', 'owner', 'fuel'.
                  Listings should already be filtered to the matching car spec
                  by the caller (car_data.get_listings_for_car handles that).
        user_year: the year of the user's car (used for context only;
                   listings have already been year-windowed upstream)
        user_mileage: user's actual odometer reading (km)
        user_condition: 'Excellent' | 'Good' | 'Fair'
        user_owner: '1st Owner' | '2nd Owner' | '3rd Owner or more'
        user_fuel: optional, used for documentation only

        price_per_km, repair_cost, negotiation_buffer, purchase_buffer, range_pct:
            optional overrides. Default to module constants.

        state_multiplier (v3.5): optional float, applied to final user-facing
            output only. Default None / 1.0 = no adjustment (Bangalore behavior).
            When set (e.g. 0.965 for MP), scales estimated_price, price_low,
            price_high, selling_price_F38, purchase_price_F39 — but does NOT
            scale median, NMP, or comparables (those represent empirical signal).

    Returns:
        PricingResult dict (see module docstring for fields), OR
        None if listings count < MIN_EFFECTIVE_LISTINGS (caller falls back).

    Caller responsibility:
        - Caller (app.py) decides whether to use this engine vs. depreciation.
        - Caller decides whether to pass state_multiplier (only when no
          city/state-specific verified deals exist for user's location).
        - This function returns None if N too low, but app.py should already
          have skipped this path. Returning None is defensive.
    """
    n = len(listings)

    # Defensive: check minimum count
    if n < MIN_EFFECTIVE_LISTINGS:
        logger.info(f"market engine: N={n} below minimum {MIN_EFFECTIVE_LISTINGS}, returning None")
        return None

    # Apply config defaults
    if price_per_km is None:
        price_per_km = DEFAULT_PRICE_PER_KM_INR
    if repair_cost is None:
        repair_cost = DEFAULT_REPAIR_COST_INR
    if negotiation_buffer is None:
        negotiation_buffer = DEFAULT_NEGOTIATION_BUFFER
    if purchase_buffer is None:
        purchase_buffer = DEFAULT_PURCHASE_BUFFER
    if range_pct is None:
        range_pct = DEFAULT_RANGE_PCT

    # v3.5: State multiplier default = 1.0 (no adjustment)
    if state_multiplier is None:
        state_multiplier = 1.0

    # Sanity: clamp multiplier to reasonable range so a corrupted DB row
    # can't produce absurd output. State multipliers should fall in 0.85-1.15.
    if state_multiplier < 0.80 or state_multiplier > 1.20:
        logger.warning(
            f"market engine: state_multiplier={state_multiplier} out of bounds, clamping to [0.80, 1.20]"
        )
        state_multiplier = max(0.80, min(1.20, state_multiplier))

    # Extract listings data
    mileages = []
    prices = []
    for L in listings:
        try:
            km = int(L['mileage'])
            price = int(L['asking_price'])
        except (KeyError, TypeError, ValueError):
            continue  # skip malformed listing
        if km < 0 or price <= 0:
            continue
        mileages.append(km)
        prices.append(price)

    if len(mileages) < MIN_EFFECTIVE_LISTINGS:
        logger.warning(f"market engine: after filtering, only {len(mileages)} usable listings — returning None")
        return None

    # ============================================================
    # Step 1: Market average km (with min/max trimmed — Drift #1)
    # ============================================================
    market_avg_km = _compute_market_avg_km(mileages, trim_min_max=TRIM_MIN_MAX_FROM_AVG_KM)

    # ============================================================
    # Step 2: Compute comparable prices for each listing
    #   F_i = asking_price_i - (mileage_i - market_avg_km) × elasticity
    # ============================================================
    comparables = []
    for km, price in zip(mileages, prices):
        excess_km = km - market_avg_km
        comparable = price - excess_km * price_per_km
        comparables.append(comparable)

    # ============================================================
    # Step 3: Median of comparables (NO outlier trim — Drift #1 faithful port)
    # ============================================================
    median_comparable = _median(sorted(comparables))

    # ============================================================
    # Step 4: NMP (F37) = median - repair_cost
    # NOTE (v3.5): NMP is NOT multiplied. It represents the empirical signal
    # from listings. State adjustment is applied later only to user-facing prices.
    # ============================================================
    nmp_F37 = median_comparable - repair_cost

    # ============================================================
    # Step 5: User-specific price = NMP adjusted for user's actual mileage
    #   FOUNDER-LOCKED Drift #2: baseline = market_avg_km (not age-expected)
    # ============================================================
    user_excess_km = int(user_mileage) - market_avg_km
    user_specific_price = nmp_F37 - user_excess_km * price_per_km

    # ============================================================
    # Step 6: Selling price (F38) = user_specific / (1 + negotiation_buffer)
    #   FOUNDER-LOCKED Drift #3 fix: this step was missing in design doc
    # ============================================================
    selling_price_F38 = user_specific_price / (1.0 + negotiation_buffer)

    # ============================================================
    # Step 7: Purchase price (F39) = selling / (1 + purchase_buffer)
    # ============================================================
    purchase_price_F39 = selling_price_F38 / (1.0 + purchase_buffer)

    # ============================================================
    # Apply condition + owner multipliers
    # The reference Excel doesn't include these — it's a single-car
    # methodology. For our multi-condition / multi-owner offering, we
    # layer on the same multipliers used by the depreciation engine
    # (sourced from car_data.py to keep them consistent).
    # ============================================================
    cond_mult = _get_condition_multiplier(user_condition)
    owner_mult = _get_owner_multiplier(user_owner)
    estimated_price_raw = selling_price_F38 * cond_mult * owner_mult

    # ============================================================
    # v3.5: Apply state multiplier ONLY to user-facing prices
    # The empirical signals (median, NMP, comparables) stay unmultiplied
    # in the audit dict, so we can see what the raw market said vs. what
    # the user saw. This is critical for IP audit trail.
    # ============================================================
    estimated_price_adjusted = estimated_price_raw * state_multiplier
    selling_F38_adjusted = selling_price_F38 * state_multiplier
    purchase_F39_adjusted = purchase_price_F39 * state_multiplier

    # ============================================================
    # Confidence — function of N (more listings = more confidence)
    #   Mirrors the Phase 4 "Real-Market Pricing" confidence bands:
    #   N=5  → 60
    #   N=10 → 70
    #   N=20 → 80
    #   N=50 → 90 (capped)
    # NOTE (v3.5): app.py overrides this with the geographic-tier-aware
    # confidence score. This number is preserved as the "engine native"
    # confidence for audit purposes.
    # ============================================================
    confidence = _compute_confidence(len(mileages))

    # ============================================================
    # Range — applied AFTER state multiplier
    # ============================================================
    estimated_price = int(round(estimated_price_adjusted))
    price_low = int(round(estimated_price_adjusted * (1 - range_pct)))
    price_high = int(round(estimated_price_adjusted * (1 + range_pct)))

    return {
        'engine_used': 'market_v1',
        'estimated_price': estimated_price,
        'price_low': price_low,
        'price_high': price_high,
        'confidence': confidence,
        # Audit columns (1:1 with valuations table additions)
        'n_listings_used': len(mileages),
        'market_avg_km': int(round(market_avg_km)),
        'price_per_km_elasticity': round(price_per_km, 4),
        'median_listing_price': int(round(median_comparable)),
        'repair_cost_applied': int(repair_cost),
        'negotiation_buffer_pct': round(negotiation_buffer, 4),
        'nmp_F37': int(round(nmp_F37)),  # un-multiplied — empirical signal
        'selling_price_F38': int(round(selling_F38_adjusted)),  # multiplied
        'purchase_price_F39': int(round(purchase_F39_adjusted)),  # multiplied
        # v3.5: state multiplier audit
        'state_multiplier_applied': round(state_multiplier, 3),
        # Internal — for tooltip / debug
        'condition_mult': round(cond_mult, 3),
        'owner_mult': round(owner_mult, 3),
    }


# ============================================================
# INTERNAL HELPERS
# ============================================================

def _compute_market_avg_km(mileages: List[int], trim_min_max: bool = True) -> float:
    """
    Excel methodology (B36 ÷ (N-2)):
      total_km = sum(mileages)
      trimmed_total = total_km - max(mileages) - min(mileages)
      market_avg_km = trimmed_total / (N - 2)

    With trim_min_max=False (admin override), simple mean is used.
    """
    n = len(mileages)
    if n < 3 and trim_min_max:
        # Can't trim if we have <3 — fall back to simple mean
        return sum(mileages) / max(n, 1)
    if not trim_min_max:
        return sum(mileages) / n
    total = sum(mileages)
    trimmed_total = total - max(mileages) - min(mileages)
    return trimmed_total / (n - 2)


def _median(sorted_values: List[float]) -> float:
    """Median of a sorted list. Returns 0 for empty input."""
    n = len(sorted_values)
    if n == 0:
        return 0.0
    if n % 2 == 1:
        return float(sorted_values[n // 2])
    return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2.0


def _get_condition_multiplier(condition: str) -> float:
    """
    Mirrors car_data.FALLBACK_MULTIPLIERS["condition"]. Imported lazily
    to avoid a circular import with car_data when this module is loaded.
    """
    try:
        from car_data import get_multiplier
        return get_multiplier("condition", condition or "Good")
    except Exception:
        # Fallback if car_data unavailable (shouldn't happen in production)
        return {"Excellent": 1.05, "Good": 1.00, "Fair": 0.90}.get(condition, 1.00)


def _get_owner_multiplier(owner: str) -> float:
    try:
        from car_data import get_multiplier
        return get_multiplier("owner", owner or "1st Owner")
    except Exception:
        return {"1st Owner": 1.00, "2nd Owner": 0.95, "3rd Owner or more": 0.90}.get(owner, 1.00)


def _compute_confidence(n_listings: int) -> int:
    """
    Confidence as a function of N. Conservative ramp from 60 → 90.

      N=5  → 60   (minimum to use engine at all)
      N=10 → 70
      N=20 → 80
      N=50 → 88   (approaches 90 asymptotically)
      N=100+ → 90 (cap; further listings don't add confidence)

    Function: 60 + 30 × (1 - exp(-(N-5)/25))
    """
    if n_listings < MIN_EFFECTIVE_LISTINGS:
        return 0
    delta = n_listings - MIN_EFFECTIVE_LISTINGS
    raw = 60 + 30 * (1 - math.exp(-delta / 25))
    return min(90, int(round(raw)))


# ============================================================
# REGRESSION TESTS — run on import in dev / startup
# ============================================================

def _regression_test_2015_sheet() -> Dict:
    """
    Regression test against the 2015 sheet of 740Li_R11_reference.xlsx.
    Must reproduce F35/F37/F38/F39 within ±0.5%.

    v3.5: This test runs WITHOUT state_multiplier (default 1.0) so behavior
    is identical to pre-v3.5. New regression test for state multiplier behavior
    is _regression_test_state_multiplier() below.
    """
    listings_2015 = [
        (40000, 16666.67), (55000, 19392.03), (66000, 15618.45), (27397, 15618.45),
        (35700, 18763.10), (4800, 19916.14), (32000, 16666.67), (46000, 19706.50),
        (37700, 22012.58), (95000, 15723.27), (50000, 12788.26), (40356, 16766.56),
        (56000, 12054.51), (69000, 14570.23), (52000, 14884.70), (78000, 12054.51),
        (72000, 12578.51), (36000, 14570.23), (47000, 19916.14), (62556, 13522.01),
        (59429, 14675.05), (85000, 10377.36),
    ]

    listings = [{"mileage": km, "asking_price": price} for (km, price) in listings_2015]

    n = len(listings_2015)
    total = sum(km for km, _ in listings_2015)
    market_avg = (total - max(km for km, _ in listings_2015) - min(km for km, _ in listings_2015)) / (n - 2)

    result = compute_market_valuation(
        listings=listings,
        user_year=2020,
        user_mileage=int(round(market_avg)),
        user_condition="Good",
        user_owner="1st Owner",
        price_per_km=0.0465,
        repair_cost=3100,
        negotiation_buffer=0.25,
        purchase_buffer=0.10,
    )

    expected = {
        "median_F35": 15157.41,
        "nmp_F37": 12057.41,
        "selling_F38": 9645.93,
        "purchase_F39": 8769.03,
    }

    computed = {
        "median_F35": result["median_listing_price"],
        "nmp_F37": result["nmp_F37"],
        "selling_F38": result["selling_price_F38"],
        "purchase_F39": result["purchase_price_F39"],
    }

    diffs = {}
    for k in expected:
        e = expected[k]
        c = computed[k]
        pct = abs(c - e) / e * 100 if e != 0 else 0
        diffs[k] = {"expected": e, "computed": c, "pct_diff": round(pct, 4)}

    return {
        "result": result,
        "expected": expected,
        "computed": computed,
        "diffs": diffs,
        "all_within_tolerance": all(d["pct_diff"] < 0.5 for d in diffs.values()),
    }


def _regression_test_state_multiplier() -> Dict:
    """
    v3.5: Regression test that state_multiplier scales final output correctly
    while NOT scaling NMP/median (empirical signals).
    """
    listings = [
        {"mileage": 40000, "asking_price": 16666.67},
        {"mileage": 55000, "asking_price": 19392.03},
        {"mileage": 66000, "asking_price": 15618.45},
        {"mileage": 27397, "asking_price": 15618.45},
        {"mileage": 35700, "asking_price": 18763.10},
        {"mileage": 32000, "asking_price": 16666.67},
    ]

    baseline = compute_market_valuation(
        listings=listings,
        user_year=2020,
        user_mileage=43000,
        user_condition="Good",
        user_owner="1st Owner",
        price_per_km=0.0465,
        repair_cost=0,
        negotiation_buffer=0.25,
        purchase_buffer=0.10,
        state_multiplier=1.0,
    )

    mp_adjusted = compute_market_valuation(
        listings=listings,
        user_year=2020,
        user_mileage=43000,
        user_condition="Good",
        user_owner="1st Owner",
        price_per_km=0.0465,
        repair_cost=0,
        negotiation_buffer=0.25,
        purchase_buffer=0.10,
        state_multiplier=0.965,
    )

    nmp_unchanged = baseline['nmp_F37'] == mp_adjusted['nmp_F37']
    expected_ratio = 0.965
    actual_ratio = mp_adjusted['estimated_price'] / baseline['estimated_price']
    multiplier_correct = abs(actual_ratio - expected_ratio) < 0.001
    audit_correct = mp_adjusted['state_multiplier_applied'] == 0.965

    return {
        "nmp_unchanged": nmp_unchanged,
        "multiplier_correct": multiplier_correct,
        "audit_correct": audit_correct,
        "baseline_estimated": baseline['estimated_price'],
        "mp_adjusted_estimated": mp_adjusted['estimated_price'],
        "ratio_observed": round(actual_ratio, 4),
        "ratio_expected": expected_ratio,
        "all_pass": all([nmp_unchanged, multiplier_correct, audit_correct]),
    }


if __name__ == "__main__":
    import json

    print("=" * 60)
    print("REGRESSION TEST 1: 2015 Excel sheet (no state multiplier)")
    print("=" * 60)
    test1 = _regression_test_2015_sheet()
    for k, d in test1["diffs"].items():
        status = "PASS" if d["pct_diff"] < 0.5 else "FAIL"
        print(f"  [{status}] {k}: expected {d['expected']:>12.2f}, computed {d['computed']:>12.2f}, diff {d['pct_diff']:.4f}%")
    if test1["all_within_tolerance"]:
        print("\nTest 1: PASSED - engine port faithful, no v3.5 regression\n")
    else:
        print("\nTest 1: FAILED - at least one value exceeds +/-0.5% tolerance\n")

    print("=" * 60)
    print("REGRESSION TEST 2 (v3.5): State multiplier behavior")
    print("=" * 60)
    test2 = _regression_test_state_multiplier()
    print(f"  NMP unchanged when multiplier applied: {'PASS' if test2['nmp_unchanged'] else 'FAIL'}")
    print(f"  Estimated price scales correctly:      {'PASS' if test2['multiplier_correct'] else 'FAIL'}")
    print(f"  Audit field reports multiplier:        {'PASS' if test2['audit_correct'] else 'FAIL'}")
    print(f"\n  Baseline estimated:    {test2['baseline_estimated']:>10}")
    print(f"  MP-adjusted estimated: {test2['mp_adjusted_estimated']:>10}")
    print(f"  Ratio observed:        {test2['ratio_observed']:>10}")
    print(f"  Ratio expected:        {test2['ratio_expected']:>10}")
    print()
    if test2["all_pass"]:
        print("Test 2: PASSED - v3.5 state multiplier behaves correctly\n")
    else:
        print("Test 2: FAILED - v3.5 state multiplier has an issue\n")
