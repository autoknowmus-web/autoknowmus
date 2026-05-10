"""
calibration_engine.py
---------------------
AutoKnowMus — Step 4.5 — Calibration Engine (v2).

WHAT CHANGED IN v2 (Sprint 2 haircut bug fix):
  The U-layer calibration multiplier is computed against LISTING ASKING
  PRICES from research_log, but the underlying formula targets ACTUAL
  SALE PRICES. Asking prices are systematically inflated above what
  cars actually transact for — typically 10–15% above. So the v1
  multiplier was biased upward by ~15%, silently overpricing every
  calibrated cell.

  v2 applies the haircut (NEGOTIATION_HAIRCUT = 0.85, i.e. 15% off
  asking) to listing prices BEFORE computing the median. The cell
  multiplier now compares effective-sale-price medians against
  formula-output medians — apples to apples.

  Architecture hook:
    The haircut is currently a single global constant (0.85). When
    feedback or volume eventually justifies it, this can be replaced
    with a per-segment or per-cell lookup without changing call sites.
    See _get_haircut_for_cell() — currently returns the global default
    for every cell; future versions can return tier- or cell-specific
    values. This isolates the change point.

WHAT THIS DOES:
  Reads listings from research_log (last 180 days, data_source='Listing Aggregator',
  include_in_calibration=true). Groups by (make, model, fuel). For each cell
  with >=3 listings, computes:

      effective_listing_prices  = listing_prices × haircut(make, model, fuel)
      calibration_multiplier    = trimmed_median(effective_listing_prices) /
                                  median(formula_prices_for_those_listings)

  Writes one row per cell to model_calibration (upsert on make+model+fuel).
  Logs the run to calibration_runs.

WHAT THIS DOESN'T DO:
  - Does NOT modify research_log entries (other than setting
    used_in_last_calibration / last_calibration_run_id flags)
  - Does NOT touch valuations, car_prices sheet, or any other table
  - Does NOT auto-trigger — admin must call run_calibration() explicitly

DESIGN DECISIONS LOCKED:
  - Calibration grain: (make, model, fuel) — variant rolled up
  - Window: last 180 days based on entry_date
  - Min sample threshold: 3 listings per cell
  - Phase mapping: 3-9 -> phase 2, 10-19 -> phase 3, 20+ -> phase 4
  - Storage: single multiplier per cell (Option B)
  - Bounds: multiplier clamped to [0.50, 1.50]
  - Outlier trim: top 10% + bottom 10% (skipped when n < 5)
  - Price field: COALESCE(negotiated_price_inr, asking_price_inr)
  - Bad formula output: skip listing, count it, continue
  - Haircut: global default 0.85, applied to ASKING prices only (when
    negotiated_price_inr is missing); negotiated prices already reflect
    actual deal value and are NOT haircut

USAGE:
  from calibration_engine import run_calibration, run_calibration_for_cell

  # Full sweep — admin "Run Calibration" button calls this:
  result = run_calibration(run_by="admin@autoknowmus.com")
  # -> dict with run_id, cells_updated, cells_skipped_low_sample, etc.

  # Single cell — post-upload re-calibration calls this:
  result = run_calibration_for_cell(
      make="Maruti Suzuki", model="Brezza", fuel="Petrol",
      run_by="auto_post_upload"
  )
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from supabase import create_client, Client

# Import the formula function from car_data
from car_data import compute_base_valuation


# ============================================================
# LOCKED CONSTANTS (decisions confirmed in Session 1 design phase)
# ============================================================

CALIBRATION_WINDOW_DAYS = 180
MIN_SAMPLES_PER_CELL    = 3

# Phase thresholds — match dashboard.html badge UX
PHASE_2_MIN = 3   # 3-9   listings -> phase 2 (Calibrated)
PHASE_3_MIN = 10  # 10-19 listings -> phase 3 (Market-verified)
PHASE_4_MIN = 20  # 20+   listings -> phase 4 (Real-Market)

# Multiplier safety bounds
MULTIPLIER_MIN = 0.50
MULTIPLIER_MAX = 1.50

# Outlier trim
TRIM_FRAC = 0.10
TRIM_MIN_SAMPLES = 5  # skip trimming when n < 5 (every point counts)

# What we read from research_log
LISTING_DATA_SOURCE = "Listing Aggregator"

# ============================================================
# HAIRCUT (negotiation gap between asking and selling price)
# ============================================================
#
# The U-layer calibration uses listing prices from research_log, which
# are predominantly ASKING prices from OLX/CarTrade/Spinny/CarDekho.
# Asking prices are systematically above the actual transacted price by
# 10–15% in the Indian used car market (the negotiation gap).
#
# The formula function (compute_base_valuation) targets actual SALE
# prices, not asking prices. To make the calibration multiplier a fair
# comparison, asking prices must be haircut DOWN by the negotiation
# gap before being compared to formula output.
#
# Default: 0.85 (15% haircut, i.e. effective_price = asking × 0.85).
#
# When a listing has a negotiated_price_inr set, that IS the deal value
# and NO haircut is applied — it's already an actual sale price.
#
# Future hook: _get_haircut_for_cell() can grow into a per-segment or
# per-cell lookup table without changing the call sites in
# _calibrate_one_cell(). See its docstring for the upgrade path.
# ============================================================

NEGOTIATION_HAIRCUT_DEFAULT = 0.85

# ============================================================
# Backward-compatible v1 surface area
# ------------------------------------------------------------
# app.py imports the following names from calibration_engine:
#   NEGOTIATION_GAP_DEFAULT
#   NEGOTIATION_GAP_HARD_MIN
#   NEGOTIATION_GAP_HARD_MAX
#   NEGOTIATION_GAP_SOFT_MIN
#   NEGOTIATION_GAP_SOFT_MAX
#   load_negotiation_gap
#   get_negotiation_gap
#
# These were the v1 names for the negotiation gap (a.k.a. the
# haircut between asking and selling price). The v2 engine renamed
# the canonical constant to NEGOTIATION_HAIRCUT_DEFAULT for clarity,
# but ALL v1 names are aliased back here so existing callers in
# app.py and any other module keep working without modification.
#
# Sanity bounds:
#   HARD_MIN  = 0.70  → 30% haircut (anything below is implausible)
#   HARD_MAX  = 1.00  → no haircut (anything above is implausible)
#   SOFT_MIN  = 0.80  → 20% haircut (below this is unusual)
#   SOFT_MAX  = 0.92  → 8%  haircut (above this is unusual)
#
# Admin UI can warn (not block) when an effective gap falls outside
# the soft band; absolute clamping happens at the hard bounds.
# ============================================================

NEGOTIATION_GAP_DEFAULT   = NEGOTIATION_HAIRCUT_DEFAULT
NEGOTIATION_GAP_HARD_MIN  = 0.70
NEGOTIATION_GAP_HARD_MAX  = 1.00
NEGOTIATION_GAP_SOFT_MIN  = 0.80
NEGOTIATION_GAP_SOFT_MAX  = 0.92


def load_negotiation_gap() -> float:
    """
    Returns the current global negotiation gap (haircut) value.

    Defensive v2 implementation: returns NEGOTIATION_GAP_DEFAULT.

    Forward path: when an admin-settings table is wired up, this can
    read the live override from Supabase and fall back to the default
    on any error. The function signature stays the same, so call sites
    don't change.
    """
    return NEGOTIATION_GAP_DEFAULT


def get_negotiation_gap(make: str = None, model: str = None, fuel: str = None) -> float:
    """
    Returns the negotiation gap (haircut) for a given (make, model, fuel)
    cell. Today this is the global default for every cell — see
    _get_haircut_for_cell() for the upgrade hook.

    All args are optional to stay compatible with any v1 caller that
    invoked this with zero, one, or all three arguments.
    """
    return _get_haircut_for_cell(make or "", model or "", fuel or "")


def _get_haircut_for_cell(make: str, model: str, fuel: str) -> float:
    """
    Returns the haircut multiplier for a given (make, model, fuel) cell.

    v2 (Sprint 2): returns the global default for every cell.

    Future expansion paths (Phase 2 of U-layer calibration):
      1. Brand-tier segmentation:
            premium  (Mercedes, BMW, Audi, etc.)    → 0.80
            mid      (Honda, Toyota, Hyundai, etc.) → 0.85
            mass     (Maruti, Tata, Renault, etc.)  → 0.88
            exotic   (Porsche, Bentley, etc.)       → 0.75
      2. Per-cell learned values from user feedback divergence
      3. Manual admin override in a haircut_overrides table

    All three can be added later without changing _calibrate_one_cell().
    """
    return NEGOTIATION_HAIRCUT_DEFAULT


# ============================================================
# SUPABASE CLIENT
# ============================================================

_supabase_client: Optional[Client] = None


def _get_supabase() -> Client:
    """
    Lazy-init Supabase client. Uses the SAME env var names as app.py:
      SUPABASE_URL, SUPABASE_SECRET_KEY
    """
    global _supabase_client
    if _supabase_client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SECRET_KEY")
        if not url or not key:
            raise RuntimeError(
                "Supabase credentials missing. Set SUPABASE_URL and "
                "SUPABASE_SECRET_KEY environment vars (same as app.py)."
            )
        _supabase_client = create_client(url, key)
    return _supabase_client


# ============================================================
# MATH HELPERS
# ============================================================

def _trimmed_median(prices: List[int]) -> Optional[int]:
    """
    Returns the trimmed median of a list of prices.

    - If n < TRIM_MIN_SAMPLES (5), no trimming, just plain median.
    - Otherwise, drops top 10% + bottom 10% before taking median.
    - Returns None on empty input.
    """
    if not prices:
        return None
    sorted_p = sorted(prices)
    n = len(sorted_p)
    if n >= TRIM_MIN_SAMPLES:
        k = int(n * TRIM_FRAC)
        if k > 0:
            sorted_p = sorted_p[k:n - k]
    return _median(sorted_p)


def _median(sorted_prices: List[int]) -> Optional[int]:
    n = len(sorted_prices)
    if n == 0:
        return None
    if n % 2 == 1:
        return int(sorted_prices[n // 2])
    return int((sorted_prices[n // 2 - 1] + sorted_prices[n // 2]) // 2)


def _clamp_multiplier(raw: float) -> float:
    """Clamp multiplier to [MULTIPLIER_MIN, MULTIPLIER_MAX]."""
    return max(MULTIPLIER_MIN, min(MULTIPLIER_MAX, raw))


def _phase_for_sample_count(n: int) -> int:
    """Map sample count to phase number (1=formula only, 4=real-market)."""
    if n >= PHASE_4_MIN:
        return 4
    if n >= PHASE_3_MIN:
        return 3
    if n >= PHASE_2_MIN:
        return 2
    return 1


# ============================================================
# DATA FETCH
# ============================================================

def _fetch_eligible_listings(
    cell_filter: Optional[Tuple[str, str, str]] = None,
) -> List[Dict]:
    """
    Pulls listings from research_log eligible for calibration.

    Filters:
      - data_source = 'Listing Aggregator'
      - include_in_calibration = true
      - entry_date >= today - 180 days
      - asking_price_inr is not null (we coalesce with negotiated below)

    If cell_filter is provided as (make, model, fuel), only fetch that cell.

    Returns list of dicts with keys:
      id, make, model, variant, fuel, year, mileage_km, condition, owners,
      asking_price_inr, negotiated_price_inr, entry_date
    """
    sb = _get_supabase()
    cutoff_date = (datetime.now(timezone.utc).date() - timedelta(days=CALIBRATION_WINDOW_DAYS)).isoformat()

    query = (
        sb.table("research_log")
        .select(
            "id, make, model, variant, fuel, year, mileage_km, "
            "condition, owners, asking_price_inr, negotiated_price_inr, "
            "entry_date"
        )
        .eq("data_source", LISTING_DATA_SOURCE)
        .eq("include_in_calibration", True)
        .gte("entry_date", cutoff_date)
    )

    if cell_filter is not None:
        make, model, fuel = cell_filter
        query = query.eq("make", make).eq("model", model).eq("fuel", fuel)

    result = query.limit(10000).execute()
    return result.data or []


# ============================================================
# CORE CALIBRATION LOGIC
# ============================================================

def _calibrate_one_cell(
    make: str,
    model: str,
    fuel: str,
    listings: List[Dict],
) -> Optional[Dict]:
    """
    Computes the calibration result for one (make, model, fuel) cell.

    Returns dict with all fields needed for the model_calibration upsert,
    or None if the cell has < MIN_SAMPLES_PER_CELL usable listings.

    PRICE-PER-LISTING DERIVATION (v2 bug fix):
      For each listing:
        1. If negotiated_price_inr is set, that IS an actual sale price.
           Use it directly. No haircut.
        2. Otherwise, use asking_price_inr × haircut. The haircut converts
           the asking price into an estimated effective sale price so
           it's comparable to formula output (which targets sale prices).

      This produces "effective" listing prices that are apples-to-apples
      with formula prices. The median of these effective prices is what
      gets divided by the formula median to compute the cell multiplier.

    "Usable" means:
      - Has a price (negotiated, or asking after haircut)
      - compute_base_valuation() returns a positive int for it

    Bad listings are silently skipped — they don't break the cell.
    """
    haircut = _get_haircut_for_cell(make, model, fuel)

    effective_listing_prices: List[int] = []   # haircut-adjusted (apples-to-apples)
    raw_asking_prices:        List[int] = []   # un-adjusted asking, for diagnostic
    formula_prices:           List[int] = []   # what our formula would predict
    used_listing_ids:         List[str] = []   # for marking used_in_last_calibration

    n_negotiated_used = 0  # diagnostic: how many had explicit negotiated price
    n_haircut_applied = 0  # diagnostic: how many got the haircut treatment

    listings_oldest = None
    listings_newest = None

    for L in listings:
        # 1) Determine effective price using v2 rule
        negotiated = L.get("negotiated_price_inr")
        asking     = L.get("asking_price_inr")

        if negotiated and negotiated > 0:
            # Actual deal value — no haircut
            effective_price = int(negotiated)
            n_negotiated_used += 1
            # For diagnostic: treat negotiated as the "raw" too (no haircut applies)
            raw_for_diag = int(negotiated)
        elif asking and asking > 0:
            # Asking price → apply haircut to get an effective sale price
            effective_price = int(round(asking * haircut))
            n_haircut_applied += 1
            raw_for_diag = int(asking)
        else:
            # No usable price at all
            continue

        if effective_price <= 0:
            continue

        # 2) Run the formula on this listing's spec
        try:
            formula_price = compute_base_valuation(
                make=L.get("make"),
                model=L.get("model"),
                variant=L.get("variant"),
                fuel=L.get("fuel"),
                year=L.get("year"),
                mileage=L.get("mileage_km") or 0,
                condition=L.get("condition"),
                owner=L.get("owners"),
            )
        except Exception:
            continue

        if formula_price is None or formula_price <= 0:
            continue

        effective_listing_prices.append(effective_price)
        raw_asking_prices.append(raw_for_diag)
        formula_prices.append(int(formula_price))
        used_listing_ids.append(L["id"])

        # Track oldest/newest dates for transparency
        entry_date = L.get("entry_date")
        if entry_date:
            if listings_oldest is None or entry_date < listings_oldest:
                listings_oldest = entry_date
            if listings_newest is None or entry_date > listings_newest:
                listings_newest = entry_date

    n = len(effective_listing_prices)
    if n < MIN_SAMPLES_PER_CELL:
        return None

    # 3) Compute medians
    median_effective = _trimmed_median(effective_listing_prices)
    median_raw       = _trimmed_median(raw_asking_prices)
    median_formula   = _trimmed_median(formula_prices)

    if not median_formula or median_formula <= 0:
        return None

    # 4) Compute multiplier and clamp
    raw_multiplier = median_effective / median_formula
    clamped_multiplier = _clamp_multiplier(raw_multiplier)

    # 5) Phase
    phase = _phase_for_sample_count(n)

    return {
        "make": make,
        "model": model,
        "fuel": fuel,
        "calibration_multiplier": round(clamped_multiplier, 4),
        "sample_count": n,
        "phase": phase,
        # v2: median_listing_price now stores the haircut-adjusted (effective)
        # median, which is what the multiplier was computed against. The raw
        # asking median is kept internally for diagnostics but not persisted
        # (column-add decision deferred per Sprint 2 scope).
        "median_listing_price": median_effective,
        "median_formula_price": median_formula,
        "listings_window_days": CALIBRATION_WINDOW_DAYS,
        "listings_oldest_date": listings_oldest,
        "listings_newest_date": listings_newest,

        # Internal — not a column, used to flag rows after upsert
        "_used_listing_ids": used_listing_ids,
        # Internal — diagnostics for the run summary
        "_raw_multiplier": round(raw_multiplier, 4),
        "_was_clamped": (raw_multiplier != clamped_multiplier),
        "_haircut_used": haircut,
        "_median_raw_asking": median_raw,
        "_n_negotiated_used": n_negotiated_used,
        "_n_haircut_applied": n_haircut_applied,
    }


def _group_listings_by_cell(listings: List[Dict]) -> Dict[Tuple[str, str, str], List[Dict]]:
    """Group listings into a dict keyed by (make, model, fuel)."""
    groups: Dict[Tuple[str, str, str], List[Dict]] = {}
    for L in listings:
        make = (L.get("make") or "").strip()
        model = (L.get("model") or "").strip()
        fuel = (L.get("fuel") or "").strip()
        if not (make and model and fuel):
            continue
        key = (make, model, fuel)
        groups.setdefault(key, []).append(L)
    return groups


# ============================================================
# DATABASE WRITES
# ============================================================

def _start_run(run_by: str, run_type: str = "manual") -> str:
    """Insert a calibration_runs row in 'running' state, return its id."""
    sb = _get_supabase()
    payload = {
        "run_by": run_by,
        "run_type": run_type,
        "status": "running",
        "window_days": CALIBRATION_WINDOW_DAYS,
    }
    result = sb.table("calibration_runs").insert(payload).execute()
    if not result.data:
        raise RuntimeError("Failed to insert calibration_runs row")
    return result.data[0]["id"]


def _finish_run(run_id: str, status: str, summary: Dict, error_message: Optional[str] = None):
    """Update the calibration_runs row with final counts and status."""
    sb = _get_supabase()
    payload = {
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "cells_evaluated": summary.get("cells_evaluated", 0),
        "cells_updated": summary.get("cells_updated", 0),
        "cells_inserted": summary.get("cells_inserted", 0),
        "cells_skipped_low_sample": summary.get("cells_skipped_low_sample", 0),
        "listings_considered": summary.get("listings_considered", 0),
        "listings_used": summary.get("listings_used", 0),
        "notes": summary.get("notes"),
    }
    if error_message:
        payload["error_message"] = error_message
    sb.table("calibration_runs").update(payload).eq("id", run_id).execute()


def _upsert_cell(cell_result: Dict, run_id: str) -> Tuple[bool, bool]:
    """
    Upsert a model_calibration row for one cell.

    Returns (was_inserted, was_updated).
    """
    sb = _get_supabase()
    make = cell_result["make"]
    model = cell_result["model"]
    fuel = cell_result["fuel"]

    existing = (
        sb.table("model_calibration")
        .select("id")
        .eq("make", make).eq("model", model).eq("fuel", fuel)
        .limit(1)
        .execute()
    )

    payload = {
        "make": make,
        "model": model,
        "fuel": fuel,
        "calibration_multiplier": cell_result["calibration_multiplier"],
        "sample_count": cell_result["sample_count"],
        "phase": cell_result["phase"],
        "median_listing_price": cell_result["median_listing_price"],
        "median_formula_price": cell_result["median_formula_price"],
        "listings_window_days": cell_result["listings_window_days"],
        "listings_oldest_date": cell_result["listings_oldest_date"],
        "listings_newest_date": cell_result["listings_newest_date"],
        "last_run_id": run_id,
        "last_run_at": datetime.now(timezone.utc).isoformat(),
    }

    if existing.data:
        sb.table("model_calibration").update(payload).eq("id", existing.data[0]["id"]).execute()
        return (False, True)
    else:
        sb.table("model_calibration").insert(payload).execute()
        return (True, False)


def _flag_used_listings(listing_ids: List[str], run_id: str):
    """
    Mark listings that fed into the current calibration.
    """
    if not listing_ids:
        return
    sb = _get_supabase()
    payload = {
        "used_in_last_calibration": True,
        "last_calibration_run_id": run_id,
    }
    CHUNK = 200
    for i in range(0, len(listing_ids), CHUNK):
        batch = listing_ids[i:i + CHUNK]
        sb.table("research_log").update(payload).in_("id", batch).execute()


def _reset_used_flags():
    """
    Clear used_in_last_calibration on ALL Listing Aggregator rows before
    a full sweep.
    """
    sb = _get_supabase()
    sb.table("research_log").update({
        "used_in_last_calibration": False,
        "last_calibration_run_id": None,
    }).eq("data_source", LISTING_DATA_SOURCE).eq(
        "used_in_last_calibration", True
    ).execute()


# ============================================================
# PUBLIC API
# ============================================================

def run_calibration(run_by: str, run_type: str = "manual") -> Dict:
    """
    FULL SWEEP. Recomputes calibration for every (make, model, fuel) cell
    that has >= MIN_SAMPLES_PER_CELL listings within the 180-day window.

    Args:
      run_by:   identifier of who triggered the run (admin email or 'system')
      run_type: 'manual' | 'scheduled' | 'post_upload'

    Returns dict:
      {
        "ok": bool,
        "run_id": str,
        "cells_evaluated": int,
        "cells_updated": int,
        "cells_inserted": int,
        "cells_skipped_low_sample": int,
        "listings_considered": int,
        "listings_used": int,
        "clamped_cells": int,
        "haircut_used_default": float,    # NEW in v2 — for run summary
        "listings_negotiated_count": int, # NEW in v2 — listings with explicit negotiated price
        "listings_haircut_count": int,    # NEW in v2 — listings where haircut applied
        "notes": str,
      }
    """
    run_id = _start_run(run_by=run_by, run_type=run_type)
    summary = {
        "cells_evaluated": 0,
        "cells_updated": 0,
        "cells_inserted": 0,
        "cells_skipped_low_sample": 0,
        "listings_considered": 0,
        "listings_used": 0,
        "clamped_cells": 0,
        "haircut_used_default": NEGOTIATION_HAIRCUT_DEFAULT,
        "listings_negotiated_count": 0,
        "listings_haircut_count": 0,
        "notes": "",
    }

    try:
        _reset_used_flags()

        listings = _fetch_eligible_listings()
        summary["listings_considered"] = len(listings)

        if not listings:
            summary["notes"] = "No eligible listings in window."
            _finish_run(run_id, "completed", summary)
            return {"ok": True, "run_id": run_id, **summary}

        groups = _group_listings_by_cell(listings)
        summary["cells_evaluated"] = len(groups)

        all_used_ids: List[str] = []
        clamped_examples: List[str] = []

        for (make, model, fuel), cell_listings in groups.items():
            cell_result = _calibrate_one_cell(make, model, fuel, cell_listings)
            if cell_result is None:
                summary["cells_skipped_low_sample"] += 1
                continue

            inserted, updated = _upsert_cell(cell_result, run_id)
            if inserted:
                summary["cells_inserted"] += 1
            elif updated:
                summary["cells_updated"] += 1

            all_used_ids.extend(cell_result["_used_listing_ids"])
            summary["listings_used"] += len(cell_result["_used_listing_ids"])
            summary["listings_negotiated_count"] += cell_result["_n_negotiated_used"]
            summary["listings_haircut_count"] += cell_result["_n_haircut_applied"]

            if cell_result["_was_clamped"]:
                summary["clamped_cells"] += 1
                if len(clamped_examples) < 5:
                    clamped_examples.append(
                        f"{make} {model} ({fuel}): raw={cell_result['_raw_multiplier']:.3f}"
                    )

        _flag_used_listings(all_used_ids, run_id)

        notes_parts = [
            f"Considered {summary['listings_considered']} listings, "
            f"used {summary['listings_used']} across "
            f"{summary['cells_evaluated']} cells "
            f"(haircut={NEGOTIATION_HAIRCUT_DEFAULT} applied to "
            f"{summary['listings_haircut_count']} asking-price listings; "
            f"{summary['listings_negotiated_count']} had explicit negotiated price).",
            f"{summary['cells_inserted']} inserted, "
            f"{summary['cells_updated']} updated, "
            f"{summary['cells_skipped_low_sample']} skipped (< {MIN_SAMPLES_PER_CELL} samples).",
        ]
        if summary["clamped_cells"] > 0:
            notes_parts.append(
                f"{summary['clamped_cells']} cells clamped to "
                f"[{MULTIPLIER_MIN}, {MULTIPLIER_MAX}] bounds."
            )
            if clamped_examples:
                notes_parts.append("Clamped: " + "; ".join(clamped_examples))
        summary["notes"] = " ".join(notes_parts)

        _finish_run(run_id, "completed", summary)
        return {"ok": True, "run_id": run_id, **summary}

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        summary["notes"] = f"Run failed: {err}"
        try:
            _finish_run(run_id, "failed", summary, error_message=err)
        except Exception:
            pass
        return {"ok": False, "run_id": run_id, "error": err, **summary}


def run_calibration_for_cell(
    make: str,
    model: str,
    fuel: str,
    run_by: str = "auto_post_upload",
    run_type: str = "post_upload",
) -> Dict:
    """
    SINGLE CELL. Recomputes calibration for one (make, model, fuel) cell only.
    Same return shape as run_calibration().
    """
    run_id = _start_run(run_by=run_by, run_type=run_type)
    summary = {
        "cells_evaluated": 1,
        "cells_updated": 0,
        "cells_inserted": 0,
        "cells_skipped_low_sample": 0,
        "listings_considered": 0,
        "listings_used": 0,
        "clamped_cells": 0,
        "haircut_used_default": _get_haircut_for_cell(make, model, fuel),
        "listings_negotiated_count": 0,
        "listings_haircut_count": 0,
        "notes": "",
    }

    try:
        listings = _fetch_eligible_listings(cell_filter=(make, model, fuel))
        summary["listings_considered"] = len(listings)

        if not listings:
            summary["notes"] = (
                f"No eligible listings for {make} {model} ({fuel}) in window."
            )
            summary["cells_skipped_low_sample"] = 1
            _finish_run(run_id, "completed", summary)
            return {"ok": True, "run_id": run_id, **summary}

        cell_result = _calibrate_one_cell(make, model, fuel, listings)
        if cell_result is None:
            summary["cells_skipped_low_sample"] = 1
            summary["notes"] = (
                f"{make} {model} ({fuel}): only {len(listings)} listing(s), "
                f"need {MIN_SAMPLES_PER_CELL}+."
            )
            _finish_run(run_id, "completed", summary)
            return {"ok": True, "run_id": run_id, **summary}

        inserted, updated = _upsert_cell(cell_result, run_id)
        if inserted:
            summary["cells_inserted"] = 1
        elif updated:
            summary["cells_updated"] = 1

        _flag_used_listings(cell_result["_used_listing_ids"], run_id)
        summary["listings_used"] = len(cell_result["_used_listing_ids"])
        summary["listings_negotiated_count"] = cell_result["_n_negotiated_used"]
        summary["listings_haircut_count"] = cell_result["_n_haircut_applied"]
        if cell_result["_was_clamped"]:
            summary["clamped_cells"] = 1

        summary["notes"] = (
            f"{make} {model} ({fuel}): "
            f"n={cell_result['sample_count']}, "
            f"haircut={cell_result['_haircut_used']} "
            f"(applied to {cell_result['_n_haircut_applied']} asking, "
            f"{cell_result['_n_negotiated_used']} had negotiated), "
            f"multiplier={cell_result['calibration_multiplier']}, "
            f"phase={cell_result['phase']}"
            + (" [clamped]" if cell_result["_was_clamped"] else "")
        )

        _finish_run(run_id, "completed", summary)
        return {"ok": True, "run_id": run_id, **summary}

    except Exception as e:
        err = f"{type(e).__name__}: {e}"
        summary["notes"] = f"Run failed: {err}"
        try:
            _finish_run(run_id, "failed", summary, error_message=err)
        except Exception:
            pass
        return {"ok": False, "run_id": run_id, "error": err, **summary}


def get_calibration_for_cell(make: str, model: str, fuel: str) -> Optional[Dict]:
    """
    Read-only helper. Returns the current calibration row for a cell, or None.
    """
    sb = _get_supabase()
    result = (
        sb.table("model_calibration")
        .select("*")
        .eq("make", make).eq("model", model).eq("fuel", fuel)
        .limit(1)
        .execute()
    )
    return result.data[0] if result.data else None


def get_recent_runs(limit: int = 10) -> List[Dict]:
    """Read-only helper. Returns recent calibration_runs rows for admin UI."""
    sb = _get_supabase()
    result = (
        sb.table("calibration_runs")
        .select("*")
        .order("started_at", desc=True)
        .limit(limit)
        .execute()
    )
    return result.data or []


# ============================================================
# CLI ENTRYPOINT (for ad-hoc testing — not used by Flask)
# ============================================================
if __name__ == "__main__":
    """
    Allows you to test from terminal:
        python calibration_engine.py
    Will run a full sweep using run_by='cli_test' and print the summary.
    """
    print("[calibration_engine v2] Running full sweep (CLI test mode)...")
    print(f"  Haircut default: {NEGOTIATION_HAIRCUT_DEFAULT}")
    result = run_calibration(run_by="cli_test", run_type="manual")
    print()
    print("=" * 60)
    print(f"  Run ID:                  {result.get('run_id')}")
    print(f"  Status:                  {'OK' if result.get('ok') else 'FAILED'}")
    print(f"  Listings considered:     {result.get('listings_considered')}")
    print(f"  Listings used:           {result.get('listings_used')}")
    print(f"    - negotiated price:    {result.get('listings_negotiated_count')}")
    print(f"    - asking + haircut:    {result.get('listings_haircut_count')}")
    print(f"  Cells evaluated:         {result.get('cells_evaluated')}")
    print(f"  Cells inserted:          {result.get('cells_inserted')}")
    print(f"  Cells updated:           {result.get('cells_updated')}")
    print(f"  Cells skipped (low n):   {result.get('cells_skipped_low_sample')}")
    print(f"  Clamped cells:           {result.get('clamped_cells')}")
    if result.get("error"):
        print(f"  Error:                   {result.get('error')}")
    print("=" * 60)
    print(f"  Notes: {result.get('notes')}")
    print()
