"""
calibration_engine.py
---------------------
AutoKnowMus — Step 4.5 — Calibration Engine (v1).

WHAT THIS DOES:
  Reads listings from research_log (last 180 days, data_source='Listing Aggregator',
  include_in_calibration=true). Groups by (make, model, fuel). For each cell
  with >=3 listings, computes:

      calibration_multiplier = trimmed_median(listing_prices) /
                               median(formula_prices_for_those_listings)

  Writes one row per cell to model_calibration (upsert on make+model+fuel).
  Logs the run to calibration_runs.

WHAT THIS DOESN'T DO:
  - Does NOT modify research_log entries (other than setting
    used_in_last_calibration / last_calibration_run_id flags)
  - Does NOT touch valuations, car_prices sheet, or any other table
  - Does NOT auto-trigger — admin must call run_calibration() explicitly
    (Session 2 will add the admin button)

DESIGN DECISIONS LOCKED IN SESSION 1:
  - Calibration grain: (make, model, fuel) — variant rolled up
  - Window: last 180 days based on entry_date
  - Min sample threshold: 3 listings per cell
  - Phase mapping: 3-9 -> phase 2, 10-19 -> phase 3, 20+ -> phase 4
  - Storage: single multiplier per cell (Option B)
  - Bounds: multiplier clamped to [0.50, 1.50]
  - Outlier trim: top 10% + bottom 10% (skipped when n < 5)
  - Price field: COALESCE(negotiated_price_inr, asking_price_inr)
  - Bad formula output: skip listing, count it, continue

USAGE:
  from calibration_engine import run_calibration, run_calibration_for_cell

  # Full sweep — admin "Run Calibration" button calls this:
  result = run_calibration(run_by="admin@autoknowmus.com")
  # -> dict with run_id, cells_updated, cells_skipped_low_sample, etc.

  # Single cell — post-upload re-calibration calls this (Session 2):
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
# SUPABASE CLIENT
# ============================================================

_supabase_client: Optional[Client] = None


def _get_supabase() -> Client:
    """Lazy-init Supabase client. Reads from env vars (same as app.py)."""
    global _supabase_client
    if _supabase_client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
        if not url or not key:
            raise RuntimeError(
                "Supabase credentials missing. Set SUPABASE_URL and "
                "SUPABASE_SERVICE_ROLE_KEY (or SUPABASE_KEY) environment vars."
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

    # Supabase python client paginates at 1000 rows by default; we ask for more
    # explicitly to be safe. If you ever exceed 10k listings in 180 days,
    # we'll need to paginate properly.
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

    "Usable" means:
      - Has a price (negotiated preferred, falls back to asking)
      - compute_base_valuation() returns a positive int for it

    Bad listings are silently skipped — they don't break the cell's calibration.
    """
    listing_prices: List[int] = []   # what the market is asking/got
    formula_prices: List[int] = []   # what our formula would predict
    used_listing_ids: List[str] = [] # for marking used_in_last_calibration

    listings_oldest = None
    listings_newest = None

    for L in listings:
        # 1) Price selection: COALESCE(negotiated, asking)
        price = L.get("negotiated_price_inr") or L.get("asking_price_inr")
        if not price or price <= 0:
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
                condition=L.get("condition"),  # None -> "Good" default in compute_base_valuation
                owner=L.get("owners"),         # None -> "1st Owner" default in compute_base_valuation
            )
        except Exception:
            # Defensive: if formula raises, skip this listing
            continue

        # 3) Skip listings the formula can't price
        if formula_price is None or formula_price <= 0:
            continue

        listing_prices.append(int(price))
        formula_prices.append(int(formula_price))
        used_listing_ids.append(L["id"])

        # Track oldest/newest dates for transparency
        entry_date = L.get("entry_date")
        if entry_date:
            if listings_oldest is None or entry_date < listings_oldest:
                listings_oldest = entry_date
            if listings_newest is None or entry_date > listings_newest:
                listings_newest = entry_date

    n = len(listing_prices)
    if n < MIN_SAMPLES_PER_CELL:
        return None

    # 4) Compute medians
    median_listing = _trimmed_median(listing_prices)
    median_formula = _trimmed_median(formula_prices)

    if not median_formula or median_formula <= 0:
        # Defensive: shouldn't happen because we filter formula_price > 0,
        # but guard against trimming wiping everything out.
        return None

    # 5) Compute multiplier and clamp
    raw_multiplier = median_listing / median_formula
    clamped_multiplier = _clamp_multiplier(raw_multiplier)

    # 6) Phase
    phase = _phase_for_sample_count(n)

    return {
        "make": make,
        "model": model,
        "fuel": fuel,
        "calibration_multiplier": round(clamped_multiplier, 4),
        "sample_count": n,
        "phase": phase,
        "median_listing_price": median_listing,
        "median_formula_price": median_formula,
        "listings_window_days": CALIBRATION_WINDOW_DAYS,
        "listings_oldest_date": listings_oldest,
        "listings_newest_date": listings_newest,
        # Internal — not a column, used to flag rows after upsert
        "_used_listing_ids": used_listing_ids,
        # Internal — diagnostics for the run summary
        "_raw_multiplier": round(raw_multiplier, 4),
        "_was_clamped": (raw_multiplier != clamped_multiplier),
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

    Returns (was_inserted, was_updated):
      - was_inserted = True if this is a brand-new (make, model, fuel) row
      - was_updated  = True if we updated an existing row
    """
    sb = _get_supabase()
    make = cell_result["make"]
    model = cell_result["model"]
    fuel = cell_result["fuel"]

    # Check if row exists
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
    Mark listings that fed into the current calibration. Sets
    used_in_last_calibration=true and last_calibration_run_id=run_id.

    This is "best effort" — if it fails, the calibration is still valid,
    we just lose the audit trail.
    """
    if not listing_ids:
        return
    sb = _get_supabase()
    payload = {
        "used_in_last_calibration": True,
        "last_calibration_run_id": run_id,
    }
    # Supabase REST has a URL length limit; chunk in groups of 200
    CHUNK = 200
    for i in range(0, len(listing_ids), CHUNK):
        batch = listing_ids[i:i + CHUNK]
        sb.table("research_log").update(payload).in_("id", batch).execute()


def _reset_used_flags():
    """
    Clear used_in_last_calibration on ALL Listing Aggregator rows before
    a full sweep. Ensures no stale flags survive from a previous run.

    Only called from run_calibration() (full sweep), NOT from
    run_calibration_for_cell() (single-cell mode).
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
        "cells_evaluated": int,            # how many distinct cells we looked at
        "cells_updated": int,              # existing rows we updated
        "cells_inserted": int,             # new rows we created
        "cells_skipped_low_sample": int,   # cells with < 3 usable listings
        "listings_considered": int,        # total listings pulled from DB
        "listings_used": int,              # listings that contributed to a cell
        "clamped_cells": int,              # cells where multiplier was clamped
        "notes": str,                      # human-readable summary
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
        "notes": "",
    }

    try:
        # 1) Reset stale flags from previous run (full-sweep only)
        _reset_used_flags()

        # 2) Pull all eligible listings
        listings = _fetch_eligible_listings()
        summary["listings_considered"] = len(listings)

        if not listings:
            summary["notes"] = "No eligible listings in window."
            _finish_run(run_id, "completed", summary)
            return {"ok": True, "run_id": run_id, **summary}

        # 3) Group by cell
        groups = _group_listings_by_cell(listings)
        summary["cells_evaluated"] = len(groups)

        # 4) Calibrate each cell
        all_used_ids: List[str] = []
        clamped_examples: List[str] = []  # for notes — which cells got clamped

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

            if cell_result["_was_clamped"]:
                summary["clamped_cells"] += 1
                if len(clamped_examples) < 5:
                    clamped_examples.append(
                        f"{make} {model} ({fuel}): raw={cell_result['_raw_multiplier']:.3f}"
                    )

        # 5) Flag used listings
        _flag_used_listings(all_used_ids, run_id)

        # 6) Build summary notes
        notes_parts = [
            f"Considered {summary['listings_considered']} listings, "
            f"used {summary['listings_used']} across "
            f"{summary['cells_evaluated']} cells.",
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
            pass  # if we can't even log the failure, don't crash the caller
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
    Useful after an upload that adds listings for a specific cell — no need to
    re-sweep the entire DB.

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
        if cell_result["_was_clamped"]:
            summary["clamped_cells"] = 1

        summary["notes"] = (
            f"{make} {model} ({fuel}): "
            f"n={cell_result['sample_count']}, "
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

    Used by car_data.py / app.py at valuation time to look up the multiplier
    and phase. (Wiring this into compute_base_valuation happens in Session 2.)

    Returns dict with all model_calibration columns, or None if not calibrated.
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
    print("[calibration_engine] Running full sweep (CLI test mode)...")
    result = run_calibration(run_by="cli_test", run_type="manual")
    print()
    print("=" * 60)
    print(f"  Run ID:                  {result.get('run_id')}")
    print(f"  Status:                  {'OK' if result.get('ok') else 'FAILED'}")
    print(f"  Listings considered:     {result.get('listings_considered')}")
    print(f"  Listings used:           {result.get('listings_used')}")
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
