"""
cardekho_route.py
-----------------
AutoKnowMus — CarDekho paste-extract Flask routes.

Registers two routes via register_cardekho_routes(app):
  GET  /admin/price-tools/cardekho-paste          — show paste form
  POST /admin/price-tools/cardekho-paste          — process paste, render preview
  POST /admin/price-tools/cardekho-paste/submit   — handle queue / quick-approve

DESIGN PRINCIPLES:
  - All Sheets reads go through car_data.py public API (refresh_prices,
    get_variants, get_variant_base_price). Never call gspread directly.
  - All Sheets writes go through sheets_writer.py. Same discipline.
  - Future Supabase migration touches car_data.py + sheets_writer.py only;
    this route module stays unchanged.

STATE HANDLING:
  Parsed rows are saved to a Supabase 'cardekho_preview_tokens' table.
  The token is a short UUID-based string passed via hidden form field.
  Tokens expire after 30 minutes. The token row stores the JSON-serialized
  preview blob so submit() can rehydrate without re-parsing.

FUZZY MATCHING:
  Conservative 90% Jaccard threshold on lowercased word-token sets.
  Below 90% is tagged 'new variant' for safety.

CACHE STRATEGY (B-Refresh):
  car_data.refresh_prices(force=True) is called at the start of every
  preview render to guarantee fresh comparison against car_prices Sheet.
  After Quick Approve writes, refresh is called again to keep cache fresh.

QUICK APPROVE WIRING (Session 2):
  _quick_approve_to_sheet() now calls sheets_writer.write_price_update_v2()
  which is the actual function in v3.7.0 sheets_writer. It writes:
    col E (ex_showroom_price) -> new_price
    col G (notes)             -> "DD-MMM-YYYY CarDekho ..."
    col I (last_known_price_date) -> "DD-MMM-YYYY"
  Source tag is "CarDekho" so the notes column distinguishes from CarWale.

DEBUG WRAPPERS (Session 2):
  Both _handle_paste_extract and _handle_submit are wrapped with try/except
  that flash exception + traceback to browser. These are TEMPORARY — remove
  in a future cleanup commit once flows are confirmed stable.
"""

import json
import logging
import os
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import flash, redirect, render_template, request, session, url_for

# Imports from our codebase
import car_data
from cardekho_parser import parse_cardekho_paste

# Supabase client — same pattern as calibration_engine.py
from supabase import Client, create_client


logger = logging.getLogger(__name__)


# ============================================================
# CONSTANTS
# ============================================================

PREVIEW_TOKEN_TTL_MINUTES = 30
FUZZY_MATCH_THRESHOLD = 0.90
MAX_PARSED_ROWS_PER_PASTE = 2000
MAX_PASTE_CHARS = 200_000

CARDEKHO_REVIEW_TYPE = "price_update_cardekho"
CARDEKHO_SCRAPER_URL = "cardekho_paste"
# Source tag written to car_prices.notes column on Quick Approve writes.
# Matches the "CarWale" / "Manual" pattern used by sheets_writer v3.7.0.
CARDEKHO_SOURCE_TAG = "CarDekho"

STATUS_MATCH = "match"
STATUS_FUZZY = "fuzzy"
STATUS_NEW = "new"
STATUS_ORPHAN = "orphan"


# ============================================================
# SUPABASE CLIENT — lazy init
# ============================================================

_supabase_client: Optional[Client] = None


def _get_supabase() -> Client:
    """Lazy Supabase client. Same env var pattern as calibration_engine.py."""
    global _supabase_client
    if _supabase_client is None:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SECRET_KEY")
        if not url or not key:
            raise RuntimeError(
                "Supabase credentials missing. Set SUPABASE_URL and "
                "SUPABASE_SECRET_KEY environment vars."
            )
        _supabase_client = create_client(url, key)
    return _supabase_client


# ============================================================
# ADMIN GATE
# ============================================================

_ADMIN_EMAILS_CACHE = None


def _is_admin_email(email: Optional[str]) -> bool:
    """Case-insensitive admin allowlist check. Mirrors app.py's helper."""
    global _ADMIN_EMAILS_CACHE
    if not email:
        return False
    if _ADMIN_EMAILS_CACHE is None:
        try:
            from app import ADMIN_EMAILS
            _ADMIN_EMAILS_CACHE = {e.lower() for e in ADMIN_EMAILS}
        except Exception:
            env_emails = os.environ.get("ADMIN_EMAILS", "")
            _ADMIN_EMAILS_CACHE = {
                e.strip().lower() for e in env_emails.split(",") if e.strip()
            }
    return email.lower() in _ADMIN_EMAILS_CACHE


def _require_admin() -> Optional[Any]:
    """Returns a Flask redirect response if not admin, else None."""
    user = session.get("user") or {}
    user_email = user.get("email")
    if not _is_admin_email(user_email):
        flash("Admin access required.", "error")
        return redirect(url_for("role"))
    return None


# ============================================================
# FUZZY MATCHER
# ============================================================

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokenize(s: str) -> set:
    """Lowercased word-token set for Jaccard similarity."""
    if not s:
        return set()
    return set(_TOKEN_RE.findall(s.lower()))


def _jaccard(a: set, b: set) -> float:
    """Jaccard similarity on token sets. 0.0 to 1.0."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    union = len(a | b)
    return inter / union if union else 0.0


def _fuzzy_match_variant(
    target_variant: str,
    target_fuel: str,
    candidate_variants: List[str],
    candidate_prices: Dict[str, int],
    threshold: float = FUZZY_MATCH_THRESHOLD,
) -> Optional[Tuple[str, float, int]]:
    """Find the best fuzzy match for target_variant. Returns None if below threshold."""
    if not candidate_variants:
        return None

    target_tokens = _tokenize(target_variant)
    if not target_tokens:
        return None

    best_score = 0.0
    best_variant = None
    for v in candidate_variants:
        score = _jaccard(target_tokens, _tokenize(v))
        if score > best_score:
            best_score = score
            best_variant = v

    if best_variant is None or best_score < threshold:
        return None

    current_price = candidate_prices.get(best_variant)
    if current_price is None:
        return None

    return (best_variant, best_score, current_price)


# ============================================================
# CATALOG INDEX
# ============================================================

def _build_catalog_index() -> Dict[Tuple[str, str], Dict]:
    """Build in-memory index of car_prices catalog grouped by (make, model)."""
    index: Dict[Tuple[str, str], Dict] = {}

    makes = car_data.get_makes()
    for make in makes:
        models = car_data.get_models(make)
        for model in models:
            fuels = car_data.get_fuels(make, model)
            variants = car_data.get_variants(make, model)

            variants_by_fuel: Dict[str, List[str]] = {}
            prices_by_fuel: Dict[str, Dict[str, int]] = {}

            for fuel in fuels:
                fuel_variants = []
                fuel_prices = {}
                for v in variants:
                    price = car_data.get_variant_base_price(make, model, v, fuel)
                    if price and price > 0:
                        fuel_variants.append(v)
                        fuel_prices[v] = int(price)
                if fuel_variants:
                    variants_by_fuel[fuel] = fuel_variants
                    prices_by_fuel[fuel] = fuel_prices

            index[(make, model)] = {
                "variants_by_fuel": variants_by_fuel,
                "prices_by_fuel": prices_by_fuel,
            }

    return index


# ============================================================
# CLASSIFY PARSED ROWS
# ============================================================

def _classify_rows(parsed_rows: List[Dict], catalog_index: Dict) -> List[Dict]:
    """Tag each parsed row with match status against the car_prices catalog."""
    pending_index = _load_pending_cardekho_index()
    enriched: List[Dict] = []

    for idx, row in enumerate(parsed_rows):
        make = row["make"]
        model = row["model"]
        variant = row["variant"]
        fuel = row["fuel"]
        proposed_price = row["price_inr"]
        # Alias price_inr -> proposed_price so preview template finds it
        row = {**row, "proposed_price": proposed_price}
        cardekho_full = f"{make} {model} {variant} ({fuel})"

        catalog_entry = catalog_index.get((make, model))
        if not catalog_entry:
            enriched.append({
                **row,
                "idx": idx,
                "status_key": STATUS_ORPHAN,
                "status_label": "Orphan",
                "current_price": None,
                "matched_variant_name": None,
                "match_confidence": 0.0,
                "delta_label": "ORPHAN",
                "delta_class": "delta-new",
                "cardekho_variant_full": cardekho_full,
                "action_description": (
                    f"{make} {model} is not in the car_prices sheet at all. "
                    f"Approving will create a new make+model+variant entry."
                ),
                "previously_pending_at": None,
            })
            continue

        variants_for_fuel = catalog_entry["variants_by_fuel"].get(fuel, [])
        prices_for_fuel = catalog_entry["prices_by_fuel"].get(fuel, {})

        # Case-insensitive exact match
        exact_match_name = None
        for v in variants_for_fuel:
            if v.lower() == variant.lower():
                exact_match_name = v
                break

        if exact_match_name:
            current_price = prices_for_fuel.get(exact_match_name)
            enriched.append({
                **row,
                "idx": idx,
                "status_key": STATUS_MATCH,
                "status_label": "Exact match",
                "current_price": current_price,
                "matched_variant_name": exact_match_name,
                "match_confidence": 1.0,
                "delta_label": _delta_label(current_price, proposed_price),
                "delta_class": _delta_class(current_price, proposed_price),
                "cardekho_variant_full": cardekho_full,
                "action_description": (
                    f"Updates car_prices row for {make} {model} "
                    f"{exact_match_name} ({fuel}) from "
                    f"₹{_inr(current_price)} to ₹{_inr(proposed_price)}."
                ),
                "previously_pending_at": _pending_date_for(
                    pending_index, make, model, exact_match_name, fuel
                ),
            })
            continue

        fuzzy = _fuzzy_match_variant(
            variant, fuel, variants_for_fuel, prices_for_fuel
        )
        if fuzzy:
            fz_variant, fz_score, fz_price = fuzzy
            enriched.append({
                **row,
                "idx": idx,
                "status_key": STATUS_FUZZY,
                "status_label": f"Fuzzy ({int(fz_score * 100)}%)",
                "current_price": fz_price,
                "matched_variant_name": fz_variant,
                "match_confidence": fz_score,
                "delta_label": _delta_label(fz_price, proposed_price),
                "delta_class": _delta_class(fz_price, proposed_price),
                "cardekho_variant_full": cardekho_full,
                "action_description": (
                    f"Fuzzy match — CarDekho variant '{variant}' looks similar "
                    f"to existing '{fz_variant}' ({int(fz_score * 100)}% token "
                    f"overlap). Approval will update '{fz_variant}' (NOT create "
                    f"a new row). Verify before approving."
                ),
                "previously_pending_at": _pending_date_for(
                    pending_index, make, model, fz_variant, fuel
                ),
            })
            continue

        enriched.append({
            **row,
            "idx": idx,
            "status_key": STATUS_NEW,
            "status_label": "New variant",
            "current_price": None,
            "matched_variant_name": None,
            "match_confidence": 0.0,
            "delta_label": "NEW",
            "delta_class": "delta-new",
            "cardekho_variant_full": cardekho_full,
            "action_description": (
                f"Inserts a new row in car_prices: {make} {model} "
                f"{variant} ({fuel}) · ₹{_inr(proposed_price)}. "
                f"Variant name will be stored as '{variant}' (CarDekho's name)."
            ),
            "previously_pending_at": _pending_date_for(
                pending_index, make, model, variant, fuel
            ),
        })

    return enriched


# ============================================================
# DELTA HELPERS
# ============================================================

def _delta_label(current: Optional[int], proposed: int) -> str:
    if current is None or current <= 0:
        return "NEW"
    diff = proposed - current
    if abs(diff) < 1:
        return "0.0%"
    pct = (diff / current) * 100
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


def _delta_class(current: Optional[int], proposed: int) -> str:
    if current is None or current <= 0:
        return "delta-new"
    diff = proposed - current
    if abs(diff) < 1:
        return "delta-flat"
    return "delta-up" if diff > 0 else "delta-down"


def _inr(n: Optional[int]) -> str:
    """Indian-style comma formatter: 825000 -> '8,25,000'. None -> '—'."""
    if n is None:
        return "—"
    s = str(abs(int(n)))
    if len(s) <= 3:
        return s
    last3 = s[-3:]
    rest = s[:-3]
    groups = []
    while len(rest) > 2:
        groups.insert(0, rest[-2:])
        rest = rest[:-2]
    if rest:
        groups.insert(0, rest)
    return ",".join(groups) + "," + last3


# ============================================================
# PENDING-REVIEWS INDEX
# ============================================================

def _load_pending_cardekho_index() -> Dict[Tuple[str, str, str, str], Dict]:
    """Load pending CarDekho rows for 'previously pending' display."""
    sb = _get_supabase()
    try:
        result = (
            sb.table("pending_reviews")
            .select("id, make, model, variant, fuel, proposed_price, scraped_at")
            .eq("review_type", CARDEKHO_REVIEW_TYPE)
            .eq("status", "pending")
            .limit(5000)
            .execute()
        )
    except Exception as e:
        logger.warning("[cardekho] Could not load pending index: %s", e)
        return {}

    idx: Dict[Tuple[str, str, str, str], Dict] = {}
    for r in result.data or []:
        key = (r["make"], r["model"], r["variant"], r["fuel"])
        idx[key] = {
            "id": r["id"],
            "scraped_at": r.get("scraped_at"),
            "proposed_price": r.get("proposed_price"),
        }
    return idx


def _pending_date_for(idx, make, model, variant, fuel) -> Optional[str]:
    """Return DD-MMM-YYYY of previous pending paste, or None."""
    rec = idx.get((make, model, variant, fuel))
    if not rec or not rec.get("scraped_at"):
        return None
    try:
        dt = datetime.fromisoformat(rec["scraped_at"].replace("Z", "+00:00"))
        return dt.strftime("%d-%b-%Y")
    except Exception:
        return None


# ============================================================
# PREVIEW TOKEN STORAGE (Supabase)
# ============================================================

PREVIEW_TOKEN_TABLE = "cardekho_preview_tokens"


def _save_preview_token(preview_blob: Dict, user_email: str) -> str:
    """Stash the preview blob in Supabase. Returns the token."""
    sb = _get_supabase()
    token = secrets.token_urlsafe(16)
    expires_at = (
        datetime.now(timezone.utc)
        + timedelta(minutes=PREVIEW_TOKEN_TTL_MINUTES)
    ).isoformat()

    try:
        sb.table(PREVIEW_TOKEN_TABLE).insert({
            "token": token,
            "expires_at": expires_at,
            "created_by": user_email or "unknown",
            "blob": preview_blob,
            "consumed": False,
        }).execute()
    except Exception as e:
        logger.error("[cardekho] Failed to save preview token: %s", e)
        raise

    return token


def _load_preview_token(token: str) -> Optional[Dict]:
    """Retrieve a non-expired, non-consumed preview blob."""
    sb = _get_supabase()
    try:
        result = (
            sb.table(PREVIEW_TOKEN_TABLE)
            .select("blob, expires_at, consumed")
            .eq("token", token)
            .limit(1)
            .execute()
        )
    except Exception as e:
        logger.error("[cardekho] Token lookup failed: %s", e)
        return None

    if not result.data:
        return None
    row = result.data[0]
    if row.get("consumed"):
        return None
    try:
        expires_at = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) > expires_at:
            return None
    except Exception:
        return None
    return row.get("blob")


def _consume_preview_token(token: str) -> None:
    """Mark a token as consumed."""
    sb = _get_supabase()
    try:
        sb.table(PREVIEW_TOKEN_TABLE).update({"consumed": True}).eq(
            "token", token
        ).execute()
    except Exception as e:
        logger.warning("[cardekho] Token consume failed: %s", e)


# ============================================================
# PENDING-REVIEWS WRITER (Option B — update existing, else insert)
# ============================================================

def _upsert_cardekho_pending_review(
    make: str,
    model: str,
    variant: str,
    fuel: str,
    proposed_price: int,
    matched_variant_name: Optional[str],
    scraper_status: str,
    current_price: Optional[int],
    final_status: str = "pending",
    reviewed_by: Optional[str] = None,
) -> Tuple[int, str]:
    """Insert or update a pending_reviews row for the CarDekho tool."""
    sb = _get_supabase()
    now_iso = datetime.now(timezone.utc).isoformat()

    if final_status == "pending":
        existing = (
            sb.table("pending_reviews")
            .select("id")
            .eq("review_type", CARDEKHO_REVIEW_TYPE)
            .eq("make", make)
            .eq("model", model)
            .eq("variant", variant)
            .eq("fuel", fuel)
            .eq("status", "pending")
            .limit(1)
            .execute()
        )
        if existing.data:
            existing_id = existing.data[0]["id"]
            sb.table("pending_reviews").update({
                "proposed_price": proposed_price,
                "current_price": current_price,
                "matched_variant_name": matched_variant_name,
                "scraper_status": scraper_status,
                "scraped_at": now_iso,
            }).eq("id", existing_id).execute()
            return (existing_id, "updated")

    insert_row = {
        "review_type": CARDEKHO_REVIEW_TYPE,
        "make": make,
        "model": model,
        "variant": variant,
        "fuel": fuel,
        "current_price": current_price,
        "proposed_price": proposed_price,
        "scraped_at": now_iso,
        "matched_variant_name": matched_variant_name,
        "scraper_status": scraper_status,
        "scraper_url": CARDEKHO_SCRAPER_URL,
        "status": final_status,
    }
    if final_status == "approved":
        insert_row["reviewed_at"] = now_iso
        insert_row["reviewed_by"] = reviewed_by or "system"
        insert_row["review_notes"] = "Quick-approved from CarDekho paste preview"

    result = sb.table("pending_reviews").insert(insert_row).execute()
    new_id = result.data[0]["id"] if result.data else 0
    return (new_id, "inserted")


# ============================================================
# QUICK-APPROVE WRITES TO car_prices SHEET
# ============================================================
#
# v2: Wired to sheets_writer.write_price_update_v2() which is the actual
# function exposed by sheets_writer.py v3.7.0. It:
#   - Updates col E (ex_showroom_price) to new_price
#   - Updates col G (notes) to "DD-MMM-YYYY CarDekho ..."
#   - Updates col I (last_known_price_date) to today's date
#
# We pass source="CarDekho" so notes column distinguishes CarDekho-sourced
# prices from CarWale scraper / manual entries.
#
# Quick Approve only ever processes status_key='match' rows, which means
# the (make, model, variant, fuel) tuple exists in car_prices. So we use
# write_price_update_v2, never write_new_variant.
# ============================================================

def _quick_approve_to_sheet(row: Dict, admin_email: str) -> Tuple[bool, str]:
    """
    Write a single approved row to car_prices Google Sheet via sheets_writer.
    Returns (ok, message).
    """
    try:
        import sheets_writer
    except Exception as e:
        return (False, f"sheets_writer import failed: {e}")

    make = row["make"]
    model = row["model"]
    fuel = row["fuel"]
    proposed_price = row["proposed_price"]
    # For exact-match rows, matched_variant_name is the variant's name as it
    # exists in car_prices (case-corrected). We use that to ensure write hits
    # the exact existing row rather than creating a duplicate due to case mismatch.
    target_variant = row.get("matched_variant_name") or row["variant"]

    # Build extra note: include CarDekho's verbose variant name if it differs
    # from our sheet's variant name, so audit trail is preserved.
    extra_note_parts = []
    cardekho_variant = row.get("variant", "")
    if cardekho_variant and cardekho_variant.lower() != target_variant.lower():
        extra_note_parts.append(f"CD:{cardekho_variant}")
    extra_note_parts.append(f"by:{admin_email}")
    extra_note = " ".join(extra_note_parts)

    try:
        # Coerce price to int — sheets_writer enforces int type strictly
        price_int = int(proposed_price)
    except (TypeError, ValueError):
        return (False, f"proposed_price not coercible to int: {proposed_price!r}")

    try:
        result = sheets_writer.write_price_update_v2(
            make=make,
            model=model,
            variant=target_variant,
            fuel=fuel,
            new_price=price_int,
            source=CARDEKHO_SOURCE_TAG,
            extra_note=extra_note,
        )
    except RuntimeError as e:
        # sheets_writer raises RuntimeError on all its failure modes:
        # row-not-found, API errors, 403, 404, 429, 500, etc.
        return (False, f"sheets_writer.write_price_update_v2: {e}")
    except Exception as e:
        return (False, f"sheets_writer.write_price_update_v2 unexpected error: {type(e).__name__}: {e}")

    if not (isinstance(result, dict) and result.get("ok")):
        return (False, f"sheets_writer returned non-ok result: {result!r}")

    return (True, "ok")


# ============================================================
# ROUTE HANDLERS
# ============================================================

def _render_paste_form():
    """GET handler — show empty paste form."""
    now_display = datetime.now(timezone.utc).strftime("%d-%b-%Y %H:%M UTC")
    return render_template(
        "admin_cardekho_paste.html",
        preview=None,
        preview_token=None,
        now_display=now_display,
    )


def _handle_paste_extract():
    """POST handler — wrapped with debug error catcher (temporary)."""
    try:
        return _handle_paste_extract_inner()
    except Exception as e:
        import traceback as _tb
        tb_str = _tb.format_exc()
        logger.error("[cardekho] Extract failed: %s", tb_str)
        flash(
            "DEBUG ERROR: " + type(e).__name__ + ": " + str(e)[:200],
            "error",
        )
        tb_lines = tb_str.split("\n")
        relevant_tb = "\n".join(tb_lines[-12:])
        flash("Traceback (last 12 lines): " + relevant_tb, "error")
        return redirect(url_for("admin_cardekho_paste"))


def _handle_paste_extract_inner():
    """Inner handler with original logic."""
    raw_text = (request.form.get("raw_text") or "").strip()
    if not raw_text:
        flash("Paste is empty.", "error")
        return redirect(url_for("admin_cardekho_paste"))

    if len(raw_text) > MAX_PASTE_CHARS:
        flash(
            f"Paste too large ({len(raw_text):,} chars). "
            f"Max is {MAX_PASTE_CHARS:,}.",
            "error",
        )
        return redirect(url_for("admin_cardekho_paste"))

    parsed = parse_cardekho_paste(raw_text)
    if not parsed.get("ok"):
        flash(f"Parser error: {parsed.get('summary', 'unknown')}", "error")
        return redirect(url_for("admin_cardekho_paste"))

    if len(parsed["rows"]) > MAX_PARSED_ROWS_PER_PASTE:
        flash(
            f"Too many rows parsed ({len(parsed['rows']):,}). "
            f"Max per paste is {MAX_PARSED_ROWS_PER_PASTE:,}. Try fewer models.",
            "error",
        )
        return redirect(url_for("admin_cardekho_paste"))

    # B-Refresh: force-reload car_data cache
    try:
        car_data.refresh_prices(force=True)
    except Exception as e:
        logger.warning("[cardekho] car_data refresh failed: %s", e)
        flash(
            "Warning: car_prices cache refresh failed; preview may show stale "
            "current prices. Continuing anyway.",
            "error",
        )

    catalog_index = _build_catalog_index()
    enriched_rows = _classify_rows(parsed["rows"], catalog_index)
    rows_by_model = _group_rows_by_model(enriched_rows)

    count_match = sum(1 for r in enriched_rows if r["status_key"] == STATUS_MATCH)
    count_fuzzy = sum(1 for r in enriched_rows if r["status_key"] == STATUS_FUZZY)
    count_new = sum(1 for r in enriched_rows if r["status_key"] == STATUS_NEW)
    count_orphan = sum(1 for r in enriched_rows if r["status_key"] == STATUS_ORPHAN)
    collapsed_models = sum(
        1 for w in parsed["warnings"] if w.get("type") == "collapsed_section"
    )

    preview_blob = {
        "models_found": parsed["models_found"],
        "variants_found": parsed["variants_found"],
        "count_match": count_match,
        "count_fuzzy": count_fuzzy,
        "count_new": count_new,
        "count_orphan": count_orphan,
        "collapsed_models": collapsed_models,
        "warnings": parsed["warnings"],
        "rows": enriched_rows,
        "rows_by_model": rows_by_model,
    }

    user = session.get("user") or {}
    user_email = user.get("email") or "unknown"
    try:
        token = _save_preview_token(preview_blob, user_email)
    except Exception as e:
        logger.error("[cardekho] preview token save failed: %s", e)
        flash(f"Could not save preview: {e}", "error")
        return redirect(url_for("admin_cardekho_paste"))

    now_display = datetime.now(timezone.utc).strftime("%d-%b-%Y %H:%M UTC")
    return render_template(
        "admin_cardekho_paste.html",
        preview=preview_blob,
        preview_token=token,
        now_display=now_display,
    )


def _group_rows_by_model(enriched_rows: List[Dict]) -> List[Dict]:
    """Group enriched rows by (make, model) for collapsible model-section UI."""
    groups: Dict[Tuple[str, str], Dict] = {}
    for row in enriched_rows:
        key = (row["make"], row["model"])
        if key not in groups:
            groups[key] = {
                "key": f"{row['make']}_{row['model']}".lower().replace(" ", "_"),
                "make": row["make"],
                "model": row["model"],
                "rows": [],
                "counts": {"match": 0, "fuzzy": 0, "new": 0, "orphan": 0},
            }
        groups[key]["rows"].append(row)
        groups[key]["counts"][row["status_key"]] = (
            groups[key]["counts"].get(row["status_key"], 0) + 1
        )

    return list(groups.values())


def _handle_submit():
    """POST handler — wrapped with debug error catcher (temporary)."""
    try:
        return _handle_submit_inner()
    except Exception as e:
        import traceback as _tb
        tb_str = _tb.format_exc()
        logger.error("[cardekho] Submit failed: %s", tb_str)
        flash(
            "DEBUG SUBMIT ERROR: " + type(e).__name__ + ": " + str(e)[:200],
            "error",
        )
        tb_lines = tb_str.split("\n")
        relevant_tb = "\n".join(tb_lines[-12:])
        flash("Traceback (last 12 lines): " + relevant_tb, "error")
        return redirect(url_for("admin_cardekho_paste"))


def _handle_submit_inner():
    """POST handler for /admin/price-tools/cardekho-paste/submit."""
    token = request.form.get("preview_token", "").strip()
    action_mode = request.form.get("action_mode", "queue").strip()
    keep_idx_raw = request.form.getlist("keep_idx")

    if not token:
        flash("Missing preview token. Please re-paste.", "error")
        return redirect(url_for("admin_cardekho_paste"))

    blob = _load_preview_token(token)
    if not blob:
        flash(
            "Preview expired or already consumed (preview tokens last 30 minutes). "
            "Please re-paste.",
            "error",
        )
        return redirect(url_for("admin_cardekho_paste"))

    keep_idx: set = set()
    for s in keep_idx_raw:
        try:
            keep_idx.add(int(s))
        except ValueError:
            continue

    selected_rows = [r for r in blob["rows"] if r["idx"] in keep_idx]
    if not selected_rows:
        flash("No rows were selected. Nothing to do.", "error")
        return redirect(url_for("admin_cardekho_paste"))

    user = session.get("user") or {}
    admin_email = user.get("email") or "admin"

    if action_mode == "quick_approve_matches":
        return _do_quick_approve(selected_rows, token, admin_email)
    else:
        return _do_queue_pending(selected_rows, token, admin_email)


def _do_queue_pending(
    selected_rows: List[Dict], token: str, admin_email: str
) -> Any:
    """Send selected rows to pending_reviews with status='pending'."""
    inserted = 0
    updated = 0
    errors: List[str] = []

    for row in selected_rows:
        scraper_status = _map_status_to_scraper(row["status_key"])
        try:
            _, action = _upsert_cardekho_pending_review(
                make=row["make"],
                model=row["model"],
                variant=row["variant"],
                fuel=row["fuel"],
                proposed_price=row["proposed_price"],
                matched_variant_name=row.get("matched_variant_name"),
                scraper_status=scraper_status,
                current_price=row.get("current_price"),
                final_status="pending",
                reviewed_by=admin_email,
            )
            if action == "inserted":
                inserted += 1
            elif action == "updated":
                updated += 1
        except Exception as e:
            errors.append(f"{row['make']} {row['model']} {row['variant']}: {e}")

    _consume_preview_token(token)

    parts = []
    if inserted:
        parts.append(f"{inserted} new pending review{'s' if inserted != 1 else ''}")
    if updated:
        parts.append(f"{updated} existing pending row{'s' if updated != 1 else ''} updated with latest prices")
    if errors:
        parts.append(f"{len(errors)} error{'s' if len(errors) != 1 else ''}")

    flash(
        f"Queued to Review: {', '.join(parts) or 'nothing'}.",
        "success" if not errors else "error",
    )
    if errors:
        for e in errors[:5]:
            flash(f"Error: {e}", "error")

    return redirect(url_for("admin_cardekho_paste"))


def _do_quick_approve(
    selected_rows: List[Dict], token: str, admin_email: str
) -> Any:
    """
    Quick-approve: write 🟢 match rows directly to car_prices sheet AND record
    pending_reviews row with status='approved'.

    Only rows with status_key='match' are processed. Others are skipped.
    Wired to sheets_writer.write_price_update_v2() via _quick_approve_to_sheet.
    """
    approved_count = 0
    skipped_non_match = 0
    sheet_errors: List[str] = []
    pending_errors: List[str] = []

    for row in selected_rows:
        if row["status_key"] != STATUS_MATCH:
            skipped_non_match += 1
            continue

        # 1) Write to sheet first
        ok, msg = _quick_approve_to_sheet(row, admin_email)
        if not ok:
            sheet_errors.append(
                f"{row['make']} {row['model']} {row['variant']}: {msg}"
            )
            continue

        # 2) Record in pending_reviews as approved
        try:
            _upsert_cardekho_pending_review(
                make=row["make"],
                model=row["model"],
                variant=row["variant"],
                fuel=row["fuel"],
                proposed_price=row["proposed_price"],
                matched_variant_name=row.get("matched_variant_name"),
                scraper_status="parsed_match",
                current_price=row.get("current_price"),
                final_status="approved",
                reviewed_by=admin_email,
            )
        except Exception as e:
            pending_errors.append(
                f"{row['make']} {row['model']} {row['variant']}: {e}"
            )

        approved_count += 1

    # 3) Refresh car_data cache so subsequent reads see new prices
    try:
        car_data.refresh_prices(force=True)
    except Exception as e:
        logger.warning("[cardekho] post-approve refresh failed: %s", e)

    _consume_preview_token(token)

    parts = [f"{approved_count} price{'s' if approved_count != 1 else ''} approved to car_prices sheet"]
    if skipped_non_match:
        parts.append(f"{skipped_non_match} non-match row{'s' if skipped_non_match != 1 else ''} skipped")
    if sheet_errors:
        parts.append(f"{len(sheet_errors)} sheet write error{'s' if len(sheet_errors) != 1 else ''}")
    if pending_errors:
        parts.append(f"{len(pending_errors)} audit log error{'s' if len(pending_errors) != 1 else ''} (sheet still written)")

    category = "success" if (not sheet_errors and not pending_errors) else "error"
    flash(f"Quick Approve: {', '.join(parts)}.", category)

    for e in (sheet_errors + pending_errors)[:5]:
        flash(f"Error: {e}", "error")

    return redirect(url_for("admin_cardekho_paste"))


def _map_status_to_scraper(status_key: str) -> str:
    """Map our status_key to the scraper_status enum value stored in DB."""
    return {
        STATUS_MATCH: "parsed_match",
        STATUS_FUZZY: "parsed_fuzzy_match",
        STATUS_NEW: "parsed_new_variant",
        STATUS_ORPHAN: "parsed_orphan",
    }.get(status_key, "parsed_match")


# ============================================================
# ROUTE REGISTRATION
# ============================================================

def register_cardekho_routes(app):
    """Register CarDekho paste-extract routes on the given Flask app."""

    @app.route("/admin/price-tools/cardekho-paste", methods=["GET", "POST"], endpoint="admin_cardekho_paste")
    def admin_cardekho_paste():
        gate = _require_admin()
        if gate:
            return gate

        if request.method == "POST":
            return _handle_paste_extract()
        return _render_paste_form()

    @app.route("/admin/price-tools/cardekho-paste/submit", methods=["POST"], endpoint="admin_cardekho_paste_submit")
    def admin_cardekho_paste_submit():
        gate = _require_admin()
        if gate:
            return gate
        return _handle_submit()

    logger.info("[cardekho_route] Routes registered: /admin/price-tools/cardekho-paste")
