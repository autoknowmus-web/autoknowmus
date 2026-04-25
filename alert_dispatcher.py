"""
alert_dispatcher.py
-------------------
AutoKnowMus Email Alerts v1 — Dispatcher

Triggers:
  1. Buyer match — on /submit-deal, matching buyer subs (make+model+variant) get email
  2. Seller match — on /submit-deal, matching seller subs get email (6h cooldown per sub)
  3. Weekly digest — Monday 9am IST cron, summary for every active sub

Design contract:
  - Fail-silent: never block or error user-facing /submit-deal
  - Dedup: check sent_alerts before sending same (subscription_id, trigger_ref, trigger_type)
  - Full-pipeline verdict: uses the SAME phase/blend/range logic as dashboard
  - Unverified-deal hybrid: sends for all deals, copy flags unverified status

Env vars required (set in Render dashboard):
  RESEND_API_KEY
  ALERT_FROM_EMAIL     (default: alerts@autoknowmus.com)
  ALERT_FROM_NAME      (default: AutoKnowMus Alerts)
  APP_BASE_URL         (default: https://autoknowmus.com)
  ALERT_DISPATCH_TOKEN (secures /internal/send-weekly-digest)
"""

import os
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import resend
from flask import render_template
from supabase import Client

# Import the SAME pricing pipeline the dashboard uses, so email verdicts match.
from car_data import (
    compute_base_valuation,
    compute_price_range,
    adjust_with_deals,
)

log = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────

RESEND_API_KEY   = os.environ.get("RESEND_API_KEY", "")
ALERT_FROM_EMAIL = os.environ.get("ALERT_FROM_EMAIL", "alerts@autoknowmus.com")
ALERT_FROM_NAME  = os.environ.get("ALERT_FROM_NAME", "AutoKnowMus Alerts")
APP_BASE_URL     = os.environ.get("APP_BASE_URL", "https://autoknowmus.com").rstrip("/")
REPLY_TO_EMAIL   = "autoknowmus@gmail.com"

SELLER_COOLDOWN_HOURS = 6
PHASE_LOOKBACK_DAYS   = 180
DEAL_YEAR_WINDOW      = 2  # ±2 years for similar-deal matching

# Configure Resend only if API key is present; allows local dev without it.
if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY
else:
    log.warning("RESEND_API_KEY not set — email dispatch will be disabled")


# ──────────────────────────────────────────────────────────────
# Formatting helpers (mirror app.py's Jinja filters)
# ──────────────────────────────────────────────────────────────

def format_inr(value) -> str:
    """680000 -> '₹6,80,000' (Indian numbering)."""
    if value is None:
        return "—"
    try:
        n = int(value)
    except (ValueError, TypeError):
        return str(value)
    s = str(abs(n))
    if len(s) <= 3:
        body = s
    else:
        last3 = s[-3:]
        rest = s[:-3]
        groups = []
        while len(rest) > 2:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.insert(0, rest)
        body = ",".join(groups) + "," + last3
    return ("-" if n < 0 else "") + "₹" + body


def format_date_ddmmmyyyy(value) -> str:
    """18-Apr-2026 format. Accepts datetime, date, or ISO string."""
    if not value:
        return "—"
    if isinstance(value, str):
        try:
            clean = value.replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean)
        except (ValueError, TypeError):
            return value
    elif hasattr(value, "strftime"):
        dt = value
    else:
        return str(value)
    return dt.strftime("%d-%b-%Y")


# ──────────────────────────────────────────────────────────────
# Full valuation pipeline — mirrors app.py's seller/buyer flow
# ──────────────────────────────────────────────────────────────

def _fetch_similar_deals(sb: Client, make, model, variant, fuel, year) -> list:
    """Mirror of app.py fetch_similar_deals — variant first, model fallback if <3."""
    try:
        year_low = int(year) - DEAL_YEAR_WINDOW
        year_high = int(year) + DEAL_YEAR_WINDOW
        cutoff = (datetime.utcnow() - timedelta(days=PHASE_LOOKBACK_DAYS)).isoformat()

        r = (sb.table("deals")
             .select("sale_price")
             .eq("make", make)
             .eq("model", model)
             .eq("variant", variant)
             .eq("fuel", fuel)
             .eq("verified", True)
             .gte("year", year_low)
             .lte("year", year_high)
             .gte("created_at", cutoff)
             .execute())
        variant_deals = [row["sale_price"] for row in (r.data or []) if row.get("sale_price")]

        if len(variant_deals) >= 3:
            return variant_deals

        r = (sb.table("deals")
             .select("sale_price")
             .eq("make", make)
             .eq("model", model)
             .eq("fuel", fuel)
             .eq("verified", True)
             .gte("year", year_low)
             .lte("year", year_high)
             .gte("created_at", cutoff)
             .execute())
        return [row["sale_price"] for row in (r.data or []) if row.get("sale_price")]
    except Exception as e:
        log.warning("_fetch_similar_deals failed: %s", e)
        return []


def _compute_phase(sb: Client, make, model) -> int:
    """Mirror of app.py compute_model_phase_data — returns phase int 1–4."""
    try:
        from car_data import determine_phase
        cutoff = (datetime.utcnow() - timedelta(days=PHASE_LOOKBACK_DAYS)).isoformat()
        r = (sb.table("deals")
             .select("user_id")
             .eq("make", make)
             .eq("model", model)
             .eq("verified", True)
             .gte("created_at", cutoff)
             .execute())
        rows = r.data or []
        deal_count = len(rows)
        distinct_users = len({row["user_id"] for row in rows if row.get("user_id")})
        return determine_phase(deal_count, distinct_users, previous_phase=1)
    except Exception as e:
        log.warning("_compute_phase failed: %s", e)
        return 1


def compute_verdict_for_deal(sb: Client, deal: dict) -> dict:
    """
    Run the FULL AutoKnowMus pipeline for a deal's car specs and classify
    the deal's sale_price against the computed fair range.

    Returns:
      {
        "estimated_price": int,
        "price_low": int,
        "price_high": int,
        "phase": int,
        "confidence": int,
        "verdict_code": "below" | "fair" | "above",
        "verdict_label": str,
        "verdict_explanation": str,
      }
    """
    make    = deal["make"]
    model   = deal["model"]
    variant = deal.get("variant")
    fuel    = deal.get("fuel") or "Petrol"
    year    = deal.get("year")
    mileage = deal.get("mileage") or 0
    condition = deal.get("condition") or "Good"
    owner     = deal.get("owner") or "1st Owner"

    # Step 1: formula price
    formula_price = compute_base_valuation(
        make=make, model=model, variant=variant, fuel=fuel,
        year=year, mileage=mileage, condition=condition, owner=owner,
    )
    if formula_price is None:
        # Can't compute a range — return a minimal verdict that degrades gracefully
        return {
            "estimated_price": None,
            "price_low": None,
            "price_high": None,
            "phase": 1,
            "confidence": 0,
            "verdict_code": "unknown",
            "verdict_label": "Range unavailable",
            "verdict_explanation": "Fair price range could not be computed for this configuration.",
        }

    # Step 2: phase based on last 180 days of verified deals
    phase = _compute_phase(sb, make, model)

    # Step 3: fetch similar deals + blend
    similar = _fetch_similar_deals(sb, make, model, variant, fuel, year)
    blended, confidence = adjust_with_deals(formula_price, similar, phase=phase)

    # Step 4: compute range from blended price
    price_low, price_high = compute_price_range(blended, phase=phase)

    # Step 5: classify sale_price vs range
    sale_price = deal.get("sale_price") or 0
    if price_low is None or price_high is None:
        code = "unknown"
        label = "Range unavailable"
        explanation = "Fair price range could not be computed."
    elif sale_price < price_low:
        code = "below"
        label = "Below fair market"
        explanation = "This price sits below our estimated fair range — a strong deal for the buyer."
    elif sale_price > price_high:
        code = "above"
        label = "Above market"
        explanation = "This price exceeds our estimated fair range — likely a premium for low mileage or special condition."
    else:
        code = "fair"
        label = "Fair price"
        explanation = "This price sits within our estimated fair market range."

    return {
        "estimated_price": blended,
        "price_low": price_low,
        "price_high": price_high,
        "phase": phase,
        "confidence": confidence,
        "verdict_code": code,
        "verdict_label": label,
        "verdict_explanation": explanation,
    }


# ──────────────────────────────────────────────────────────────
# Subscription matching
# ──────────────────────────────────────────────────────────────

def _find_matching_subscriptions(sb: Client, deal: dict, role: str) -> list:
    """
    Active, email-enabled, non-expired subscriptions matching deal on
    (make, model, variant) for the given role.
    Deals with NULL variant are skipped (can't match exactly).
    """
    if not deal.get("variant"):
        return []
    now_iso = datetime.utcnow().isoformat()
    try:
        r = (sb.table("alert_subscriptions")
             .select("*")
             .eq("role", role)
             .eq("active", True)
             .eq("email_enabled", True)
             .eq("make", deal["make"])
             .eq("model", deal["model"])
             .eq("variant", deal["variant"])
             .gt("expires_at", now_iso)
             .execute())
        return r.data or []
    except Exception as e:
        log.warning("_find_matching_subscriptions (%s) failed: %s", role, e)
        return []


# ──────────────────────────────────────────────────────────────
# Dedup + audit
# ──────────────────────────────────────────────────────────────

def _already_sent(sb: Client, subscription_id, trigger_ref, trigger_type) -> bool:
    try:
        r = (sb.table("sent_alerts")
             .select("id")
             .eq("subscription_id", subscription_id)
             .eq("trigger_ref", trigger_ref)
             .eq("trigger_type", trigger_type)
             .in_("status", ["sent", "queued"])
             .limit(1)
             .execute())
        return bool(r.data)
    except Exception as e:
        log.warning("_already_sent check failed: %s", e)
        return False  # On error, allow send — better than silent drop


def _log_sent_alert(
    sb: Client,
    subscription_id: Optional[int],
    user_id: int,
    trigger_type: str,
    trigger_ref: Optional[str],
    recipient: str,
    subject: Optional[str],
    status: str,
    provider_id: Optional[str] = None,
    error_message: Optional[str] = None,
) -> None:
    try:
        sb.table("sent_alerts").insert({
            "subscription_id": subscription_id,
            "user_id": user_id,
            "trigger_type": trigger_type,
            "channel": "email",
            "trigger_ref": trigger_ref,
            "recipient": recipient,
            "subject": subject,
            "status": status,
            "provider_id": provider_id,
            "error_message": error_message,
        }).execute()
    except Exception as e:
        log.exception("Failed to write sent_alerts audit row: %s", e)


# ──────────────────────────────────────────────────────────────
# Seller cooldown
# ──────────────────────────────────────────────────────────────

def _seller_cooldown_active(sub: dict) -> bool:
    """True if this sub's last_alerted_at is within the 6h cooldown window."""
    last = sub.get("last_alerted_at")
    if not last:
        return False
    try:
        if isinstance(last, str):
            clean = last.replace("Z", "+00:00")
            last_dt = datetime.fromisoformat(clean)
        else:
            last_dt = last
        # Normalize to naive UTC for comparison
        if last_dt.tzinfo is not None:
            last_dt = last_dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return False
    cutoff = datetime.utcnow() - timedelta(hours=SELLER_COOLDOWN_HOURS)
    return last_dt > cutoff


def _bump_sub_counters(sb: Client, subscription_id: int) -> None:
    """Update last_alerted_at + alert_count after a successful send."""
    try:
        cur = (sb.table("alert_subscriptions")
               .select("alert_count")
               .eq("id", subscription_id)
               .single()
               .execute())
        current_count = (cur.data or {}).get("alert_count", 0) or 0
        sb.table("alert_subscriptions").update({
            "last_alerted_at": datetime.utcnow().isoformat(),
            "alert_count": current_count + 1,
        }).eq("id", subscription_id).execute()
    except Exception:
        log.exception("Failed to bump subscription counters for id=%s", subscription_id)


# ──────────────────────────────────────────────────────────────
# Resend send
# ──────────────────────────────────────────────────────────────

def _send_email(to: str, subject: str, html: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """Returns (ok, provider_id, error_message). Never raises."""
    if not RESEND_API_KEY:
        return False, None, "RESEND_API_KEY not configured"
    try:
        params = {
            "from": f"{ALERT_FROM_NAME} <{ALERT_FROM_EMAIL}>",
            "to": [to],
            "reply_to": [REPLY_TO_EMAIL],
            "subject": subject,
            "html": html,
        }
        result = resend.Emails.send(params)
        provider_id = result.get("id") if isinstance(result, dict) else None
        return True, provider_id, None
    except Exception as e:
        log.exception("Resend send failed")
        return False, None, str(e)


# ──────────────────────────────────────────────────────────────
# Email body rendering
# ──────────────────────────────────────────────────────────────

def _render_buyer_email(sub: dict, deal: dict, verdict: dict) -> Tuple[str, str]:
    car_label = f"{sub['make']} {sub['model']} {sub['variant']}"
    subject = f"New deal alert: {car_label} sold at {format_inr(deal['sale_price'])}"
    html = render_template(
        "email/buyer_alert.html",
        car_label=car_label,
        sub=sub,
        deal=deal,
        verdict=verdict,
        sale_price_fmt=format_inr(deal["sale_price"]),
        estimated_fmt=format_inr(verdict.get("estimated_price")),
        price_low_fmt=format_inr(verdict.get("price_low")),
        price_high_fmt=format_inr(verdict.get("price_high")),
        deal_date_fmt=format_date_ddmmmyyyy(deal.get("transaction_date") or deal.get("created_at")),
        is_verified=bool(deal.get("verified")),
        app_url=APP_BASE_URL,
    )
    return subject, html


def _render_seller_email(sub: dict, deal: dict, verdict: dict) -> Tuple[str, str]:
    car_label = f"{sub['make']} {sub['model']} {sub['variant']}"
    subject = f"Market signal: {car_label} just sold for {format_inr(deal['sale_price'])}"
    html = render_template(
        "email/seller_alert.html",
        car_label=car_label,
        sub=sub,
        deal=deal,
        verdict=verdict,
        sale_price_fmt=format_inr(deal["sale_price"]),
        estimated_fmt=format_inr(verdict.get("estimated_price")),
        price_low_fmt=format_inr(verdict.get("price_low")),
        price_high_fmt=format_inr(verdict.get("price_high")),
        deal_date_fmt=format_date_ddmmmyyyy(deal.get("transaction_date") or deal.get("created_at")),
        reference_price_fmt=format_inr(sub.get("reference_asking_price")),
        is_verified=bool(deal.get("verified")),
        app_url=APP_BASE_URL,
    )
    return subject, html


# ──────────────────────────────────────────────────────────────
# Per-role dispatch
# ──────────────────────────────────────────────────────────────

def _dispatch_one(
    sb: Client,
    deal: dict,
    verdict: dict,
    sub: dict,
    trigger_type: str,
    render_fn,
) -> str:
    """
    Dispatch one email for one subscription. Returns status string:
    'sent' | 'skipped' | 'failed'.
    """
    trigger_ref = f"deal_{deal['id']}"
    recipient = sub.get("email_at_subscribe")

    # Cooldown check (sellers only)
    if trigger_type == "seller_match" and _seller_cooldown_active(sub):
        _log_sent_alert(sb, sub["id"], sub["user_id"], trigger_type,
                        trigger_ref, recipient or "", None,
                        "skipped_duplicate",
                        error_message=f"Cooldown active ({SELLER_COOLDOWN_HOURS}h)")
        return "skipped"

    # Dedup check
    if _already_sent(sb, sub["id"], trigger_ref, trigger_type):
        return "skipped"

    if not recipient:
        _log_sent_alert(sb, sub["id"], sub["user_id"], trigger_type,
                        trigger_ref, "", None, "failed",
                        error_message="No recipient email")
        return "failed"

    try:
        subject, html = render_fn(sub, deal, verdict)
    except Exception as e:
        log.exception("%s email render failed", trigger_type)
        _log_sent_alert(sb, sub["id"], sub["user_id"], trigger_type,
                        trigger_ref, recipient, None, "failed",
                        error_message=f"Render failed: {e}")
        return "failed"

    ok, provider_id, err = _send_email(recipient, subject, html)
    if ok:
        _log_sent_alert(sb, sub["id"], sub["user_id"], trigger_type,
                        trigger_ref, recipient, subject, "sent", provider_id)
        _bump_sub_counters(sb, sub["id"])
        return "sent"
    else:
        _log_sent_alert(sb, sub["id"], sub["user_id"], trigger_type,
                        trigger_ref, recipient, subject, "failed",
                        error_message=err)
        return "failed"


def _dispatch_role(sb: Client, deal: dict, verdict: dict, role: str) -> dict:
    subs = _find_matching_subscriptions(sb, deal, role=role)
    counts = {"matched": len(subs), "sent": 0, "skipped": 0, "failed": 0}
    if not subs:
        return counts

    trigger_type = "buyer_match" if role == "buyer" else "seller_match"
    render_fn = _render_buyer_email if role == "buyer" else _render_seller_email

    for sub in subs:
        outcome = _dispatch_one(sb, deal, verdict, sub, trigger_type, render_fn)
        counts[outcome] += 1

    return counts


# ──────────────────────────────────────────────────────────────
# PUBLIC: called synchronously by /submit-deal background thread
# ──────────────────────────────────────────────────────────────

def dispatch_deal_alerts_async(sb: Client, deal: dict, app_instance=None) -> None:
    """
    Fire-and-forget entry point. Spawns a daemon thread so /submit-deal
    returns to the user instantly. Must be called with the full deal row
    (including id) already inserted in Supabase.

    app_instance: pass `app` from Flask so we can push an app context inside
    the thread — Jinja render_template requires it.
    """
    def _run():
        try:
            # Flask's render_template requires an app context. Without it,
            # the background thread crashes on the first template render.
            if app_instance is not None:
                ctx = app_instance.app_context()
                ctx.push()
            try:
                verdict = compute_verdict_for_deal(sb, deal)
                buyer_counts = _dispatch_role(sb, deal, verdict, role="buyer")
                seller_counts = _dispatch_role(sb, deal, verdict, role="seller")
                log.info(
                    "Deal %s alerts dispatched — buyers: %s, sellers: %s",
                    deal.get("id"), buyer_counts, seller_counts,
                )
            finally:
                if app_instance is not None:
                    ctx.pop()
        except Exception:
            log.exception("Background alert dispatch crashed for deal %s", deal.get("id"))

    t = threading.Thread(target=_run, daemon=True, name="alert_dispatch")
    t.start()


# ──────────────────────────────────────────────────────────────
# Weekly digest
# ──────────────────────────────────────────────────────────────

def send_weekly_digest(sb: Client) -> dict:
    """
    Send weekly digest to every user with at least one active, email-enabled,
    non-expired subscription — even if no matches in the last 7 days.
    One email per user, summarizing all their subs (buyer + seller) together.
    """
    now = datetime.utcnow()
    now_iso = now.isoformat()
    week_ago_iso = (now - timedelta(days=7)).isoformat()

    try:
        resp = (sb.table("alert_subscriptions")
                .select("*")
                .eq("active", True)
                .eq("email_enabled", True)
                .gt("expires_at", now_iso)
                .execute())
        subs = resp.data or []
    except Exception as e:
        log.exception("Weekly digest: failed to fetch subscriptions: %s", e)
        return {"users": 0, "sent": 0, "failed": 0, "skipped": 0}

    # Group by user
    user_subs = {}
    for s in subs:
        user_subs.setdefault(s["user_id"], []).append(s)

    counts = {"users": len(user_subs), "sent": 0, "failed": 0, "skipped": 0}
    digest_ref = f"digest_{now.strftime('%Y-%m-%d')}"

    for user_id, user_sub_list in user_subs.items():
        recipient = next(
            (s.get("email_at_subscribe") for s in user_sub_list if s.get("email_at_subscribe")),
            None,
        )
        if not recipient:
            counts["failed"] += 1
            continue

        user_trigger_ref = f"{digest_ref}_user_{user_id}"
        if _already_sent(sb, None, user_trigger_ref, "weekly_digest"):
            counts["skipped"] += 1
            continue

        # Build digest items — one per sub, with last-7-day matching deals
        digest_items = []
        for s in user_sub_list:
            try:
                deals_resp = (sb.table("deals")
                              .select("id, make, model, variant, sale_price, transaction_date, verified, buyer_type")
                              .eq("make", s["make"])
                              .eq("model", s["model"])
                              .eq("variant", s["variant"])
                              .gte("created_at", week_ago_iso)
                              .order("created_at", desc=True)
                              .limit(5)
                              .execute())
                week_deals = deals_resp.data or []
            except Exception as e:
                log.warning("Weekly digest: fetch deals for sub %s failed: %s", s.get("id"), e)
                week_deals = []

            digest_items.append({
                "sub": s,
                "deals": week_deals,
                "car_label": f"{s['make']} {s['model']} {s['variant']}",
                "role": s.get("role", "buyer"),
            })

        try:
            html = render_template(
                "email/weekly_digest.html",
                digest_items=digest_items,
                format_inr=format_inr,
                format_date=format_date_ddmmmyyyy,
                week_start=format_date_ddmmmyyyy(now - timedelta(days=7)),
                week_end=format_date_ddmmmyyyy(now),
                app_url=APP_BASE_URL,
            )
            subject = f"Your AutoKnowMus weekly digest — {format_date_ddmmmyyyy(now)}"
        except Exception as e:
            log.exception("Weekly digest render failed")
            _log_sent_alert(sb, None, user_id, "weekly_digest",
                            user_trigger_ref, recipient, None, "failed",
                            error_message=f"Render failed: {e}")
            counts["failed"] += 1
            continue

        ok, provider_id, err = _send_email(recipient, subject, html)
        if ok:
            _log_sent_alert(sb, None, user_id, "weekly_digest",
                            user_trigger_ref, recipient, subject, "sent", provider_id)
            counts["sent"] += 1
        else:
            _log_sent_alert(sb, None, user_id, "weekly_digest",
                            user_trigger_ref, recipient, subject, "failed",
                            error_message=err)
            counts["failed"] += 1

    return counts
