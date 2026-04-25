"""
alert_dispatcher.py
-------------------
AutoKnowMus Email Alerts v2.1 — Dispatcher with Magic-Link Auto-Login

Triggers:
  1. Buyer match — on /submit-deal, matching buyer subs (make+model+variant) get email
  2. Seller match — on /submit-deal, matching seller subs get email (6h cooldown per sub)
  3. Weekly digest — Monday 9am IST cron, summary for every active sub
  4. Admin test endpoints — on-demand testing (send_test_buyer_alert, send_test_seller_alert,
     send_test_digest) — used by /admin/test-email-* routes in app.py

Design contract:
  - Fail-silent: never block or error user-facing /submit-deal
  - Dedup: check sent_alerts before sending same (subscription_id, trigger_ref, trigger_type)
  - Full-pipeline verdict: uses the SAME phase/blend/range logic as dashboard
  - Unverified-deal hybrid: sends for all deals, copy flags unverified status
  - Magic links: every CTA gets a single-use, time-limited auto-login URL
  - Graceful degradation: if magic-link generation fails, email still sends with plain login URL

v2 additions (vs v1):
  - generate_magic_link wired into all CTAs (instant alerts + digest)
  - first_name + subscription_age_days personalization in render context
  - LOGO_URL constant for email branding
  - 3 admin test functions: send_test_buyer_alert, send_test_seller_alert, send_test_digest

v2.1 fixes (vs v2):
  - format_inr() now uses lakh/K format ('₹6.80L', '₹95K') matching dashboard rule
  - _first_name() properly capitalizes (handles 'rajeev', 'RAJEEV', 'RaJeEv' all → 'Rajeev')
  - Render context now includes 'sub_criteria' dict (year, fuel, owner, condition, mileage)
  - Render context now includes 'year_mismatch' flag (True when deal.year != sub.year)
  - format_inr_precise() retained for cases where exact rupee value is needed
"""

import os
import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from urllib.parse import urlencode

import resend
from flask import render_template
from supabase import Client

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

LOGO_URL = f"{APP_BASE_URL}/static/logo-email.png"

SELLER_COOLDOWN_HOURS = 6
PHASE_LOOKBACK_DAYS   = 180
DEAL_YEAR_WINDOW      = 2

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY
else:
    log.warning("RESEND_API_KEY not set — email dispatch will be disabled")


# ──────────────────────────────────────────────────────────────
# Magic link helpers
# ──────────────────────────────────────────────────────────────

def _get_magic_link_func():
    try:
        from app import generate_magic_link
        return generate_magic_link
    except (ImportError, AttributeError) as e:
        log.warning("Could not import generate_magic_link from app: %s", e)
        return None


def _build_cta_url(user_id, purpose: str, redirect_path: str) -> str:
    generate_magic_link = _get_magic_link_func()
    if generate_magic_link is None:
        return f"{APP_BASE_URL}/role"
    try:
        url = generate_magic_link(user_id, purpose, redirect_path)
        if url:
            return url
    except Exception as e:
        log.warning("Magic link generation failed for user %s: %s", user_id, e)
    return f"{APP_BASE_URL}/role"


def _build_dashboard_redirect_path(role: str, sub: dict) -> str:
    params = {
        "make":    sub.get("make") or "",
        "fuel":    sub.get("fuel") or "",
        "model":   sub.get("model") or "",
        "variant": sub.get("variant") or "",
        "year":    sub.get("year") or "",
        "owner":   sub.get("owner") or "1st Owner",
        "mileage": sub.get("mileage") or "",
        "condition": sub.get("condition") or "Good",
    }
    if sub.get("reference_asking_price"):
        params["asking_price"] = sub["reference_asking_price"]
    params = {k: v for k, v in params.items() if v not in (None, "", 0)}
    qs = urlencode(params)
    if role == "seller":
        return f"/seller?{qs}" if qs else "/seller"
    return f"/buyer-dashboard?{qs}" if qs else "/buyer-dashboard"


# ──────────────────────────────────────────────────────────────
# Personalization helpers (v2 / v2.1)
# ──────────────────────────────────────────────────────────────

def _first_name(user_or_email) -> str:
    """
    Extract first name from a user dict, properly capitalized.
    Handles all of: 'rajeev', 'RAJEEV', 'rAjEeV' → 'Rajeev'.
    Returns empty string if name not available.
    """
    if not user_or_email:
        return ""
    name = (user_or_email.get("name") or "").strip() if isinstance(user_or_email, dict) else ""
    if not name:
        return ""
    first_word = name.split(" ")[0]
    return first_word.capitalize()


def _get_user_for_sub(sb: Client, sub: dict) -> dict:
    if not sub or not sub.get("user_id"):
        return {}
    try:
        r = sb.table("users").select("id, name, email").eq("id", sub["user_id"]).limit(1).execute()
        return (r.data or [{}])[0]
    except Exception as e:
        log.warning("_get_user_for_sub failed: %s", e)
        return {}


def _subscription_age_days(sub: dict) -> int:
    created = sub.get("created_at")
    if not created:
        return 0
    try:
        if isinstance(created, str):
            clean = created.replace("Z", "+00:00")
            created_dt = datetime.fromisoformat(clean)
            if created_dt.tzinfo is not None:
                created_dt = created_dt.astimezone(timezone.utc).replace(tzinfo=None)
        elif hasattr(created, "tzinfo"):
            created_dt = created
            if created_dt.tzinfo is not None:
                created_dt = created_dt.astimezone(timezone.utc).replace(tzinfo=None)
        else:
            return 0
    except Exception:
        return 0
    delta = datetime.utcnow() - created_dt
    return max(0, delta.days)


def _build_sub_criteria(sub: dict) -> dict:
    """
    Build a dict of pre-formatted strings describing what the user subscribed for.
    Used by templates to show "What you subscribed for" boxes.
    """
    year = sub.get("year")
    fuel = sub.get("fuel") or "—"
    owner = sub.get("owner") or "—"
    condition = sub.get("condition") or "—"
    mileage = sub.get("mileage")
    asking_price = sub.get("reference_asking_price")

    return {
        "year": str(year) if year else "—",
        "fuel": fuel,
        "owner": owner,
        "condition": condition,
        "mileage_display": f"~{mileage:,} km" if mileage else "—",
        "asking_price_display": format_inr(asking_price) if asking_price else None,
    }


def _is_year_mismatch(sub: dict, deal: dict) -> bool:
    """True if user subscribed for one year but deal's car is a different year."""
    sub_year = sub.get("year")
    deal_year = deal.get("year")
    if not sub_year or not deal_year:
        return False
    try:
        return int(sub_year) != int(deal_year)
    except (ValueError, TypeError):
        return False


# ──────────────────────────────────────────────────────────────
# Formatting helpers (v2.1: lakh/K format matching dashboard)
# ──────────────────────────────────────────────────────────────

def format_inr(value) -> str:
    """
    Format INR using lakh/K convention (matches dashboard's lakh2dp filter):
      < 1,000:        '₹680'
      1K – 1L:        '₹95K'  (1 decimal: '₹95.5K')
      1L – 1Cr:       '₹6.80L' (always 2 decimals)
      ≥ 1Cr:          '₹1.25 Crore' (always 2 decimals)
    """
    if value is None:
        return "—"
    try:
        n = int(value)
    except (ValueError, TypeError):
        return str(value)

    abs_n = abs(n)
    sign = "-" if n < 0 else ""

    if abs_n >= 10000000:
        return f"{sign}₹{abs_n / 10000000:.2f} Crore"
    elif abs_n >= 100000:
        return f"{sign}₹{abs_n / 100000:.2f}L"
    elif abs_n >= 1000:
        k_value = abs_n / 1000
        if k_value == int(k_value):
            return f"{sign}₹{int(k_value)}K"
        return f"{sign}₹{k_value:.1f}K"
    else:
        return f"{sign}₹{abs_n}"


def format_inr_precise(value) -> str:
    """Precise INR with Indian commas: 680000 -> '₹6,80,000'. Used rarely."""
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
# Full valuation pipeline — mirrors app.py
# ──────────────────────────────────────────────────────────────

def _fetch_similar_deals(sb: Client, make, model, variant, fuel, year) -> list:
    try:
        year_low = int(year) - DEAL_YEAR_WINDOW
        year_high = int(year) + DEAL_YEAR_WINDOW
        cutoff = (datetime.utcnow() - timedelta(days=PHASE_LOOKBACK_DAYS)).isoformat()

        r = (sb.table("deals")
             .select("sale_price")
             .eq("make", make).eq("model", model).eq("variant", variant).eq("fuel", fuel)
             .eq("verified", True)
             .gte("year", year_low).lte("year", year_high)
             .gte("created_at", cutoff)
             .execute())
        variant_deals = [row["sale_price"] for row in (r.data or []) if row.get("sale_price")]
        if len(variant_deals) >= 3:
            return variant_deals

        r = (sb.table("deals")
             .select("sale_price")
             .eq("make", make).eq("model", model).eq("fuel", fuel)
             .eq("verified", True)
             .gte("year", year_low).lte("year", year_high)
             .gte("created_at", cutoff)
             .execute())
        return [row["sale_price"] for row in (r.data or []) if row.get("sale_price")]
    except Exception as e:
        log.warning("_fetch_similar_deals failed: %s", e)
        return []


def _compute_phase(sb: Client, make, model) -> int:
    try:
        from car_data import determine_phase
        cutoff = (datetime.utcnow() - timedelta(days=PHASE_LOOKBACK_DAYS)).isoformat()
        r = (sb.table("deals")
             .select("user_id")
             .eq("make", make).eq("model", model).eq("verified", True)
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
    make    = deal["make"]
    model   = deal["model"]
    variant = deal.get("variant")
    fuel    = deal.get("fuel") or "Petrol"
    year    = deal.get("year")
    mileage = deal.get("mileage") or 0
    condition = deal.get("condition") or "Good"
    owner     = deal.get("owner") or "1st Owner"

    formula_price = compute_base_valuation(
        make=make, model=model, variant=variant, fuel=fuel,
        year=year, mileage=mileage, condition=condition, owner=owner,
    )
    if formula_price is None:
        return {
            "estimated_price": None, "price_low": None, "price_high": None,
            "phase": 1, "confidence": 0,
            "verdict_code": "unknown", "verdict_label": "Range unavailable",
            "verdict_explanation": "Fair price range could not be computed for this configuration.",
        }

    phase = _compute_phase(sb, make, model)
    similar = _fetch_similar_deals(sb, make, model, variant, fuel, year)
    blended, confidence = adjust_with_deals(formula_price, similar, phase=phase)
    price_low, price_high = compute_price_range(blended, phase=phase)

    sale_price = deal.get("sale_price") or 0
    if price_low is None or price_high is None:
        code = "unknown"
        label = "Range unavailable"
        explanation = "Fair price range could not be computed."
    elif sale_price < price_low:
        code = "below"
        label = "Below fair market"
        explanation = "This price sits below the estimated fair range — a strong deal for the buyer."
    elif sale_price > price_high:
        code = "above"
        label = "Above market"
        explanation = "This price exceeds the estimated fair range — likely a premium for low mileage or special condition."
    else:
        code = "fair"
        label = "Fair price"
        explanation = "This price sits within the estimated fair market range."

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
    if not deal.get("variant"):
        return []
    now_iso = datetime.utcnow().isoformat()
    try:
        r = (sb.table("alert_subscriptions")
             .select("*")
             .eq("role", role).eq("active", True).eq("email_enabled", True)
             .eq("make", deal["make"]).eq("model", deal["model"]).eq("variant", deal["variant"])
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
        return False


def _log_sent_alert(sb: Client, subscription_id, user_id, trigger_type, trigger_ref,
                    recipient, subject, status, provider_id=None, error_message=None):
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
    last = sub.get("last_alerted_at")
    if not last:
        return False
    try:
        if isinstance(last, str):
            clean = last.replace("Z", "+00:00")
            last_dt = datetime.fromisoformat(clean)
        else:
            last_dt = last
        if last_dt.tzinfo is not None:
            last_dt = last_dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        return False
    cutoff = datetime.utcnow() - timedelta(hours=SELLER_COOLDOWN_HOURS)
    return last_dt > cutoff


def _bump_sub_counters(sb: Client, subscription_id: int) -> None:
    try:
        cur = (sb.table("alert_subscriptions")
               .select("alert_count").eq("id", subscription_id).single().execute())
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
# Email body rendering (v2.1 — full personalization + criteria)
# ──────────────────────────────────────────────────────────────

def _render_buyer_email(sub: dict, deal: dict, verdict: dict, sb: Client = None) -> Tuple[str, str]:
    car_label = f"{sub['make']} {sub['model']} {sub['variant']}"
    subject = f"Deal alert: {car_label} sold at {format_inr(deal['sale_price'])}"

    user = _get_user_for_sub(sb, sub) if sb else {}
    first_name = _first_name(user)
    sub_age_days = _subscription_age_days(sub)
    sub_criteria = _build_sub_criteria(sub)
    year_mismatch = _is_year_mismatch(sub, deal)

    redirect_path = _build_dashboard_redirect_path("buyer", sub)
    cta_url = _build_cta_url(sub.get("user_id"), purpose="alert", redirect_path=redirect_path)

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
        first_name=first_name,
        subscription_age_days=sub_age_days,
        cta_url=cta_url,
        logo_url=LOGO_URL,
        sub_criteria=sub_criteria,
        year_mismatch=year_mismatch,
    )
    return subject, html


def _render_seller_email(sub: dict, deal: dict, verdict: dict, sb: Client = None) -> Tuple[str, str]:
    car_label = f"{sub['make']} {sub['model']} {sub['variant']}"
    subject = f"Market signal: A {car_label} just sold for {format_inr(deal['sale_price'])}"

    user = _get_user_for_sub(sb, sub) if sb else {}
    first_name = _first_name(user)
    sub_age_days = _subscription_age_days(sub)
    sub_criteria = _build_sub_criteria(sub)
    year_mismatch = _is_year_mismatch(sub, deal)

    redirect_path = _build_dashboard_redirect_path("seller", sub)
    cta_url = _build_cta_url(sub.get("user_id"), purpose="alert", redirect_path=redirect_path)

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
        first_name=first_name,
        subscription_age_days=sub_age_days,
        cta_url=cta_url,
        logo_url=LOGO_URL,
        sub_criteria=sub_criteria,
        year_mismatch=year_mismatch,
    )
    return subject, html


# ──────────────────────────────────────────────────────────────
# Per-role dispatch
# ──────────────────────────────────────────────────────────────

def _dispatch_one(sb, deal, verdict, sub, trigger_type, render_fn) -> str:
    trigger_ref = f"deal_{deal['id']}"
    recipient = sub.get("email_at_subscribe")

    if trigger_type == "seller_match" and _seller_cooldown_active(sub):
        _log_sent_alert(sb, sub["id"], sub["user_id"], trigger_type,
                        trigger_ref, recipient or "", None,
                        "skipped_duplicate",
                        error_message=f"Cooldown active ({SELLER_COOLDOWN_HOURS}h)")
        return "skipped"

    if _already_sent(sb, sub["id"], trigger_ref, trigger_type):
        return "skipped"

    if not recipient:
        _log_sent_alert(sb, sub["id"], sub["user_id"], trigger_type,
                        trigger_ref, "", None, "failed",
                        error_message="No recipient email")
        return "failed"

    try:
        subject, html = render_fn(sub, deal, verdict, sb)
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
# PUBLIC: called by /submit-deal
# ──────────────────────────────────────────────────────────────

def dispatch_deal_alerts_async(sb: Client, deal: dict, app_instance=None) -> None:
    def _run():
        try:
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
# Weekly digest (v2.1)
# ──────────────────────────────────────────────────────────────

def send_weekly_digest(sb: Client) -> dict:
    now = datetime.utcnow()
    now_iso = now.isoformat()
    week_ago_iso = (now - timedelta(days=7)).isoformat()

    try:
        resp = (sb.table("alert_subscriptions")
                .select("*")
                .eq("active", True).eq("email_enabled", True)
                .gt("expires_at", now_iso)
                .execute())
        subs = resp.data or []
    except Exception as e:
        log.exception("Weekly digest: failed to fetch subscriptions: %s", e)
        return {"users": 0, "sent": 0, "failed": 0, "skipped": 0}

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

        user = _get_user_for_sub(sb, user_sub_list[0])
        first_name = _first_name(user)

        digest_items = []
        seller_count = 0
        buyer_count = 0

        for s in user_sub_list:
            try:
                deals_resp = (sb.table("deals")
                              .select("id, make, model, variant, year, sale_price, transaction_date, verified, buyer_type")
                              .eq("make", s["make"]).eq("model", s["model"]).eq("variant", s["variant"])
                              .gte("created_at", week_ago_iso)
                              .order("created_at", desc=True)
                              .limit(5)
                              .execute())
                week_deals = deals_resp.data or []
            except Exception as e:
                log.warning("Weekly digest: fetch deals for sub %s failed: %s", s.get("id"), e)
                week_deals = []

            role = s.get("role", "buyer")
            if role == "seller":
                seller_count += 1
            else:
                buyer_count += 1

            redirect_path = _build_dashboard_redirect_path(role, s)
            item_cta_url = _build_cta_url(user_id, purpose="digest", redirect_path=redirect_path)

            digest_items.append({
                "sub": s,
                "sub_criteria": _build_sub_criteria(s),
                "deals": week_deals,
                "car_label": f"{s['make']} {s['model']} {s['variant']}",
                "role": role,
                "cta_url": item_cta_url,
                "subscription_age_days": _subscription_age_days(s),
            })

        digest_items.sort(key=lambda x: (0 if x["role"] == "seller" else 1))

        top_cta_url = _build_cta_url(user_id, purpose="digest", redirect_path="/role")

        try:
            html = render_template(
                "email/weekly_digest.html",
                digest_items=digest_items,
                format_inr=format_inr,
                format_date=format_date_ddmmmyyyy,
                week_start=format_date_ddmmmyyyy(now - timedelta(days=7)),
                week_end=format_date_ddmmmyyyy(now),
                app_url=APP_BASE_URL,
                first_name=first_name,
                logo_url=LOGO_URL,
                cta_url=top_cta_url,
                seller_count=seller_count,
                buyer_count=buyer_count,
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


# ══════════════════════════════════════════════════════════════
# ADMIN TEST FUNCTIONS
# ══════════════════════════════════════════════════════════════

_FAKE_SUB_BUYER = {
    "id": 99999,
    "user_id": None,
    "role": "buyer",
    "make": "Maruti Suzuki",
    "model": "Baleno",
    "variant": "Zeta",
    "fuel": "Petrol",
    "year": 2022,
    "owner": "1st Owner",
    "mileage": 35000,
    "condition": "Good",
    "reference_asking_price": 720000,
    "email_enabled": True,
    "whatsapp_enabled": False,
    "email_at_subscribe": None,
    "created_at": (datetime.utcnow() - timedelta(days=5)).isoformat(),
    "expires_at": (datetime.utcnow() + timedelta(days=25)).isoformat(),
    "active": True,
    "alert_count": 0,
    "last_alerted_at": None,
}

_FAKE_SUB_SELLER = {
    **_FAKE_SUB_BUYER,
    "id": 99998,
    "role": "seller",
}

_FAKE_DEAL = {
    "id": 99999999,
    "make": "Maruti Suzuki",
    "model": "Baleno",
    "variant": "Zeta",
    "fuel": "Petrol",
    "year": 2022,
    "mileage": 38000,
    "condition": "Good",
    "owner": "1st Owner",
    "buyer_type": "Private",
    "sale_price": 680000,
    "asking_price": 720000,
    "transaction_date": (datetime.utcnow() - timedelta(days=2)).date().isoformat(),
    "created_at": (datetime.utcnow() - timedelta(hours=3)).isoformat(),
    "verified": True,
}


def _get_admin_test_sub(sb: Client, user: dict, role: str) -> dict:
    if not user or not user.get("id"):
        raise ValueError("Admin user required for test email")

    try:
        now_iso = datetime.utcnow().isoformat()
        r = (sb.table("alert_subscriptions")
             .select("*")
             .eq("user_id", user["id"]).eq("role", role).eq("active", True)
             .gt("expires_at", now_iso)
             .order("created_at", desc=True)
             .limit(1)
             .execute())
        if r.data:
            real_sub = r.data[0]
            real_sub["email_at_subscribe"] = user.get("email") or real_sub.get("email_at_subscribe")
            return real_sub
    except Exception as e:
        log.warning("_get_admin_test_sub real lookup failed: %s", e)

    fake = (_FAKE_SUB_SELLER if role == "seller" else _FAKE_SUB_BUYER).copy()
    fake["user_id"] = user["id"]
    fake["email_at_subscribe"] = user.get("email")
    return fake


def _get_admin_test_deal(sub: dict) -> dict:
    deal = _FAKE_DEAL.copy()
    deal["make"] = sub["make"]
    deal["model"] = sub["model"]
    deal["variant"] = sub["variant"]
    deal["fuel"] = sub.get("fuel") or "Petrol"
    deal["year"] = sub.get("year") or 2022
    deal["mileage"] = (sub.get("mileage") or 35000) + 3000
    deal["condition"] = sub.get("condition") or "Good"
    deal["owner"] = sub.get("owner") or "1st Owner"

    ref_price = sub.get("reference_asking_price")
    if ref_price:
        deal["sale_price"] = int(ref_price * 0.95)
        deal["asking_price"] = int(ref_price)
    else:
        deal["sale_price"] = 680000
        deal["asking_price"] = 720000

    return deal


def send_test_buyer_alert(sb: Client, user: dict, app_instance=None) -> dict:
    if not user or not user.get("email"):
        return {"ok": False, "error": "Admin user has no email", "used_real_data": False}

    sub = _get_admin_test_sub(sb, user, role="buyer")
    used_real = sub["id"] != 99999
    deal = _get_admin_test_deal(sub)
    verdict = compute_verdict_for_deal(sb, deal)

    try:
        if app_instance is not None:
            with app_instance.app_context():
                subject, html = _render_buyer_email(sub, deal, verdict, sb)
        else:
            subject, html = _render_buyer_email(sub, deal, verdict, sb)
    except Exception as e:
        log.exception("Test buyer email render failed")
        return {
            "ok": False, "recipient": user["email"], "subject": None,
            "provider_id": None, "error": f"Render failed: {e}",
            "used_real_data": used_real,
            "car_label": f"{sub['make']} {sub['model']} {sub['variant']}",
        }

    ok, provider_id, err = _send_email(user["email"], subject, html)

    try:
        _log_sent_alert(
            sb, None, user["id"], "admin_test_buyer",
            f"admin_test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            user["email"], subject, "sent" if ok else "failed",
            provider_id, err
        )
    except Exception:
        pass

    return {
        "ok": ok, "recipient": user["email"], "subject": subject,
        "provider_id": provider_id, "error": err,
        "used_real_data": used_real,
        "car_label": f"{sub['make']} {sub['model']} {sub['variant']}",
    }


def send_test_seller_alert(sb: Client, user: dict, app_instance=None) -> dict:
    if not user or not user.get("email"):
        return {"ok": False, "error": "Admin user has no email", "used_real_data": False}

    sub = _get_admin_test_sub(sb, user, role="seller")
    used_real = sub["id"] != 99998
    deal = _get_admin_test_deal(sub)
    verdict = compute_verdict_for_deal(sb, deal)

    try:
        if app_instance is not None:
            with app_instance.app_context():
                subject, html = _render_seller_email(sub, deal, verdict, sb)
        else:
            subject, html = _render_seller_email(sub, deal, verdict, sb)
    except Exception as e:
        log.exception("Test seller email render failed")
        return {
            "ok": False, "recipient": user["email"], "subject": None,
            "provider_id": None, "error": f"Render failed: {e}",
            "used_real_data": used_real,
            "car_label": f"{sub['make']} {sub['model']} {sub['variant']}",
        }

    ok, provider_id, err = _send_email(user["email"], subject, html)

    try:
        _log_sent_alert(
            sb, None, user["id"], "admin_test_seller",
            f"admin_test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            user["email"], subject, "sent" if ok else "failed",
            provider_id, err
        )
    except Exception:
        pass

    return {
        "ok": ok, "recipient": user["email"], "subject": subject,
        "provider_id": provider_id, "error": err,
        "used_real_data": used_real,
        "car_label": f"{sub['make']} {sub['model']} {sub['variant']}",
    }


def send_test_digest(sb: Client, user: dict, app_instance=None) -> dict:
    if not user or not user.get("email"):
        return {"ok": False, "error": "Admin user has no email", "used_real_data": False}

    now = datetime.utcnow()
    week_ago_iso = (now - timedelta(days=7)).isoformat()

    real_subs = []
    try:
        now_iso = now.isoformat()
        r = (sb.table("alert_subscriptions")
             .select("*")
             .eq("user_id", user["id"]).eq("active", True)
             .gt("expires_at", now_iso)
             .execute())
        real_subs = r.data or []
    except Exception as e:
        log.warning("send_test_digest: real subs lookup failed: %s", e)

    used_real = bool(real_subs)

    if used_real:
        sub_list = real_subs
    else:
        fake_seller = _FAKE_SUB_SELLER.copy()
        fake_seller["user_id"] = user["id"]
        fake_seller["email_at_subscribe"] = user["email"]
        fake_buyer = _FAKE_SUB_BUYER.copy()
        fake_buyer["user_id"] = user["id"]
        fake_buyer["email_at_subscribe"] = user["email"]
        sub_list = [fake_seller, fake_buyer]

    first_name = _first_name(user)
    digest_items = []
    seller_count = 0
    buyer_count = 0

    for s in sub_list:
        try:
            deals_resp = (sb.table("deals")
                          .select("id, make, model, variant, year, sale_price, transaction_date, verified, buyer_type")
                          .eq("make", s["make"]).eq("model", s["model"]).eq("variant", s["variant"])
                          .gte("created_at", week_ago_iso)
                          .order("created_at", desc=True)
                          .limit(5)
                          .execute())
            week_deals = deals_resp.data or []
        except Exception:
            week_deals = []

        if not week_deals and not used_real:
            fake_deal_for_item = _get_admin_test_deal(s)
            week_deals = [{
                "id": fake_deal_for_item["id"],
                "make": fake_deal_for_item["make"],
                "model": fake_deal_for_item["model"],
                "variant": fake_deal_for_item["variant"],
                "year": fake_deal_for_item["year"],
                "sale_price": fake_deal_for_item["sale_price"],
                "transaction_date": fake_deal_for_item["transaction_date"],
                "verified": True,
                "buyer_type": "Private",
            }]

        role = s.get("role", "buyer")
        if role == "seller":
            seller_count += 1
        else:
            buyer_count += 1

        redirect_path = _build_dashboard_redirect_path(role, s)
        item_cta_url = _build_cta_url(user["id"], purpose="admin_test", redirect_path=redirect_path)

        digest_items.append({
            "sub": s,
            "sub_criteria": _build_sub_criteria(s),
            "deals": week_deals,
            "car_label": f"{s['make']} {s['model']} {s['variant']}",
            "role": role,
            "cta_url": item_cta_url,
            "subscription_age_days": _subscription_age_days(s),
        })

    digest_items.sort(key=lambda x: (0 if x["role"] == "seller" else 1))

    top_cta_url = _build_cta_url(user["id"], purpose="admin_test", redirect_path="/role")

    try:
        if app_instance is not None:
            with app_instance.app_context():
                html = render_template(
                    "email/weekly_digest.html",
                    digest_items=digest_items,
                    format_inr=format_inr,
                    format_date=format_date_ddmmmyyyy,
                    week_start=format_date_ddmmmyyyy(now - timedelta(days=7)),
                    week_end=format_date_ddmmmyyyy(now),
                    app_url=APP_BASE_URL,
                    first_name=first_name,
                    logo_url=LOGO_URL,
                    cta_url=top_cta_url,
                    seller_count=seller_count,
                    buyer_count=buyer_count,
                )
        else:
            html = render_template(
                "email/weekly_digest.html",
                digest_items=digest_items,
                format_inr=format_inr,
                format_date=format_date_ddmmmyyyy,
                week_start=format_date_ddmmmyyyy(now - timedelta(days=7)),
                week_end=format_date_ddmmmyyyy(now),
                app_url=APP_BASE_URL,
                first_name=first_name,
                logo_url=LOGO_URL,
                cta_url=top_cta_url,
                seller_count=seller_count,
                buyer_count=buyer_count,
            )
        subject = f"[TEST] Your AutoKnowMus weekly digest — {format_date_ddmmmyyyy(now)}"
    except Exception as e:
        log.exception("Test digest render failed")
        return {
            "ok": False, "recipient": user["email"], "subject": None,
            "provider_id": None, "error": f"Render failed: {e}",
            "used_real_data": used_real, "items_count": len(digest_items),
        }

    ok, provider_id, err = _send_email(user["email"], subject, html)

    try:
        _log_sent_alert(
            sb, None, user["id"], "admin_test_digest",
            f"admin_test_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            user["email"], subject, "sent" if ok else "failed",
            provider_id, err
        )
    except Exception:
        pass

    return {
        "ok": ok, "recipient": user["email"], "subject": subject,
        "provider_id": provider_id, "error": err,
        "used_real_data": used_real, "items_count": len(digest_items),
        "seller_count": seller_count, "buyer_count": buyer_count,
    }
