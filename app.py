# ============================================================
# app.py — Part 1
# ------------------------------------------------------------
# Paste this BLOCK FIRST as line 1 of your new app.py.
# Continue pasting Part 2 IMMEDIATELY below the last line of this block.
# ============================================================

import os
import re
import csv
import io
import uuid
import json
import math
import secrets
import logging
import threading
import bcrypt
from datetime import datetime, timedelta
from functools import wraps
from collections import Counter, defaultdict
from flask import Flask, render_template, request, redirect, url_for, session, flash, Response, make_response, jsonify
from authlib.integrations.flask_client import OAuth
from supabase import create_client, Client

from car_data import (
    CAR_DATA, get_makes, get_models, get_variants, get_fuels,
    compute_base_valuation, compute_price_range, adjust_with_deals,
    get_base_price, get_variant_base_price, get_phase_display,
    determine_phase, CURRENT_YEAR,
    BASE_PRICE_DATA_VERSION, BASE_PRICE_LAST_UPDATED,
    PHASE_THRESHOLDS, PHASE_BLEND,
    refresh_prices, refresh_module_constants, get_cache_status,
    get_listings_for_car,  # v3.0: listings cache for market engine routing
)
import car_data as _car_data_module

# v3.0: Market pricing engine — invoked by router when N>=5 listings available.
# Falls back to depreciation engine (compute_base_valuation) otherwise.
# v3.5: Now accepts state_multiplier param (applied at final step only).
from pricing_engine import compute_market_valuation, MIN_EFFECTIVE_LISTINGS

# === Phase 3 (admin price tools) imports — added v3.7.0 ===
from datetime import datetime, timedelta, date as date_cls
import price_scraper
import sheets_writer
import car_data

# Alerts dispatcher (v1 email alerts)
from alert_dispatcher import dispatch_deal_alerts_async, send_weekly_digest

# Test dispatcher functions are imported lazily inside admin endpoints
# so this module loads even if alert_dispatcher hasn't been updated yet.
# ============================================================
# Phase 3 admin price tools — helper functions
# ============================================================

def _is_admin(user) -> bool:
    """True if the logged-in user is the admin. Mirrors layout.html convention."""
    if not user or not user.get('email'):
        return False
    return user['email'].lower() == 'autoknowmus@gmail.com'


def _ddmmmyyyy(dt) -> str:
    """Format datetime/date as DD-MMM-YYYY (e.g. 04-May-2026)."""
    if dt is None:
        return ''
    if isinstance(dt, str):
        return dt
    return dt.strftime('%d-%b-%Y')


def _age_class(date_str: str) -> str:
    """
    Returns CSS class for price-age coloring:
      pt-age-fresh  (≤ 1 month)
      pt-age-warn   (1–3 months)
      pt-age-stale  (> 3 months)
      pt-age-never  (no date set)
    Input: 'DD-MMM-YYYY' string or empty.
    """
    if not date_str:
        return 'pt-age-never'
    try:
        dt = datetime.strptime(date_str, '%d-%b-%Y').date()
    except (ValueError, TypeError):
        return 'pt-age-never'
    age_days = (date_cls.today() - dt).days
    if age_days <= 31:
        return 'pt-age-fresh'
    if age_days <= 92:
        return 'pt-age-warn'
    return 'pt-age-stale'


def _create_pending_review(supabase, review_type, make, model, variant, fuel,
                           current_price=None, proposed_price=None,
                           matched_variant_name=None, scraper_status=None,
                           scraper_url=None) -> int:
    """
    Insert a row into pending_reviews. Returns the new review id.
    Skips insertion if an identical pending review already exists
    (same review_type + car identity + status='pending').
    """
    # Dedup: don't queue the same review twice
    dup_check = supabase.table('pending_reviews').select('id').eq(
        'review_type', review_type
    ).eq('make', make).eq('model', model).eq('variant', variant).eq(
        'fuel', fuel
    ).eq('status', 'pending').execute()
    if dup_check.data:
        return dup_check.data[0]['id']  # return existing id, don't double-queue

    payload = {
        'review_type': review_type,
        'make': make,
        'model': model,
        'variant': variant,
        'fuel': fuel,
        'current_price': current_price,
        'proposed_price': proposed_price,
        'scraped_at': datetime.utcnow().isoformat(),
        'matched_variant_name': matched_variant_name,
        'scraper_status': scraper_status,
        'scraper_url': scraper_url,
        'status': 'pending',
    }
    result = supabase.table('pending_reviews').insert(payload).execute()
    return result.data[0]['id']
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-change-me')

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ============================================================
# ADMIN ALLOWLIST
# ============================================================
ADMIN_EMAILS = {
    'autoknowmus@gmail.com',
}


def _is_admin_email(email):
    """Helper — case-insensitive admin allowlist check."""
    if not email:
        return False
    return email.lower() in {e.lower() for e in ADMIN_EMAILS}


# ---------- Jinja filters ----------
@app.template_filter('firstname')
def firstname_filter(full_name):
    if not full_name:
        return ''
    return str(full_name).split(' ')[0]


@app.template_filter('inr')
def inr_filter(value):
    if value is None:
        return '—'
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
        body = ','.join(groups) + ',' + last3
    return ('-' if n < 0 else '') + body


@app.template_filter('lakh')
def lakh_filter(value):
    if value is None:
        return '—'
    try:
        n = int(value)
    except (ValueError, TypeError):
        return str(value)
    if n >= 10000000:
        return f"{n / 10000000:.2f} Crore"
    if n >= 100000:
        return f"{n / 100000:.2f} Lakh"
    if n >= 1000:
        return f"{n / 1000:.1f} K"
    return str(n)


@app.template_filter('ddmmmyyyy')
def ddmmmyyyy_filter(value):
    if not value:
        return '—'
    if isinstance(value, str):
        try:
            v = value.replace('Z', '+00:00')
            dt = datetime.fromisoformat(v)
        except (ValueError, TypeError):
            return value
    elif isinstance(value, datetime):
        dt = value
    else:
        return str(value)
    return dt.strftime('%d-%b-%Y')


SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_SECRET_KEY = os.environ.get('SUPABASE_SECRET_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# ========== CONSTANTS ==========

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
PHONE_RE = re.compile(r'^\d{10}$')

RTO_STATE_RE    = re.compile(r'^[A-Z]{2}$')
RTO_DISTRICT_RE = re.compile(r'^[0-9]{2}$')
REG_SERIES_RE   = re.compile(r'^[A-Z]{0,3}$')
REG_NUMBER_RE   = re.compile(r'^[0-9]{1,4}$')

RTO_STATES = [
    'AN', 'AP', 'AR', 'AS', 'BR', 'CG', 'CH', 'DD', 'DL', 'DN',
    'GA', 'GJ', 'HP', 'HR', 'JH', 'JK', 'KA', 'KL', 'LA', 'LD',
    'MH', 'ML', 'MN', 'MP', 'MZ', 'NL', 'OD', 'PB', 'PY', 'RJ',
    'SK', 'TN', 'TR', 'TS', 'UK', 'UP', 'WB'
]

CONDITIONS = ['Excellent', 'Good', 'Fair']
OWNERS = ['1st Owner', '2nd Owner', '3rd Owner or more']
BUYER_TYPES = ['Dealer', 'Private']
FUEL_ORDER = ['Petrol', 'Diesel', 'CNG', 'HEV', 'PHEV', 'BEV']
YEAR_START = 2011
YEAR_END = 2026
YEARS = list(range(YEAR_END, YEAR_START - 1, -1))
VALUATION_COST = 100
BUYER_SEARCH_COST = 100
CREDIT_REQUEST_AMOUNT = 500
ALERT_SUBSCRIPTION_COST = 500
ALERT_SUBSCRIPTION_DAYS = 30
MAX_ACTIVE_ALERTS = 5
DEAL_REWARD_AMOUNT = 100
MAX_DEALS_PER_WEEK = 3

PHASE_LOOKBACK_DAYS = 180

GUEST_CREDITS = 100
GUEST_LOCKOUT_DAYS = 30
GUEST_COOKIE_NAME = 'ak_guest_token'
GUEST_COOKIE_MAX_AGE = 60 * 60 * 24 * GUEST_LOCKOUT_DAYS

ALERT_DISPATCH_TOKEN = os.environ.get('ALERT_DISPATCH_TOKEN', '')
APP_BASE_URL = os.environ.get('APP_BASE_URL', 'https://autoknowmus.com').rstrip('/')

# Magic link expiry policy (locked: 24h instant, 7d digest, 24h admin test)
MAGIC_LINK_EXPIRY_HOURS_ALERT = 24
MAGIC_LINK_EXPIRY_DAYS_DIGEST = 7
MAGIC_LINK_EXPIRY_HOURS_ADMIN_TEST = 24

HIGH_DEMAND_BRANDS = {'Maruti Suzuki', 'Hyundai', 'Honda', 'Toyota', 'Tata', 'Kia', 'Mahindra'}
MEDIUM_DEMAND_BRANDS = {'Ford', 'Renault', 'Nissan', 'Volkswagen', 'Skoda', 'MG'}
LUXURY_BRANDS = {'Audi', 'BMW', 'Mercedes-Benz', 'Jaguar', 'Land Rover', 'Lexus', 'Volvo'}

# v2.8: Used-car valuation cap as a fraction of new ex-showroom price.
EX_SHOWROOM_USED_CEILING = 0.95

# v3.0: Listing matching window for the market engine.
LISTING_YEAR_WINDOW = 2

# v3.1: Public-knowledge depreciation curve (NOT engine-internal).
PUBLIC_DEPRECIATION_CURVE = [
    1.00,  # Year 0 (new)
    0.85,  # Year 1
    0.75,  # Year 2
    0.66,  # Year 3
    0.58,  # Year 4
    0.51,  # Year 5
    0.45,  # Year 6
    0.40,  # Year 7
    0.36,  # Year 8
    0.32,  # Year 9
    0.29,  # Year 10
    0.26,  # Year 11
    0.23,  # Year 12
    0.21,  # Year 13
    0.19,  # Year 14
    0.17,  # Year 15+
]

# v3.1: Public mileage benchmark
PUBLIC_KMS_PER_YEAR = 10000

TRANSACTION_TYPE_LABELS = {
    'signup_bonus': 'Signup Bonus',
    'valuation_charge': 'Seller Valuation',
    'buyer_search': 'Buyer Search',
    'alert_subscription': 'Deal Alert Subscription',
    'credit_request_approved': 'Credit Top-up',
    'deal_reward': 'Deal Reward',
    'alert_cancelled': 'Deal Alert Cancelled',
    # v3.5: feedback rewards
    'feedback_reward': 'Feedback Reward',
}

# ============================================================
# v3.5 STATE EXPANSION CONSTANTS
# ============================================================

# Default state and city for new users / backward compatibility.
# Karnataka / Bangalore was the launch city — all existing valuations and deals
# default to KA/Bangalore, and the state_multiplier for KA is 1.000.
DEFAULT_STATE_CODE = 'KA'
DEFAULT_CITY = 'Bangalore'

# Cities by state for dropdown. "Other" lets users enter freeform city.
# Locked in v3.5: top 3-7 cities per state. UI sorts alphabetically except
# "Other" stays last (per UI rule: dropdowns alphabetical, "Other" pinned).
INDIAN_CITIES_BY_STATE = {
    'AN': ['Port Blair', 'Other'],
    'AP': ['Visakhapatnam', 'Vijayawada', 'Tirupati', 'Guntur', 'Other'],
    'AR': ['Itanagar', 'Naharlagun', 'Other'],
    'AS': ['Guwahati', 'Silchar', 'Dibrugarh', 'Jorhat', 'Other'],
    'BR': ['Patna', 'Gaya', 'Bhagalpur', 'Muzaffarpur', 'Other'],
    'CG': ['Raipur', 'Bilaspur', 'Bhilai', 'Other'],
    'CH': ['Chandigarh', 'Other'],
    'DD': ['Daman', 'Diu', 'Silvassa', 'Other'],
    'DL': ['New Delhi', 'Other'],
    'DN': ['Silvassa', 'Other'],
    'GA': ['Panaji', 'Margao', 'Vasco da Gama', 'Other'],
    'GJ': ['Ahmedabad', 'Surat', 'Vadodara', 'Rajkot', 'Gandhinagar', 'Other'],
    'HP': ['Shimla', 'Manali', 'Dharamshala', 'Other'],
    'HR': ['Gurugram', 'Faridabad', 'Panchkula', 'Karnal', 'Other'],
    'JH': ['Ranchi', 'Jamshedpur', 'Dhanbad', 'Other'],
    'JK': ['Srinagar', 'Jammu', 'Other'],
    'KA': ['Bangalore', 'Mysore', 'Mangalore', 'Hubli', 'Belgaum', 'Other'],
    'KL': ['Kochi', 'Thiruvananthapuram', 'Kozhikode', 'Thrissur', 'Other'],
    'LA': ['Leh', 'Kargil', 'Other'],
    'LD': ['Kavaratti', 'Other'],
    'MH': ['Mumbai', 'Pune', 'Nagpur', 'Nashik', 'Aurangabad', 'Thane', 'Other'],
    'ML': ['Shillong', 'Other'],
    'MN': ['Imphal', 'Other'],
    'MP': ['Bhopal', 'Indore', 'Gwalior', 'Jabalpur', 'Ujjain', 'Other'],
    'MZ': ['Aizawl', 'Other'],
    'NL': ['Kohima', 'Dimapur', 'Other'],
    'OD': ['Bhubaneswar', 'Cuttack', 'Rourkela', 'Other'],
    'PB': ['Ludhiana', 'Amritsar', 'Jalandhar', 'Chandigarh', 'Other'],
    'PY': ['Puducherry', 'Other'],
    'RJ': ['Jaipur', 'Jodhpur', 'Udaipur', 'Kota', 'Ajmer', 'Other'],
    'SK': ['Gangtok', 'Other'],
    'TN': ['Chennai', 'Coimbatore', 'Madurai', 'Tiruchirappalli', 'Salem', 'Other'],
    'TR': ['Agartala', 'Other'],
    'TS': ['Hyderabad', 'Warangal', 'Nizamabad', 'Other'],
    'UK': ['Dehradun', 'Haridwar', 'Rishikesh', 'Other'],
    'UP': ['Lucknow', 'Kanpur', 'Agra', 'Varanasi', 'Noida', 'Ghaziabad', 'Meerut', 'Other'],
    'WB': ['Kolkata', 'Howrah', 'Durgapur', 'Siliguri', 'Other'],
}

# v3.5.1: Feedback reward — unified at 50 credits for BOTH reactions (helpful, wayoff).
# Both reactions now require actual_price + source. The 'close' reaction is dropped.
# Net per (search + feedback) = -100 + 50 = -50 credits, so misuse is naturally bounded.
# Submitting a verified deal still earns 100 credits (DEAL_REWARD_AMOUNT).
FEEDBACK_REWARD = 50

# Geo tier minimum thresholds for hierarchical sourcing
MIN_DEALS_FOR_GEO_TIER = 5        # Minimum verified deals per tier (city/state/national)
GEO_DEALS_LOOKBACK_DAYS = 90      # Window for geo-deal sourcing (vs 180d for phase calc)

# State multiplier in-memory cache (refreshed every 5 min from DB).
# Avoids DB roundtrip on every valuation. Thread-safe via lock.
_STATE_MULTIPLIER_CACHE = {
    'data': {},          # {state_code: multiplier (float)}
    'last_loaded': None, # datetime
    'lock': threading.Lock(),
}
STATE_MULTIPLIER_CACHE_TTL = 300  # seconds (5 min)


def _format_txn_date(iso_str):
    if not iso_str:
        return ''
    try:
        clean = iso_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(clean)
        return dt.strftime('%d-%b-%Y %H:%M')
    except Exception:
        return iso_str


def _live_data_version():
    try:
        return _car_data_module.BASE_PRICE_DATA_VERSION or BASE_PRICE_DATA_VERSION
    except Exception:
        return BASE_PRICE_DATA_VERSION


def _live_last_updated():
    try:
        return _car_data_module.BASE_PRICE_LAST_UPDATED or BASE_PRICE_LAST_UPDATED
    except Exception:
        return BASE_PRICE_LAST_UPDATED


# ============================================================
# v2.8: EX-SHOWROOM CEILING — prevents used > new anomaly
# ============================================================
def _apply_ex_showroom_ceiling(make, model, variant, fuel, estimated, price_low, price_high):
    """
    Cap used-car valuation at EX_SHOWROOM_USED_CEILING (95%) of new ex-showroom price.
    """
    ex_showroom = None
    try:
        ex_showroom = get_variant_base_price(make, model, variant, fuel)
    except Exception:
        pass
    if not ex_showroom:
        try:
            ex_showroom = get_base_price(make, model)
        except Exception:
            ex_showroom = None

    if not ex_showroom or ex_showroom <= 0:
        return estimated, price_low, price_high

    ceiling = int(round(ex_showroom * EX_SHOWROOM_USED_CEILING))

    if estimated <= ceiling and (price_high or 0) <= ceiling:
        return estimated, price_low, price_high

    capped_estimated = min(estimated, ceiling)
    capped_high = min(price_high, ceiling) if price_high else ceiling
    capped_low = price_low
    if capped_low and capped_low > capped_high:
        capped_low = capped_high

    if estimated != capped_estimated or price_high != capped_high:
        app.logger.info(
            f"v2.8 ex-showroom ceiling applied: {make} {model} {variant} {fuel} | "
            f"new={ex_showroom} ceiling={ceiling} | "
            f"estimated {estimated}->{capped_estimated} | "
            f"high {price_high}->{capped_high}"
        )

    return capped_estimated, capped_low, capped_high


# ============================================================
# v3.5 STATE MULTIPLIER HELPERS
# ============================================================

def load_state_multipliers(force_refresh=False):
    """
    Load state multipliers from DB into in-memory cache.
    Refreshes every STATE_MULTIPLIER_CACHE_TTL seconds.
    Thread-safe.
    Returns dict {state_code: multiplier}.
    """
    cache = _STATE_MULTIPLIER_CACHE
    with cache['lock']:
        now = datetime.utcnow()
        last = cache.get('last_loaded')
        if not force_refresh and last and (now - last).total_seconds() < STATE_MULTIPLIER_CACHE_TTL:
            return cache['data']

        try:
            r = supabase.table('state_multipliers').select('state_code, multiplier').execute()
            data = {}
            for row in (r.data or []):
                sc = row.get('state_code')
                mult = row.get('multiplier')
                if sc and mult is not None:
                    try:
                        data[sc.upper()] = float(mult)
                    except (ValueError, TypeError):
                        pass
            cache['data'] = data
            cache['last_loaded'] = now
            app.logger.info(f"State multipliers cache refreshed: {len(data)} states loaded")
            return data
        except Exception as e:
            app.logger.error(f"load_state_multipliers failed: {e}")
            return cache.get('data', {})


def get_state_multiplier(state_code):
    """
    Look up multiplier for a state. Defaults to 1.0 (Karnataka baseline) if
    state not found or DB query failed.
    """
    if not state_code:
        return 1.0
    data = load_state_multipliers()
    return data.get(state_code.upper(), 1.0)


def normalize_state_code(state_code):
    """
    Normalize and validate state code. Returns valid 2-letter code or DEFAULT_STATE_CODE.
    """
    if not state_code:
        return DEFAULT_STATE_CODE
    sc = str(state_code).strip().upper()
    if sc in RTO_STATES or sc in INDIAN_CITIES_BY_STATE:
        return sc
    return DEFAULT_STATE_CODE


def normalize_city(city, state_code):
    """
    Normalize city. If 'Other' or empty, defaults to first city in state's list,
    or DEFAULT_CITY as ultimate fallback.
    """
    if not city:
        cities = INDIAN_CITIES_BY_STATE.get(state_code, [DEFAULT_CITY])
        return cities[0] if cities else DEFAULT_CITY
    c = str(city).strip()
    if not c or c.lower() == 'other':
        cities = INDIAN_CITIES_BY_STATE.get(state_code, [DEFAULT_CITY])
        return cities[0] if cities else DEFAULT_CITY
    return c[:100]  # cap to schema limit


# ============================================================
# v3.5 GEO-AWARE DEAL/LISTING FETCHING
# State derived from rto_code (first 2 chars). City stored separately.
# ============================================================

def fetch_deals_by_geo(make, model, variant, fuel, year, user_state, user_city,
                       window_years=2, lookback_days=GEO_DEALS_LOOKBACK_DAYS):
    """
    Hierarchical deal lookup for v3.5 geo expansion.

    Returns dict with city/state/national counts and prices, plus tier_used
    indicating which tier has >= MIN_DEALS_FOR_GEO_TIER deals (or 'none').

    Variant-preference: if >=3 variant-specific verified deals exist for the
    car, those are preferred. Otherwise model-level deals are used.
    All queries exclude is_test_data=true.

    v3.5.1: Geographic matching uses user_state + user_city (transaction location)
    per locked rule #7 — registration (rto_code) and transaction location are
    independent. Falls back to rto_code prefix / legacy city column if the
    new columns are NULL (handles old rows from before Migration 4 backfill,
    just in case).
    """
    out = {
        'city_count': 0,
        'state_count': 0,
        'national_count': 0,
        'city_prices': [],
        'state_prices': [],
        'national_prices': [],
        'tier_used': 'none',
    }

    try:
        year_low = int(year) - window_years
        year_high = int(year) + window_years
        cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()

        # v3.5.1: Select user_state and user_city alongside rto_code/city for fallback
        select_cols = 'sale_price, rto_code, city, user_state, user_city'

        # First try variant-specific
        r_var = (supabase.table('deals')
                 .select(select_cols)
                 .eq('make', make)
                 .eq('model', model)
                 .eq('variant', variant)
                 .eq('fuel', fuel)
                 .eq('verified', True)
                 .eq('is_test_data', False)
                 .gte('year', year_low)
                 .lte('year', year_high)
                 .gte('created_at', cutoff)
                 .execute())
        variant_rows = r_var.data or []

        if len(variant_rows) >= 3:
            rows = variant_rows
        else:
            # Fall back to model-level
            r_model = (supabase.table('deals')
                       .select(select_cols)
                       .eq('make', make)
                       .eq('model', model)
                       .eq('fuel', fuel)
                       .eq('verified', True)
                       .eq('is_test_data', False)
                       .gte('year', year_low)
                       .lte('year', year_high)
                       .gte('created_at', cutoff)
                       .execute())
            rows = r_model.data or []
    except Exception as e:
        app.logger.warning(f"fetch_deals_by_geo query failed: {e}")
        return out

    user_state_upper = (user_state or '').upper()
    user_city_lower = (user_city or '').strip().lower()

    for row in rows:
        price = row.get('sale_price')
        if not price:
            continue
        try:
            price_int = int(price)
        except (ValueError, TypeError):
            continue

        # v3.5.1: Prefer user_state/user_city (transaction location).
        # Fall back to rto_code prefix and legacy city column if NULL.
        deal_state = (row.get('user_state') or '').upper()
        if not deal_state:
            rto = (row.get('rto_code') or '').upper()
            deal_state = rto[:2] if len(rto) >= 2 else ''

        deal_city = (row.get('user_city') or '').strip().lower()
        if not deal_city:
            deal_city = (row.get('city') or '').strip().lower()

        out['national_prices'].append(price_int)
        if user_state_upper and deal_state == user_state_upper:
            out['state_prices'].append(price_int)
            if user_city_lower and deal_city == user_city_lower:
                out['city_prices'].append(price_int)

    out['city_count'] = len(out['city_prices'])
    out['state_count'] = len(out['state_prices'])
    out['national_count'] = len(out['national_prices'])

    if out['city_count'] >= MIN_DEALS_FOR_GEO_TIER:
        out['tier_used'] = 'city'
    elif out['state_count'] >= MIN_DEALS_FOR_GEO_TIER:
        out['tier_used'] = 'state'
    elif out['national_count'] >= MIN_DEALS_FOR_GEO_TIER:
        out['tier_used'] = 'national'
    else:
        out['tier_used'] = 'none'

    return out


def fetch_listings_by_geo(make, model, variant, fuel, year, user_state, user_city):
    """
    Geo-aware listings fetch for the market engine.

    Hierarchy:
      1. City-level listings (>= MIN_EFFECTIVE_LISTINGS) → no multiplier
      2. State-level listings (>= MIN_EFFECTIVE_LISTINGS) → no multiplier
      3. National listings (>= MIN_EFFECTIVE_LISTINGS) → multiplier if user not in KA

    Returns dict:
      {
        'listings': [...],
        'tier_used': 'city' | 'state' | 'national' | 'none',
        'multiplier_should_apply': bool,
      }

    NOTE: Listings cache may not yet have city/state metadata. In that case,
    all listings are treated as 'national' and the multiplier applies for
    non-Bangalore users.
    """
    out = {
        'listings': [],
        'tier_used': 'none',
        'multiplier_should_apply': False,
    }

    try:
        all_listings = get_listings_for_car(
            make=make, model=model, variant=variant, fuel=fuel,
            year=int(year), year_window=LISTING_YEAR_WINDOW,
        )
    except Exception as e:
        app.logger.warning(f"fetch_listings_by_geo: get_listings_for_car failed: {e}")
        return out

    if not all_listings:
        return out

    user_state_upper = (user_state or '').upper()
    user_city_lower = (user_city or '').strip().lower()

    city_listings = []
    state_listings = []
    for L in all_listings:
        listing_city = (L.get('city') or '').strip().lower()
        listing_state = (L.get('state_code') or L.get('state') or '').upper()
        rto = (L.get('rto_code') or '').upper()
        if not listing_state and len(rto) >= 2:
            listing_state = rto[:2]

        if user_city_lower and listing_city and listing_city == user_city_lower:
            city_listings.append(L)
        if user_state_upper and listing_state and listing_state == user_state_upper:
            state_listings.append(L)

    if len(city_listings) >= MIN_EFFECTIVE_LISTINGS:
        out['listings'] = city_listings
        out['tier_used'] = 'city'
        out['multiplier_should_apply'] = False
    elif len(state_listings) >= MIN_EFFECTIVE_LISTINGS:
        out['listings'] = state_listings
        out['tier_used'] = 'state'
        out['multiplier_should_apply'] = False
    elif len(all_listings) >= MIN_EFFECTIVE_LISTINGS:
        out['listings'] = all_listings
        out['tier_used'] = 'national'
        # Only apply multiplier if user not in KA (the listings-cache baseline)
        out['multiplier_should_apply'] = (user_state_upper != DEFAULT_STATE_CODE)
    else:
        out['listings'] = []
        out['tier_used'] = 'none'
        out['multiplier_should_apply'] = False

    return out


# ============================================================
# v3.5 GEO-AWARE CONFIDENCE MATRIX
# Combines model phase (1-4) with geo tier (city/state/national/formula)
# Result range: 55-95
# ============================================================

CONFIDENCE_MATRIX = {
    (4, 'city'):     95,
    (4, 'state'):    90,
    (4, 'national'): 85,
    (4, 'formula'):  78,
    (3, 'city'):     90,
    (3, 'state'):    85,
    (3, 'national'): 78,
    (3, 'formula'):  70,
    (2, 'city'):     85,
    (2, 'state'):    78,
    (2, 'national'): 70,
    (2, 'formula'):  62,
    (1, 'city'):     78,
    (1, 'state'):    70,
    (1, 'national'): 62,
    (1, 'formula'):  55,
}


def calculate_geo_aware_confidence(phase, geo_tier):
    """
    Look up confidence from the matrix.
    Returns int confidence percentage in range 55-95.
    """
    try:
        phase = int(phase) if phase else 1
    except (ValueError, TypeError):
        phase = 1
    if phase < 1: phase = 1
    if phase > 4: phase = 4
    if geo_tier not in ('city', 'state', 'national', 'formula'):
        geo_tier = 'formula'
    return CONFIDENCE_MATRIX.get((phase, geo_tier), 55)


def get_confidence_message(geo_tier, city_count, state_count, national_count, user_city):
    """
    Generate user-facing tooltip message.
    Returns dict: { 'tier_label', 'message', 'action', 'confidence_band' }

    v3.5.1 (revised after user testing): Tooltip now shows a STATIC 4-tier
    confidence ladder instead of dynamic counts. Showing "0 verified deals
    in your city" was eroding user trust even though it was technically
    accurate. The new tooltip educates users about what each confidence
    range means without naming raw counts.

    Tier copy (per user's exact wording, locked):
      95-100% → Highly calibrated due to verified local transactions
      80-94%  → Well calibrated due to strong market data availability
      60-79%  → Moderately calibrated with available market data
      <60%    → Pricing model based estimate, Limited market data

    The geo_tier still drives the action prompt under the ladder, since that's
    what nudges users to submit deals.
    """
    if geo_tier == 'city':
        action = f'Strong local data — pricing is well-calibrated for {user_city}.'
    elif geo_tier == 'state':
        action = f'Submit a verified deal in {user_city} to unlock city-specific pricing.'
    elif geo_tier == 'national':
        action = f'Submit a verified deal in {user_city} to improve local accuracy.'
    else:
        action = f'Submit a verified deal in {user_city} — earn 100 credits and improve accuracy.'

    return {
        'tier_label': geo_tier or 'formula',
        # message + action are kept for backward-compat with any code reading them,
        # but the new dashboard template uses the 4-tier ladder rendered in HTML
        # rather than a single message string.
        'message': '',
        'action':  action,
    }


# ============================================================
# v3.0 / v3.5: BINARY ENGINE ROUTER WITH HIERARCHICAL GEO SOURCING
# ----------------------------------------------------
# Single point of control over which pricing engine runs.
#
# v3.5 Hierarchical sourcing (locked):
#   STEP 1: City verified deals (>=5 in user_city)         → use directly
#   STEP 2: State verified deals (>=5 in user_state)       → use directly
#   STEP 3: Listings (>=5 from any city/state/national)    → market engine
#                                                          ├─ city listings: no multiplier
#                                                          ├─ state listings: no multiplier
#                                                          └─ national listings: × user multiplier (if not KA)
#   STEP 4: National verified deals (>=5)                  → use, × user multiplier
#   STEP 5: Depreciation engine                            → × user multiplier
#
# Confidence: 4×4 matrix (phase × geo_tier), range 55-95.
# State multiplier applied at tiers 3-5 ONLY when data is not from user's locality.
# ============================================================
def _route_valuation(make, model, variant, fuel, year, mileage, condition, owner,
                     allow_market_engine=True, user_state=None, user_city=None):
    """
    Decide which engine/data tier to use, run it, apply ex-showroom ceiling,
    and return a unified (result, audit) tuple.

    v3.5 args:
        user_state: 2-letter state code (defaults to KA if None/invalid)
        user_city: city name (defaults to Bangalore if None)

    Returns:
        (result, audit) tuple, or (None, None) if BOTH engines fail to compute.
    """
    # ============================================================
    # v3.5: Normalize geo inputs and look up state multiplier
    # ============================================================
    user_state = normalize_state_code(user_state)
    user_city = normalize_city(user_city, user_state)
    state_mult = get_state_multiplier(user_state)

    phase_data = compute_model_phase_data(make, model)
    phase = phase_data['phase']
    data_version = _live_data_version()

    # ============================================================
    # v3.5 STEP 1 + 2: Check verified deals by geography (city → state)
    # ============================================================
    geo_deals = fetch_deals_by_geo(
        make=make, model=model, variant=variant, fuel=fuel, year=year,
        user_state=user_state, user_city=user_city,
    )

    # ----- STEP 1: City-level verified deals -----
    if geo_deals['tier_used'] == 'city' and geo_deals['city_count'] >= MIN_DEALS_FOR_GEO_TIER:
        prices = sorted(geo_deals['city_prices'])
        n = len(prices)
        median_price = prices[n // 2] if n % 2 == 1 else (prices[n // 2 - 1] + prices[n // 2]) / 2
        # Apply condition + owner multipliers (anchored to the local median)
        cond_mult = _car_data_module.get_multiplier("condition", condition or "Good") if hasattr(_car_data_module, 'get_multiplier') else 1.0
        owner_mult = _car_data_module.get_multiplier("owner", owner or "1st Owner") if hasattr(_car_data_module, 'get_multiplier') else 1.0
        estimated = int(round(median_price * cond_mult * owner_mult))
        # Use phase-aware price range (tighter for high phases — consistent with rest of engine)
        price_low, price_high = compute_price_range(estimated, phase=phase)

        # Apply ex-showroom ceiling
        estimated, price_low, price_high = _apply_ex_showroom_ceiling(
            make=make, model=model, variant=variant, fuel=fuel,
            estimated=estimated, price_low=price_low, price_high=price_high,
        )

        confidence = calculate_geo_aware_confidence(phase, 'city')

        result = {
            'estimated': estimated,
            'price_low': price_low,
            'price_high': price_high,
            'confidence': confidence,
            'phase': phase,
            'phase_data': phase_data,
            'listings_raw': [],
            # v3.5 additions
            'geo_tier': 'city',
            'user_state': user_state,
            'user_city': user_city,
            'state_multiplier_applied': 1.000,
            'city_count': geo_deals['city_count'],
            'state_count': geo_deals['state_count'],
            'national_count': geo_deals['national_count'],
        }
        audit = {
            'engine_used': 'verified_deals_city',
            'phase_at_valuation': phase,
            'confidence': confidence,
            'n_listings_used': 0,
            'n_deals_used': n,
            'data_version': data_version,
            'market_avg_km': None,
            'price_per_km_elasticity': None,
            'median_listing_price': int(round(median_price)),
            'repair_cost_applied': None,
            'negotiation_buffer_pct': None,
            'nmp_f37': None,
            'selling_price_f38': None,
            'purchase_price_f39': None,
            # v3.5 audit
            'geo_tier': 'city',
            'user_state': user_state,
            'user_city': user_city,
            'state_multiplier_applied': 1.000,
        }
        app.logger.info(
            f"_route_valuation v3.5: city tier | {make} {model} {variant} {fuel} {year} | "
            f"city={user_city} | N={n} | estimated={estimated}"
        )
        return result, audit

    # ----- STEP 2: State-level verified deals -----
    if geo_deals['tier_used'] == 'state' and geo_deals['state_count'] >= MIN_DEALS_FOR_GEO_TIER:
        prices = sorted(geo_deals['state_prices'])
        n = len(prices)
        median_price = prices[n // 2] if n % 2 == 1 else (prices[n // 2 - 1] + prices[n // 2]) / 2
        cond_mult = _car_data_module.get_multiplier("condition", condition or "Good") if hasattr(_car_data_module, 'get_multiplier') else 1.0
        owner_mult = _car_data_module.get_multiplier("owner", owner or "1st Owner") if hasattr(_car_data_module, 'get_multiplier') else 1.0
        estimated = int(round(median_price * cond_mult * owner_mult))
        # Phase-aware range (state tier — same as city, consistent with engine)
        price_low, price_high = compute_price_range(estimated, phase=phase)

        estimated, price_low, price_high = _apply_ex_showroom_ceiling(
            make=make, model=model, variant=variant, fuel=fuel,
            estimated=estimated, price_low=price_low, price_high=price_high,
        )

        confidence = calculate_geo_aware_confidence(phase, 'state')

        result = {
            'estimated': estimated,
            'price_low': price_low,
            'price_high': price_high,
            'confidence': confidence,
            'phase': phase,
            'phase_data': phase_data,
            'listings_raw': [],
            'geo_tier': 'state',
            'user_state': user_state,
            'user_city': user_city,
            'state_multiplier_applied': 1.000,
            'city_count': geo_deals['city_count'],
            'state_count': geo_deals['state_count'],
            'national_count': geo_deals['national_count'],
        }
        audit = {
            'engine_used': 'verified_deals_state',
            'phase_at_valuation': phase,
            'confidence': confidence,
            'n_listings_used': 0,
            'n_deals_used': n,
            'data_version': data_version,
            'market_avg_km': None,
            'price_per_km_elasticity': None,
            'median_listing_price': int(round(median_price)),
            'repair_cost_applied': None,
            'negotiation_buffer_pct': None,
            'nmp_f37': None,
            'selling_price_f38': None,
            'purchase_price_f39': None,
            'geo_tier': 'state',
            'user_state': user_state,
            'user_city': user_city,
            'state_multiplier_applied': 1.000,
        }
        app.logger.info(
            f"_route_valuation v3.5: state tier | {make} {model} {variant} {fuel} {year} | "
            f"state={user_state} | N={n} | estimated={estimated}"
        )
        return result, audit

    # ============================================================
    # v3.5 STEP 3: Market engine with hierarchical listings
    # ============================================================
    listings = []
    geo_listings = {'listings': [], 'tier_used': 'none', 'multiplier_should_apply': False}
    if allow_market_engine:
        try:
            geo_listings = fetch_listings_by_geo(
                make=make, model=model, variant=variant, fuel=fuel, year=year,
                user_state=user_state, user_city=user_city,
            )
            listings = geo_listings['listings']
        except Exception as e:
            app.logger.warning(f"_route_valuation v3.5: fetch_listings_by_geo failed: {e}")
            listings = []

    n_listings = len(listings)

    if allow_market_engine and n_listings >= MIN_EFFECTIVE_LISTINGS:
        applied_mult = state_mult if geo_listings['multiplier_should_apply'] else 1.0
        try:
            engine_result = compute_market_valuation(
                listings=listings,
                user_year=int(year),
                user_mileage=int(mileage),
                user_condition=condition,
                user_owner=owner,
                user_fuel=fuel,
                state_multiplier=applied_mult,  # v3.5
            )
        except Exception as e:
            app.logger.error(f"_route_valuation: market engine raised: {e}")
            engine_result = None

        if engine_result is not None:
            estimated = engine_result['estimated_price']
            price_low = engine_result['price_low']
            price_high = engine_result['price_high']

            estimated, price_low, price_high = _apply_ex_showroom_ceiling(
                make=make, model=model, variant=variant, fuel=fuel,
                estimated=estimated, price_low=price_low, price_high=price_high,
            )

            listing_tier = geo_listings['tier_used']  # 'city' | 'state' | 'national'
            confidence = calculate_geo_aware_confidence(phase, listing_tier)

            result = {
                'estimated': estimated,
                'price_low': price_low,
                'price_high': price_high,
                'confidence': confidence,
                'phase': phase,
                'phase_data': phase_data,
                'listings_raw': listings,
                'geo_tier': listing_tier,
                'user_state': user_state,
                'user_city': user_city,
                'state_multiplier_applied': engine_result.get('state_multiplier_applied', 1.0),
                'city_count': geo_deals['city_count'],
                'state_count': geo_deals['state_count'],
                'national_count': geo_deals['national_count'],
            }
            audit = {
                'engine_used': engine_result['engine_used'],
                'phase_at_valuation': phase,
                'confidence': confidence,
                'n_listings_used': engine_result['n_listings_used'],
                'n_deals_used': None,
                'data_version': data_version,
                'market_avg_km': engine_result['market_avg_km'],
                'price_per_km_elasticity': engine_result['price_per_km_elasticity'],
                'median_listing_price': engine_result['median_listing_price'],
                'repair_cost_applied': engine_result['repair_cost_applied'],
                'negotiation_buffer_pct': engine_result['negotiation_buffer_pct'],
                'nmp_f37': engine_result['nmp_F37'],
                'selling_price_f38': engine_result['selling_price_F38'],
                'purchase_price_f39': engine_result['purchase_price_F39'],
                'geo_tier': listing_tier,
                'user_state': user_state,
                'user_city': user_city,
                'state_multiplier_applied': engine_result.get('state_multiplier_applied', 1.0),
            }
            app.logger.info(
                f"_route_valuation v3.5: market engine fired | {make} {model} {variant} {fuel} "
                f"{year} | listing_tier={listing_tier} | N_listings={n_listings} | "
                f"mult={applied_mult} | estimated={estimated}"
            )
            return result, audit

    # ============================================================
    # v3.5 STEP 4 + 5: Depreciation fallback (with national deals + state multiplier)
    # ============================================================
    estimated_raw = compute_base_valuation(
        make=make, model=model, variant=variant, fuel=fuel,
        year=int(year), mileage=int(mileage),
        condition=condition, owner=owner,
    )
    if estimated_raw is None:
        return None, None

    similar_prices = fetch_similar_deals(
        make=make, model=model, variant=variant, fuel=fuel, year=int(year),
    )
    adjusted_raw, conf_legacy = adjust_with_deals(estimated_raw, similar_prices, phase=phase)
    price_low_raw, price_high_raw = compute_price_range(adjusted_raw, phase=phase)

    # v3.5: Apply state multiplier (clamp safe range)
    if state_mult < 0.80 or state_mult > 1.20:
        state_mult = max(0.80, min(1.20, state_mult))

    adjusted = int(round(adjusted_raw * state_mult))
    price_low = int(round(price_low_raw * state_mult))
    price_high = int(round(price_high_raw * state_mult))

    adjusted, price_low, price_high = _apply_ex_showroom_ceiling(
        make=make, model=model, variant=variant, fuel=fuel,
        estimated=adjusted, price_low=price_low, price_high=price_high,
    )

    # Determine geo tier for confidence
    if geo_deals['national_count'] >= MIN_DEALS_FOR_GEO_TIER:
        confidence_tier = 'national'
    else:
        confidence_tier = 'formula'

    confidence = calculate_geo_aware_confidence(phase, confidence_tier)

    result = {
        'estimated': adjusted,
        'price_low': price_low,
        'price_high': price_high,
        'confidence': confidence,
        'phase': phase,
        'phase_data': phase_data,
        'listings_raw': [],
        'geo_tier': confidence_tier,
        'user_state': user_state,
        'user_city': user_city,
        'state_multiplier_applied': round(state_mult, 3),
        'city_count': geo_deals['city_count'],
        'state_count': geo_deals['state_count'],
        'national_count': geo_deals['national_count'],
    }
    audit = {
        'engine_used': 'depreciation_fallback',
        'phase_at_valuation': phase,
        'confidence': confidence,
        'n_listings_used': n_listings,
        'n_deals_used': len(similar_prices) if similar_prices else 0,
        'data_version': data_version,
        'market_avg_km': None,
        'price_per_km_elasticity': None,
        'median_listing_price': None,
        'repair_cost_applied': None,
        'negotiation_buffer_pct': None,
        'nmp_f37': None,
        'selling_price_f38': None,
        'purchase_price_f39': None,
        'geo_tier': confidence_tier,
        'user_state': user_state,
        'user_city': user_city,
        'state_multiplier_applied': round(state_mult, 3),
    }
    app.logger.info(
        f"_route_valuation v3.5: depreciation fallback | {make} {model} {variant} {fuel} "
        f"{year} | tier={confidence_tier} | mult={state_mult} | estimated={adjusted}"
    )
    return result, audit


# ============================================================
# AUTH HELPERS (preserved from current — no v3.5 changes)
# ============================================================

def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(plain: str, hashed: str) -> bool:
    if not hashed:
        return False
    try:
        return bcrypt.checkpw(plain.encode('utf-8'), hashed.encode('utf-8'))
    except Exception:
        return False

def get_user_by_email(email: str):
    try:
        r = supabase.table('users').select('*').eq('email', email.lower()).limit(1).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        app.logger.error(f"get_user_by_email error: {e}")
        return None

def get_user_by_id(user_id):
    try:
        r = supabase.table('users').select('*').eq('id', user_id).limit(1).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        app.logger.error(f"get_user_by_id error: {e}")
        return None

def create_user(name, email, password_hash=None, phone=None, whatsapp_phone=None,
                is_whatsapp=True, google_id=None, auth_method='manual', credits=500):
    payload = {
        'name': name.strip(),
        'email': email.lower().strip(),
        'password_hash': password_hash,
        'phone': phone,
        'whatsapp_phone': whatsapp_phone,
        'is_whatsapp': is_whatsapp,
        'google_id': google_id,
        'auth_method': auth_method,
        'credits': credits,
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    r = supabase.table('users').insert(payload).execute()
    return r.data[0] if r.data else None

def update_user(user_id, fields: dict):
    r = supabase.table('users').update(fields).eq('id', user_id).execute()
    return r.data[0] if r.data else None

def touch_last_login(user_id):
    try:
        supabase.table('users').update(
            {'last_login_at': datetime.utcnow().isoformat()}
        ).eq('id', user_id).execute()
    except Exception as e:
        app.logger.warning(f"touch_last_login failed: {e}")

def count_active_alert_subscriptions(user_id):
    """Count ALL active subscriptions (buyer + seller combined — per locked design)."""
    try:
        r = (supabase.table('alert_subscriptions')
             .select('id', count='exact')
             .eq('user_id', user_id)
             .eq('active', True)
             .gt('expires_at', datetime.utcnow().isoformat())
             .execute())
        return r.count or 0
    except Exception as e:
        app.logger.warning(f"count_active_alert_subscriptions failed: {e}")
        return 0

def count_recent_deals(user_id, days=7):
    """Count user's recent deals — INCLUDING test data, since this drives the per-user weekly cap."""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
        r = (supabase.table('deals')
             .select('id', count='exact')
             .eq('user_id', user_id)
             .gte('created_at', cutoff)
             .execute())
        return r.count or 0
    except Exception as e:
        app.logger.warning(f"count_recent_deals failed: {e}")
        return 0

# ============================================================
# END app.py — Part 1
# Continue with Part 2 immediately below.
# ============================================================
# ============================================================
# app.py — Part 2
# ------------------------------------------------------------
# Paste this BLOCK immediately below the last line of Part 1.
# Continue pasting Part 3 immediately below the last line of this block.
# ============================================================


# ============================================================
# MAGIC LINK HELPERS (preserved from current)
# ============================================================

def generate_magic_link(user_id, purpose, redirect_path):
    if purpose not in ('alert', 'digest', 'admin_test'):
        app.logger.error(f"generate_magic_link: invalid purpose '{purpose}'")
        return None
    if not user_id:
        app.logger.error("generate_magic_link: user_id is required")
        return None

    now = datetime.utcnow()
    if purpose == 'digest':
        expires = now + timedelta(days=MAGIC_LINK_EXPIRY_DAYS_DIGEST)
    else:
        expires = now + timedelta(hours=MAGIC_LINK_EXPIRY_HOURS_ALERT)

    token = secrets.token_urlsafe(32)

    try:
        supabase.table('email_magic_links').insert({
            'token': token,
            'user_id': user_id,
            'purpose': purpose,
            'redirect_path': redirect_path or '/role',
            'created_at': now.isoformat(),
            'expires_at': expires.isoformat(),
        }).execute()
    except Exception as e:
        app.logger.error(f"generate_magic_link insert failed: {e}")
        return None

    return f"{APP_BASE_URL}/m/{token}?after={purpose}"


def consume_magic_link(token, ip_address=None, user_agent=None):
    if not token:
        return None
    try:
        r = (supabase.table('email_magic_links')
             .select('*')
             .eq('token', token)
             .limit(1)
             .execute())
        record = r.data[0] if r.data else None
    except Exception as e:
        app.logger.error(f"consume_magic_link lookup failed: {e}")
        return None

    if not record:
        return None

    try:
        exp_str = (record.get('expires_at') or '').replace('Z', '').split('+')[0].split('.')[0]
        expires_dt = datetime.fromisoformat(exp_str)
        if expires_dt < datetime.utcnow():
            return None
    except (ValueError, AttributeError, TypeError):
        return None

    if record.get('used_at') is not None:
        return None

    now = datetime.utcnow()
    try:
        update_result = (supabase.table('email_magic_links')
                         .update({
                             'used_at': now.isoformat(),
                             'ip_at_use': (ip_address or '')[:100],
                             'user_agent_at_use': (user_agent or '')[:500],
                         })
                         .eq('token', token)
                         .is_('used_at', 'null')
                         .execute())
        if not update_result.data:
            return None
    except Exception as e:
        app.logger.error(f"consume_magic_link update failed: {e}")
        return None

    return (record['user_id'], record.get('redirect_path') or '/role')


def cleanup_expired_magic_links():
    try:
        now_iso = datetime.utcnow().isoformat()
        r = (supabase.table('email_magic_links')
             .delete()
             .lt('expires_at', now_iso)
             .is_('used_at', 'null')
             .execute())
        deleted = len(r.data) if r.data else 0

        cutoff_30d = (datetime.utcnow() - timedelta(days=30)).isoformat()
        r2 = (supabase.table('email_magic_links')
              .delete()
              .lt('used_at', cutoff_30d)
              .execute())
        deleted_old = len(r2.data) if r2.data else 0

        return deleted + deleted_old
    except Exception as e:
        app.logger.error(f"cleanup_expired_magic_links failed: {e}")
        return -1


# ============================================================
# GUEST HELPERS (preserved from current)
# ============================================================

def has_valid_guest_usage(token: str) -> bool:
    if not token:
        return False
    try:
        now_iso = datetime.utcnow().isoformat()
        r = (supabase.table('guest_usage')
             .select('id')
             .eq('guest_token', token)
             .gt('expires_at', now_iso)
             .limit(1)
             .execute())
        return bool(r.data)
    except Exception as e:
        app.logger.warning(f"has_valid_guest_usage failed: {e}")
        return False


def record_guest_usage(token: str, action_type: str) -> bool:
    if not token:
        return False
    try:
        now = datetime.utcnow()
        expires = now + timedelta(days=GUEST_LOCKOUT_DAYS)
        payload = {
            'guest_token': token,
            'ip_address': request.headers.get('X-Forwarded-For', request.remote_addr or ''),
            'user_agent': (request.user_agent.string or '')[:500],
            'used_at': now.isoformat(),
            'expires_at': expires.isoformat(),
            'action_type': action_type,
        }
        supabase.table('guest_usage').insert(payload).execute()
        return True
    except Exception as e:
        app.logger.error(f"record_guest_usage failed: {e}")
        return False


def start_guest_session(token: str):
    session.clear()
    session['is_guest'] = True
    session['guest_token'] = token
    session['credits'] = GUEST_CREDITS
    session['active_alerts_count'] = 0
    session['guest_used'] = False


def login_user_session(user: dict):
    session.clear()
    session['user_id'] = user['id']
    session['user'] = {
        'name': user.get('name'),
        'email': user.get('email'),
        'credits': user.get('credits', 0)
    }
    session['credits'] = user.get('credits', 0)
    session['active_alerts_count'] = count_active_alert_subscriptions(user['id'])
    session['is_guest'] = False
    touch_last_login(user['id'])

def refresh_session_user(user: dict):
    session['user'] = {
        'name': user.get('name'),
        'email': user.get('email'),
        'credits': user.get('credits', 0)
    }
    session['credits'] = user.get('credits', 0)
    session['active_alerts_count'] = count_active_alert_subscriptions(user['id'])

def current_user():
    uid = session.get('user_id')
    if not uid:
        return None
    u = get_user_by_id(uid)
    if u:
        refresh_session_user(u)
    return u


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('is_guest'):
            return f(*args, **kwargs)
        if not session.get('user_id'):
            legacy = session.get('user')
            if legacy and legacy.get('email'):
                existing = get_user_by_email(legacy['email'])
                if existing:
                    login_user_session(existing)
                    return f(*args, **kwargs)
                migrated = create_user(
                    name=legacy.get('name', 'User'),
                    email=legacy['email'],
                    credits=legacy.get('credits', 500),
                    auth_method='manual'
                )
                if migrated:
                    login_user_session(migrated)
                    return f(*args, **kwargs)
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated


def no_guest(redirect_endpoint='signup', message=None):
    def outer(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if session.get('is_guest'):
                flash(message or 'Please sign up to access this feature.', 'error')
                return redirect(url_for(redirect_endpoint))
            return f(*args, **kwargs)
        return decorated
    return outer


def admin_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('is_guest'):
            return redirect(url_for('role'))
        user = current_user()
        if not user or not _is_admin_email(user.get('email')):
            return redirect(url_for('role'))
        return f(*args, **kwargs)
    return decorated


def log_credit_transaction(user_id, type_, description, amount, balance_after):
    try:
        supabase.table('transactions').insert({
            'user_id': user_id,
            'type': type_,
            'description': description,
            'amount': amount,
            'balance_after': balance_after,
        }).execute()
    except Exception as e:
        app.logger.error(f"Failed to log transaction: {e}")


# ============================================================
# PHASE + COMPS HELPERS (preserved from current — no v3.5 changes)
# Note: fetch_similar_deals is the legacy national-scope query.
# v3.5 introduces fetch_deals_by_geo (above) which returns hierarchical results.
# Both coexist: legacy is used by older code paths, geo by _route_valuation.
# ============================================================

def fetch_similar_deals(make, model, variant, fuel, year, window_years=2,
                        lookback_days=PHASE_LOOKBACK_DAYS):
    try:
        year_low = int(year) - window_years
        year_high = int(year) + window_years
        cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()

        r = (supabase.table('deals')
             .select('sale_price')
             .eq('make', make)
             .eq('model', model)
             .eq('variant', variant)
             .eq('fuel', fuel)
             .eq('verified', True)
             .eq('is_test_data', False)
             .gte('year', year_low)
             .lte('year', year_high)
             .gte('created_at', cutoff)
             .execute())
        variant_deals = [row['sale_price'] for row in (r.data or []) if row.get('sale_price')]

        if len(variant_deals) >= 3:
            return variant_deals

        r = (supabase.table('deals')
             .select('sale_price')
             .eq('make', make)
             .eq('model', model)
             .eq('fuel', fuel)
             .eq('verified', True)
             .eq('is_test_data', False)
             .gte('year', year_low)
             .lte('year', year_high)
             .gte('created_at', cutoff)
             .execute())
        return [row['sale_price'] for row in (r.data or []) if row.get('sale_price')]
    except Exception as e:
        app.logger.warning(f"fetch_similar_deals failed: {e}")
        return []


def compute_model_phase_data(make, model):
    try:
        cutoff = (datetime.utcnow() - timedelta(days=PHASE_LOOKBACK_DAYS)).isoformat()
        r = (supabase.table('deals')
             .select('user_id, sale_price')
             .eq('make', make)
             .eq('model', model)
             .eq('verified', True)
             .eq('is_test_data', False)
             .gte('created_at', cutoff)
             .execute())
        rows = r.data or []
    except Exception as e:
        app.logger.warning(f"compute_model_phase_data failed: {e}")
        rows = []

    deal_count = len(rows)
    distinct_users = len({row['user_id'] for row in rows if row.get('user_id')})
    phase = determine_phase(deal_count, distinct_users, previous_phase=1)

    return {
        'phase': phase,
        'deal_count_180d': deal_count,
        'distinct_users_180d': distinct_users,
        'display': get_phase_display(phase),
    }


def compute_demand(make, year):
    age = max(0, CURRENT_YEAR - int(year))
    if make in HIGH_DEMAND_BRANDS:
        if age <= 7:    return 'HIGH'
        elif age <= 12: return 'MEDIUM'
        return 'LOW'
    if make in MEDIUM_DEMAND_BRANDS:
        if age <= 5: return 'MEDIUM'
        return 'LOW'
    if make in LUXURY_BRANDS:
        if age <= 6: return 'MEDIUM'
        return 'LOW'
    return 'LOW'


def compute_days_to_sell(demand, price):
    if price < 500000:
        price_tier = 'budget'
    elif price < 1500000:
        price_tier = 'mid'
    elif price < 3500000:
        price_tier = 'premium'
    else:
        price_tier = 'luxury'

    base_days = {
        ('HIGH', 'budget'):   15, ('HIGH', 'mid'):      18,
        ('HIGH', 'premium'):  25, ('HIGH', 'luxury'):   32,
        ('MEDIUM','budget'):  22, ('MEDIUM','mid'):     28,
        ('MEDIUM','premium'): 35, ('MEDIUM','luxury'):  40,
        ('LOW',   'budget'):  30, ('LOW',   'mid'):     35,
        ('LOW',   'premium'): 40, ('LOW',   'luxury'):  45,
    }
    return base_days.get((demand, price_tier), 30)


def compute_depreciation_series(current_price, days=90):
    series = []
    for d in range(0, days + 1):
        frac = d / days if days else 0
        decay = 1.0 - 0.05 * (1 - math.exp(-2.5 * frac))
        price = int(round(current_price * decay))
        series.append({'day': d, 'price': price})
    return series


def compute_depreciation_series_monthly(current_price, months=60):
    yearly_rates = [0.15, 0.12, 0.10, 0.08, 0.07]
    series = [{'month': 0, 'price': int(round(current_price))}]

    price = float(current_price)
    for year_idx in range(5):
        rate = yearly_rates[year_idx]
        monthly_multiplier = (1 - rate) ** (1 / 12)
        for m_in_year in range(1, 13):
            price = price * monthly_multiplier
            month_num = year_idx * 12 + m_in_year
            if month_num > months:
                break
            series.append({'month': month_num, 'price': int(round(price))})
        if year_idx * 12 + 12 >= months:
            break
    return series


def compute_buyer_distribution(price_low, price_high, confidence):
    if confidence >= 80:
        distribution = [5, 20, 50, 20, 5]
    elif confidence >= 65:
        distribution = [8, 22, 40, 22, 8]
    elif confidence >= 50:
        distribution = [12, 22, 32, 22, 12]
    else:
        distribution = [15, 22, 26, 22, 15]

    if price_low and price_high and price_low < price_high:
        span = price_high - price_low
        boundaries = [
            price_low,
            price_low + span // 4,
            price_low + span // 2,
            price_low + (3 * span) // 4,
            price_high,
        ]
    else:
        boundaries = [0, 0, 0, 0, 0]

    bands = [
        {'low': None,            'high': boundaries[0],  'pct': distribution[0], 'color': '#6c757d'},
        {'low': boundaries[0],   'high': boundaries[1],  'pct': distribution[1], 'color': '#ffa500'},
        {'low': boundaries[1],   'high': boundaries[3],  'pct': distribution[2], 'color': '#28a745'},
        {'low': boundaries[3],   'high': boundaries[4],  'pct': distribution[3], 'color': '#ffa500'},
        {'low': boundaries[4],   'high': None,           'pct': distribution[4], 'color': '#6c757d'},
    ]
    return bands


def get_market_stats(make, model):
    """Public market stats — exclude test data."""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).isoformat()
        r_recent = (supabase.table('deals')
                    .select('id', count='exact')
                    .eq('make', make)
                    .eq('model', model)
                    .eq('verified', True)
                    .eq('is_test_data', False)
                    .gte('created_at', cutoff)
                    .execute())
        recent = r_recent.count or 0
    except Exception as e:
        app.logger.warning(f"market_stats recent failed: {e}")
        recent = 0

    try:
        r_all = (supabase.table('deals')
                 .select('id', count='exact')
                 .eq('make', make)
                 .eq('model', model)
                 .eq('verified', True)
                 .eq('is_test_data', False)
                 .execute())
        all_time = r_all.count or 0
    except Exception as e:
        app.logger.warning(f"market_stats all_time failed: {e}")
        all_time = 0

    return recent, all_time


def get_active_alert_subscription(user_id, make, model, variant):
    """One sub per car across BOTH roles."""
    try:
        r = (supabase.table('alert_subscriptions')
             .select('*')
             .eq('user_id', user_id)
             .eq('make', make)
             .eq('model', model)
             .eq('variant', variant)
             .eq('active', True)
             .gt('expires_at', datetime.utcnow().isoformat())
             .limit(1)
             .execute())
        return r.data[0] if r.data else None
    except Exception as e:
        app.logger.warning(f"get_active_alert_subscription failed: {e}")
        return None


# ============================================================
# v3.1 DASHBOARD ADDITIONS (preserved from current — no v3.5 changes)
# ============================================================

def _get_ex_showroom_safe(make, model, variant, fuel):
    try:
        p = get_variant_base_price(make, model, variant, fuel)
        if p and p > 0:
            return int(p)
    except Exception:
        pass
    try:
        p = get_base_price(make, model)
        if p and p > 0:
            return int(p)
    except Exception:
        pass
    return None


def _compute_anchor_data(make, model, variant, fuel, year, current_estimated):
    ex_showroom = _get_ex_showroom_safe(make, model, variant, fuel)
    if not ex_showroom or not current_estimated:
        return None

    try:
        years_old = max(0, CURRENT_YEAR - int(year))
    except (ValueError, TypeError):
        years_old = 0

    if ex_showroom > 0:
        pct_off_new = int(round((1.0 - (current_estimated / ex_showroom)) * 100))
        pct_off_new = max(0, min(95, pct_off_new))
    else:
        pct_off_new = 0

    return {
        'ex_showroom': ex_showroom,
        'years_old': years_old,
        'pct_off_new': pct_off_new,
    }


def _compute_starting_baseline_market_only(make, model, variant, fuel, year, audit):
    """
    Hybrid disclosure:
      - market_v1 / verified_deals_*: show baseline number
      - depreciation_fallback: return None (template hides number)
    v3.5: Now also shows for verified_deals_city / verified_deals_state since those
    are also non-formula-only paths.
    """
    if not audit:
        return None
    engine = audit.get('engine_used') or 'depreciation_fallback'
    # Show baseline for any non-fallback engine
    if engine == 'depreciation_fallback':
        return None

    ex_showroom = _get_ex_showroom_safe(make, model, variant, fuel)
    if not ex_showroom:
        return None

    try:
        age = max(0, CURRENT_YEAR - int(year))
    except (ValueError, TypeError):
        age = 0

    idx = min(age, len(PUBLIC_DEPRECIATION_CURVE) - 1)
    factor = PUBLIC_DEPRECIATION_CURVE[idx]
    baseline = int(round(ex_showroom * factor))
    return baseline


def _compute_qualitative_adjustments(condition, owner, mileage, year):
    rows = []

    try:
        m = int(mileage)
        y = int(year)
        age_years = max(1, CURRENT_YEAR - y)
        typical_km = age_years * PUBLIC_KMS_PER_YEAR

        if m == 0:
            mileage_dir = 'up_strong'
            mileage_desc = "Very low km — well below typical"
        elif m < typical_km * 0.7:
            mileage_dir = 'up_strong'
            mileage_desc = f"Below typical {typical_km:,} km for this age"
        elif m < typical_km * 0.9:
            mileage_dir = 'up'
            mileage_desc = "Slightly below typical for this age"
        elif m <= typical_km * 1.1:
            mileage_dir = 'neutral'
            mileage_desc = "About average for the car's age"
        elif m <= typical_km * 1.3:
            mileage_dir = 'down'
            mileage_desc = "Slightly above typical for this age"
        else:
            mileage_dir = 'down_strong'
            mileage_desc = f"Well above typical {typical_km:,} km for this age"

        m_str = f"{m:,} km"
    except (ValueError, TypeError):
        mileage_dir = 'neutral'
        mileage_desc = "—"
        m_str = "—"

    rows.append({
        'icon': '🛣️',
        'label': 'Mileage',
        'value': m_str,
        'description': mileage_desc,
        'direction': mileage_dir,
    })

    cond_map = {
        'Excellent': ('up_strong', 'Better than typical — premium for clean condition'),
        'Good':      ('up',         'Better than the typical used car at this age'),
        'Fair':      ('down',       'Below typical — wear and tear visible'),
    }
    cond_dir, cond_desc = cond_map.get(condition, ('neutral', '—'))
    rows.append({
        'icon': '✨',
        'label': 'Condition',
        'value': condition or '—',
        'description': cond_desc,
        'direction': cond_dir,
    })

    owner_map = {
        '1st Owner':            ('up',         'Buyers value clean ownership chain'),
        '2nd Owner':            ('neutral',    'Standard for this age'),
        '3rd Owner or more':    ('down',       'Multiple owners — buyers negotiate harder'),
    }
    own_dir, own_desc = owner_map.get(owner, ('neutral', '—'))
    rows.append({
        'icon': '👤',
        'label': 'Owner history',
        'value': owner or '—',
        'description': own_desc,
        'direction': own_dir,
    })

    return rows


def _compute_chart_loss_pct_seller(depreciation_series):
    safe = {'pct': 1.5, 'total_rupees': 0, 'weekly_rupees': 0}
    if not depreciation_series or len(depreciation_series) < 31:
        return safe
    try:
        d0 = depreciation_series[0]['price']
        d30 = depreciation_series[30]['price']
        if d0 <= 0:
            return safe
        loss_pct = ((d0 - d30) / d0) * 100
        total_rupees = max(0, int(round(d0 - d30)))
        weekly_rupees = int(round(total_rupees / 4.3)) if total_rupees > 0 else 0
        return {
            'pct': round(loss_pct, 1),
            'total_rupees': total_rupees,
            'weekly_rupees': weekly_rupees,
        }
    except (KeyError, IndexError, TypeError):
        return safe


def _compute_chart_loss_pct_buyer(depreciation_series_monthly):
    safe = {'pct': 42, 'total_rupees': 0, 'monthly_rupees': 0}
    if not depreciation_series_monthly or len(depreciation_series_monthly) < 2:
        return safe
    try:
        today_price = depreciation_series_monthly[0]['price']
        last_entry = depreciation_series_monthly[-1]
        last_price = last_entry['price']
        if today_price <= 0:
            return safe
        loss_pct = ((today_price - last_price) / today_price) * 100
        total_rupees = max(0, int(round(today_price - last_price)))
        months_in_series = max(1, last_entry.get('month') or (len(depreciation_series_monthly) - 1))
        monthly_rupees = int(round(total_rupees / months_in_series)) if total_rupees > 0 else 0
        return {
            'pct': int(round(loss_pct)),
            'total_rupees': total_rupees,
            'monthly_rupees': monthly_rupees,
        }
    except (KeyError, IndexError, TypeError):
        return safe


def _format_listings_for_display(listings, max_rows=4):
    if not listings:
        return None

    safe_rows = []
    for L in listings:
        try:
            yr = L.get('year') or '—'
            km = L.get('mileage')
            km_str = f"{int(km):,}" if km else '—'
            cond = L.get('condition') or '—'
            own = L.get('owner') or '—'
            own_str = str(own)
            if '1st' in own_str:
                own_short = '1st'
            elif '2nd' in own_str:
                own_short = '2nd'
            elif '3rd' in own_str or 'more' in own_str.lower():
                own_short = '3rd+'
            else:
                own_short = own_str[:6]

            price = L.get('asking_price') or L.get('price')
            if not price:
                continue
            price_int = int(price)

            safe_rows.append({
                'year': yr,
                'mileage_str': (km_str + ' km') if km_str != '—' else '—',
                'condition': cond,
                'owner_short': own_short,
                'price': price_int,
            })
        except (ValueError, TypeError, AttributeError):
            continue

    if not safe_rows:
        return None

    total = len(safe_rows)
    shown = safe_rows[:max_rows]
    extra = max(0, total - max_rows)

    return {
        'rows': shown,
        'total_count': total,
        'shown_count': len(shown),
        'extra_count': extra,
    }


# ========== ROUTES ==========

@app.route('/')
def index():
    if session.get('user_id') or session.get('is_guest'):
        return redirect(url_for('role'))
    prefill_email = request.args.get('email', '')
    error = request.args.get('error', '')
    return render_template('index.html', prefill_email=prefill_email, error=error)


@app.route('/m/<token>')
def magic_link_consume(token):
    """Consume a magic-link token from an email CTA."""
    ip = request.headers.get('X-Forwarded-For', request.remote_addr or '')
    ua = request.user_agent.string or ''

    result = consume_magic_link(token, ip_address=ip, user_agent=ua)

    if not result:
        flash('This link has expired or already been used. Please log in to continue.', 'error')
        return redirect(url_for('index'))

    user_id, redirect_path = result

    user = get_user_by_id(user_id)
    if not user:
        app.logger.warning(f"magic_link_consume: user_id {user_id} not found")
        flash('Account not found. Please log in normally.', 'error')
        return redirect(url_for('index'))

    login_user_session(user)
    app.logger.info(f"magic_link_consume: user {user_id} auto-logged in, redirecting to {redirect_path}")

    if not redirect_path.startswith('/'):
        redirect_path = '/role'

    return redirect(redirect_path)


# ========== GUEST ACCESS ==========

@app.route('/guest-access')
def guest_access():
    if session.get('user_id'):
        return redirect(url_for('role'))

    existing_token = request.cookies.get(GUEST_COOKIE_NAME)

    if existing_token and has_valid_guest_usage(existing_token):
        return redirect(url_for('index',
            error='Your guest trial was already used. Please sign up to continue exploring — it only takes a minute and you get 500 free credits.'))

    token = existing_token or str(uuid.uuid4())
    start_guest_session(token)

    resp = make_response(redirect(url_for('role')))
    if not existing_token:
        resp.set_cookie(
            GUEST_COOKIE_NAME,
            token,
            max_age=GUEST_COOKIE_MAX_AGE,
            httponly=True,
            samesite='Lax',
            secure=request.is_secure,
        )
    return resp


@app.route('/guest-exit')
def guest_exit():
    session.clear()
    return redirect(url_for('index'))


# ========== AUTH ==========

@app.route('/login', methods=['POST'])
def login():
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password') or ''

    if not email or not password:
        return redirect(url_for('index', error='Email and password are required.'))

    user = get_user_by_email(email)
    if not user:
        return redirect(url_for('index', error='No account found. Please sign up first.'))

    if not user.get('password_hash'):
        return redirect(url_for('index', error='This email is linked to Google. Please use "Continue with Google".'))

    if not verify_password(password, user['password_hash']):
        return redirect(url_for('index', error='Incorrect password. Please try again.'))

    login_user_session(user)
    return redirect(url_for('role'))

@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if session.get('user_id'):
        return redirect(url_for('role'))

    if request.method == 'GET':
        return render_template('signup.html', form={}, error='')

    form = {
        'name': (request.form.get('name') or '').strip(),
        'email': (request.form.get('email') or '').strip().lower(),
        'password': request.form.get('password') or '',
        'phone': (request.form.get('phone') or '').strip(),
        'wa_same': request.form.get('wa_same') == 'on',
        'whatsapp': (request.form.get('whatsapp') or '').strip(),
    }

    if not form['name'] or len(form['name']) < 2:
        return render_template('signup.html', form=form, error='Please enter your full name.')
    if not EMAIL_RE.match(form['email']):
        return render_template('signup.html', form=form, error='Please enter a valid email address.')
    if len(form['password']) < 8:
        return render_template('signup.html', form=form, error='Password must be at least 8 characters.')
    if not PHONE_RE.match(form['phone']):
        return render_template('signup.html', form=form, error='Please enter a valid 10-digit phone number.')

    existing = get_user_by_email(form['email'])
    if existing:
        return redirect(url_for('index', email=form['email'],
                                error='An account with that email already exists. Please log in.'))

    if form['wa_same']:
        whatsapp_final = form['phone']
    else:
        whatsapp_final = form['whatsapp'] or form['phone']
        if form['whatsapp'] and not PHONE_RE.match(form['whatsapp']):
            return render_template('signup.html', form=form, error='Please enter a valid 10-digit WhatsApp number.')

    try:
        new_user = create_user(
            name=form['name'],
            email=form['email'],
            password_hash=hash_password(form['password']),
            phone=form['phone'],
            whatsapp_phone=whatsapp_final,
            is_whatsapp=True,
            auth_method='manual',
            credits=500
        )
    except Exception as e:
        app.logger.error(f"Signup insert failed: {e}")
        return render_template('signup.html', form=form, error='Something went wrong. Please try again.')

    if not new_user:
        return render_template('signup.html', form=form, error='Could not create account. Please try again.')

    log_credit_transaction(new_user['id'], 'signup_bonus', 'Welcome bonus', 500, 500)

    login_user_session(new_user)
    session['show_welcome'] = True
    return redirect(url_for('role'))

@app.route('/google-login')
def google_login():
    redirect_uri = url_for('google_callback', _external=True)
    return google.authorize_redirect(redirect_uri)

@app.route('/google-callback')
def google_callback():
    try:
        token = google.authorize_access_token()
        userinfo = token.get('userinfo') or google.parse_id_token(token)
    except Exception as e:
        app.logger.error(f"Google OAuth error: {e}")
        return redirect(url_for('index', error='Google sign-in failed. Please try again.'))

    if not userinfo or not userinfo.get('email'):
        return redirect(url_for('index', error='Could not retrieve Google profile.'))

    email = userinfo['email'].lower()
    name = userinfo.get('name') or email.split('@')[0]
    google_id = userinfo.get('sub')

    existing = get_user_by_email(email)
    if existing:
        if not existing.get('google_id'):
            update_user(existing['id'], {'google_id': google_id})
        login_user_session(existing)
        if not existing.get('phone'):
            return redirect(url_for('complete_profile'))
        return redirect(url_for('role'))

    new_user = create_user(
        name=name, email=email, google_id=google_id,
        auth_method='google', credits=500
    )
    if not new_user:
        return redirect(url_for('index', error='Could not create account. Please try again.'))

    log_credit_transaction(new_user['id'], 'signup_bonus', 'Welcome bonus', 500, 500)

    login_user_session(new_user)
    session['show_welcome'] = True
    return redirect(url_for('complete_profile'))

@app.route('/complete-profile', methods=['GET', 'POST'])
@login_required
@no_guest()
def complete_profile():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    if user.get('phone'):
        return redirect(url_for('role'))

    if request.method == 'GET':
        return render_template('complete_profile.html', user=user, error='')

    phone = (request.form.get('phone') or '').strip()
    wa_same = request.form.get('wa_same') == 'on'
    whatsapp = (request.form.get('whatsapp') or '').strip()

    if not PHONE_RE.match(phone):
        return render_template('complete_profile.html', user=user, error='Please enter a valid 10-digit phone number.')

    if wa_same:
        whatsapp_final = phone
    else:
        whatsapp_final = whatsapp or phone
        if whatsapp and not PHONE_RE.match(whatsapp):
            return render_template('complete_profile.html', user=user, error='Please enter a valid 10-digit WhatsApp number.')

    update_user(user['id'], {
        'phone': phone,
        'whatsapp_phone': whatsapp_final,
        'is_whatsapp': True
    })
    return redirect(url_for('role'))

@app.route('/role')
@login_required
def role():
    if session.get('is_guest'):
        token = session.get('guest_token')
        guest_used = has_valid_guest_usage(token) or session.get('guest_used', False)
        return render_template('role.html',
            user=None,
            first_name='Guest',
            show_welcome=False,
            active_alerts_count=0,
            guest_used=guest_used,
            is_admin=False)

    user = current_user()
    if not user:
        return redirect(url_for('index'))
    if not user.get('phone'):
        return redirect(url_for('complete_profile'))
    first_name = firstname_filter(user.get('name'))
    show_welcome = session.pop('show_welcome', False)
    active_alerts_count = count_active_alert_subscriptions(user['id'])

    is_admin = _is_admin_email(user.get('email'))

    return render_template('role.html', user=user, first_name=first_name,
                           show_welcome=show_welcome,
                           active_alerts_count=active_alerts_count,
                           guest_used=False,
                           is_admin=is_admin)


def _guest_block_if_used():
    if session.get('is_guest'):
        token = session.get('guest_token')
        if has_valid_guest_usage(token) or session.get('guest_used', False):
            flash('Your guest trial was used. Please sign up to continue — get 500 free credits.', 'error')
            return redirect(url_for('signup'))
    return None


# ============================================================
# v3.5 SELLER FLOW (state + city accepted from form, defaults KA/Bangalore)
# ============================================================

@app.route('/seller', methods=['GET', 'POST'])
@login_required
def seller():
    block = _guest_block_if_used()
    if block:
        return block

    is_guest = session.get('is_guest', False)
    user = None if is_guest else current_user()
    if not is_guest and not user:
        return redirect(url_for('index'))

    def render_form(form_data, error='', show_credit_request=False):
        return render_template(
            'seller.html',
            form=form_data,
            error=error,
            show_credit_request=show_credit_request,
            makes=get_makes(),
            years=YEARS,
            owners=OWNERS,
            conditions=CONDITIONS,
            car_data_json=json.dumps(CAR_DATA),
            # v3.5: state/city dropdown data
            rto_states=RTO_STATES,
            cities_by_state_json=json.dumps(INDIAN_CITIES_BY_STATE),
            default_state=DEFAULT_STATE_CODE,
            default_city=DEFAULT_CITY,
        )

    if request.method == 'GET':
        prefill = {
            'make':      request.args.get('make', ''),
            'fuel':      request.args.get('fuel', ''),
            'model':     request.args.get('model', ''),
            'variant':   request.args.get('variant', ''),
            'year':      request.args.get('year', ''),
            'owner':     request.args.get('owner', ''),
            'mileage':   request.args.get('mileage', ''),
            'condition': request.args.get('condition', ''),
            # v3.5
            'user_state': request.args.get('user_state', '') or DEFAULT_STATE_CODE,
            'user_city':  request.args.get('user_city', '')  or DEFAULT_CITY,
        }
        return render_form(prefill)

    form_data = {
        'make':      (request.form.get('make') or '').strip(),
        'fuel':      (request.form.get('fuel') or '').strip(),
        'model':     (request.form.get('model') or '').strip(),
        'variant':   (request.form.get('variant') or '').strip(),
        'year':      (request.form.get('year') or '').strip(),
        'owner':     (request.form.get('owner') or '').strip(),
        'mileage':   (request.form.get('mileage') or '').strip(),
        'condition': (request.form.get('condition') or '').strip(),
        # v3.5
        'user_state': (request.form.get('user_state') or '').strip().upper() or DEFAULT_STATE_CODE,
        'user_city':  (request.form.get('user_city') or '').strip()         or DEFAULT_CITY,
    }

    for key in ('make', 'fuel', 'model', 'variant', 'year'):
        if not form_data[key]:
            return render_form(form_data, error='Please fill in all required fields.')

    if form_data['make'] not in CAR_DATA:
        return render_form(form_data, error='Invalid Make selected.')
    if form_data['model'] not in CAR_DATA[form_data['make']]:
        return render_form(form_data, error='Invalid Model selected.')
    if form_data['variant'] not in CAR_DATA[form_data['make']][form_data['model']]['variants']:
        return render_form(form_data, error='Invalid Variant selected.')
    if form_data['fuel'] not in CAR_DATA[form_data['make']][form_data['model']]['fuels']:
        return render_form(form_data, error='Selected Fuel is not available for this Model.')

    if not form_data['owner']:
        form_data['owner'] = '1st Owner'
    if not form_data['condition']:
        form_data['condition'] = 'Good'

    if form_data['owner'] not in OWNERS:
        return render_form(form_data, error='Invalid Owner selected.')
    if form_data['condition'] not in CONDITIONS:
        return render_form(form_data, error='Invalid Condition selected.')

    try:
        year_int = int(form_data['year'])
        if year_int < YEAR_START or year_int > YEAR_END:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Invalid Year.')

    # v3.5.1: Mileage is mandatory — no auto-default fallback.
    if not form_data['mileage']:
        return render_form(form_data, error='Mileage is required. Please enter the car\'s odometer reading.')

    try:
        mileage_int = int(form_data['mileage'])
        if mileage_int < 0 or mileage_int > 500000:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Mileage must be a number between 0 and 500000.')

    # v3.5: Normalize geo (defaults to KA/Bangalore if invalid)
    user_state_norm = normalize_state_code(form_data['user_state'])
    user_city_norm = normalize_city(form_data['user_city'], user_state_norm)
    form_data['user_state'] = user_state_norm
    form_data['user_city'] = user_city_norm

    current_credits = session.get('credits', 0) if is_guest else (user.get('credits', 0) or 0)
    if current_credits < VALUATION_COST:
        return render_form(form_data,
                           error=f'Insufficient credits. You need {VALUATION_COST} credits to run a valuation.',
                           show_credit_request=not is_guest)

    # v3.5: Binary engine router with hierarchical geo sourcing
    result, audit = _route_valuation(
        make=form_data['make'], model=form_data['model'],
        variant=form_data['variant'], fuel=form_data['fuel'],
        year=year_int, mileage=mileage_int,
        condition=form_data['condition'], owner=form_data['owner'],
        user_state=user_state_norm,
        user_city=user_city_norm,
    )
    if result is None:
        return render_form(form_data, error='Could not compute a price for this combination. Please check inputs.')

    adjusted = result['estimated']
    price_low = result['price_low']
    price_high = result['price_high']

    val_payload = {
        'make': form_data['make'],
        'model': form_data['model'],
        'variant': form_data['variant'],
        'fuel': form_data['fuel'],
        'year': year_int,
        'mileage': mileage_int,
        'condition': form_data['condition'],
        'owner': form_data['owner'],
        'estimated_price': adjusted,
        'price_low': price_low,
        'price_high': price_high,
        # v3.0 audit columns
        'engine_used':              audit['engine_used'],
        'phase_at_valuation':       audit['phase_at_valuation'],
        'confidence':               audit['confidence'],
        'n_listings_used':          audit['n_listings_used'],
        'n_deals_used':             audit['n_deals_used'],
        'data_version':             audit['data_version'],
        'market_avg_km':            audit['market_avg_km'],
        'price_per_km_elasticity':  audit['price_per_km_elasticity'],
        'median_listing_price':     audit['median_listing_price'],
        'repair_cost_applied':      audit['repair_cost_applied'],
        'negotiation_buffer_pct':   audit['negotiation_buffer_pct'],
        'nmp_f37':                  audit['nmp_f37'],
        'selling_price_f38':        audit['selling_price_f38'],
        'purchase_price_f39':       audit['purchase_price_f39'],
        # v3.5 audit columns
        'user_state':               user_state_norm,
        'user_city':                user_city_norm,
        'geo_tier':                 audit.get('geo_tier'),
        'state_multiplier_applied': audit.get('state_multiplier_applied', 1.000),
    }

    if not is_guest:
        val_payload['user_id'] = user['id']
        try:
            ins = supabase.table('valuations').insert(val_payload).execute()
            valuation_row = ins.data[0] if ins.data else None
        except Exception as e:
            app.logger.error(f"Valuation insert failed: {e}")
            return render_form(form_data, error='Something went wrong saving your valuation. Please try again.')
        if not valuation_row:
            return render_form(form_data, error='Could not save valuation. Please try again.')

        new_balance = current_credits - VALUATION_COST
        try:
            update_user(user['id'], {'credits': new_balance})
            log_credit_transaction(
                user_id=user['id'],
                type_='valuation_charge',
                description=f"Valuation: {form_data['year']} {form_data['make']} {form_data['model']} {form_data['variant']}",
                amount=-VALUATION_COST,
                balance_after=new_balance,
            )
            session['credits'] = new_balance
            session['user']['credits'] = new_balance
        except Exception as e:
            app.logger.error(f"Credit deduction failed: {e}")

        return redirect(url_for('seller_dashboard', valuation_id=valuation_row['id']))

    # GUEST path
    session['credits'] = current_credits - VALUATION_COST
    session['guest_used'] = True
    record_guest_usage(session.get('guest_token'), 'seller')
    guest_val = dict(val_payload)
    guest_val['id'] = 'guest'
    session['guest_valuation'] = guest_val
    return redirect(url_for('seller_dashboard', valuation_id=0))


@app.route('/seller-dashboard/<int:valuation_id>')
@login_required
def seller_dashboard(valuation_id):
    is_guest = session.get('is_guest', False)

    if is_guest:
        val = session.get('guest_valuation')
        if not val:
            return redirect(url_for('seller'))
        user = None
    else:
        user = current_user()
        try:
            r = supabase.table('valuations').select('*').eq('id', valuation_id).eq('user_id', user['id']).limit(1).execute()
            val = r.data[0] if r.data else None
        except Exception as e:
            app.logger.error(f"Load valuation failed: {e}")
            val = None

        if not val:
            return render_template('placeholder.html', user=user, page_title='Valuation Not Found',
                                   message='We could not find that valuation.')

    estimated  = val['estimated_price']
    price_low  = val['price_low']
    price_high = val['price_high']

    phase_data = compute_model_phase_data(val['make'], val['model'])
    phase = phase_data['phase']

    # v3.5: confidence prefer stored geo-aware value
    confidence = val.get('confidence')
    if confidence is None:
        similar_prices = fetch_similar_deals(
            make=val['make'], model=val['model'],
            variant=val['variant'], fuel=val['fuel'],
            year=val['year'],
        )
        _, confidence = adjust_with_deals(estimated, similar_prices, phase=phase)

    demand       = compute_demand(val['make'], val['year'])
    days_to_sell = compute_days_to_sell(demand, estimated)
    depreciation = compute_depreciation_series(estimated, days=90)
    buyer_dist   = compute_buyer_distribution(price_low, price_high, confidence)
    verified_180d, verified_all_time = get_market_stats(val['make'], val['model'])

    if is_guest:
        active_sub = None
        active_alerts_count = 0
    else:
        active_sub = get_active_alert_subscription(user['id'], val['make'], val['model'], val['variant'])
        active_alerts_count = count_active_alert_subscriptions(user['id'])

    # v3.5: pass geo back to form prefill
    val_user_state = val.get('user_state') or DEFAULT_STATE_CODE
    val_user_city = val.get('user_city') or DEFAULT_CITY

    back_prefill = {
        'make':      val['make'],
        'fuel':      val['fuel'],
        'model':     val['model'],
        'variant':   val['variant'],
        'year':      val['year'],
        'owner':     val['owner'],
        'mileage':   val['mileage'],
        'condition': val['condition'],
        'user_state': val_user_state,
        'user_city':  val_user_city,
    }

    # v3.5: Build geo confidence message for tooltip
    # Re-fetch geo deal counts so tooltip reflects current state (deals submitted
    # since this valuation may have changed counts).
    try:
        geo_now = fetch_deals_by_geo(
            make=val['make'], model=val['model'],
            variant=val['variant'], fuel=val['fuel'],
            year=val['year'],
            user_state=val_user_state, user_city=val_user_city,
        )
    except Exception as e:
        app.logger.warning(f"seller_dashboard fetch_deals_by_geo failed: {e}")
        geo_now = {'city_count': 0, 'state_count': 0, 'national_count': 0, 'tier_used': 'none'}

    geo_tier = val.get('geo_tier') or 'formula'
    confidence_msg = get_confidence_message(
        geo_tier=geo_tier,
        city_count=geo_now['city_count'],
        state_count=geo_now['state_count'],
        national_count=geo_now['national_count'],
        user_city=val_user_city,
    )

    # v3.1 dashboard sections
    stored_audit = {
        'engine_used': val.get('engine_used') or 'depreciation_fallback',
        'n_listings_used': val.get('n_listings_used') or 0,
    }

    anchor_data = _compute_anchor_data(
        make=val['make'], model=val['model'],
        variant=val['variant'], fuel=val['fuel'],
        year=val['year'], current_estimated=estimated,
    )

    starting_baseline = _compute_starting_baseline_market_only(
        make=val['make'], model=val['model'],
        variant=val['variant'], fuel=val['fuel'],
        year=val['year'], audit=stored_audit,
    )

    qual_adjustments = _compute_qualitative_adjustments(
        condition=val['condition'], owner=val['owner'],
        mileage=val['mileage'], year=val['year'],
    )

    chart_loss_pct = _compute_chart_loss_pct_seller(depreciation)

    listings_data = None
    if stored_audit['engine_used'] == 'market_v1':
        try:
            fresh_listings = get_listings_for_car(
                make=val['make'], model=val['model'],
                variant=val['variant'], fuel=val['fuel'],
                year=int(val['year']), year_window=LISTING_YEAR_WINDOW,
            )
            listings_data = _format_listings_for_display(fresh_listings, max_rows=4)
        except Exception as e:
            app.logger.warning(f"seller_dashboard listings refresh failed: {e}")
            listings_data = None

    return render_template(
        'dashboard.html',
        user=user,
        val=val,
        estimated=estimated,
        price_low=price_low,
        price_high=price_high,
        confidence=confidence,
        demand=demand,
        days_to_sell=days_to_sell,
        depreciation_json=json.dumps(depreciation),
        buyer_dist=buyer_dist,
        verified_180d=verified_180d,
        verified_all_time=verified_all_time,
        back_prefill=back_prefill,
        phase_data=phase_data,
        data_version=_live_data_version(),
        active_sub=active_sub,
        active_alerts_count=active_alerts_count,
        alert_cost=ALERT_SUBSCRIPTION_COST,
        alert_days=ALERT_SUBSCRIPTION_DAYS,
        max_alerts=MAX_ACTIVE_ALERTS,
        anchor_data=anchor_data,
        starting_baseline=starting_baseline,
        qual_adjustments=qual_adjustments,
        chart_loss_pct=chart_loss_pct,
        listings_data=listings_data,
        engine_used=stored_audit['engine_used'],
        # v3.5 additions
        geo_tier=geo_tier,
        user_state=val_user_state,
        user_city=val_user_city,
        confidence_msg=confidence_msg,
        state_multiplier_applied=val.get('state_multiplier_applied') or 1.000,
    )

# ============================================================
# END app.py — Part 2
# Continue with Part 3 immediately below.
# ============================================================
# ============================================================
# app.py — Part 3
# ------------------------------------------------------------
# Paste this BLOCK immediately below the last line of Part 2.
# Continue pasting Part 4 immediately below the last line of this block.
# ============================================================


@app.route('/request-credits', methods=['POST'])
@login_required
@no_guest(message='Sign up to request free credits — 500 credits on signup.')
def request_credits():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    current = user.get('credits', 0) or 0
    new_balance = current + CREDIT_REQUEST_AMOUNT
    try:
        update_user(user['id'], {'credits': new_balance})
        log_credit_transaction(
            user_id=user['id'],
            type_='credit_request_approved',
            description=f'Credit top-up request auto-approved ({CREDIT_REQUEST_AMOUNT} credits)',
            amount=CREDIT_REQUEST_AMOUNT,
            balance_after=new_balance,
        )
        session['credits'] = new_balance
        if 'user' in session:
            session['user']['credits'] = new_balance
        flash(f'{CREDIT_REQUEST_AMOUNT} credits added. Your balance is now {new_balance} credits.', 'success')
    except Exception as e:
        app.logger.error(f"Credit request failed: {e}")
        flash('Could not process credit request. Please try again.', 'error')

    return_to = (request.form.get('return_to') or '').strip()
    if return_to == 'seller':
        kwargs = {
            'make':      request.form.get('keep_make') or '',
            'fuel':      request.form.get('keep_fuel') or '',
            'model':     request.form.get('keep_model') or '',
            'variant':   request.form.get('keep_variant') or '',
            'year':      request.form.get('keep_year') or '',
            'owner':     request.form.get('keep_owner') or '',
            'mileage':   request.form.get('keep_mileage') or '',
            'condition': request.form.get('keep_condition') or '',
            # v3.5
            'user_state': request.form.get('keep_user_state') or '',
            'user_city':  request.form.get('keep_user_city') or '',
        }
        kwargs = {k: v for k, v in kwargs.items() if v}
        return redirect(url_for('seller', **kwargs))

    if return_to == 'buyer':
        kwargs = {
            'make':         request.form.get('keep_make') or '',
            'fuel':         request.form.get('keep_fuel') or '',
            'model':        request.form.get('keep_model') or '',
            'variant':      request.form.get('keep_variant') or '',
            'year':         request.form.get('keep_year') or '',
            'owner':        request.form.get('keep_owner') or '',
            'mileage':      request.form.get('keep_mileage') or '',
            'condition':    request.form.get('keep_condition') or '',
            'asking_price': request.form.get('keep_asking_price') or '',
            # v3.5
            'user_state':   request.form.get('keep_user_state') or '',
            'user_city':    request.form.get('keep_user_city') or '',
        }
        kwargs = {k: v for k, v in kwargs.items() if v}
        return redirect(url_for('buyer', **kwargs))

    if return_to == 'buyer_dashboard':
        kwargs = {
            'make':         request.form.get('keep_make') or '',
            'fuel':         request.form.get('keep_fuel') or '',
            'model':        request.form.get('keep_model') or '',
            'variant':      request.form.get('keep_variant') or '',
            'year':         request.form.get('keep_year') or '',
            'owner':        request.form.get('keep_owner') or '',
            'mileage':      request.form.get('keep_mileage') or '',
            'condition':    request.form.get('keep_condition') or '',
            'asking_price': request.form.get('keep_asking_price') or '',
            # v3.5
            'user_state':   request.form.get('keep_user_state') or '',
            'user_city':    request.form.get('keep_user_city') or '',
        }
        kwargs = {k: v for k, v in kwargs.items() if v}
        return redirect(url_for('buyer_dashboard', **kwargs))

    if return_to == 'seller_dashboard_keep':
        valuation_id = request.form.get('valuation_id', '0')
        return redirect(url_for('seller_dashboard', valuation_id=valuation_id))

    ref = request.referrer or url_for('seller')
    return redirect(ref)


# ============================================================
# v3.5 BUYER FLOW (state + city accepted from form)
# ============================================================

@app.route('/buyer', methods=['GET', 'POST'])
@login_required
def buyer():
    block = _guest_block_if_used()
    if block:
        return block

    is_guest = session.get('is_guest', False)
    user = None if is_guest else current_user()
    if not is_guest and not user:
        return redirect(url_for('index'))

    first_name = 'Guest' if is_guest else firstname_filter(user.get('name'))

    def render_form(form_data, error='', show_credit_request=False):
        return render_template(
            'buyer.html',
            user=user,
            first_name=first_name,
            form=form_data,
            prefill=form_data,
            error=error,
            show_credit_request=show_credit_request,
            makes=get_makes(),
            fuels=FUEL_ORDER,
            years=YEARS,
            owners=OWNERS,
            conditions=CONDITIONS,
            car_data_json=json.dumps(CAR_DATA),
            # v3.5
            rto_states=RTO_STATES,
            cities_by_state_json=json.dumps(INDIAN_CITIES_BY_STATE),
            default_state=DEFAULT_STATE_CODE,
            default_city=DEFAULT_CITY,
        )

    if request.method == 'GET':
        prefill = {
            'make':         request.args.get('make', ''),
            'fuel':         request.args.get('fuel', ''),
            'model':        request.args.get('model', ''),
            'variant':      request.args.get('variant', ''),
            'year':         request.args.get('year', ''),
            'owner':        request.args.get('owner', ''),
            'mileage':      request.args.get('mileage', ''),
            'condition':    request.args.get('condition', ''),
            'asking_price': request.args.get('asking_price', ''),
            # v3.5
            'user_state':   request.args.get('user_state', '') or DEFAULT_STATE_CODE,
            'user_city':    request.args.get('user_city', '')  or DEFAULT_CITY,
        }
        return render_form(prefill)

    form_data = {
        'make':         (request.form.get('make') or '').strip(),
        'fuel':         (request.form.get('fuel') or '').strip(),
        'model':        (request.form.get('model') or '').strip(),
        'variant':      (request.form.get('variant') or '').strip(),
        'year':         (request.form.get('year') or '').strip(),
        'owner':        (request.form.get('owner') or '').strip(),
        'mileage':      (request.form.get('mileage') or '').strip(),
        'condition':    (request.form.get('condition') or '').strip(),
        'asking_price': (request.form.get('asking_price') or '').strip(),
        # v3.5
        'user_state':   (request.form.get('user_state') or '').strip().upper() or DEFAULT_STATE_CODE,
        'user_city':    (request.form.get('user_city') or '').strip()         or DEFAULT_CITY,
    }

    for key in ('make', 'fuel', 'model', 'variant', 'year'):
        if not form_data[key]:
            return render_form(form_data, error='Please fill in all required fields.')

    if form_data['make'] not in CAR_DATA:
        return render_form(form_data, error='Invalid Make selected.')
    if form_data['model'] not in CAR_DATA[form_data['make']]:
        return render_form(form_data, error='Invalid Model selected.')
    if form_data['variant'] not in CAR_DATA[form_data['make']][form_data['model']]['variants']:
        return render_form(form_data, error='Invalid Variant selected.')
    if form_data['fuel'] not in CAR_DATA[form_data['make']][form_data['model']]['fuels']:
        return render_form(form_data, error='Selected Fuel is not available for this Model.')

    if not form_data['owner']:
        form_data['owner'] = '1st Owner'
    if not form_data['condition']:
        form_data['condition'] = 'Good'

    if form_data['owner'] not in OWNERS:
        return render_form(form_data, error='Invalid Owner selected.')
    if form_data['condition'] not in CONDITIONS:
        return render_form(form_data, error='Invalid Condition selected.')

    try:
        year_int = int(form_data['year'])
        if year_int < YEAR_START or year_int > YEAR_END:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Invalid Year.')

    if not form_data['mileage']:
        age = max(1, YEAR_END - year_int)
        form_data['mileage'] = str(age * 10000)

    try:
        mileage_int = int(form_data['mileage'])
        if mileage_int < 0 or mileage_int > 500000:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Mileage must be a number between 0 and 500000.')

    asking_price_int = None
    if form_data['asking_price']:
        cleaned = form_data['asking_price'].replace(',', '').replace('₹', '').strip()
        try:
            asking_price_int = int(cleaned)
            if asking_price_int < 0:
                raise ValueError
        except (ValueError, TypeError):
            return render_form(form_data, error='Asking price must be a valid number.')

    # v3.5: Normalize geo
    user_state_norm = normalize_state_code(form_data['user_state'])
    user_city_norm = normalize_city(form_data['user_city'], user_state_norm)
    form_data['user_state'] = user_state_norm
    form_data['user_city'] = user_city_norm

    current_credits = session.get('credits', 0) if is_guest else (user.get('credits', 0) or 0)
    if current_credits < BUYER_SEARCH_COST:
        return render_form(form_data,
                           error=f'Insufficient credits. You need {BUYER_SEARCH_COST} credits to run a search.',
                           show_credit_request=not is_guest)

    new_balance = current_credits - BUYER_SEARCH_COST
    if is_guest:
        session['credits'] = new_balance
        session['guest_used'] = True
        record_guest_usage(session.get('guest_token'), 'buyer')
    else:
        try:
            update_user(user['id'], {'credits': new_balance})
            log_credit_transaction(
                user_id=user['id'],
                type_='buyer_search',
                description=f"Buyer search: {form_data['year']} {form_data['make']} {form_data['model']} {form_data['variant']}",
                amount=-BUYER_SEARCH_COST,
                balance_after=new_balance,
            )
            session['credits'] = new_balance
            if 'user' in session:
                session['user']['credits'] = new_balance
        except Exception as e:
            app.logger.error(f"Buyer credit deduction failed: {e}")

    params = {
        'make':       form_data['make'],
        'fuel':       form_data['fuel'],
        'model':      form_data['model'],
        'variant':    form_data['variant'],
        'year':       form_data['year'],
        'owner':      form_data['owner'],
        'mileage':    form_data['mileage'],
        'condition':  form_data['condition'],
        # v3.5
        'user_state': user_state_norm,
        'user_city':  user_city_norm,
    }
    if asking_price_int is not None:
        params['asking_price'] = asking_price_int

    return redirect(url_for('buyer_dashboard', **params))


@app.route('/buyer-dashboard')
@login_required
def buyer_dashboard():
    is_guest = session.get('is_guest', False)
    user = None if is_guest else current_user()
    if not is_guest and not user:
        return redirect(url_for('index'))

    make      = request.args.get('make', '').strip()
    fuel      = request.args.get('fuel', '').strip()
    model     = request.args.get('model', '').strip()
    variant   = request.args.get('variant', '').strip()
    year      = request.args.get('year', '').strip()
    owner     = request.args.get('owner', '').strip()
    mileage_raw = request.args.get('mileage', '').strip()
    condition = request.args.get('condition', '').strip()
    asking_price_raw = request.args.get('asking_price', '').strip()
    # v3.5
    user_state = (request.args.get('user_state', '').strip().upper() or DEFAULT_STATE_CODE)
    user_city  = (request.args.get('user_city', '').strip() or DEFAULT_CITY)

    if not all([make, fuel, model, variant, year, owner, mileage_raw, condition]):
        return redirect(url_for('buyer'))

    try:
        year_int = int(year)
    except (ValueError, TypeError):
        return redirect(url_for('buyer'))

    try:
        mileage_int = int(mileage_raw)
    except (ValueError, TypeError):
        return redirect(url_for('buyer'))

    if owner not in OWNERS or condition not in CONDITIONS:
        return redirect(url_for('buyer'))

    asking_price_int = None
    if asking_price_raw:
        try:
            asking_price_int = int(asking_price_raw)
        except (ValueError, TypeError):
            asking_price_int = None

    user_state_norm = normalize_state_code(user_state)
    user_city_norm = normalize_city(user_city, user_state_norm)

    # v3.5: Binary engine router with hierarchical geo sourcing
    result, audit = _route_valuation(
        make=make, model=model, variant=variant, fuel=fuel,
        year=year_int, mileage=mileage_int,
        condition=condition, owner=owner,
        user_state=user_state_norm,
        user_city=user_city_norm,
    )
    if result is None:
        return render_template('placeholder.html', user=user,
                               page_title='Analysis Unavailable',
                               message='Could not compute market analysis for this combination. Please try different filters.')

    adjusted = result['estimated']
    price_low = result['price_low']
    price_high = result['price_high']
    confidence = result['confidence']
    phase_data = result['phase_data']

    asking_position = None
    asking_pct_diff = None
    if asking_price_int is not None:
        if asking_price_int < price_low:
            asking_position = 'below'
        elif asking_price_int > price_high:
            asking_position = 'above'
        else:
            asking_position = 'within'
        if adjusted:
            asking_pct_diff = round(((asking_price_int - adjusted) / adjusted) * 100, 1)

    dealer_price = int(round(adjusted * 1.12))
    private_price = adjusted

    demand       = compute_demand(make, year_int)
    days_to_sell = compute_days_to_sell(demand, adjusted)
    depreciation = compute_depreciation_series_monthly(adjusted, months=60)
    verified_180d, verified_all_time = get_market_stats(make, model)

    if is_guest:
        active_sub = None
        active_alerts_count = 0
    else:
        active_sub = get_active_alert_subscription(user['id'], make, model, variant)
        active_alerts_count = count_active_alert_subscriptions(user['id'])

    back_prefill = {
        'make':         make,
        'fuel':         fuel,
        'model':        model,
        'variant':      variant,
        'year':         year,
        'owner':        owner,
        'mileage':      mileage_int,
        'condition':    condition,
        'asking_price': asking_price_raw,
        'user_state':   user_state_norm,
        'user_city':    user_city_norm,
    }

    # v3.5: confidence message
    geo_tier = (audit.get('geo_tier') if audit else 'formula') or 'formula'
    confidence_msg = get_confidence_message(
        geo_tier=geo_tier,
        city_count=result.get('city_count', 0),
        state_count=result.get('state_count', 0),
        national_count=result.get('national_count', 0),
        user_city=user_city_norm,
    )

    anchor_data = _compute_anchor_data(
        make=make, model=model, variant=variant, fuel=fuel,
        year=year_int, current_estimated=adjusted,
    )

    starting_baseline = _compute_starting_baseline_market_only(
        make=make, model=model, variant=variant, fuel=fuel,
        year=year_int, audit=audit,
    )

    qual_adjustments = _compute_qualitative_adjustments(
        condition=condition, owner=owner,
        mileage=mileage_int, year=year_int,
    )

    chart_loss_pct = _compute_chart_loss_pct_buyer(depreciation)

    listings_data = None
    if audit and audit.get('engine_used') == 'market_v1':
        listings_raw = result.get('listings_raw') or []
        listings_data = _format_listings_for_display(listings_raw, max_rows=4)

    return render_template(
        'buyer_dashboard.html',
        user=user,
        first_name='Guest' if is_guest else firstname_filter(user.get('name')),
        make=make, fuel=fuel, model=model, variant=variant,
        year=year_int, owner=owner, mileage=mileage_int, condition=condition,
        asking_price=asking_price_int,
        asking_position=asking_position,
        asking_pct_diff=asking_pct_diff,
        estimated=adjusted,
        price_low=price_low,
        price_high=price_high,
        dealer_price=dealer_price,
        private_price=private_price,
        confidence=confidence,
        demand=demand,
        days_to_sell=days_to_sell,
        depreciation_json=json.dumps(depreciation),
        verified_180d=verified_180d,
        verified_all_time=verified_all_time,
        back_prefill=back_prefill,
        active_sub=active_sub,
        active_alerts_count=active_alerts_count,
        alert_cost=ALERT_SUBSCRIPTION_COST,
        alert_days=ALERT_SUBSCRIPTION_DAYS,
        max_alerts=MAX_ACTIVE_ALERTS,
        phase_data=phase_data,
        data_version=_live_data_version(),
        anchor_data=anchor_data,
        starting_baseline=starting_baseline,
        qual_adjustments=qual_adjustments,
        chart_loss_pct=chart_loss_pct,
        listings_data=listings_data,
        engine_used=(audit.get('engine_used') if audit else 'depreciation_fallback'),
        # v3.5
        geo_tier=geo_tier,
        user_state=user_state_norm,
        user_city=user_city_norm,
        confidence_msg=confidence_msg,
        state_multiplier_applied=(audit.get('state_multiplier_applied') if audit else 1.000) or 1.000,
    )


# ============================================================
# ALERT SUBSCRIPTIONS (preserved from current — no v3.5 changes)
# ============================================================

def _create_alert_subscription(user, role, form, return_endpoint, return_kwargs_fn):
    make    = (form.get('make') or '').strip()
    model   = (form.get('model') or '').strip()
    variant = (form.get('variant') or '').strip()
    fuel    = (form.get('fuel') or '').strip()
    year_raw = (form.get('year') or '').strip()
    owner    = (form.get('owner') or '').strip()
    mileage_raw   = (form.get('mileage') or '').strip()
    condition     = (form.get('condition') or '').strip()
    asking_price_raw = (form.get('asking_price') or '').strip()

    return_kwargs = return_kwargs_fn(form) if return_kwargs_fn else {}
    car_label = f"{make} {model} {variant}".strip()

    if not all([make, model, variant]):
        flash('Missing car details. Please try again from the dashboard.', 'error')
        return redirect(url_for(return_endpoint, **return_kwargs))

    existing = get_active_alert_subscription(user['id'], make, model, variant)
    if existing:
        existing_role = existing.get('role', 'buyer')
        flash(f'You already have active {existing_role} deal alerts for {car_label}. '
              f'One subscription per car is allowed across both seller and buyer roles.', 'success')
        return redirect(url_for(return_endpoint, **return_kwargs))

    active_count = count_active_alert_subscriptions(user['id'])
    if active_count >= MAX_ACTIVE_ALERTS:
        flash(f'Maximum {MAX_ACTIVE_ALERTS} deal alerts already active (combined across seller and buyer). '
              f'Cancel an existing alert before subscribing to deal alerts for {car_label}.', 'error')
        return redirect(url_for(return_endpoint, **return_kwargs))

    current_credits = user.get('credits', 0) or 0
    if current_credits < ALERT_SUBSCRIPTION_COST:
        flash(f'Need ({ALERT_SUBSCRIPTION_COST} Credits) to subscribe for deal alerts on {car_label}. '
              f'You currently have {current_credits} credits. '
              f'Tap "Get {CREDIT_REQUEST_AMOUNT} Free Credits" below.', 'error')
        return redirect(url_for(return_endpoint, **return_kwargs))

    try:
        year_int = int(year_raw) if year_raw else None
    except (ValueError, TypeError):
        year_int = None
    try:
        mileage_int = int(mileage_raw) if mileage_raw else None
    except (ValueError, TypeError):
        mileage_int = None
    try:
        asking_price_int = int(asking_price_raw) if asking_price_raw else None
    except (ValueError, TypeError):
        asking_price_int = None

    now = datetime.utcnow()
    expires = now + timedelta(days=ALERT_SUBSCRIPTION_DAYS)

    payload = {
        'user_id': user['id'],
        'role': role,
        'make': make,
        'model': model,
        'variant': variant,
        'fuel': fuel or None,
        'year': year_int,
        'owner': owner or None,
        'mileage': mileage_int,
        'condition': condition or None,
        'reference_asking_price': asking_price_int,
        'email_enabled': True,
        'whatsapp_enabled': False,
        'email_at_subscribe': user.get('email'),
        'whatsapp_at_subscribe': user.get('whatsapp_phone'),
        'created_at': now.isoformat(),
        'expires_at': expires.isoformat(),
        'active': True,
        'credits_spent': ALERT_SUBSCRIPTION_COST,
    }

    try:
        ins = supabase.table('alert_subscriptions').insert(payload).execute()
        sub_row = ins.data[0] if ins.data else None
    except Exception as e:
        app.logger.error(f"Alert subscription insert failed: {e}")
        flash('Could not create deal alert subscription. Please try again.', 'error')
        return redirect(url_for(return_endpoint, **return_kwargs))

    if not sub_row:
        flash('Could not create deal alert subscription. Please try again.', 'error')
        return redirect(url_for(return_endpoint, **return_kwargs))

    new_balance = current_credits - ALERT_SUBSCRIPTION_COST
    try:
        update_user(user['id'], {'credits': new_balance})
        log_credit_transaction(
            user_id=user['id'],
            type_='alert_subscription',
            description=f"Deal alert subscription ({role}, {ALERT_SUBSCRIPTION_DAYS} days): {car_label}",
            amount=-ALERT_SUBSCRIPTION_COST,
            balance_after=new_balance,
        )
        session['credits'] = new_balance
        if 'user' in session:
            session['user']['credits'] = new_balance
        session['active_alerts_count'] = active_count + 1
    except Exception as e:
        app.logger.error(f"Alert subscription credit deduction failed: {e}")

    role_label = 'Buyer' if role == 'buyer' else 'Seller'
    flash(
        f"✅ {role_label} deal alerts active for {car_label} until "
        f"{expires.strftime('%d-%b-%Y')}. You'll get an email when a verified deal "
        f"is submitted on AutoKnowMus for a matching car.",
        'success'
    )
    return redirect(url_for(return_endpoint, **return_kwargs))


@app.route('/subscribe-alert', methods=['POST'])
@login_required
@no_guest(message='Sign up to set deal alerts on cars — free with 500 signup credits.')
def subscribe_alert():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    def _return_kwargs(form):
        return_kwargs = {
            'make':         (form.get('make') or '').strip(),
            'fuel':         (form.get('fuel') or '').strip(),
            'model':        (form.get('model') or '').strip(),
            'variant':      (form.get('variant') or '').strip(),
            'year':         (form.get('year') or '').strip(),
            'owner':        (form.get('owner') or '').strip(),
            'mileage':      (form.get('mileage') or '').strip(),
            'condition':    (form.get('condition') or '').strip(),
            'asking_price': (form.get('asking_price') or '').strip(),
            # v3.5
            'user_state':   (form.get('user_state') or '').strip(),
            'user_city':    (form.get('user_city') or '').strip(),
        }
        return {k: v for k, v in return_kwargs.items() if v}

    return _create_alert_subscription(
        user=user,
        role='buyer',
        form=request.form,
        return_endpoint='buyer_dashboard',
        return_kwargs_fn=_return_kwargs,
    )


@app.route('/subscribe-seller-alert', methods=['POST'])
@login_required
@no_guest(message='Sign up to set deal alerts on your car — free with 500 signup credits.')
def subscribe_seller_alert():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    valuation_id = (request.form.get('valuation_id') or '').strip()

    def _return_kwargs(form):
        return {'valuation_id': valuation_id or '0'}

    return _create_alert_subscription(
        user=user,
        role='seller',
        form=request.form,
        return_endpoint='seller_dashboard',
        return_kwargs_fn=_return_kwargs,
    )


@app.route('/my-alerts')
@login_required
@no_guest(message='Sign up to manage deal alert subscriptions.')
def my_alerts():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    now = datetime.utcnow()

    try:
        result = (supabase.table('alert_subscriptions')
                  .select('*')
                  .eq('user_id', user['id'])
                  .order('created_at', desc=True)
                  .execute())
        all_subs = result.data or []
    except Exception as e:
        app.logger.error(f"Load alerts failed: {e}")
        all_subs = []

    active_buyer_subs = []
    active_seller_subs = []
    expired_subs = []

    for sub in all_subs:
        is_active_flag = sub.get('active', False)
        expires_at_str = sub.get('expires_at')
        created_at_str = sub.get('created_at')

        try:
            exp_clean = (expires_at_str or '').replace('Z', '').split('+')[0].split('.')[0]
            cre_clean = (created_at_str or '').replace('Z', '').split('+')[0].split('.')[0]
            expires_dt = datetime.fromisoformat(exp_clean)
            created_dt = datetime.fromisoformat(cre_clean)
        except (ValueError, AttributeError, TypeError):
            continue

        sub['created_display'] = created_dt.strftime('%d-%b-%Y')
        sub['expires_display'] = expires_dt.strftime('%d-%b-%Y')

        days_remaining = (expires_dt - now).days
        sub['days_remaining'] = max(days_remaining, 0)

        sub['role'] = sub.get('role') or 'buyer'
        sub['role_label'] = 'Buyer' if sub['role'] == 'buyer' else 'Seller'

        if is_active_flag and expires_dt > now:
            if sub['role'] == 'seller':
                active_seller_subs.append(sub)
            else:
                active_buyer_subs.append(sub)
        else:
            if not is_active_flag:
                sub['expired_reason'] = 'Cancelled'
            else:
                sub['expired_reason'] = 'Expired'
            expired_subs.append(sub)

    active_count = len(active_buyer_subs) + len(active_seller_subs)
    session['active_alerts_count'] = active_count

    return render_template(
        'my_alerts.html',
        user=user,
        first_name=firstname_filter(user.get('name')),
        active_buyer_subs=active_buyer_subs,
        active_seller_subs=active_seller_subs,
        expired_subs=expired_subs,
        active_count=active_count,
        max_alerts=MAX_ACTIVE_ALERTS
    )


@app.route('/cancel-alert/<alert_id>', methods=['POST'])
@login_required
@no_guest(message='Sign up to manage deal alerts.')
def cancel_alert(alert_id):
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    try:
        result = (supabase.table('alert_subscriptions')
                  .select('*')
                  .eq('id', alert_id)
                  .eq('user_id', user['id'])
                  .eq('active', True)
                  .execute())
        subs = result.data or []
    except Exception as e:
        app.logger.error(f"Load alert for cancel failed: {e}")
        flash('Could not process cancellation. Please try again.', 'error')
        return redirect(url_for('my_alerts'))

    if not subs:
        flash('Deal alert not found or already cancelled.', 'error')
        return redirect(url_for('my_alerts'))

    sub = subs[0]

    try:
        supabase.table('alert_subscriptions') \
            .update({'active': False}) \
            .eq('id', alert_id) \
            .eq('user_id', user['id']) \
            .execute()
    except Exception as e:
        app.logger.error(f"Cancel alert update failed: {e}")
        flash('Could not cancel deal alert. Please try again.', 'error')
        return redirect(url_for('my_alerts'))

    car_label = f"{sub.get('make', '')} {sub.get('model', '')} {sub.get('variant', '')}".strip()
    role_label = 'Buyer' if sub.get('role') == 'buyer' else 'Seller'
    try:
        supabase.table('transactions').insert({
            'user_id': user['id'],
            'type': 'alert_cancelled',
            'amount': 0,
            'balance_after': user.get('credits', 0),
            'description': f'Cancelled {role_label.lower()} deal alert for {car_label} (no refund)'
        }).execute()
    except Exception as e:
        app.logger.warning(f"Log cancel transaction failed: {e}")

    session['active_alerts_count'] = count_active_alert_subscriptions(user['id'])

    flash(f'{role_label} deal alert for {car_label} cancelled. Slot freed (no credit refund).', 'success')
    return redirect(url_for('my_alerts'))


# ============================================================
# v3.5 SUBMIT DEAL — accepts city in form (state derived from rto_code as before)
# ============================================================

@app.route('/submit-deal', methods=['GET', 'POST'])
@login_required
@no_guest(message='Sign up to submit verified deals and earn 100 credits per deal.')
def submit_deal():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    is_admin = _is_admin_email(user.get('email'))

    def render_form(form_data, error='', weekly_count=None):
        return render_template(
            'submit_deal.html',
            user=user,
            first_name=firstname_filter(user.get('name')),
            form=form_data,
            error=error,
            makes=get_makes(),
            years=YEARS,
            owners=OWNERS,
            conditions=CONDITIONS,
            buyer_types=BUYER_TYPES,
            rto_states=RTO_STATES,
            car_data_json=json.dumps(CAR_DATA),
            weekly_count=weekly_count if weekly_count is not None else count_recent_deals(user['id'], 7),
            max_per_week=MAX_DEALS_PER_WEEK,
            deal_reward=DEAL_REWARD_AMOUNT,
            # v3.5.1: Transaction location dropdowns (independent of reg_state)
            cities_by_state_json=json.dumps(INDIAN_CITIES_BY_STATE),
            default_state=DEFAULT_STATE_CODE,
            default_city=DEFAULT_CITY,
        )

    if request.method == 'GET':
        prefill = {
            'make':              request.args.get('make', ''),
            'fuel':              request.args.get('fuel', ''),
            'model':             request.args.get('model', ''),
            'variant':           request.args.get('variant', ''),
            'year':              request.args.get('year', ''),
            'owner':             request.args.get('owner', ''),
            'mileage':           request.args.get('mileage', ''),
            'condition':         request.args.get('condition', ''),
            'buyer_type':        request.args.get('buyer_type', ''),
            'reg_state':         request.args.get('reg_state', ''),
            'reg_district':      request.args.get('reg_district', ''),
            'reg_series':        request.args.get('reg_series', ''),
            'reg_number':        request.args.get('reg_number', ''),
            'transaction_date':  request.args.get('transaction_date', ''),
            'asking_price':      request.args.get('asking_price', ''),
            'sale_price':        request.args.get('sale_price', ''),
            'has_proof':         request.args.get('has_proof', ''),
            # v3.5.1: user_state + user_city are INDEPENDENT of reg_state/reg_district.
            # Registration tells you where the car is registered; user_state/user_city
            # tells you where the transaction happened (a KA-registered car can be sold
            # in MH if the new buyer is in MH). Default to KA/Bangalore.
            'user_state':        request.args.get('user_state', '') or DEFAULT_STATE_CODE,
            'user_city':         request.args.get('user_city', '')  or DEFAULT_CITY,
            # v3.5 legacy 'city' param: kept for backwards compat (alias of user_city)
            'city':              request.args.get('city', ''),
        }
        return render_form(prefill)

    form_data = {
        'make':              (request.form.get('make') or '').strip(),
        'fuel':              (request.form.get('fuel') or '').strip(),
        'model':             (request.form.get('model') or '').strip(),
        'variant':           (request.form.get('variant') or '').strip(),
        'year':              (request.form.get('year') or '').strip(),
        'owner':             (request.form.get('owner') or '').strip(),
        'mileage':           (request.form.get('mileage') or '').strip(),
        'condition':         (request.form.get('condition') or '').strip(),
        'buyer_type':        (request.form.get('buyer_type') or '').strip(),
        'reg_state':         (request.form.get('reg_state') or '').strip().upper(),
        'reg_district':      (request.form.get('reg_district') or '').strip(),
        'reg_series':        (request.form.get('reg_series') or '').strip().upper(),
        'reg_number':        (request.form.get('reg_number') or '').strip(),
        'transaction_date':  (request.form.get('transaction_date') or '').strip(),
        'asking_price':      (request.form.get('asking_price') or '').strip(),
        'sale_price':        (request.form.get('sale_price') or '').strip(),
        'has_proof':         request.form.get('has_proof') == 'on',
        # v3.5.1: user_state + user_city are independent of reg_state/reg_district.
        # Default to KA/Bangalore when not provided.
        'user_state':        (request.form.get('user_state') or '').strip().upper() or DEFAULT_STATE_CODE,
        'user_city':         (request.form.get('user_city') or '').strip()         or DEFAULT_CITY,
        # v3.5 legacy 'city' field: still accepted (will be overwritten by user_city below).
        'city':              (request.form.get('city') or '').strip(),
    }

    weekly_count = count_recent_deals(user['id'], 7)
    if not is_admin and weekly_count >= MAX_DEALS_PER_WEEK:
        flash(
            f'You have already submitted {weekly_count} deals in the past 7 days. '
            f'Weekly limit is {MAX_DEALS_PER_WEEK}. Please try again next week.',
            'error'
        )
        return redirect(url_for('role'))

    required = ['make', 'fuel', 'model', 'variant', 'year', 'owner',
                'mileage', 'condition', 'buyer_type',
                'reg_state', 'reg_district', 'reg_series', 'reg_number',
                'transaction_date', 'sale_price']
    for key in required:
        if not form_data[key]:
            return render_form(form_data, error='Please fill in all required fields.')

    if form_data['make'] not in CAR_DATA:
        return render_form(form_data, error='Invalid Make selected.')
    if form_data['model'] not in CAR_DATA[form_data['make']]:
        return render_form(form_data, error='Invalid Model selected.')
    if form_data['variant'] not in CAR_DATA[form_data['make']][form_data['model']]['variants']:
        return render_form(form_data, error='Invalid Variant selected.')
    if form_data['fuel'] not in CAR_DATA[form_data['make']][form_data['model']]['fuels']:
        return render_form(form_data, error='Selected Fuel is not available for this Model.')
    if form_data['owner'] not in OWNERS:
        return render_form(form_data, error='Invalid Owner selected.')
    if form_data['condition'] not in CONDITIONS:
        return render_form(form_data, error='Invalid Condition selected.')
    if form_data['buyer_type'] not in BUYER_TYPES:
        return render_form(form_data, error='Invalid Transaction Type.')

    try:
        year_int = int(form_data['year'])
        if year_int < YEAR_START or year_int > YEAR_END:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Invalid Year.')

    try:
        mileage_int = int(form_data['mileage'])
        if mileage_int < 0 or mileage_int > 500000:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Mileage must be a number between 0 and 500000.')

    if form_data['reg_state'] not in RTO_STATES:
        return render_form(form_data, error='Invalid State RTO code.')
    if not RTO_DISTRICT_RE.match(form_data['reg_district']):
        return render_form(form_data, error='District code must be 2 digits (01-99).')
    if not REG_SERIES_RE.match(form_data['reg_series']):
        return render_form(form_data, error='Series must be 1-3 letters (e.g. MK, AB).')
    if not form_data['reg_series']:
        return render_form(form_data, error='Registration series is required.')
    if not REG_NUMBER_RE.match(form_data['reg_number']):
        return render_form(form_data, error='Registration number must be 1-4 digits.')

    rto_code_combined = form_data['reg_state'] + form_data['reg_district']

    # v3.5.1: Independent transaction location (per locked rule #7).
    # user_state + user_city tell us WHERE the transaction happened — a KA-
    # registered car can be sold in MH if the new buyer is in MH. The deals
    # table records both: rto_code (registration) AND user_state/user_city
    # (transaction location).
    user_state_norm = normalize_state_code(form_data['user_state'])
    user_city_norm  = normalize_city(form_data['user_city'], user_state_norm)
    form_data['user_state'] = user_state_norm
    form_data['user_city']  = user_city_norm

    # Legacy 'city' column on deals — keep populated for backwards compat
    # (older code paths may still query it). It always equals user_city now.
    deal_city = user_city_norm
    form_data['city'] = deal_city

    try:
        tx_date = datetime.strptime(form_data['transaction_date'], '%Y-%m-%d').date()
        today = datetime.utcnow().date()
        if tx_date > today:
            return render_form(form_data, error='Transaction date cannot be in the future.')
        if tx_date.year < 2010:
            return render_form(form_data, error='Transaction date must be on or after 2010.')
    except (ValueError, TypeError):
        return render_form(form_data, error='Invalid transaction date.')

    cleaned_sale = form_data['sale_price'].replace(',', '').replace('₹', '').strip()
    try:
        sale_price_int = int(cleaned_sale)
        if sale_price_int <= 0 or sale_price_int > 100000000:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Sale price must be a positive number up to ₹10 Crore.')

    asking_price_int = None
    if form_data['asking_price']:
        cleaned_ask = form_data['asking_price'].replace(',', '').replace('₹', '').strip()
        try:
            asking_price_int = int(cleaned_ask)
            if asking_price_int <= 0 or asking_price_int > 100000000:
                raise ValueError
        except (ValueError, TypeError):
            return render_form(form_data, error='Asking price, if provided, must be a positive number.')

    if form_data['buyer_type'] == 'Dealer' and not form_data['has_proof']:
        return render_form(form_data, error='Dealer transactions require proof of sale. Please confirm you have proof.')

    verified_flag = form_data['has_proof']

    payload = {
        'user_id':           user['id'],
        'make':              form_data['make'],
        'model':             form_data['model'],
        'variant':           form_data['variant'],
        'fuel':              form_data['fuel'],
        'year':              year_int,
        'mileage':           mileage_int,
        'condition':         form_data['condition'],
        'owner':             form_data['owner'],
        'buyer_type':        form_data['buyer_type'],
        'sale_price':        sale_price_int,
        'asking_price':      asking_price_int,
        'transaction_date':  tx_date.isoformat(),
        'rto_code':          rto_code_combined,
        'reg_series':        form_data['reg_series'],
        'reg_number':        form_data['reg_number'],
        'has_proof':         form_data['has_proof'],
        'verified':          verified_flag,
        'is_test_data':      is_admin,
        # v3.5: legacy city column (kept populated for backwards compat)
        'city':              deal_city,
        # v3.5.1: independent transaction location (per locked rule #7)
        'user_state':        user_state_norm,
        'user_city':         user_city_norm,
    }

    try:
        ins = supabase.table('deals').insert(payload).execute()
        deal_row = ins.data[0] if ins.data else None
    except Exception as e:
        app.logger.error(f"Deal insert failed: {e}")
        return render_form(form_data, error='Could not save your deal. Please try again.')

    if not deal_row:
        return render_form(form_data, error='Could not save your deal. Please try again.')

    try:
        dispatch_deal_alerts_async(supabase, deal_row, app_instance=app)
    except Exception as e:
        app.logger.warning(f"dispatch_deal_alerts_async raised at call site: {e}")

    current_credits = user.get('credits', 0) or 0
    new_balance = current_credits + DEAL_REWARD_AMOUNT
    try:
        update_user(user['id'], {'credits': new_balance})
        log_credit_transaction(
            user_id=user['id'],
            type_='deal_reward',
            description=f"Deal submitted: {year_int} {form_data['make']} {form_data['model']} {form_data['variant']} ({form_data['buyer_type']})",
            amount=DEAL_REWARD_AMOUNT,
            balance_after=new_balance,
        )
        session['credits'] = new_balance
        if 'user' in session:
            session['user']['credits'] = new_balance
    except Exception as e:
        app.logger.error(f"Deal reward credit award failed: {e}")

    verified_msg = '' if verified_flag else ' Flagged as unverified — thanks for contributing!'
    test_msg = ' [TEST DATA — excluded from public market stats]' if is_admin else ''
    flash(
        f"Deal recorded! +{DEAL_REWARD_AMOUNT} credits awarded. New balance: {new_balance} credits.{verified_msg}{test_msg}",
        'success'
    )
    return redirect(url_for('role'))

# ============================================================
# END app.py — Part 3
# Continue with Part 4 immediately below.
# ============================================================
# ============================================================
# app.py — Part 4
# ------------------------------------------------------------
# Paste this BLOCK immediately below the last line of Part 3.
# Continue pasting Part 5 immediately below the last line of this block.
# ============================================================


# ============================================================
# CREDIT HISTORY (preserved from current — no v3.5 changes)
# ============================================================

@app.route('/credit-history')
@login_required
@no_guest(message='Sign up to view credit history.')
def credit_history():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    filter_type = request.args.get('type', 'all').strip()
    try:
        page = max(1, int(request.args.get('page', 1)))
    except (ValueError, TypeError):
        page = 1
    per_page = 20

    try:
        all_txns_resp = (
            supabase.table('transactions')
            .select('*')
            .eq('user_id', user['id'])
            .order('created_at', desc=False)
            .execute()
        )
        all_txns = all_txns_resp.data or []
    except Exception as e:
        app.logger.error(f"credit_history load failed: {e}")
        all_txns = []

    running = 0
    for t in all_txns:
        amt = int(t.get('amount') or 0)
        running += amt
        ba = t.get('balance_after')
        t['balance_after_display'] = int(ba) if ba is not None else running

    all_txns.reverse()

    if filter_type != 'all':
        filtered = [t for t in all_txns if t.get('type') == filter_type]
    else:
        filtered = all_txns

    total = len(filtered)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = min(page, total_pages)
    start = (page - 1) * per_page
    end = start + per_page
    page_txns = filtered[start:end]

    for t in page_txns:
        t['date_display'] = _format_txn_date(t.get('created_at'))
        t['type_label'] = TRANSACTION_TYPE_LABELS.get(t.get('type'), t.get('type', '—'))
        amt = int(t.get('amount') or 0)
        if amt > 0:
            t['amount_display'] = f'+{amt:,}'
            t['amount_class'] = 'credit-in'
        elif amt < 0:
            t['amount_display'] = f'{amt:,}'
            t['amount_class'] = 'credit-out'
        else:
            t['amount_display'] = '0'
            t['amount_class'] = 'credit-zero'

    type_options = sorted(
        [{'value': k, 'label': v} for k, v in TRANSACTION_TYPE_LABELS.items()],
        key=lambda x: x['label']
    )

    return render_template(
        'credit_history.html',
        user=user,
        first_name=firstname_filter(user.get('name')),
        txns=page_txns,
        page=page,
        total_pages=total_pages,
        total=total,
        per_page=per_page,
        filter_type=filter_type,
        type_options=type_options,
        current_credits=user.get('credits', 0),
    )


@app.route('/credit-history/export')
@login_required
@no_guest(message='Sign up to export credit history.')
def credit_history_export():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    filter_type = request.args.get('type', 'all').strip()

    try:
        all_txns_resp = (
            supabase.table('transactions')
            .select('*')
            .eq('user_id', user['id'])
            .order('created_at', desc=False)
            .execute()
        )
        all_txns = all_txns_resp.data or []
    except Exception as e:
        app.logger.error(f"credit_history_export load failed: {e}")
        all_txns = []

    running = 0
    for t in all_txns:
        amt = int(t.get('amount') or 0)
        running += amt
        ba = t.get('balance_after')
        t['balance_after_display'] = int(ba) if ba is not None else running

    all_txns.reverse()

    if filter_type != 'all':
        all_txns = [t for t in all_txns if t.get('type') == filter_type]

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Date', 'Type', 'Description', 'Amount', 'Balance After'])

    for t in all_txns:
        writer.writerow([
            _format_txn_date(t.get('created_at')),
            TRANSACTION_TYPE_LABELS.get(t.get('type'), t.get('type', '')),
            t.get('description', '') or '',
            t.get('amount', 0),
            t.get('balance_after_display', 0),
        ])

    csv_data = output.getvalue()
    output.close()

    filename = f"autoknowmus_credits_{datetime.utcnow().strftime('%d-%b-%Y')}.csv"

    return Response(
        csv_data,
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'},
    )


# ============================================================
# INTERNAL CRON ENDPOINTS (preserved from current)
# ============================================================

@app.route('/internal/send-weekly-digest', methods=['GET', 'POST'])
def internal_send_weekly_digest():
    provided_token = request.args.get('token') or request.headers.get('X-Dispatch-Token') or ''

    if not ALERT_DISPATCH_TOKEN:
        app.logger.error("internal_send_weekly_digest: ALERT_DISPATCH_TOKEN not configured")
        return jsonify({"ok": False, "error": "server_not_configured"}), 503

    if provided_token != ALERT_DISPATCH_TOKEN:
        app.logger.warning("internal_send_weekly_digest: invalid token attempt")
        return jsonify({"ok": False, "error": "invalid_token"}), 403

    try:
        counts = send_weekly_digest(supabase)
        app.logger.info(f"Weekly digest dispatched: {counts}")
        return jsonify({"ok": True, "counts": counts}), 200
    except Exception as e:
        app.logger.exception("Weekly digest crashed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/internal/cleanup-magic-links', methods=['GET', 'POST'])
def internal_cleanup_magic_links():
    provided_token = request.args.get('token') or request.headers.get('X-Dispatch-Token') or ''

    if not ALERT_DISPATCH_TOKEN:
        app.logger.error("internal_cleanup_magic_links: ALERT_DISPATCH_TOKEN not configured")
        return jsonify({"ok": False, "error": "server_not_configured"}), 503

    if provided_token != ALERT_DISPATCH_TOKEN:
        app.logger.warning("internal_cleanup_magic_links: invalid token attempt")
        return jsonify({"ok": False, "error": "invalid_token"}), 403

    deleted = cleanup_expired_magic_links()
    if deleted < 0:
        return jsonify({"ok": False, "error": "cleanup_failed"}), 500

    app.logger.info(f"Magic-link cleanup: {deleted} tokens deleted")
    return jsonify({"ok": True, "deleted": deleted}), 200


# ============================================================
# ADMIN — TEST EMAIL ENDPOINTS (preserved from current)
# ============================================================

@app.route('/admin/test-email-buyer-alert')
@login_required
@admin_required
def admin_test_buyer_alert():
    user = current_user()
    if not user:
        return jsonify({"ok": False, "error": "no_user"}), 401

    try:
        from alert_dispatcher import send_test_buyer_alert
    except ImportError:
        return jsonify({
            "ok": False,
            "error": "dispatcher_not_updated",
            "hint": "alert_dispatcher.py needs to be updated to v2 (Batch 3) before this works."
        }), 503

    try:
        result = send_test_buyer_alert(supabase, user, app_instance=app)
        return jsonify({"ok": True, "result": result}), 200
    except AttributeError as e:
        return jsonify({
            "ok": False,
            "error": "dispatcher_missing_function",
            "details": str(e),
        }), 503
    except Exception as e:
        app.logger.exception("admin_test_buyer_alert failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/admin/test-email-seller-alert')
@login_required
@admin_required
def admin_test_seller_alert():
    user = current_user()
    if not user:
        return jsonify({"ok": False, "error": "no_user"}), 401

    try:
        from alert_dispatcher import send_test_seller_alert
    except ImportError:
        return jsonify({
            "ok": False,
            "error": "dispatcher_not_updated",
            "hint": "alert_dispatcher.py needs to be updated to v2 (Batch 3) before this works."
        }), 503

    try:
        result = send_test_seller_alert(supabase, user, app_instance=app)
        return jsonify({"ok": True, "result": result}), 200
    except AttributeError as e:
        return jsonify({"ok": False, "error": "dispatcher_missing_function", "details": str(e)}), 503
    except Exception as e:
        app.logger.exception("admin_test_seller_alert failed")
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/admin/test-email-digest')
@login_required
@admin_required
def admin_test_digest():
    user = current_user()
    if not user:
        return jsonify({"ok": False, "error": "no_user"}), 401

    try:
        from alert_dispatcher import send_test_digest
    except ImportError:
        return jsonify({
            "ok": False,
            "error": "dispatcher_not_updated",
            "hint": "alert_dispatcher.py needs to be updated to v2 (Batch 3) before this works."
        }), 503

    try:
        result = send_test_digest(supabase, user, app_instance=app)
        return jsonify({"ok": True, "result": result}), 200
    except AttributeError as e:
        return jsonify({"ok": False, "error": "dispatcher_missing_function", "details": str(e)}), 503
    except Exception as e:
        app.logger.exception("admin_test_digest failed")
        return jsonify({"ok": False, "error": str(e)}), 500


# ============================================================
# ADMIN — DATA HEALTH DASHBOARD (preserved from current)
# ============================================================

def _fetch_all_deals_180d():
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).isoformat()
        r = (supabase.table('deals')
             .select('id, user_id, make, model, variant, sale_price, created_at, verified')
             .eq('verified', True)
             .eq('is_test_data', False)
             .gte('created_at', cutoff)
             .execute())
        return r.data or []
    except Exception as e:
        app.logger.error(f"admin _fetch_all_deals_180d failed: {e}")
        return []


def _fetch_all_deals_30d():
    try:
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        r = (supabase.table('deals')
             .select('id', count='exact')
             .eq('verified', True)
             .eq('is_test_data', False)
             .gte('created_at', cutoff)
             .execute())
        return r.count or 0
    except Exception as e:
        app.logger.error(f"admin _fetch_all_deals_30d failed: {e}")
        return 0


def _fetch_all_deals_30_to_60d():
    try:
        cutoff_start = (datetime.utcnow() - timedelta(days=60)).isoformat()
        cutoff_end = (datetime.utcnow() - timedelta(days=30)).isoformat()
        r = (supabase.table('deals')
             .select('id', count='exact')
             .eq('verified', True)
             .eq('is_test_data', False)
             .gte('created_at', cutoff_start)
             .lt('created_at', cutoff_end)
             .execute())
        return r.count or 0
    except Exception as e:
        app.logger.error(f"admin _fetch_all_deals_30_to_60d failed: {e}")
        return 0


def _compute_phase_distribution(deals_180d):
    by_model = defaultdict(list)
    for d in deals_180d:
        key = (d.get('make'), d.get('model'))
        if key[0] and key[1]:
            by_model[key].append(d)

    model_phases = []
    for make in sorted(CAR_DATA.keys()):
        for model in sorted(CAR_DATA[make].keys()):
            key = (make, model)
            deals = by_model.get(key, [])
            deal_count = len(deals)
            distinct_users = len({d['user_id'] for d in deals if d.get('user_id')})
            phase = determine_phase(deal_count, distinct_users)
            model_phases.append({
                'make': make,
                'model': model,
                'phase': phase,
                'deal_count': deal_count,
                'distinct_users': distinct_users,
            })
    return model_phases


def _compute_upgrade_queue(model_phases):
    queue = []
    for mp in model_phases:
        current = mp['phase']
        if current >= 4:
            continue
        next_phase = current + 1
        deals_needed = PHASE_THRESHOLDS[next_phase][0] - mp['deal_count']
        users_needed = PHASE_THRESHOLDS[next_phase][1] - mp['distinct_users']
        if deals_needed <= 10 and mp['deal_count'] > 0:
            queue.append({
                **mp,
                'next_phase': next_phase,
                'deals_needed': max(0, deals_needed),
                'users_needed': max(0, users_needed),
                'threshold_deals': PHASE_THRESHOLDS[next_phase][0],
                'threshold_users': PHASE_THRESHOLDS[next_phase][1],
            })
    queue.sort(key=lambda x: (x['deals_needed'], -x['deal_count']))
    return queue[:15]


def _compute_guardrail_flags(deals_180d, model_phases):
    flags = []
    by_model = defaultdict(list)
    for d in deals_180d:
        key = (d.get('make'), d.get('model'))
        if key[0] and key[1] and d.get('sale_price'):
            by_model[key].append(d.get('sale_price'))

    for mp in model_phases:
        if mp['phase'] < 2:
            continue
        key = (mp['make'], mp['model'])
        prices = sorted(by_model.get(key, []))
        if len(prices) < 5:
            continue
        n = len(prices)
        median = prices[n // 2] if n % 2 == 1 else (prices[n // 2 - 1] + prices[n // 2]) // 2

        anchor = get_base_price(mp['make'], mp['model'])
        if not anchor:
            continue

        anchor_aged = anchor * 0.77
        deviation = (median - anchor_aged) / anchor_aged

        if abs(deviation) > 0.15:
            flags.append({
                'make': mp['make'],
                'model': mp['model'],
                'deal_count': len(prices),
                'median_price': int(median),
                'expected_price': int(anchor_aged),
                'deviation_pct': round(deviation * 100, 1),
            })

    flags.sort(key=lambda x: -abs(x['deviation_pct']))
    return flags[:10]


def _compute_broker_signals(lookback_days=60):
    try:
        cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()
        r = (supabase.table('deals')
             .select('id, user_id, make, model, sale_price, created_at')
             .eq('is_test_data', False)
             .gte('created_at', cutoff)
             .execute())
        deals = r.data or []
    except Exception as e:
        app.logger.error(f"admin broker signals fetch failed: {e}")
        return []

    by_user = defaultdict(list)
    for d in deals:
        if d.get('user_id'):
            by_user[d['user_id']].append(d)

    signals = []
    for uid, user_deals in by_user.items():
        total = len(user_deals)
        if total < 3:
            continue

        flags = []

        model_counts = Counter((d['make'], d['model']) for d in user_deals)
        top_model, top_count = model_counts.most_common(1)[0] if model_counts else (None, 0)
        if top_count >= 5:
            flags.append(f"Clustered: {top_count} deals of {top_model[0]} {top_model[1]}")

        prices = [d.get('sale_price', 0) for d in user_deals if d.get('sale_price')]
        if prices:
            mn, mx = min(prices), max(prices)
            if mn > 0 and mx > 0 and mx / mn >= 10:
                flags.append(f"Mixed segments: ₹{mn:,} to ₹{mx:,}")

        if total >= 6:
            flags.append(f"{total} deals in {lookback_days} days (cap-hitter)")

        if not flags:
            continue

        user_info = get_user_by_id(uid)
        if not user_info:
            continue

        signals.append({
            'user_id': uid,
            'name': user_info.get('name', '—'),
            'email': user_info.get('email', '—'),
            'deal_count': total,
            'flags': flags,
        })

    signals.sort(key=lambda x: (-len(x['flags']), -x['deal_count']))
    return signals[:20]


@app.route('/admin/data-health')
@login_required
@admin_required
def admin_data_health():
    user = current_user()

    deals_180d = _fetch_all_deals_180d()
    deals_30d_count = _fetch_all_deals_30d()
    deals_30_60d_count = _fetch_all_deals_30_to_60d()

    if deals_30_60d_count > 0:
        trend_pct = round(((deals_30d_count - deals_30_60d_count) / deals_30_60d_count) * 100, 1)
    else:
        trend_pct = None

    total_verified_180d = len(deals_180d)
    unique_submitters_180d = len({d['user_id'] for d in deals_180d if d.get('user_id')})

    model_phases = _compute_phase_distribution(deals_180d)

    phase_counts = Counter(mp['phase'] for mp in model_phases)
    total_models = len(model_phases)

    phase_distribution = []
    for p in [4, 3, 2, 1]:
        count = phase_counts.get(p, 0)
        pct = round((count / total_models) * 100, 1) if total_models else 0
        phase_distribution.append({
            'phase': p,
            'label': PHASE_BLEND[p]['badge'],
            'count': count,
            'pct': pct,
        })

    top_models = sorted(
        [mp for mp in model_phases if mp['deal_count'] > 0],
        key=lambda x: -x['deal_count']
    )[:20]

    upgrade_queue = _compute_upgrade_queue(model_phases)
    guardrail_flags = _compute_guardrail_flags(deals_180d, model_phases)
    broker_signals = _compute_broker_signals()

    try:
        cutoff_90d = (datetime.utcnow() - timedelta(days=90)).isoformat()
        r = (supabase.table('deals')
             .select('make, model')
             .eq('verified', True)
             .eq('is_test_data', False)
             .gte('created_at', cutoff_90d)
             .execute())
        recent_model_keys = {(d.get('make'), d.get('model')) for d in (r.data or [])}
    except Exception:
        recent_model_keys = set()

    stale_count = sum(
        1 for make in CAR_DATA
        for model in CAR_DATA[make]
        if (make, model) not in recent_model_keys
    )

    try:
        cache_status = get_cache_status()
    except Exception as e:
        app.logger.warning(f"get_cache_status failed: {e}")
        cache_status = {
            'source': 'unknown', 'last_fetch_age_seconds': 0, 'last_error': str(e),
            'cache_ttl': 3600, 'makes': 0, 'data_version': '?', 'last_updated': '?',
        }

    return render_template(
        'admin_data_health.html',
        user=user,
        first_name=firstname_filter(user.get('name')),
        total_models=total_models,
        phase_distribution=phase_distribution,
        total_verified_180d=total_verified_180d,
        deals_30d_count=deals_30d_count,
        trend_pct=trend_pct,
        unique_submitters_180d=unique_submitters_180d,
        top_models=top_models,
        upgrade_queue=upgrade_queue,
        guardrail_flags=guardrail_flags,
        broker_signals=broker_signals,
        stale_count=stale_count,
        data_version=_live_data_version(),
        last_updated=_live_last_updated(),
        cache_status=cache_status,
        now_display=datetime.utcnow().strftime('%d-%b-%Y %H:%M UTC'),
    )


@app.route('/admin/flag-user/<user_id>', methods=['POST'])
@login_required
@admin_required
def admin_flag_user(user_id):
    admin = current_user()
    target = get_user_by_id(user_id)
    if not target:
        flash('User not found.', 'error')
        return redirect(url_for('admin_data_health'))

    try:
        supabase.table('transactions').insert({
            'user_id': user_id,
            'type': 'alert_cancelled',
            'amount': 0,
            'balance_after': target.get('credits', 0),
            'description': f'[ADMIN FLAG] Suspected broker — flagged by {admin.get("email")}',
        }).execute()
        flash(f'User {target.get("email")} flagged for review. (Not blocked — this is a soft flag.)', 'success')
    except Exception as e:
        app.logger.error(f"admin_flag_user failed: {e}")
        flash('Could not flag user. Please try again.', 'error')

    return redirect(url_for('admin_data_health'))


# ============================================================
# END app.py — Part 4
# Continue with Part 5 immediately below.
# ============================================================
# ============================================================
# app.py — Part 5
# ------------------------------------------------------------
# Paste this BLOCK immediately below the last line of Part 4.
# Continue pasting Part 6 immediately below the last line of this block.
#
# v3.6.4 update to /admin/test-sheets-connection: adds a hard
# wall-clock timeout via SIGALRM around the health_check() call,
# plus step-by-step logging so we can see exactly where any future
# hang is happening — even if google-auth's own internal timeouts
# fail to fire. Other routes in this Part are unchanged from prior.
# ============================================================


# ============================================================
# v2.9: ADMIN — USER ACTIVITY DASHBOARD (preserved from current)
# ============================================================

def _format_admin_relative_date(dt_or_str):
    if not dt_or_str:
        return '—'
    try:
        if isinstance(dt_or_str, str):
            clean = dt_or_str.replace('Z', '').split('+')[0].split('.')[0]
            dt = datetime.fromisoformat(clean)
        else:
            dt = dt_or_str
        delta = datetime.utcnow() - dt
        days = delta.days
        if days == 0:
            rel = 'today'
        elif days == 1:
            rel = '1d ago'
        elif days < 30:
            rel = f'{days}d ago'
        elif days < 365:
            rel = f'{days // 30}mo ago'
        else:
            rel = f'{days // 365}y ago'
        return f"{dt.strftime('%d-%b-%y')} ({rel})"
    except (ValueError, AttributeError, TypeError):
        return str(dt_or_str)


def _compute_user_activity_stats():
    now = datetime.utcnow()
    cutoff_1d = (now - timedelta(days=1)).isoformat()
    cutoff_3d = (now - timedelta(days=3)).isoformat()
    cutoff_7d = (now - timedelta(days=7)).isoformat()
    cutoff_30d = (now - timedelta(days=30)).isoformat()

    stats = {
        'stats_1d': {'signups': 0, 'searches': 0, 'deals': 0, 'alert_subs': 0},
        'stats_3d': {'signups': 0, 'searches': 0, 'deals': 0, 'alert_subs': 0},
        'stats_7d': {'signups': 0, 'searches': 0, 'deals': 0, 'alert_subs': 0},
        'stats_30d': {'signups': 0, 'searches': 0, 'deals': 0, 'alert_subs': 0},
        'stats_all': {'signups': 0, 'searches': 0, 'deals': 0, 'alert_subs': 0},
    }

    windows = [
        ('stats_1d', cutoff_1d),
        ('stats_3d', cutoff_3d),
        ('stats_7d', cutoff_7d),
        ('stats_30d', cutoff_30d),
    ]

    try:
        for window_key, cutoff in windows:
            r = (supabase.table('users').select('id', count='exact').gte('created_at', cutoff).execute())
            stats[window_key]['signups'] = r.count or 0
        r_all = supabase.table('users').select('id', count='exact').execute()
        stats['stats_all']['signups'] = r_all.count or 0
    except Exception as e:
        app.logger.warning(f"user activity signups query failed: {e}")

    try:
        for window_key, cutoff in windows:
            r = (supabase.table('valuations').select('id', count='exact').gte('created_at', cutoff).execute())
            stats[window_key]['searches'] += r.count or 0
        r_all = supabase.table('valuations').select('id', count='exact').execute()
        stats['stats_all']['searches'] += r_all.count or 0
    except Exception as e:
        app.logger.warning(f"user activity valuations query failed: {e}")

    try:
        for window_key, cutoff in windows:
            r = (supabase.table('transactions').select('id', count='exact').eq('type', 'buyer_search').gte('created_at', cutoff).execute())
            stats[window_key]['searches'] += r.count or 0
        r_all = supabase.table('transactions').select('id', count='exact').eq('type', 'buyer_search').execute()
        stats['stats_all']['searches'] += r_all.count or 0
    except Exception as e:
        app.logger.warning(f"user activity buyer_search query failed: {e}")

    try:
        for window_key, cutoff in windows:
            r = (supabase.table('deals').select('id', count='exact').gte('created_at', cutoff).execute())
            stats[window_key]['deals'] = r.count or 0
        r_all = supabase.table('deals').select('id', count='exact').execute()
        stats['stats_all']['deals'] = r_all.count or 0
    except Exception as e:
        app.logger.warning(f"user activity deals query failed: {e}")

    try:
        for window_key, cutoff in windows:
            r = (supabase.table('alert_subscriptions').select('id', count='exact').gte('created_at', cutoff).execute())
            stats[window_key]['alert_subs'] = r.count or 0
        r_all = supabase.table('alert_subscriptions').select('id', count='exact').execute()
        stats['stats_all']['alert_subs'] = r_all.count or 0
    except Exception as e:
        app.logger.warning(f"user activity alert_subs query failed: {e}")

    return stats


def _compute_user_activity_rows():
    now = datetime.utcnow()

    try:
        r = (supabase.table('users')
             .select('id, name, email, phone, credits, created_at, last_login_at, auth_method')
             .order('created_at', desc=True)
             .execute())
        users = r.data or []
    except Exception as e:
        app.logger.error(f"user activity fetch users failed: {e}")
        return []

    try:
        r = (supabase.table('valuations')
             .select('user_id, make, model, variant, fuel, year, created_at')
             .order('created_at', desc=True)
             .execute())
        all_valuations = r.data or []
    except Exception as e:
        app.logger.warning(f"user activity fetch valuations failed: {e}")
        all_valuations = []

    valuations_by_user = defaultdict(list)
    for v in all_valuations:
        if v.get('user_id') is not None:
            valuations_by_user[v['user_id']].append(v)

    try:
        r = (supabase.table('transactions')
             .select('user_id, description, created_at')
             .eq('type', 'buyer_search')
             .order('created_at', desc=True)
             .execute())
        all_buyer_searches = r.data or []
    except Exception as e:
        app.logger.warning(f"user activity fetch buyer_search failed: {e}")
        all_buyer_searches = []

    buyer_searches_by_user = defaultdict(list)
    for t in all_buyer_searches:
        if t.get('user_id') is not None:
            buyer_searches_by_user[t['user_id']].append(t)

    try:
        r = (supabase.table('deals').select('user_id').execute())
        deals = r.data or []
    except Exception as e:
        app.logger.warning(f"user activity fetch deals failed: {e}")
        deals = []

    deals_count_by_user = Counter(d['user_id'] for d in deals if d.get('user_id') is not None)

    try:
        r = (supabase.table('alert_subscriptions')
             .select('user_id')
             .eq('active', True)
             .gt('expires_at', now.isoformat())
             .execute())
        alerts = r.data or []
    except Exception as e:
        app.logger.warning(f"user activity fetch alerts failed: {e}")
        alerts = []

    alerts_count_by_user = Counter(a['user_id'] for a in alerts if a.get('user_id') is not None)

    rows = []
    cutoff_30d = now - timedelta(days=30)

    for u in users:
        uid = u.get('id')
        seller_searches = len(valuations_by_user.get(uid, []))
        buyer_searches = len(buyer_searches_by_user.get(uid, []))
        deals_count = deals_count_by_user.get(uid, 0)
        active_alerts = alerts_count_by_user.get(uid, 0)
        total_searches = seller_searches + buyer_searches

        is_dead_cohort = total_searches == 0
        is_active_cohort = total_searches >= 1
        is_power_cohort = (deals_count >= 1) or (active_alerts >= 1)

        days_since_signup = 0
        try:
            cre = (u.get('created_at') or '').replace('Z', '').split('+')[0].split('.')[0]
            if cre:
                cre_dt = datetime.fromisoformat(cre)
                days_since_signup = (now - cre_dt).days
        except (ValueError, AttributeError, TypeError):
            pass

        is_inactive_30d = False
        try:
            last = (u.get('last_login_at') or '').replace('Z', '').split('+')[0].split('.')[0]
            if last:
                last_dt = datetime.fromisoformat(last)
                if last_dt < cutoff_30d:
                    is_inactive_30d = True
        except (ValueError, AttributeError, TypeError):
            pass

        last_search_label = None
        last_search_date = None
        last_search_dt = None

        seller_recent = valuations_by_user.get(uid, [])
        if seller_recent:
            v = seller_recent[0]
            try:
                s_str = (v.get('created_at') or '').replace('Z', '').split('+')[0].split('.')[0]
                s_dt = datetime.fromisoformat(s_str) if s_str else None
                if s_dt and (last_search_dt is None or s_dt > last_search_dt):
                    last_search_dt = s_dt
                    last_search_label = f"{v.get('make','')} {v.get('model','')} {v.get('variant','')} ({v.get('fuel','')})"
            except (ValueError, AttributeError, TypeError):
                pass

        buyer_recent = buyer_searches_by_user.get(uid, [])
        if buyer_recent:
            t = buyer_recent[0]
            try:
                t_str = (t.get('created_at') or '').replace('Z', '').split('+')[0].split('.')[0]
                t_dt = datetime.fromisoformat(t_str) if t_str else None
                if t_dt and (last_search_dt is None or t_dt > last_search_dt):
                    last_search_dt = t_dt
                    desc = t.get('description', '') or ''
                    label = desc.replace('Buyer search:', '').strip() or '(buyer search)'
                    last_search_label = label
            except (ValueError, AttributeError, TypeError):
                pass

        if last_search_dt:
            last_search_date = last_search_dt.strftime('%d-%b-%y')

        rows.append({
            'id': uid,
            'name': u.get('name'),
            'email': u.get('email'),
            'phone': u.get('phone'),
            'credits': u.get('credits', 0),
            'auth_method': u.get('auth_method'),
            'is_admin': _is_admin_email(u.get('email')),
            'signup_display': _format_admin_relative_date(u.get('created_at')),
            'last_login_display': _format_admin_relative_date(u.get('last_login_at')) if u.get('last_login_at') else 'never',
            'seller_searches': seller_searches,
            'buyer_searches': buyer_searches,
            'deals_submitted': deals_count,
            'active_alerts': active_alerts,
            'total_searches': total_searches,
            'is_dead_cohort': is_dead_cohort,
            'is_active_cohort': is_active_cohort,
            'is_power_cohort': is_power_cohort,
            'is_inactive_30d': is_inactive_30d,
            'days_since_signup': days_since_signup,
            'last_search_label': last_search_label,
            'last_search_date': last_search_date,
            '_signup_dt': u.get('created_at') or '',
            '_last_login_dt': u.get('last_login_at') or '',
        })

    return rows


@app.route('/admin/user-activity')
@login_required
@admin_required
def admin_user_activity():
    user = current_user()

    active_filter = (request.args.get('f') or 'all').strip().lower()
    if active_filter not in ('all', 'active', 'dead', 'power'):
        active_filter = 'all'

    active_sort = (request.args.get('sort') or 'recent').strip().lower()
    if active_sort not in ('recent', 'last_login', 'most_active'):
        active_sort = 'recent'

    stats = _compute_user_activity_stats()

    all_rows = _compute_user_activity_rows()
    total_users = len(all_rows)

    cohort_active = sum(1 for r in all_rows if r['is_active_cohort'])
    cohort_dead = sum(1 for r in all_rows if r['is_dead_cohort'])
    cohort_power = sum(1 for r in all_rows if r['is_power_cohort'])

    def _pct(n, total):
        if total <= 0:
            return 0
        return round((n / total) * 100, 1)

    cohort_active_pct = _pct(cohort_active, total_users)
    cohort_dead_pct = _pct(cohort_dead, total_users)
    cohort_power_pct = _pct(cohort_power, total_users)

    if active_filter == 'active':
        rows = [r for r in all_rows if r['is_active_cohort']]
    elif active_filter == 'dead':
        rows = [r for r in all_rows if r['is_dead_cohort']]
    elif active_filter == 'power':
        rows = [r for r in all_rows if r['is_power_cohort']]
    else:
        rows = list(all_rows)

    if active_sort == 'last_login':
        rows.sort(key=lambda r: r['_last_login_dt'] or '', reverse=True)
    elif active_sort == 'most_active':
        rows.sort(key=lambda r: (r['total_searches'] + r['deals_submitted'] + r['active_alerts']), reverse=True)
    else:
        rows.sort(key=lambda r: r['_signup_dt'] or '', reverse=True)

    return render_template(
        'admin_user_activity.html',
        user=user,
        first_name=firstname_filter(user.get('name')),
        now_display=datetime.utcnow().strftime('%d-%b-%Y %H:%M UTC'),
        stats_1d=stats['stats_1d'],
        stats_3d=stats['stats_3d'],
        stats_7d=stats['stats_7d'],
        stats_30d=stats['stats_30d'],
        stats_all=stats['stats_all'],
        users=rows,
        total_users=total_users,
        cohort_active=cohort_active,
        cohort_dead=cohort_dead,
        cohort_power=cohort_power,
        cohort_active_pct=cohort_active_pct,
        cohort_dead_pct=cohort_dead_pct,
        cohort_power_pct=cohort_power_pct,
        active_filter=active_filter,
        active_sort=active_sort,
    )


@app.route('/admin/refresh-prices', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_refresh_prices():
    try:
        result = refresh_prices(force=True)
        refresh_module_constants()
    except Exception as e:
        app.logger.error(f"admin_refresh_prices failed: {e}")
        flash(f'Refresh failed: {e}', 'error')
        return redirect(url_for('admin_data_health'))

    status = result.get('status', '?')
    source = result.get('source', '?')
    errors = result.get('errors')
    makes = result.get('makes', 0)
    variants = result.get('variants', 0)
    version = result.get('data_version', '?')
    overrides = result.get('sheet_overrides', 0)

    if status == 'refreshed':
        flash(
            f'✅ Prices refreshed from Google Sheets. Source: {source}. '
            f'{makes} makes, {variants} variants loaded. '
            f'Data version: {version}. Sheet overrides: {overrides}.',
            'success'
        )
    elif status == 'partial_error':
        flash(
            f'⚠️ Partial refresh (some tabs failed). Source: {source}. '
            f'Loaded {makes} makes, {variants} variants. Errors: {errors}',
            'error'
        )
    elif status == 'cache_hit':
        flash(
            f'ℹ️ Cache still fresh (age: {result.get("age_seconds")}s). '
            f'Source: {source}. No reload was needed.',
            'success'
        )
    else:
        flash(f'Refresh returned status: {status}', 'success')

    return redirect(url_for('admin_data_health'))


# ============================================================
# v3.6.4: SHEETS WRITER TEST ENDPOINT  ← UPDATED IN THIS RELEASE
# ------------------------------------------------------------
# Runs sheets_writer.health_check() under a wall-clock SIGALRM
# timeout so the route ALWAYS returns within 15 seconds, no matter
# what's happening inside google-auth or gspread. Also logs each
# step so the next failure (if any) is fully traceable from the
# Render runtime log.
#
# Why SIGALRM: gspread/google-auth's own timeouts have proven
# unreliable on Render's network (multiple network layers each
# with their own buffers). A POSIX signal-based timeout is the
# nuclear option — it works at the kernel level and CAN'T be
# silently ignored by an HTTP library.
# ============================================================

# ============================================================
# DIAGNOSTIC ENDPOINT — paste this anywhere in app.py for now
# (e.g. right above the existing admin_test_sheets_connection)
# ------------------------------------------------------------
# Tests Render's egress to Google's APIs at every layer:
#   1. DNS resolution
#   2. Raw TCP connect
#   3. TLS handshake
#   4. HTTPS GET (no auth)
#   5. POST to oauth2.googleapis.com/token (the actual call that hangs)
#
# This bypasses google-auth and gspread entirely. If THIS endpoint
# also hangs, we know it's a Render egress problem. If THIS works
# fine, we know the hang is specifically inside google-auth.
#
# After diagnosis, you can delete this entire route from app.py.
# ============================================================

@app.route('/admin/diag-egress')
@login_required
@admin_required
def admin_diag_egress():
    """Run raw network tests against Google APIs from Render."""
    import socket
    import ssl
    import time
    import urllib.request
    import urllib.error
    import json as json_mod

    results = []

    def step(name, fn):
        start = time.time()
        try:
            fn_result = fn()
            elapsed = time.time() - start
            results.append({
                "step": name,
                "ok": True,
                "elapsed_s": round(elapsed, 3),
                "detail": fn_result,
            })
        except Exception as e:
            elapsed = time.time() - start
            results.append({
                "step": name,
                "ok": False,
                "elapsed_s": round(elapsed, 3),
                "error": f"{type(e).__name__}: {str(e)[:200]}",
            })

    # ---- Step 1: DNS resolution ----
    def _dns():
        ips = socket.gethostbyname_ex('oauth2.googleapis.com')
        return {"hostname": ips[0], "addresses": ips[2][:3]}
    step("dns_resolve_oauth2", _dns)

    # ---- Step 2: raw TCP connect (5s timeout) ----
    def _tcp():
        sock = socket.create_connection(('oauth2.googleapis.com', 443), timeout=5)
        sock.close()
        return "connected"
    step("tcp_connect_oauth2_443", _tcp)

    # ---- Step 3: TLS handshake (5s timeout) ----
    def _tls():
        ctx = ssl.create_default_context()
        with socket.create_connection(('oauth2.googleapis.com', 443), timeout=5) as raw:
            with ctx.wrap_socket(raw, server_hostname='oauth2.googleapis.com') as tls:
                cert = tls.getpeercert()
                return {
                    "tls_version": tls.version(),
                    "cipher": tls.cipher()[0] if tls.cipher() else None,
                    "cert_subject_cn": next(
                        (v for k, v in cert.get('subject', [[]])[0] if k == 'commonName'),
                        None
                    ) if cert.get('subject') else None,
                }
    step("tls_handshake_oauth2", _tls)

    # ---- Step 4: HTTPS GET to oauth2 endpoint ----
    def _https_get():
        # urllib uses its own timeout. We expect 4xx because GET isn't supported,
        # but ANY HTTP response at all proves egress works.
        req = urllib.request.Request('https://oauth2.googleapis.com/token')
        try:
            resp = urllib.request.urlopen(req, timeout=5)
            return {"status": resp.status, "note": "unexpectedly succeeded"}
        except urllib.error.HTTPError as e:
            return {"status": e.code, "note": "got HTTP response (good — means egress works)"}
    step("https_get_oauth2_token", _https_get)

    # ---- Step 5: requests library with explicit timeout ----
    def _requests_post():
        import requests
        # Empty POST — we expect 400 from Google. Just want to confirm
        # `requests` library can complete the call within timeout.
        resp = requests.post(
            'https://oauth2.googleapis.com/token',
            data={"grant_type": "test"},
            timeout=(5, 5),
        )
        return {"status_code": resp.status_code, "elapsed": "see step elapsed_s"}
    step("requests_post_oauth2_token", _requests_post)

    # ---- Step 6: same but to sheets API ----
    def _https_get_sheets():
        import requests
        resp = requests.get(
            'https://sheets.googleapis.com/v4/spreadsheets/test',
            timeout=(5, 5),
        )
        return {"status_code": resp.status_code, "note": "401/403 = egress OK"}
    step("requests_get_sheets_api", _https_get_sheets)

    return jsonify({
        "ok": True,
        "tests": results,
        "summary": {
            "all_passed": all(r.get("ok") for r in results),
            "any_slow": any(r.get("elapsed_s", 0) > 3 for r in results),
        },
    }), 200

@app.route('/admin/diag-scraper-fetch')
@login_required
@admin_required
def admin_diag_scraper_fetch():
    """
    DIAGNOSTIC: Fetch a CarWale URL and dump the actual response so we can
    see what HTML is being served from Render's IP. Helps debug cases where
    the page loads (HTTP 200) but __INITIAL_STATE__ isn't in the body.

    Query params:
      url (optional): full URL to fetch. Defaults to the Maruti Swift page.

    Delete this route once the scraper is proven working.
    """
    import requests as _rq

    target_url = (request.args.get('url') or '').strip()
    if not target_url:
        target_url = 'https://www.carwale.com/maruti-suzuki-cars/swift/'

    if not target_url.startswith('https://www.carwale.com/'):
        return jsonify({"ok": False, "error": "url_not_allowed",
                        "detail": "Only carwale.com URLs allowed."}), 400

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IN,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    try:
        resp = _rq.get(target_url, headers=headers, timeout=(8, 20), allow_redirects=True)
    except Exception as e:
        return jsonify({"ok": False, "error": "fetch_exception",
                        "detail": f"{type(e).__name__}: {e}"}), 500

    body = resp.text or ""
    body_len = len(body)

    needles = {
        "__INITIAL_STATE__":   body.count("__INITIAL_STATE__"),
        "INITIAL_STATE":       body.count("INITIAL_STATE"),
        "__NEXT_DATA__":       body.count("__NEXT_DATA__"),
        "__NUXT__":            body.count("__NUXT__"),
        "dataLayer":           body.count("dataLayer"),
        "window.__":           body.count("window.__"),
        "exShowRoomPrice":     body.count("exShowRoomPrice"),
        "modelPage":           body.count("modelPage"),
        "versionName":         body.count("versionName"),
        "<script":             body.count("<script"),
        "Just a moment":       body.count("Just a moment"),
        "Access denied":       body.count("Access denied"),
        "captcha":             body.lower().count("captcha"),
        "cf-browser-verify":   body.count("cf-browser-verify"),
    }

    history_chain = [
        {"status": h.status_code, "url": h.url} for h in (resp.history or [])
    ]

    interesting_headers = {
        k: resp.headers.get(k)
        for k in (
            "Server", "Content-Type", "Content-Length", "Content-Encoding",
            "Set-Cookie", "CF-RAY", "CF-Cache-Status", "X-Cache",
            "X-Akamai-Transformed", "Via", "Vary",
        )
        if resp.headers.get(k) is not None
    }

    return jsonify({
        "ok": True,
        "fetched_url": target_url,
        "final_url": resp.url,
        "redirect_chain": history_chain,
        "http_status": resp.status_code,
        "body_length_bytes": body_len,
        "interesting_headers": interesting_headers,
        "all_headers": dict(resp.headers),
        "needle_counts": needles,
        "body_first_2000_chars": body[:2000],
        "body_last_500_chars": body[-500:] if body_len > 500 else "",
    }), 200


@app.route('/admin/test-sheets-connection')
@login_required
@admin_required
def admin_test_sheets_connection():
    """
    End-to-end test of the Google Sheets API setup, with hard wall-clock
    timeout via SIGALRM. Always returns within ~15 seconds.

    Step-by-step logging so any future hang is traceable:
      [step 1] importing sheets_writer
      [step 2] reading service-account email
      [step 3] running health_check()
      [step 4] returning success JSON

    On hang/failure, the log will tell you which step never finished.
    """
    import signal
    app.logger.info("[test-sheets] starting endpoint")

    # ─── Wall-clock timeout via SIGALRM ─────────────────────────
    # 15 seconds is enough for: cred load (instant) + OAuth handshake
    # (~2s) + spreadsheet open (~1s) + cell read (~1s) + comfortable
    # buffer. If any step exceeds this, we raise TimeoutError and
    # return a clean error JSON. This is BELOW gunicorn's 30s limit.
    HARD_TIMEOUT_SECONDS = 15

    class _SheetsTimeout(Exception):
        pass

    def _alarm_handler(signum, frame):
        raise _SheetsTimeout(
            f"Health check exceeded {HARD_TIMEOUT_SECONDS}s wall-clock limit "
            "(SIGALRM fired). Something inside Google's API call is hanging "
            "and not honoring its own timeouts. See sheets_writer.py logs "
            "for which step was last entered."
        )

    # Capture previous handler so we can restore it after — important
    # in case any other code in the same worker also uses SIGALRM.
    previous_handler = signal.signal(signal.SIGALRM, _alarm_handler)
    signal.alarm(HARD_TIMEOUT_SECONDS)

    sa_email = "<unknown — credentials not loaded>"
    try:
        # ─── Step 1: import the module ──────────────────────────
        app.logger.info("[test-sheets] step 1: importing sheets_writer")
        try:
            import sheets_writer
        except ImportError as e:
            signal.alarm(0)  # cancel pending alarm
            return jsonify({
                "ok": False,
                "error": "sheets_writer_module_missing",
                "detail": str(e),
                "hint": "sheets_writer.py is not in the repo or has a syntax error.",
            }), 500

        # ─── Step 2: read service-account email ─────────────────
        # This decodes the env var + parses JSON. Network-free.
        # Worth a separate step because the email is useful in
        # error responses even when later steps fail.
        app.logger.info("[test-sheets] step 2: reading service-account email")
        try:
            sa_email = sheets_writer.get_service_account_email()
            app.logger.info("[test-sheets] step 2 OK: email=%s", sa_email)
        except Exception as e:
            app.logger.warning("[test-sheets] step 2 FAILED: %s", e)
            # not fatal — keep going; health_check() will surface the
            # underlying credential-load error

        # ─── Step 3: run the real health check ──────────────────
        app.logger.info("[test-sheets] step 3: running health_check()")
        result = sheets_writer.health_check()

        # ─── Step 4: success ────────────────────────────────────
        app.logger.info(
            "[test-sheets] step 4 OK | sheet=%s | tabs=%s | first_cell=%r",
            result.get("sheet_title"),
            result.get("tabs"),
            result.get("first_cell"),
        )
        return jsonify(result), 200

    except _SheetsTimeout as e:
        app.logger.error("[test-sheets] HARD TIMEOUT: %s", e)
        return jsonify({
            "ok": False,
            "error": "wall_clock_timeout",
            "detail": str(e),
            "service_account_email": sa_email,
            "hint": (
                "The Google API call took longer than "
                f"{HARD_TIMEOUT_SECONDS}s. Most likely cause: malformed "
                "private_key in env var (the OAuth handshake hangs "
                "silently). Re-encode the JSON as base64 and ensure "
                "you pasted the FULL ~3160-char string into the Render "
                "env var. Check the Render runtime log for the last "
                "[test-sheets] step entered to confirm where the hang "
                "occurred."
            ),
        }), 500
    except RuntimeError as e:
        app.logger.warning("[test-sheets] step 3 FAILED (RuntimeError): %s", e)
        return jsonify({
            "ok": False,
            "error": "health_check_failed",
            "detail": str(e),
            "service_account_email": sa_email,
        }), 500
    except Exception as e:
        app.logger.exception("[test-sheets] step 3 FAILED (unexpected)")
        return jsonify({
            "ok": False,
            "error": "unexpected_error",
            "detail": str(e),
            "service_account_email": sa_email,
        }), 500
    finally:
        # ALWAYS cancel the alarm and restore the previous handler.
        # Otherwise the timer might fire during a later request
        # handled by the same worker.
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)
@app.route("/admin/test-scraper")
@login_required
@admin_required
def admin_test_scraper():
    """
    Proof-of-concept endpoint for price_scraper.py.
    Pass query params: make, model, variant, fuel.
    """
    make = (request.args.get("make") or "").strip()
    model = (request.args.get("model") or "").strip()
    variant = (request.args.get("variant") or "").strip()
    fuel = (request.args.get("fuel") or "").strip()

    if not (make and model and variant and fuel):
        return jsonify({
            "ok": False,
            "error": "missing_params",
            "detail": (
                "All four query params required: make, model, variant, fuel. "
                "Example: /admin/test-scraper?make=Maruti+Suzuki&model=Swift"
                "&variant=VXI&fuel=Petrol"
            ),
        }), 400

    app.logger.info(
        "[test-scraper] request: make=%r model=%r variant=%r fuel=%r",
        make, model, variant, fuel,
    )

    try:
        import price_scraper
    except ImportError as e:
        return jsonify({
            "ok": False,
            "error": "import_failed",
            "detail": f"Could not import price_scraper: {e}",
        }), 500

    try:
        result = price_scraper.fetch_price(make, model, variant, fuel)
    except Exception as e:
        app.logger.exception("[test-scraper] unexpected error")
        return jsonify({
            "ok": False,
            "error": "scrape_exception",
            "detail": f"{type(e).__name__}: {e}",
        }), 500

    response = {"ok": result["status"] in ("found", "ambiguous"), **result}
    app.logger.info(
        "[test-scraper] result: status=%s for %s/%s/%s/%s",
        result.get("status"), make, model, variant, fuel,
    )
    return jsonify(response)

# ============================================================
# END app.py — Part 5
# Continue with Part 6 immediately below.
# ============================================================
# ============================================================
# app.py — Part 6
# ------------------------------------------------------------
# Paste this BLOCK immediately below the last line of Part 5.
# Continue pasting Part 7 immediately below the last line of this block.
# ============================================================


# ============================================================
# v3.5 NEW ROUTES — FEEDBACK API + ADMIN MULTIPLIERS + ADMIN FEEDBACK
# ============================================================

@app.route('/api/feedback', methods=['POST'])
@login_required
@no_guest(message='Sign up to leave feedback and earn credits.')
def api_feedback():
    """
    POST endpoint to capture user feedback on a valuation.

    v3.5.1 changes:
      - Two reactions only: 'helpful' / 'wayoff' ('close' dropped)
      - Both require actual_price + source
      - Both award 50 credits (FEEDBACK_REWARD)
      - Adds source_other (free text) when source == 'Other'
      - Inserts into the actual feedback table schema (user_email, state_code, city,
        actual_price_inr, price_source, predicted_price_inr, fuel_type, gap_pct,
        confidence_score, confidence_tier, status)
      - Dedup by (valuation_id, user_email) via the partial unique index added in
        Migration 3 (the DB itself enforces it)

    Body (form or JSON):
      valuation_id: int (FK to valuations.id) — required
      reaction: 'helpful' | 'wayoff' — required
      actual_price: int (rupees) — required
      source: str — required. One of: OLX, Dealer, CarTrade, CarDekho,
              Spinny, Cars24, Friend/Family, Other
      source_other: str — required only if source == 'Other' (max 120 chars)
      notes: str — ignored (no column on table; kept for backward-compat clients)

    Returns JSON:
      { ok: True, credits_awarded: int, new_balance: int, message: str }
      or { ok: False, error: str, detail?: str }
    """
    user = current_user()
    if not user:
        return jsonify({'ok': False, 'error': 'not_logged_in'}), 401

    user_email = (user.get('email') or '').lower().strip()
    if not user_email:
        return jsonify({'ok': False, 'error': 'no_user_email'}), 400

    # Accept form-encoded or JSON
    payload_in = request.get_json(silent=True) or {}
    def _get(key, default=''):
        return (payload_in.get(key) if payload_in else None) or request.form.get(key) or default

    valuation_id_raw = _get('valuation_id', '')
    reaction = (_get('reaction', '') or '').strip().lower()
    actual_price_raw = _get('actual_price', '')
    source_raw = (_get('source', '') or '').strip()
    source_other_raw = (_get('source_other', '') or '').strip()

    # ---- Validate valuation_id ----
    try:
        valuation_id_int = int(valuation_id_raw)
        if valuation_id_int <= 0:
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'error': 'invalid_valuation_id'}), 400

    # ---- Validate reaction (v3.5.1: only helpful + wayoff) ----
    if reaction not in ('helpful', 'wayoff'):
        return jsonify({'ok': False, 'error': 'invalid_reaction',
                        'detail': 'Reaction must be helpful or wayoff.'}), 400

    # ---- Validate source ----
    ALLOWED_SOURCES = {
        'OLX', 'Dealer', 'CarTrade', 'CarDekho',
        'Spinny', 'Cars24', 'Friend/Family', 'Other',
    }
    if not source_raw:
        return jsonify({'ok': False, 'error': 'source_required'}), 400
    if source_raw not in ALLOWED_SOURCES:
        return jsonify({'ok': False, 'error': 'invalid_source'}), 400

    # If 'Other', source_other is required
    source_other = None
    if source_raw == 'Other':
        if not source_other_raw:
            return jsonify({'ok': False, 'error': 'source_other_required',
                            'detail': 'Please specify the source.'}), 400
        source_other = source_other_raw[:120]

    # ---- Validate actual_price ----
    if not actual_price_raw:
        return jsonify({'ok': False, 'error': 'actual_price_required'}), 400
    try:
        cleaned_price = str(actual_price_raw).replace(',', '').replace('₹', '').strip()
        actual_price_int = int(cleaned_price)
        if actual_price_int <= 0 or actual_price_int > 100000000:
            raise ValueError
    except (ValueError, TypeError):
        return jsonify({'ok': False, 'error': 'invalid_actual_price'}), 400

    # ---- Look up the valuation (must belong to this user) ----
    try:
        r = (supabase.table('valuations')
             .select('id, user_id, make, model, variant, fuel, year, '
                     'estimated_price, user_state, user_city, '
                     'confidence, geo_tier')
             .eq('id', valuation_id_int)
             .eq('user_id', user['id'])
             .limit(1)
             .execute())
        valuation = r.data[0] if r.data else None
    except Exception as e:
        app.logger.error(f"api_feedback: valuation lookup failed: {e}")
        return jsonify({'ok': False, 'error': 'lookup_failed'}), 500

    if not valuation:
        return jsonify({'ok': False, 'error': 'valuation_not_found'}), 404

    # ---- Check for duplicate feedback (1 per user_email per valuation_id) ----
    # The DB has a partial unique index that enforces this, but we check first
    # so the error is clean (409 Conflict) rather than a generic insert failure.
    try:
        r_dup = (supabase.table('feedback')
                 .select('id')
                 .eq('valuation_id', valuation_id_int)
                 .eq('user_email', user_email)
                 .limit(1)
                 .execute())
        if r_dup.data:
            return jsonify({'ok': False, 'error': 'already_submitted',
                            'detail': 'You have already submitted feedback for this valuation.'}), 409
    except Exception as e:
        app.logger.warning(f"api_feedback: duplicate check failed (continuing): {e}")

    # ---- Compute gap_pct = (actual - predicted) / predicted * 100 ----
    predicted_price = valuation.get('estimated_price') or 0
    gap_pct = None
    if predicted_price and predicted_price > 0:
        try:
            gap_pct = round(((actual_price_int - predicted_price) / predicted_price) * 100, 2)
        except (TypeError, ZeroDivisionError):
            gap_pct = None

    # ---- Build the price_source field. If 'Other', append the free text. ----
    if source_raw == 'Other':
        # Store full descriptor in price_source so admin queries don't need to JOIN
        # source_other separately. source_other_raw also stored in its own column.
        price_source_value = f"Other: {source_other}"[:120]
    else:
        price_source_value = source_raw

    # ---- Build insert payload — MATCHING the actual feedback table schema ----
    fb_payload = {
        # Required NOT NULL columns
        'reaction':             reaction,
        'make':                 valuation.get('make'),
        'model':                valuation.get('model'),
        'state_code':           (valuation.get('user_state') or DEFAULT_STATE_CODE).upper(),
        'city':                 valuation.get('user_city') or DEFAULT_CITY,
        'predicted_price_inr':  int(predicted_price) if predicted_price else 0,
        # Nullable but useful columns
        'user_email':           user_email,
        'variant':              valuation.get('variant'),
        'year':                 valuation.get('year'),
        'fuel_type':            valuation.get('fuel'),
        'actual_price_inr':     actual_price_int,
        'price_source':         price_source_value,
        'gap_pct':              gap_pct,
        'confidence_score':     valuation.get('confidence'),
        'confidence_tier':      valuation.get('geo_tier') or 'formula',
        # v3.5.1 fix: feedback_status_check allows ('new','reviewed','actioned','dismissed').
        # 'new' = just submitted, awaiting admin review. Credit award already happened
        # via the credits ledger; status is for admin workflow only.
        'status':               'new',
        # v3.5.1 NEW columns (added by Migration 3)
        'valuation_id':         valuation_id_int,
        'source_other':         source_other,
    }

    try:
        ins = supabase.table('feedback').insert(fb_payload).execute()
        if not ins.data:
            return jsonify({'ok': False, 'error': 'insert_failed',
                            'detail': 'Database returned no rows.'}), 500
    except Exception as e:
        # Catch the unique-index violation specifically and return 409
        err_str = str(e).lower()
        if 'duplicate key' in err_str or 'unique' in err_str:
            return jsonify({'ok': False, 'error': 'already_submitted',
                            'detail': 'Feedback already submitted for this valuation.'}), 409
        app.logger.error(f"api_feedback insert failed: {e}")
        return jsonify({'ok': False, 'error': 'insert_failed', 'detail': str(e)}), 500

    # ---- Award credits (v3.5.1: 50 for both reactions) ----
    reward = FEEDBACK_REWARD
    current_credits = user.get('credits', 0) or 0
    new_balance = current_credits + reward
    try:
        update_user(user['id'], {'credits': new_balance})
        car_label = (
            f"{valuation.get('make','')} {valuation.get('model','')} "
            f"{valuation.get('variant','')}"
        ).strip()
        log_credit_transaction(
            user_id=user['id'],
            type_='feedback_reward',
            description=f"Feedback ({reaction}) on {car_label}",
            amount=reward,
            balance_after=new_balance,
        )
        session['credits'] = new_balance
        if 'user' in session:
            session['user']['credits'] = new_balance
    except Exception as e:
        app.logger.error(f"api_feedback credit award failed: {e}")

    return jsonify({
        'ok': True,
        'credits_awarded': reward,
        'new_balance': new_balance,
        'message': f'Thank you! +{reward} credits awarded.',
    }), 200


@app.route('/admin/multipliers', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_multipliers():
    """
    Admin view + edit for state_multipliers table.

    GET: shows table of all 36 states/UTs with: state_code, state_name, multiplier,
         road_tax_pct, rto_fee_inr, data_quality, updated_at, notes.
    POST: updates a single state's editable fields. Form fields:
          state_code (required), multiplier, road_tax_pct, rto_fee_inr,
          data_quality, notes.
    """
    admin = current_user()

    if request.method == 'POST':
        state_code = (request.form.get('state_code') or '').strip().upper()
        if not state_code:
            flash('State code is required.', 'error')
            return redirect(url_for('admin_multipliers'))

        update_fields = {}
        # multiplier
        mult_raw = (request.form.get('multiplier') or '').strip()
        if mult_raw:
            try:
                m = float(mult_raw)
                if m < 0.50 or m > 1.50:
                    flash(f'Multiplier {m} out of safe range [0.50, 1.50]. Not updated.', 'error')
                    return redirect(url_for('admin_multipliers'))
                update_fields['multiplier'] = round(m, 3)
            except (ValueError, TypeError):
                flash('Invalid multiplier value.', 'error')
                return redirect(url_for('admin_multipliers'))

        # road_tax_pct
        rt_raw = (request.form.get('road_tax_pct') or '').strip()
        if rt_raw:
            try:
                rt = float(rt_raw)
                if rt < 0 or rt > 50:
                    flash('Road tax % out of range [0, 50].', 'error')
                    return redirect(url_for('admin_multipliers'))
                update_fields['road_tax_pct'] = round(rt, 2)
            except (ValueError, TypeError):
                flash('Invalid road tax value.', 'error')
                return redirect(url_for('admin_multipliers'))

        # rto_fee_inr
        rf_raw = (request.form.get('rto_fee_inr') or '').strip()
        if rf_raw:
            try:
                rf = int(rf_raw.replace(',', ''))
                if rf < 0 or rf > 1000000:
                    flash('RTO fee out of range.', 'error')
                    return redirect(url_for('admin_multipliers'))
                update_fields['rto_fee_inr'] = rf
            except (ValueError, TypeError):
                flash('Invalid RTO fee value.', 'error')
                return redirect(url_for('admin_multipliers'))

        # data_quality
        dq_raw = (request.form.get('data_quality') or '').strip()
        if dq_raw:
            if dq_raw not in ('high', 'medium', 'low'):
                flash('Invalid data_quality value.', 'error')
                return redirect(url_for('admin_multipliers'))
            update_fields['data_quality'] = dq_raw

        # notes
        notes_raw = (request.form.get('notes') or '').strip()
        if notes_raw:
            update_fields['notes'] = notes_raw[:1000]

        if not update_fields:
            flash('No changes provided.', 'error')
            return redirect(url_for('admin_multipliers'))

        update_fields['updated_at'] = datetime.utcnow().isoformat()
        update_fields['updated_by'] = admin.get('email')

        try:
            supabase.table('state_multipliers').update(update_fields).eq('state_code', state_code).execute()
            # Also write audit log row
            try:
                audit_payload = {
                    'state_code': state_code,
                    'changed_by': admin.get('email'),
                    'changes': json.dumps(update_fields),
                    'changed_at': datetime.utcnow().isoformat(),
                }
                supabase.table('state_multipliers_audit').insert(audit_payload).execute()
            except Exception as e_audit:
                app.logger.warning(f"state_multipliers_audit insert skipped (table may not exist): {e_audit}")

            # Force-refresh in-memory cache
            load_state_multipliers(force_refresh=True)
            flash(f'Updated {state_code}: {update_fields}', 'success')
        except Exception as e:
            app.logger.error(f"admin_multipliers update failed: {e}")
            flash(f'Update failed: {e}', 'error')

        return redirect(url_for('admin_multipliers'))

    # GET: load table
    try:
        r = (supabase.table('state_multipliers')
             .select('*')
             .order('state_code', desc=False)
             .execute())
        rows = r.data or []
        # v3.5.1-r5.3: actual column is updated_at. Alias to last_updated so
        # the template (which references row.last_updated) keeps working,
        # and compute last_updated_display for the DD-MMM-YYYY UI rule.
        for row in rows:
            row['last_updated'] = row.get('updated_at')
            row['last_updated_display'] = _format_txn_date(row.get('updated_at'))
    except Exception as e:
        app.logger.error(f"admin_multipliers fetch failed: {e}")
        rows = []
        flash(f'Could not load state multipliers: {e}', 'error')

    # Optional: load audit history (last 50 entries)
    audit_rows = []
    try:
        r_audit = (supabase.table('state_multipliers_audit')
                   .select('*')
                   .order('changed_at', desc=True)
                   .limit(50)
                   .execute())
        audit_rows = r_audit.data or []
        for ar in audit_rows:
            ar['changed_at_display'] = _format_txn_date(ar.get('changed_at'))
    except Exception as e:
        app.logger.warning(f"admin_multipliers audit fetch failed (table may not exist): {e}")

    return render_template(
        'admin_multipliers.html',
        user=admin,
        first_name=firstname_filter(admin.get('name')),
        rows=rows,
        audit_rows=audit_rows,
        now_display=datetime.utcnow().strftime('%d-%b-%Y %H:%M UTC'),
    )


# ============================================================
# END app.py — Part 6
# Continue with Part 7 immediately below.
# ============================================================
# ============================================================
# app.py — Part 7
# ------------------------------------------------------------
# Paste this BLOCK immediately below the last line of Part 6.
# Continue pasting Part 8 immediately below the last line of this block.
# ============================================================


@app.route('/admin/feedback')
@login_required
@admin_required
def admin_feedback():
    """
    Admin view of feedback aggregated by Brand × State × Model.
    Sorted by wayoff% desc; flags combos with N>=5 AND wayoff%>=40%.

    v3.5.1-r2: Column references updated to match the ACTUAL feedback table
    schema (which uses *_inr / state_code / fuel_type naming, not the names
    that an earlier draft of this route assumed).
    """
    admin = current_user()

    try:
        # v3.5.1-r2: Real columns on the feedback table.
        r = (supabase.table('feedback')
             .select('id, valuation_id, user_email, reaction, '
                     'actual_price_inr, price_source, source_other, '
                     'make, model, variant, fuel_type, year, '
                     'predicted_price_inr, state_code, city, '
                     'gap_pct, confidence_score, confidence_tier, '
                     'status, admin_notes, created_at')
             .order('created_at', desc=True)
             .execute())
        all_feedback = r.data or []
    except Exception as e:
        app.logger.error(f"admin_feedback fetch failed: {e}")
        flash(f'Could not load feedback: {e}', 'error')
        all_feedback = []

    # Aggregate by (make, state_code, model)
    aggregates = defaultdict(lambda: {
        'helpful': 0,
        'wayoff': 0,
        'total': 0,
        'sample_actual_prices': [],
        'sample_estimates': [],
        'sample_gap_pcts': [],
    })

    for fb in all_feedback:
        make = fb.get('make') or 'Unknown'
        model = fb.get('model') or 'Unknown'
        state = fb.get('state_code') or DEFAULT_STATE_CODE
        key = (make, state, model)
        agg = aggregates[key]
        reaction = (fb.get('reaction') or '').lower()
        # v3.5.1: only helpful/wayoff are valid reactions now ('close' was dropped)
        if reaction in ('helpful', 'wayoff'):
            agg[reaction] += 1
            agg['total'] += 1
        if fb.get('actual_price_inr'):
            agg['sample_actual_prices'].append(int(fb['actual_price_inr']))
        if fb.get('predicted_price_inr'):
            agg['sample_estimates'].append(int(fb['predicted_price_inr']))
        if fb.get('gap_pct') is not None:
            try:
                agg['sample_gap_pcts'].append(float(fb['gap_pct']))
            except (ValueError, TypeError):
                pass

    # Build display rows
    table_rows = []
    for (make, state, model), agg in aggregates.items():
        total = agg['total']
        if total == 0:
            continue
        wayoff_pct = round((agg['wayoff'] / total) * 100, 1)
        helpful_pct = round((agg['helpful'] / total) * 100, 1)
        # Auto-flag rule: 5+ feedback rows AND >=40% wayoff means this combo
        # likely has a calibration issue worth investigating.
        flagged = (total >= 5 and wayoff_pct >= 40.0)

        median_actual = None
        median_estimate = None
        median_gap_pct = None
        try:
            actuals = sorted(agg['sample_actual_prices'])
            estimates = sorted(agg['sample_estimates'])
            gaps = sorted(agg['sample_gap_pcts'])
            if actuals:
                n = len(actuals)
                median_actual = actuals[n // 2] if n % 2 == 1 else (actuals[n // 2 - 1] + actuals[n // 2]) // 2
            if estimates:
                n = len(estimates)
                median_estimate = estimates[n // 2] if n % 2 == 1 else (estimates[n // 2 - 1] + estimates[n // 2]) // 2
            if gaps:
                n = len(gaps)
                median_gap_pct = round(gaps[n // 2] if n % 2 == 1 else (gaps[n // 2 - 1] + gaps[n // 2]) / 2, 1)
        except Exception:
            pass

        table_rows.append({
            'make': make,
            'state': state,
            'model': model,
            'total': total,
            'helpful': agg['helpful'],
            'wayoff': agg['wayoff'],
            'helpful_pct': helpful_pct,
            'wayoff_pct': wayoff_pct,
            'flagged': flagged,
            'median_actual': median_actual,
            'median_estimate': median_estimate,
            'median_gap_pct': median_gap_pct,
        })

    # Sort: flagged first, then by wayoff_pct desc, then by total desc
    table_rows.sort(key=lambda x: (not x['flagged'], -x['wayoff_pct'], -x['total']))

    # Recent raw feedback (last 50) for the list section
    recent = []
    for fb in all_feedback[:50]:
        recent.append({
            **fb,
            'created_display': _format_txn_date(fb.get('created_at')),
            'reaction_label': (fb.get('reaction') or '—').title(),
        })

    # Top-line stats
    total_count = len(all_feedback)
    helpful_count = sum(1 for f in all_feedback if (f.get('reaction') or '').lower() == 'helpful')
    wayoff_count  = sum(1 for f in all_feedback if (f.get('reaction') or '').lower() == 'wayoff')

    return render_template(
        'admin_feedback.html',
        user=admin,
        first_name=firstname_filter(admin.get('name')),
        table_rows=table_rows,
        recent=recent,
        total_count=total_count,
        helpful_count=helpful_count,
        wayoff_count=wayoff_count,
        now_display=datetime.utcnow().strftime('%d-%b-%Y %H:%M UTC'),
    )


# ============================================================
# ADMIN · RESEARCH LOG (v3.5.1-r4)
# ============================================================
# Manual-entry research data — mystery shopping observations + friends/family
# deal references. Used for INITIAL FORMULA CALIBRATION ONLY. Does NOT feed
# the live valuation engine (that's the verified `deals` table's job).
#
# Routes:
#   GET  /admin/research                 → list + add-form page
#   POST /admin/research                 → create new entry
#   POST /admin/research/<id>/edit       → update existing entry
#   POST /admin/research/<id>/delete     → permanently delete
# ============================================================

# Source values must match the dropdown in admin_research.html
RESEARCH_SOURCES = ['Mystery Shopping', 'Friends & Family']
# v3.5.1-r5.1: Use the canonical OWNERS / CONDITIONS lists from app.py module scope
# (defined ~line 159-160) so the research form mirrors seller / submit_deal exactly.
# Before, this was a bespoke 4-value list ('1st'/'2nd'/'3rd'/'4th+') which broke
# parity with the rest of the app.
RESEARCH_OWNERS = OWNERS                  # ['1st Owner', '2nd Owner', '3rd Owner or more']
RESEARCH_CONDITIONS = CONDITIONS          # ['Excellent', 'Good', 'Fair']


def _parse_research_int(raw, field_name, min_val, max_val, allow_none=True):
    """Parse a numeric field from the research form. Returns int or None.
       Strips Indian-format commas, the ₹ prefix, and whitespace."""
    if raw is None:
        return None
    s = str(raw).replace(',', '').replace('₹', '').strip()
    if not s:
        return None
    try:
        n = int(float(s))
    except (ValueError, TypeError):
        raise ValueError(f"{field_name} must be a number.")
    if n < min_val or n > max_val:
        raise ValueError(f"{field_name} must be between {min_val:,} and {max_val:,}.")
    return n


def _validate_research_form(form, is_edit=False):
    """Pull and validate fields from a research-log POST form.
       Returns (payload_dict, error_message_or_None)."""
    # Required: car identity
    make    = (form.get('make') or '').strip()
    model   = (form.get('model') or '').strip()
    variant = (form.get('variant') or '').strip()
    fuel    = (form.get('fuel') or '').strip()
    year_raw = (form.get('year') or '').strip()

    for label, val in [('Make', make), ('Model', model), ('Variant', variant), ('Fuel', fuel)]:
        if not val:
            return None, f"{label} is required."
    try:
        year = int(year_raw)
        if year < 2000 or year > 2030:
            return None, "Year must be between 2000 and 2030."
    except (ValueError, TypeError):
        return None, "Year is required."

    # Required: source + entry_date
    data_source = (form.get('data_source') or '').strip()
    if data_source not in RESEARCH_SOURCES:
        return None, f"Source must be one of: {', '.join(RESEARCH_SOURCES)}."

    entry_date_raw = (form.get('entry_date') or '').strip()
    if not entry_date_raw:
        entry_date_raw = datetime.utcnow().strftime('%Y-%m-%d')
    # Accept either ISO yyyy-mm-dd (HTML date input) or DD-MMM-YYYY
    entry_date_iso = None
    for fmt in ('%Y-%m-%d', '%d-%b-%Y'):
        try:
            entry_date_iso = datetime.strptime(entry_date_raw, fmt).strftime('%Y-%m-%d')
            break
        except ValueError:
            continue
    if not entry_date_iso:
        return None, "Entry date format is invalid. Use DD-MMM-YYYY (e.g. 02-May-2026)."

    # Optional: car-state fields
    try:
        mileage_km = _parse_research_int(form.get('mileage_km'), 'Mileage', 0, 1000000)
    except ValueError as e:
        return None, str(e)

    owners = (form.get('owners') or '').strip() or None
    if owners and owners not in RESEARCH_OWNERS:
        return None, f"Owners must be one of: {', '.join(RESEARCH_OWNERS)}."

    condition = (form.get('condition') or '').strip() or None
    if condition and condition not in RESEARCH_CONDITIONS:
        return None, f"Condition must be one of: {', '.join(RESEARCH_CONDITIONS)}."

    # Optional: location
    state_code = (form.get('state_code') or '').strip().upper() or None
    city = (form.get('city') or '').strip() or None

    # Optional: pricing
    try:
        asking_price = _parse_research_int(form.get('asking_price_inr'), 'Asking price', 1, 100000000)
        negotiated_price = _parse_research_int(form.get('negotiated_price_inr'), 'Negotiated price', 1, 100000000)
    except ValueError as e:
        return None, str(e)

    # Optional: dealer / listing context
    dealer_name = (form.get('dealer_name') or '').strip() or None
    dealer_phone = (form.get('dealer_phone') or '').strip() or None
    listing_url = (form.get('listing_url') or '').strip() or None
    notes = (form.get('notes') or '').strip() or None
    if notes and len(notes) > 2000:
        return None, "Notes are limited to 2000 characters."

    # v3.5.1-r5: include_in_calibration defaults to TRUE for new entries.
    # On edit, the form sends explicit '0' or '1' so we honor it.
    incl_raw = form.get('include_in_calibration')
    if incl_raw is None:
        include_in_calibration = True  # Default TRUE per user spec
    else:
        include_in_calibration = (str(incl_raw).strip() in ('1', 'on', 'true', 'True', 'yes'))

    exclusion_reason = (form.get('exclusion_reason') or '').strip() or None
    if exclusion_reason and len(exclusion_reason) > 500:
        return None, "Exclusion reason is limited to 500 characters."
    # Only persist exclusion_reason when actually excluded
    if include_in_calibration:
        exclusion_reason = None

    payload = {
        'make': make,
        'model': model,
        'variant': variant,
        'year': year,
        'fuel': fuel,
        'data_source': data_source,
        'entry_date': entry_date_iso,
        'mileage_km': mileage_km,
        'owners': owners,
        'condition': condition,
        'state_code': state_code,
        'city': city,
        'asking_price_inr': asking_price,
        'negotiated_price_inr': negotiated_price,
        'dealer_name': dealer_name,
        'dealer_phone': dealer_phone,
        'listing_url': listing_url,
        'notes': notes,
        'include_in_calibration': include_in_calibration,
        'exclusion_reason': exclusion_reason,
    }
    return payload, None


@app.route('/admin/research', methods=['GET', 'POST'])
@login_required
@admin_required
def admin_research():
    """List + add research log entries. Filters via querystring."""
    admin = current_user()

    # ── POST: create a new entry ──────────────────────────────
    if request.method == 'POST':
        payload, err = _validate_research_form(request.form)
        if err:
            flash(err, 'error')
            return redirect(url_for('admin_research'))
        payload['created_by'] = admin.get('email') or 'unknown'
        try:
            supabase.table('research_log').insert(payload).execute()
            flash(
                f"Research entry added: {payload['year']} {payload['make']} "
                f"{payload['model']} ({payload['data_source']}).",
                'success'
            )
        except Exception as e:
            app.logger.error(f"admin_research insert failed: {e}")
            flash(f"Could not save: {e}", 'error')
        return redirect(url_for('admin_research'))

    # ── GET: load entries with optional filters ───────────────
    f_source = (request.args.get('source') or '').strip()
    f_state = (request.args.get('state') or '').strip().upper()
    f_make = (request.args.get('make') or '').strip()

    try:
        q = supabase.table('research_log').select('*')
        if f_source and f_source in RESEARCH_SOURCES:
            q = q.eq('data_source', f_source)
        if f_state:
            q = q.eq('state_code', f_state)
        if f_make:
            q = q.eq('make', f_make)
        q = q.order('entry_date', desc=True).order('created_at', desc=True).limit(500)
        r = q.execute()
        rows = r.data or []
    except Exception as e:
        app.logger.error(f"admin_research fetch failed: {e}")
        rows = []
        flash(f"Could not load research log: {e}", 'error')

    # Decorate each row with a display date and gap_pct sign
    for row in rows:
        ed = row.get('entry_date')
        if ed:
            try:
                row['entry_date_display'] = datetime.strptime(ed, '%Y-%m-%d').strftime('%d-%b-%Y')
            except (ValueError, TypeError):
                row['entry_date_display'] = ed
        else:
            row['entry_date_display'] = '—'

    # Aggregate stats
    total = len(rows)
    by_mystery = sum(1 for r_ in rows if r_.get('data_source') == 'Mystery Shopping')
    by_ff = sum(1 for r_ in rows if r_.get('data_source') == 'Friends & Family')
    # v3.5.1-r5: Inclusion split for the calibration corpus
    n_included = sum(1 for r_ in rows if r_.get('include_in_calibration'))
    n_excluded = total - n_included
    # Average gap_pct (mystery-shop entries with both prices)
    gaps = [float(r_['gap_pct']) for r_ in rows
            if r_.get('gap_pct') is not None and r_.get('data_source') == 'Mystery Shopping']
    avg_gap = round(sum(gaps) / len(gaps), 1) if gaps else None

    stats = {
        'total': total,
        'mystery_count': by_mystery,
        'ff_count': by_ff,
        'avg_mystery_gap_pct': avg_gap,
        'gap_sample_size': len(gaps),
        'included_count': n_included,
        'excluded_count': n_excluded,
    }

    # Distinct values for filter dropdowns
    distinct_states = sorted({r_.get('state_code') for r_ in rows if r_.get('state_code')})
    distinct_makes = sorted({r_.get('make') for r_ in rows if r_.get('make')})

    # v3.5.1-r5: Compute calibration suggestions from the FULL row set
    # (don't apply UI filters to calibration math — that would be misleading).
    # We need to query unfiltered when computing suggestions.
    try:
        if f_source or f_state or f_make:
            # Filtered view — re-fetch full set just for suggestion math
            r_all = (supabase.table('research_log')
                     .select('*')
                     .eq('include_in_calibration', True)
                     .order('entry_date', desc=True)
                     .limit(2000)
                     .execute())
            calib_corpus = r_all.data or []
        else:
            calib_corpus = rows
        suggestions = _compute_calibration_suggestions(calib_corpus)
    except Exception as e:
        app.logger.error(f"calibration suggestions failed: {e}")
        suggestions = []

    return render_template(
        'admin_research.html',
        user=admin,
        first_name=firstname_filter(admin.get('name')),
        rows=rows,
        stats=stats,
        suggestions=suggestions,
        sources=RESEARCH_SOURCES,
        owners_choices=RESEARCH_OWNERS,
        condition_choices=RESEARCH_CONDITIONS,
        car_data=CAR_DATA,
        cities_by_state=INDIAN_CITIES_BY_STATE,
        rto_states=RTO_STATES,
        default_state=DEFAULT_STATE_CODE,
        default_city=DEFAULT_CITY,
        active_source=f_source,
        active_state=f_state,
        active_make=f_make,
        distinct_states=distinct_states,
        distinct_makes=distinct_makes,
        calib_min_entries=CALIB_MIN_ENTRIES,
        now_display=datetime.utcnow().strftime('%d-%b-%Y %H:%M UTC'),
        today_iso=datetime.utcnow().strftime('%Y-%m-%d'),
    )


# ============================================================
# END app.py — Part 7
# Continue with Part 8 immediately below.
# ============================================================
# ============================================================
# app.py — Part last
# ------------------------------------------------------------
# Paste this BLOCK immediately below the last line of Part 7.
# This is the FINAL part — once pasted, your app.py is complete.
# ============================================================


@app.route('/admin/research/<entry_id>/edit', methods=['POST'])
@login_required
@admin_required
def admin_research_edit(entry_id):
    """Update an existing research-log entry."""
    payload, err = _validate_research_form(request.form, is_edit=True)
    if err:
        flash(err, 'error')
        return redirect(url_for('admin_research'))
    try:
        supabase.table('research_log').update(payload).eq('id', entry_id).execute()
        flash("Research entry updated.", 'success')
    except Exception as e:
        app.logger.error(f"admin_research_edit failed: {e}")
        flash(f"Could not update: {e}", 'error')
    return redirect(url_for('admin_research'))


@app.route('/admin/research/<entry_id>/delete', methods=['POST'])
@login_required
@admin_required
def admin_research_delete(entry_id):
    """Permanently delete a research-log entry. (Soft-delete is overkill
    for an admin-only research table — the admin can re-enter on typo.)"""
    try:
        supabase.table('research_log').delete().eq('id', entry_id).execute()
        flash("Research entry deleted.", 'success')
    except Exception as e:
        app.logger.error(f"admin_research_delete failed: {e}")
        flash(f"Could not delete: {e}", 'error')
    return redirect(url_for('admin_research'))


# ============================================================
# v3.5.1-r5: CALIBRATION SUGGESTIONS — semi-automated multiplier updates
# ============================================================
# Three new pieces:
#   1. _compute_calibration_suggestions(rows)
#        Given the loaded research_log rows, returns a list of suggestion
#        objects (one per state) where ≥3 included entries exist.
#   2. POST /admin/research/<id>/toggle-calibration
#        Flip include_in_calibration boolean. Optional exclusion_reason.
#   3. POST /admin/research/apply-suggestion/<state_code>
#        Apply the suggested multiplier to state_multipliers, log to audit.
# ============================================================

# Tunable thresholds — change here to adjust suggestion sensitivity
CALIB_MIN_ENTRIES = 3                    # Need ≥3 entries before suggesting
CALIB_OUTLIER_TRIM_PCT = 10              # Drop top/bottom 10% of ratios
CALIB_MAX_AGE_DAYS = 90                  # Skip entries older than 90 days
CALIB_MIN_GAP_PCT_TO_SUGGEST = 1.0       # Don't suggest changes <1%
CALIB_MAX_REASONABLE_RATIO = 1.30        # Sanity: skip extreme outlier rows
CALIB_MIN_REASONABLE_RATIO = 0.70


def _percentile_trim(values, trim_pct):
    """Drop the top and bottom trim_pct% of a sorted numeric list.
    Returns the trimmed list. If trimming would leave <3 items,
    returns the original (don't over-trim small samples)."""
    if not values:
        return values
    n = len(values)
    if n <= 4:
        return sorted(values)
    sorted_vals = sorted(values)
    drop_n = max(1, int(n * trim_pct / 100))
    trimmed = sorted_vals[drop_n:n - drop_n]
    return trimmed if len(trimmed) >= 3 else sorted_vals


def _median(values):
    """Median of a numeric list. Returns None for empty."""
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    return s[n // 2] if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2


def _compute_calibration_suggestions(rows):
    """
    For each state with ≥CALIB_MIN_ENTRIES included research entries,
    compute a suggested state multiplier using the simple-ratio method:

        ratio_i = research_negotiated_price_i / engine_estimated_price_i
        median_ratio = median(ratios after outlier trim)
        suggested_mult = current_multiplier × median_ratio

    Returns a list of dicts (one per state), sorted with biggest gaps first.
    """
    # Group included rows by state_code
    by_state = defaultdict(list)
    today = datetime.utcnow().date()

    for r in rows:
        # Inclusion gate
        if not r.get('include_in_calibration'):
            continue
        if not r.get('state_code'):
            continue  # Can't calibrate state without a state code
        if not r.get('negotiated_price_inr'):
            continue  # Need actual price (F&F entries should have this in negotiated)

        # Age gate
        ed = r.get('entry_date')
        if ed:
            try:
                entry_d = datetime.strptime(ed, '%Y-%m-%d').date()
                age_days = (today - entry_d).days
                if age_days > CALIB_MAX_AGE_DAYS:
                    continue
            except (ValueError, TypeError):
                pass  # If date is malformed, include it anyway

        by_state[r['state_code']].append(r)

    suggestions = []
    current_mults = load_state_multipliers()  # {state_code: float}

    for sc, entries in by_state.items():
        if len(entries) < CALIB_MIN_ENTRIES:
            continue
        current_mult = current_mults.get(sc, 1.000)

        # Compute engine prediction for each entry, then the ratio
        ratios = []
        contributing_ids = []
        skipped_count = 0

        for r in entries:
            try:
                # Map research-log fields → valuation engine inputs.
                # v3.5.1-r5.1: research now uses the canonical OWNERS list
                # (matches seller/submit_deal), so no remapping needed.
                eng_owner = r.get('owners') or '1st Owner'
                eng_condition = r.get('condition') or 'Good'
                eng_mileage = r.get('mileage_km') or 50000  # reasonable default

                result, _audit = _route_valuation(
                    make=r['make'], model=r['model'], variant=r['variant'],
                    fuel=r['fuel'], year=r['year'],
                    mileage=eng_mileage,
                    condition=eng_condition,
                    owner=eng_owner,
                    allow_market_engine=True,
                    user_state=sc,
                    user_city=r.get('city'),
                )
                # v3.5.1-r5.2 BUGFIX: _route_valuation() returns the price under
                # the key 'estimated' (NOT 'estimated_price' — that's the key
                # used internally by the engine's own intermediate dict, before
                # _route_valuation rewraps it). Using the wrong key made every
                # entry silently fail this guard, dropping the suggestion to
                # zero contributing entries and showing "No suggestions yet"
                # even with 3+ valid included entries.
                if not result or not result.get('estimated'):
                    skipped_count += 1
                    continue

                eng_pred = float(result['estimated'])
                if eng_pred <= 0:
                    skipped_count += 1
                    continue

                ratio = float(r['negotiated_price_inr']) / eng_pred
                # Sanity: drop completely unreasonable ratios (engine bug or data error)
                if ratio < CALIB_MIN_REASONABLE_RATIO or ratio > CALIB_MAX_REASONABLE_RATIO:
                    skipped_count += 1
                    continue

                ratios.append(ratio)
                contributing_ids.append(r['id'])
            except Exception as e:
                app.logger.warning(f"calib ratio compute failed for entry {r.get('id')}: {e}")
                skipped_count += 1

        n_used = len(ratios)
        if n_used < CALIB_MIN_ENTRIES:
            continue

        # Outlier trim (top/bottom 10% by ratio value)
        trimmed_ratios = _percentile_trim(ratios, CALIB_OUTLIER_TRIM_PCT)
        median_ratio = _median(trimmed_ratios)
        if median_ratio is None:
            continue

        suggested_mult = round(current_mult * median_ratio, 3)

        # Compute gap pct (how different is suggestion from current)
        gap_pct = round(((suggested_mult - current_mult) / current_mult) * 100, 1) if current_mult else 0

        # Don't surface trivial nudges
        if abs(gap_pct) < CALIB_MIN_GAP_PCT_TO_SUGGEST:
            confidence = 'in_sync'
        elif n_used >= 6:
            confidence = 'high'
        elif n_used >= 4:
            confidence = 'medium'
        else:
            confidence = 'low'

        # Source breakdown for context
        n_mystery = sum(1 for r in entries if r.get('data_source') == 'Mystery Shopping')
        n_ff = sum(1 for r in entries if r.get('data_source') == 'Friends & Family')

        # Date range of contributing entries
        dates = [r.get('entry_date') for r in entries if r.get('entry_date')]
        date_min = min(dates) if dates else None
        date_max = max(dates) if dates else None

        # Format the date range for display
        def fmt_d(s):
            try:
                return datetime.strptime(s, '%Y-%m-%d').strftime('%d-%b-%Y')
            except (ValueError, TypeError):
                return s or '?'

        suggestions.append({
            'state_code': sc,
            'current_multiplier': round(current_mult, 3),
            'suggested_multiplier': suggested_mult,
            'gap_pct': gap_pct,
            'direction': 'up' if gap_pct > 0 else ('down' if gap_pct < 0 else 'same'),
            'sample_size': n_used,
            'sample_total': len(entries),
            'skipped_count': skipped_count,
            'mystery_count': n_mystery,
            'ff_count': n_ff,
            'median_ratio': round(median_ratio, 4),
            'confidence': confidence,  # 'in_sync', 'low', 'medium', 'high'
            'date_range_display': (
                f"{fmt_d(date_min)} → {fmt_d(date_max)}" if date_min and date_max else '—'
            ),
            'contributing_ids': contributing_ids,
        })

    # Sort: biggest absolute gaps first (most actionable)
    suggestions.sort(key=lambda s: (-abs(s['gap_pct']), -s['sample_size']))
    return suggestions


@app.route('/admin/research/<entry_id>/toggle-calibration', methods=['POST'])
@login_required
@admin_required
def admin_research_toggle_calibration(entry_id):
    """Flip the include_in_calibration flag on a research entry.
    Optionally accepts an exclusion_reason form field for context."""
    try:
        # Read current value
        r = supabase.table('research_log').select('include_in_calibration').eq('id', entry_id).execute()
        if not r.data:
            flash("Entry not found.", 'error')
            return redirect(url_for('admin_research'))
        cur = r.data[0].get('include_in_calibration', True)
        new_val = not cur

        update_payload = {'include_in_calibration': new_val}
        if not new_val:
            # Excluding — capture reason if provided
            reason = (request.form.get('exclusion_reason') or '').strip()
            if reason:
                update_payload['exclusion_reason'] = reason[:500]
        else:
            # Re-including — clear any prior exclusion reason
            update_payload['exclusion_reason'] = None

        supabase.table('research_log').update(update_payload).eq('id', entry_id).execute()
        flash(
            f"Entry {'included in' if new_val else 'excluded from'} calibration.",
            'success'
        )
    except Exception as e:
        app.logger.error(f"admin_research_toggle_calibration failed: {e}")
        flash(f"Could not toggle: {e}", 'error')
    return redirect(url_for('admin_research'))


@app.route('/admin/research/apply-suggestion/<state_code>', methods=['POST'])
@login_required
@admin_required
def admin_research_apply_suggestion(state_code):
    """Apply a suggested multiplier from the calibration panel.
    Expects 'suggested_multiplier' in the form (recomputed server-side
    for safety — never trust client value alone)."""
    admin = current_user()
    state_code = (state_code or '').strip().upper()
    if not state_code:
        flash("State code required.", 'error')
        return redirect(url_for('admin_research'))

    # Recompute the suggestion server-side so we apply the FRESH math,
    # not whatever value the client posted (which could be stale or tampered).
    try:
        r = (supabase.table('research_log')
             .select('*')
             .eq('state_code', state_code)
             .eq('include_in_calibration', True)
             .execute())
        rows = r.data or []
    except Exception as e:
        app.logger.error(f"apply-suggestion fetch failed: {e}")
        flash(f"Could not load research: {e}", 'error')
        return redirect(url_for('admin_research'))

    suggestions = _compute_calibration_suggestions(rows)
    matching = next((s for s in suggestions if s['state_code'] == state_code), None)
    if not matching:
        flash(
            f"No suggestion available for {state_code} — likely fewer than "
            f"{CALIB_MIN_ENTRIES} included entries, or all entries failed engine prediction.",
            'error'
        )
        return redirect(url_for('admin_research'))

    suggested_mult = matching['suggested_multiplier']
    current_mult = matching['current_multiplier']

    # Sanity bounds (matches the validation in admin_multipliers route)
    if suggested_mult < 0.50 or suggested_mult > 1.50:
        flash(
            f"Suggested multiplier {suggested_mult} is outside safe range [0.50, 1.50]. "
            f"Aborting auto-apply for safety. You can apply manually in /admin/multipliers if you really want to.",
            'error'
        )
        return redirect(url_for('admin_research'))

    # Apply to state_multipliers + write audit row
    try:
        update_fields = {
            'multiplier': suggested_mult,
            'updated_at': datetime.utcnow().isoformat(),
            'updated_by': admin.get('email'),
        }
        supabase.table('state_multipliers').update(update_fields).eq('state_code', state_code).execute()

        # Rich audit entry — captures the research basis
        try:
            audit_payload = {
                'state_code': state_code,
                'changed_by': admin.get('email'),
                'changes': json.dumps({
                    'multiplier': suggested_mult,
                    'previous_multiplier': current_mult,
                    'source': 'research_calibration',
                    'sample_size': matching['sample_size'],
                    'mystery_count': matching['mystery_count'],
                    'ff_count': matching['ff_count'],
                    'median_ratio': matching['median_ratio'],
                    'date_range': matching['date_range_display'],
                    'contributing_entry_ids': matching['contributing_ids'],
                }),
                'changed_at': datetime.utcnow().isoformat(),
            }
            supabase.table('state_multipliers_audit').insert(audit_payload).execute()
        except Exception as e_audit:
            app.logger.warning(f"audit insert failed: {e_audit}")

        # Force-refresh in-memory cache so the new multiplier takes effect immediately
        load_state_multipliers(force_refresh=True)

        flash(
            f"Applied: {state_code} multiplier {current_mult:.3f} → {suggested_mult:.3f} "
            f"(based on {matching['sample_size']} research entries).",
            'success'
        )
    except Exception as e:
        app.logger.error(f"apply-suggestion update failed: {e}")
        flash(f"Update failed: {e}", 'error')

    return redirect(url_for('admin_research'))


# ============================================================
# MISC ROUTES (preserved from current)
# ============================================================

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/db-test')
def db_test():
    try:
        r = supabase.table('users').select('id').limit(1).execute()
        return f"✅ Supabase Connected! Users table reachable. Sample rows: {len(r.data)}"
    except Exception as e:
        return f"❌ DB error: {e}", 500
# ============================================================
# Phase 3 admin price tools — routes (added v3.7.0)
# ============================================================
# These routes are gated behind _is_admin(). Non-admin users get redirected.
# All write operations go through the pending_reviews table — no direct
# writes to the sheet outside of /admin/price-tools/approve.
# ============================================================

@app.route('/admin/price-tools')
def price_tools():
    """Render the 4-tab admin Price Tools page."""
    user = session.get('user')
    if not _is_admin(user):
        flash('Admin access required.', 'error')
        return redirect(url_for('role'))

    # Fetch pending reviews for the queue
    pending_resp = supabase.table('pending_reviews').select('*').eq(
        'status', 'pending'
    ).order('created_at', desc=True).limit(200).execute()
    pending_rows = pending_resp.data or []

    # Annotate each row for display
    for r in pending_rows:
        cp = r.get('current_price')
        pp = r.get('proposed_price')
        if cp and pp and cp > 0:
            r['diff_pct'] = ((pp - cp) / cp) * 100
        else:
            r['diff_pct'] = None
        # human-readable created_at
        try:
            ca_str = r.get('created_at', '')
            if ca_str:
                ca = datetime.fromisoformat(ca_str.replace('Z', '+00:00'))
                r['created_at_display'] = ca.strftime('%d-%b %H:%M')
            else:
                r['created_at_display'] = ''
        except Exception:
            r['created_at_display'] = (r.get('created_at') or '')[:16]

    # Counts for tab badges
    pending_count = len(pending_rows)
    try:
        missing = car_data.find_missing_prices()
    except Exception as e:
        app.logger.warning(f'find_missing_prices failed: {e}')
        missing = []
    missing_count = len(missing)

    # Build car_data JSON for the Refresh One picker (make -> model -> True)
    car_data_for_picker = {}
    try:
        cd_dict = dict(car_data.CAR_DATA) if hasattr(car_data, 'CAR_DATA') else {}
    except Exception:
        cd_dict = {}

    for make, models in cd_dict.items():
        car_data_for_picker[make] = {}
        for model, _data in models.items():
            car_data_for_picker[make][model] = True

    # Currently flagged discontinued variants (read from cache)
    discontinued_rows = []
    for make, models in cd_dict.items():
        for model, mdata in models.items():
            vfs = (mdata or {}).get('variant_fuel_status', {}) or {}
            vfd = (mdata or {}).get('variant_fuel_dates', {}) or {}
            vfp = (mdata or {}).get('variant_fuel_prices', {}) or {}
            for variant, fuel_map in vfs.items():
                for fuel, st in fuel_map.items():
                    if st == 'discontinued':
                        date_str = (vfd.get(variant) or {}).get(fuel) or ''
                        price = (vfp.get(variant) or {}).get(fuel)
                        discontinued_rows.append({
                            'make': make, 'model': model,
                            'variant': variant, 'fuel': fuel,
                            'price': price,
                            'last_known_price_date': date_str,
                            'age_class': _age_class(date_str),
                        })

    return render_template(
        'admin_price_tools.html',
        user=user,
        makes=sorted(cd_dict.keys()),
        car_data_json=json.dumps(car_data_for_picker),
        pending_reviews=pending_rows,
        pending_count=pending_count,
        missing_rows=missing,
        missing_count=missing_count,
        discontinued_rows=discontinued_rows,
    )


@app.route('/admin/price-tools/scrape-model', methods=['POST'])
def price_tools_scrape_model():
    """
    Tab 1: Scrape every variant of a (make, model) from CarWale.
    Compare against sheet. Queue reviews for any that differ or are new.
    Returns JSON: {ok, rows: [...], reviews_created}
    """
    user = session.get('user')
    if not _is_admin(user):
        return jsonify({'ok': False, 'error': 'admin only'}), 403

    make = request.form.get('make', '').strip()
    model = request.form.get('model', '').strip()
    if not (make and model):
        return jsonify({'ok': False, 'error': 'make and model required'}), 400

    try:
        # Pull all sheet rows for this (make, model) — we scrape per-variant
        sheet_rows_for_model = []
        for r in sheets_writer.read_car_prices():
            if r.get('make') == make and r.get('model') == model:
                sheet_rows_for_model.append(r)

        if not sheet_rows_for_model:
            return jsonify({
                'ok': False,
                'error': f'No rows found in sheet for {make} {model}'
            }), 400

        # Probe with first row to get all_variants list + scraper_url
        probe = sheet_rows_for_model[0]
        scrape_result = price_scraper.fetch_price(
            make=make, model=model,
            variant=probe['variant'], fuel=probe['fuel'],
        )
        if not scrape_result.get('ok') and scrape_result.get('status') == 'error':
            return jsonify({
                'ok': False,
                'error': scrape_result.get('error', 'scraper error')
            }), 500

        all_carwale_variants = scrape_result.get('all_variants') or []
        scraper_url = scrape_result.get('url')

        rows_out = []
        reviews_created = 0

        # Per-variant fetch (rate-limited internally by price_scraper)
        for sheet_row in sheet_rows_for_model:
            v = sheet_row['variant']
            f = sheet_row['fuel']
            try:
                cur_price_str = (sheet_row.get('ex_showroom_price') or '').replace(',', '').strip()
                current_price = int(cur_price_str) if cur_price_str else None
            except (ValueError, TypeError):
                current_price = None

            r = price_scraper.fetch_price(make=make, model=model, variant=v, fuel=f)
            scraper_status = r.get('status', 'error')
            matched = r.get('matched_variant') or v
            proposed_price = r.get('ex_showroom_inr')

            outcome = 'unchanged'
            review_type = None
            diff_pct = None

            if scraper_status in ('not_found', 'not_found_fuel'):
                review_type = 'discontinued'
                outcome = 'queued'
                _create_pending_review(
                    supabase, 'discontinued', make, model, v, f,
                    current_price=current_price,
                    proposed_price=None,
                    matched_variant_name=matched,
                    scraper_status=scraper_status,
                    scraper_url=scraper_url,
                )
                reviews_created += 1
            elif proposed_price and current_price:
                diff_pct = ((proposed_price - current_price) / current_price) * 100
                if abs(diff_pct) < 0.5:
                    outcome = 'unchanged'
                else:
                    review_type = 'price_update'
                    outcome = 'queued'
                    _create_pending_review(
                        supabase, 'price_update', make, model, v, f,
                        current_price=current_price,
                        proposed_price=proposed_price,
                        matched_variant_name=matched,
                        scraper_status=scraper_status,
                        scraper_url=scraper_url,
                    )
                    reviews_created += 1
            elif proposed_price and not current_price:
                review_type = 'price_update'
                outcome = 'queued'
                _create_pending_review(
                    supabase, 'price_update', make, model, v, f,
                    current_price=None,
                    proposed_price=proposed_price,
                    matched_variant_name=matched,
                    scraper_status=scraper_status,
                    scraper_url=scraper_url,
                )
                reviews_created += 1
            else:
                outcome = 'no_price'

            rows_out.append({
                'variant': v,
                'fuel': f,
                'current_price': current_price,
                'proposed_price': proposed_price,
                'diff_pct': diff_pct,
                'matched_variant_name': matched,
                'scraper_status': scraper_status,
                'review_type': review_type,
                'outcome': outcome,
            })

        return jsonify({
            'ok': True,
            'make': make,
            'model': model,
            'rows': rows_out,
            'reviews_created': reviews_created,
            'all_carwale_variants_count': len(all_carwale_variants),
        })

    except Exception as e:
        app.logger.exception('price_tools_scrape_model failed')
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/admin/price-tools/scrape-one', methods=['POST'])
def price_tools_scrape_one():
    """
    Tab 2 (Find Missing): Scrape a single (make, model, variant, fuel) and
    queue a review. Returns JSON: {ok, review_id, ...}
    """
    user = session.get('user')
    if not _is_admin(user):
        return jsonify({'ok': False, 'error': 'admin only'}), 403

    make = request.form.get('make', '').strip()
    model = request.form.get('model', '').strip()
    variant = request.form.get('variant', '').strip()
    fuel = request.form.get('fuel', '').strip()

    if not all([make, model, variant, fuel]):
        return jsonify({'ok': False, 'error': 'all fields required'}), 400

    try:
        r = price_scraper.fetch_price(make=make, model=model,
                                       variant=variant, fuel=fuel)
        if not r.get('ok'):
            # Even on not_found, queue a discontinued review so admin sees it
            if r.get('status') in ('not_found', 'not_found_fuel'):
                rid = _create_pending_review(
                    supabase, 'discontinued', make, model, variant, fuel,
                    current_price=None,
                    proposed_price=None,
                    matched_variant_name=None,
                    scraper_status=r.get('status'),
                    scraper_url=r.get('url'),
                )
                return jsonify({'ok': True, 'review_id': rid,
                                'review_type': 'discontinued'})
            return jsonify({'ok': False,
                            'error': r.get('error', 'scraper error')}), 500

        proposed_price = r.get('ex_showroom_inr')
        if not proposed_price:
            return jsonify({'ok': True, 'review_id': None,
                            'message': 'No price returned'})

        rid = _create_pending_review(
            supabase, 'price_update', make, model, variant, fuel,
            current_price=None,
            proposed_price=proposed_price,
            matched_variant_name=r.get('matched_variant'),
            scraper_status=r.get('status'),
            scraper_url=r.get('url'),
        )
        return jsonify({'ok': True, 'review_id': rid,
                        'review_type': 'price_update'})

    except Exception as e:
        app.logger.exception('price_tools_scrape_one failed')
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/admin/price-tools/approve', methods=['POST'])
def price_tools_approve():
    """
    Tab 4: Approve a pending review. Writes to the sheet, marks review as
    approved, refreshes car_data cache.
    """
    user = session.get('user')
    if not _is_admin(user):
        return jsonify({'ok': False, 'error': 'admin only'}), 403

    review_id = request.form.get('review_id', '').strip()
    if not review_id:
        return jsonify({'ok': False, 'error': 'review_id required'}), 400

    try:
        # Fetch the review
        resp = supabase.table('pending_reviews').select('*').eq(
            'id', int(review_id)).limit(1).execute()
        if not resp.data:
            return jsonify({'ok': False, 'error': 'review not found'}), 404
        r = resp.data[0]

        if r['status'] != 'pending':
            return jsonify({'ok': False,
                            'error': f"review already {r['status']}"}), 400

        # Dispatch to the right write function
        rt = r['review_type']
        if rt == 'price_update':
            sheets_writer.write_price_update_v2(
                make=r['make'], model=r['model'],
                variant=r['variant'], fuel=r['fuel'],
                new_price=int(r['proposed_price']),
                source='CarWale',
            )
        elif rt == 'discontinued':
            sheets_writer.write_discontinued_flag(
                make=r['make'], model=r['model'],
                variant=r['variant'], fuel=r['fuel'],
            )
        elif rt == 'new_variant':
            sheets_writer.write_new_variant(
                make=r['make'], model=r['model'],
                variant=r['variant'], fuel=r['fuel'],
                ex_showroom_price=int(r['proposed_price']),
                source='CarWale',
            )
        else:
            return jsonify({'ok': False,
                            'error': f'unknown review_type: {rt}'}), 400

        # Mark review as approved
        supabase.table('pending_reviews').update({
            'status': 'approved',
            'reviewed_at': datetime.utcnow().isoformat(),
            'reviewed_by': user['email'],
        }).eq('id', int(review_id)).execute()

        # Refresh car_data cache so the change is live immediately
        try:
            car_data.refresh_prices(force=True)
        except Exception as e:
            app.logger.warning(f'car_data.refresh_prices failed after approval: {e}')

        return jsonify({'ok': True, 'review_id': int(review_id)})

    except Exception as e:
        app.logger.exception('price_tools_approve failed')
        return jsonify({'ok': False, 'error': str(e)}), 500


@app.route('/admin/price-tools/reject', methods=['POST'])
def price_tools_reject():
    """
    Tab 4: Reject a pending review. No sheet change. Marks review as rejected.
    """
    user = session.get('user')
    if not _is_admin(user):
        return jsonify({'ok': False, 'error': 'admin only'}), 403

    review_id = request.form.get('review_id', '').strip()
    if not review_id:
        return jsonify({'ok': False, 'error': 'review_id required'}), 400

    try:
        resp = supabase.table('pending_reviews').update({
            'status': 'rejected',
            'reviewed_at': datetime.utcnow().isoformat(),
            'reviewed_by': user['email'],
        }).eq('id', int(review_id)).eq('status', 'pending').execute()

        if not resp.data:
            return jsonify({'ok': False,
                            'error': 'review not found or already handled'}), 404

        return jsonify({'ok': True, 'review_id': int(review_id)})

    except Exception as e:
        app.logger.exception('price_tools_reject failed')
        return jsonify({'ok': False, 'error': str(e)}), 500
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)

# ============================================================
# END app.py — Part last
# Your app.py is now complete. Save the file and commit.
# ============================================================
