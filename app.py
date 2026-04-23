import os
import re
import csv
import io
import uuid
import json
import math
import logging
import bcrypt
from datetime import datetime, timedelta
from functools import wraps
from collections import Counter, defaultdict
from flask import Flask, render_template, request, redirect, url_for, session, flash, Response, make_response
from authlib.integrations.flask_client import OAuth
from supabase import create_client, Client

from car_data import (
    CAR_DATA, get_makes, get_models, get_variants, get_fuels,
    compute_base_valuation, compute_price_range, adjust_with_deals,
    get_base_price, get_variant_base_price, get_phase_display,
    determine_phase, CURRENT_YEAR,
    BASE_PRICE_DATA_VERSION, BASE_PRICE_LAST_UPDATED,
    PHASE_THRESHOLDS, PHASE_BLEND,
)

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-change-me')

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ============================================================
# ADMIN ALLOWLIST — swap this placeholder with your email(s)
# TODO: migrate to a role flag on users table when you add a second admin
# ============================================================
ADMIN_EMAILS = {
    'YOUR_EMAIL@example.com',   # <-- REPLACE WITH YOUR EMAIL
}


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

# Phase detection lookback window
PHASE_LOOKBACK_DAYS = 180

# Guest access
GUEST_CREDITS = 100
GUEST_LOCKOUT_DAYS = 30
GUEST_COOKIE_NAME = 'ak_guest_token'
GUEST_COOKIE_MAX_AGE = 60 * 60 * 24 * GUEST_LOCKOUT_DAYS

HIGH_DEMAND_BRANDS = {'Maruti Suzuki', 'Hyundai', 'Honda', 'Toyota', 'Tata', 'Kia', 'Mahindra'}
MEDIUM_DEMAND_BRANDS = {'Ford', 'Renault', 'Nissan', 'Volkswagen', 'Skoda', 'MG'}
LUXURY_BRANDS = {'Audi', 'BMW', 'Mercedes-Benz', 'Jaguar', 'Land Rover', 'Lexus', 'Volvo'}

TRANSACTION_TYPE_LABELS = {
    'signup_bonus': 'Signup Bonus',
    'valuation_charge': 'Seller Valuation',
    'buyer_search': 'Buyer Search',
    'alert_subscription': 'Alert Subscription',
    'credit_request_approved': 'Credit Top-up',
    'deal_reward': 'Deal Reward',
    'alert_cancelled': 'Alert Cancelled',
}


def _format_txn_date(iso_str):
    if not iso_str:
        return ''
    try:
        clean = iso_str.replace('Z', '+00:00')
        dt = datetime.fromisoformat(clean)
        return dt.strftime('%d-%b-%Y %H:%M')
    except Exception:
        return iso_str


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


# ========== GUEST HELPERS ==========

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
    """Only users whose email is in ADMIN_EMAILS can access."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if session.get('is_guest'):
            return redirect(url_for('role'))
        user = current_user()
        if not user or (user.get('email') or '').lower() not in {e.lower() for e in ADMIN_EMAILS}:
            # Silently redirect to role — don't reveal admin route exists
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
# PHASE + COMPS HELPERS
# ============================================================

def fetch_similar_deals(make, model, variant, fuel, year, window_years=2,
                        lookback_days=PHASE_LOOKBACK_DAYS):
    """
    Fetch verified deals matching make+model+variant+fuel within year±2,
    submitted in last `lookback_days`.
    Fallback: if fewer than 3 variant-specific deals, widen to model-level.
    Returns list of sale_price ints.
    """
    try:
        year_low = int(year) - window_years
        year_high = int(year) + window_years
        cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()

        # Primary: variant-specific
        r = (supabase.table('deals')
             .select('sale_price')
             .eq('make', make)
             .eq('model', model)
             .eq('variant', variant)
             .eq('fuel', fuel)
             .eq('verified', True)
             .gte('year', year_low)
             .lte('year', year_high)
             .gte('created_at', cutoff)
             .execute())
        variant_deals = [row['sale_price'] for row in (r.data or []) if row.get('sale_price')]

        if len(variant_deals) >= 3:
            return variant_deals

        # Fallback: widen to model-wide (drop variant filter)
        r = (supabase.table('deals')
             .select('sale_price')
             .eq('make', make)
             .eq('model', model)
             .eq('fuel', fuel)
             .eq('verified', True)
             .gte('year', year_low)
             .lte('year', year_high)
             .gte('created_at', cutoff)
             .execute())
        return [row['sale_price'] for row in (r.data or []) if row.get('sale_price')]
    except Exception as e:
        app.logger.warning(f"fetch_similar_deals failed: {e}")
        return []


def compute_model_phase_data(make, model):
    """
    Compute phase for a (make, model) using verified deals in last 180 days.
    Returns dict:
      {
        'phase': int 1-4,
        'deal_count_180d': int,
        'distinct_users_180d': int,
        'display': {badge, detail, tooltip},
      }
    """
    try:
        cutoff = (datetime.utcnow() - timedelta(days=PHASE_LOOKBACK_DAYS)).isoformat()
        r = (supabase.table('deals')
             .select('user_id, sale_price')
             .eq('make', make)
             .eq('model', model)
             .eq('verified', True)
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
    """
    Returns (verified_180d, verified_all_time) — HONEST counts only.
    Removed fabricated "buyers_last_30d" synthetic metric.
    Templates should display: "N verified transactions in last 6 months".
    """
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).isoformat()
        r_recent = (supabase.table('deals')
                    .select('id', count='exact')
                    .eq('make', make)
                    .eq('model', model)
                    .eq('verified', True)
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
                 .execute())
        all_time = r_all.count or 0
    except Exception as e:
        app.logger.warning(f"market_stats all_time failed: {e}")
        all_time = 0

    return recent, all_time


def get_active_alert_subscription(user_id, make, model, variant):
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


# ========== ROUTES ==========

@app.route('/')
def index():
    if session.get('user_id') or session.get('is_guest'):
        return redirect(url_for('role'))
    prefill_email = request.args.get('email', '')
    error = request.args.get('error', '')
    return render_template('index.html', prefill_email=prefill_email, error=error)


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
            guest_used=guest_used)

    user = current_user()
    if not user:
        return redirect(url_for('index'))
    if not user.get('phone'):
        return redirect(url_for('complete_profile'))
    first_name = firstname_filter(user.get('name'))
    show_welcome = session.pop('show_welcome', False)
    active_alerts_count = count_active_alert_subscriptions(user['id'])

    is_admin = (user.get('email') or '').lower() in {e.lower() for e in ADMIN_EMAILS}

    return render_template('role.html', user=user, first_name=first_name,
                           show_welcome=show_welcome,
                           active_alerts_count=active_alerts_count,
                           guest_used=False,
                           is_admin=is_admin)


# ========== SELLER FLOW ==========

def _guest_block_if_used():
    if session.get('is_guest'):
        token = session.get('guest_token')
        if has_valid_guest_usage(token) or session.get('guest_used', False):
            flash('Your guest trial was used. Please sign up to continue — get 500 free credits.', 'error')
            return redirect(url_for('signup'))
    return None


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

    current_credits = session.get('credits', 0) if is_guest else (user.get('credits', 0) or 0)
    if current_credits < VALUATION_COST:
        return render_form(form_data,
                           error=f'Insufficient credits. You need {VALUATION_COST} credits to run a valuation.',
                           show_credit_request=not is_guest)

    estimated = compute_base_valuation(
        make=form_data['make'], model=form_data['model'], variant=form_data['variant'],
        fuel=form_data['fuel'], year=year_int, mileage=mileage_int,
        condition=form_data['condition'], owner=form_data['owner'],
    )
    if estimated is None:
        return render_form(form_data, error='Could not compute a price for this combination. Please check inputs.')

    # === PHASE + BLEND ===
    phase_data = compute_model_phase_data(form_data['make'], form_data['model'])
    phase = phase_data['phase']

    similar_prices = fetch_similar_deals(
        make=form_data['make'], model=form_data['model'],
        variant=form_data['variant'], fuel=form_data['fuel'],
        year=year_int,
    )
    adjusted, confidence = adjust_with_deals(estimated, similar_prices, phase=phase)
    price_low, price_high = compute_price_range(adjusted, phase=phase)

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

    # === PHASE + CONFIDENCE ===
    phase_data = compute_model_phase_data(val['make'], val['model'])
    phase = phase_data['phase']

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

    back_prefill = {
        'make':      val['make'],
        'fuel':      val['fuel'],
        'model':     val['model'],
        'variant':   val['variant'],
        'year':      val['year'],
        'owner':     val['owner'],
        'mileage':   val['mileage'],
        'condition': val['condition'],
    }

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
        data_version=BASE_PRICE_DATA_VERSION,
    )


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
        }
        kwargs = {k: v for k, v in kwargs.items() if v}
        return redirect(url_for('buyer_dashboard', **kwargs))

    ref = request.referrer or url_for('seller')
    return redirect(ref)


# ========== BUYER FLOW ==========

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
        'make':      form_data['make'],
        'fuel':      form_data['fuel'],
        'model':     form_data['model'],
        'variant':   form_data['variant'],
        'year':      form_data['year'],
        'owner':     form_data['owner'],
        'mileage':   form_data['mileage'],
        'condition': form_data['condition'],
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

    estimated = compute_base_valuation(
        make=make, model=model, variant=variant, fuel=fuel,
        year=year_int, mileage=mileage_int,
        condition=condition, owner=owner,
    )

    if estimated is None:
        return render_template('placeholder.html', user=user,
                               page_title='Analysis Unavailable',
                               message='Could not compute market analysis for this combination. Please try different filters.')

    # === PHASE + BLEND ===
    phase_data = compute_model_phase_data(make, model)
    phase = phase_data['phase']

    similar_prices = fetch_similar_deals(
        make=make, model=model, variant=variant, fuel=fuel, year=year_int,
    )
    adjusted, confidence = adjust_with_deals(estimated, similar_prices, phase=phase)
    price_low, price_high = compute_price_range(adjusted, phase=phase)

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
    }

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
        phase_data=phase_data,
        data_version=BASE_PRICE_DATA_VERSION,
    )


@app.route('/subscribe-alert', methods=['POST'])
@login_required
@no_guest(message='Sign up to set alert subscriptions on cars — free with 500 signup credits.')
def subscribe_alert():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    make     = (request.form.get('make') or '').strip()
    model    = (request.form.get('model') or '').strip()
    variant  = (request.form.get('variant') or '').strip()
    fuel     = (request.form.get('fuel') or '').strip()
    year_raw = (request.form.get('year') or '').strip()
    owner    = (request.form.get('owner') or '').strip()
    mileage_raw   = (request.form.get('mileage') or '').strip()
    condition     = (request.form.get('condition') or '').strip()
    asking_price_raw = (request.form.get('asking_price') or '').strip()

    email_enabled    = request.form.get('email_enabled') == 'on'
    whatsapp_enabled = request.form.get('whatsapp_enabled') == 'on'

    return_kwargs = {
        'make': make, 'fuel': fuel, 'model': model, 'variant': variant,
        'year': year_raw, 'owner': owner, 'mileage': mileage_raw,
        'condition': condition, 'asking_price': asking_price_raw,
    }
    return_kwargs = {k: v for k, v in return_kwargs.items() if v}

    if not all([make, model, variant]):
        flash('Missing car details. Please try again from the dashboard.', 'error')
        return redirect(url_for('buyer'))

    if not (email_enabled or whatsapp_enabled):
        flash('Please select at least one alert channel (Email or WhatsApp).', 'error')
        return redirect(url_for('buyer_dashboard', **return_kwargs))

    existing = get_active_alert_subscription(user['id'], make, model, variant)
    if existing:
        flash(f'You already have an active alert subscription for {make} {model} {variant}.', 'success')
        return redirect(url_for('buyer_dashboard', **return_kwargs))

    active_count = count_active_alert_subscriptions(user['id'])
    if active_count >= MAX_ACTIVE_ALERTS:
        flash(f'Maximum {MAX_ACTIVE_ALERTS} active alerts reached. Cancel an existing alert or wait for one to expire before subscribing to a new car.', 'error')
        return redirect(url_for('buyer_dashboard', **return_kwargs))

    current_credits = user.get('credits', 0) or 0
    if current_credits < ALERT_SUBSCRIPTION_COST:
        flash(f'Insufficient credits. You need {ALERT_SUBSCRIPTION_COST} credits to subscribe. Tap "Get {CREDIT_REQUEST_AMOUNT} Free Credits" below.', 'error')
        return redirect(url_for('buyer_dashboard', **return_kwargs))

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
        'make': make,
        'model': model,
        'variant': variant,
        'fuel': fuel or None,
        'year': year_int,
        'owner': owner or None,
        'mileage': mileage_int,
        'condition': condition or None,
        'reference_asking_price': asking_price_int,
        'email_enabled': bool(email_enabled),
        'whatsapp_enabled': bool(whatsapp_enabled),
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
        flash('Could not create subscription. Please try again.', 'error')
        return redirect(url_for('buyer_dashboard', **return_kwargs))

    if not sub_row:
        flash('Could not create subscription. Please try again.', 'error')
        return redirect(url_for('buyer_dashboard', **return_kwargs))

    new_balance = current_credits - ALERT_SUBSCRIPTION_COST
    try:
        update_user(user['id'], {'credits': new_balance})
        log_credit_transaction(
            user_id=user['id'],
            type_='alert_subscription',
            description=f"Alert subscription ({ALERT_SUBSCRIPTION_DAYS} days): {make} {model} {variant}",
            amount=-ALERT_SUBSCRIPTION_COST,
            balance_after=new_balance,
        )
        session['credits'] = new_balance
        if 'user' in session:
            session['user']['credits'] = new_balance
        session['active_alerts_count'] = active_count + 1
    except Exception as e:
        app.logger.error(f"Alert subscription credit deduction failed: {e}")

    channels = []
    if email_enabled:    channels.append('Email')
    if whatsapp_enabled: channels.append('WhatsApp')
    channels_str = ' and '.join(channels)
    flash(f"✅ Alerts active for {make} {model} {variant} via {channels_str} until {expires.strftime('%d-%b-%Y')}.", 'success')
    return redirect(url_for('buyer_dashboard', **return_kwargs))


@app.route('/my-alerts')
@login_required
@no_guest(message='Sign up to manage alert subscriptions.')
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

    active_subs = []
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

        if is_active_flag and expires_dt > now:
            active_subs.append(sub)
        else:
            if not is_active_flag:
                sub['expired_reason'] = 'Cancelled'
            else:
                sub['expired_reason'] = 'Expired'
            expired_subs.append(sub)

    active_count = len(active_subs)
    session['active_alerts_count'] = active_count

    return render_template(
        'my_alerts.html',
        user=user,
        first_name=firstname_filter(user.get('name')),
        active_subs=active_subs,
        expired_subs=expired_subs,
        active_count=active_count,
        max_alerts=MAX_ACTIVE_ALERTS
    )


@app.route('/cancel-alert/<alert_id>', methods=['POST'])
@login_required
@no_guest(message='Sign up to manage alerts.')
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
        flash('Alert not found or already cancelled.', 'error')
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
        flash('Could not cancel alert. Please try again.', 'error')
        return redirect(url_for('my_alerts'))

    car_label = f"{sub.get('make', '')} {sub.get('model', '')} {sub.get('variant', '')}".strip()
    try:
        supabase.table('transactions').insert({
            'user_id': user['id'],
            'type': 'alert_cancelled',
            'amount': 0,
            'balance_after': user.get('credits', 0),
            'description': f'Cancelled alert for {car_label} (no refund)'
        }).execute()
    except Exception as e:
        app.logger.warning(f"Log cancel transaction failed: {e}")

    session['active_alerts_count'] = count_active_alert_subscriptions(user['id'])

    flash(f'Alert for {car_label} cancelled. Slot freed (no credit refund).', 'success')
    return redirect(url_for('my_alerts'))


# ========== SUBMIT DEAL ==========

@app.route('/submit-deal', methods=['GET', 'POST'])
@login_required
@no_guest(message='Sign up to submit verified deals and earn 100 credits per deal.')
def submit_deal():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

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
    }

    weekly_count = count_recent_deals(user['id'], 7)
    if weekly_count >= MAX_DEALS_PER_WEEK:
        return render_form(
            form_data,
            error=f'Weekly limit reached. You can submit up to {MAX_DEALS_PER_WEEK} deals every 7 days. Please try again later.',
            weekly_count=weekly_count
        )

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
    }

    try:
        ins = supabase.table('deals').insert(payload).execute()
        deal_row = ins.data[0] if ins.data else None
    except Exception as e:
        app.logger.error(f"Deal insert failed: {e}")
        return render_form(form_data, error='Could not save your deal. Please try again.')

    if not deal_row:
        return render_form(form_data, error='Could not save your deal. Please try again.')

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
    flash(
        f"Deal recorded! +{DEAL_REWARD_AMOUNT} credits awarded. New balance: {new_balance} credits.{verified_msg}",
        'success'
    )
    return redirect(url_for('role'))


# ========== CREDIT HISTORY ==========

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
# ADMIN — DATA HEALTH DASHBOARD
# ============================================================

def _fetch_all_deals_180d():
    """Fetch all verified deals from the last 180 days for admin analysis."""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=180)).isoformat()
        r = (supabase.table('deals')
             .select('id, user_id, make, model, variant, sale_price, created_at, verified')
             .eq('verified', True)
             .gte('created_at', cutoff)
             .execute())
        return r.data or []
    except Exception as e:
        app.logger.error(f"admin _fetch_all_deals_180d failed: {e}")
        return []


def _fetch_all_deals_30d():
    """Used for trend comparison."""
    try:
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        r = (supabase.table('deals')
             .select('id', count='exact')
             .eq('verified', True)
             .gte('created_at', cutoff)
             .execute())
        return r.count or 0
    except Exception as e:
        app.logger.error(f"admin _fetch_all_deals_30d failed: {e}")
        return 0


def _fetch_all_deals_30_to_60d():
    """Prev-period count for trend delta."""
    try:
        cutoff_start = (datetime.utcnow() - timedelta(days=60)).isoformat()
        cutoff_end = (datetime.utcnow() - timedelta(days=30)).isoformat()
        r = (supabase.table('deals')
             .select('id', count='exact')
             .eq('verified', True)
             .gte('created_at', cutoff_start)
             .lt('created_at', cutoff_end)
             .execute())
        return r.count or 0
    except Exception as e:
        app.logger.error(f"admin _fetch_all_deals_30_to_60d failed: {e}")
        return 0


def _compute_phase_distribution(deals_180d):
    """Compute phase for every known model. Returns list of dicts."""
    # Group deals by (make, model)
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
    """Models close to next phase — sort by progress toward upgrade."""
    queue = []
    for mp in model_phases:
        current = mp['phase']
        if current >= 4:
            continue  # already top phase
        next_phase = current + 1
        deals_needed = PHASE_THRESHOLDS[next_phase][0] - mp['deal_count']
        users_needed = PHASE_THRESHOLDS[next_phase][1] - mp['distinct_users']
        # Only show models within striking distance (deals_needed <= 10)
        if deals_needed <= 10 and mp['deal_count'] > 0:
            queue.append({
                **mp,
                'next_phase': next_phase,
                'deals_needed': max(0, deals_needed),
                'users_needed': max(0, users_needed),
                'threshold_deals': PHASE_THRESHOLDS[next_phase][0],
                'threshold_users': PHASE_THRESHOLDS[next_phase][1],
            })
    # Sort by fewest deals needed (closest to upgrade first)
    queue.sort(key=lambda x: (x['deals_needed'], -x['deal_count']))
    return queue[:15]  # top 15


def _compute_guardrail_flags(deals_180d, model_phases):
    """Models where real-deal median deviates >15% from formula base estimate."""
    flags = []
    by_model = defaultdict(list)
    for d in deals_180d:
        key = (d.get('make'), d.get('model'))
        if key[0] and key[1] and d.get('sale_price'):
            by_model[key].append(d.get('sale_price'))

    for mp in model_phases:
        if mp['phase'] < 2:
            continue  # only flag for models with real data
        key = (mp['make'], mp['model'])
        prices = sorted(by_model.get(key, []))
        if len(prices) < 5:
            continue
        n = len(prices)
        median = prices[n // 2] if n % 2 == 1 else (prices[n // 2 - 1] + prices[n // 2]) // 2

        # Compare to model anchor (base variant price as rough model centroid)
        anchor = get_base_price(mp['make'], mp['model'])
        if not anchor:
            continue

        # Age-adjust anchor for ~3-year-old car (rough depreciation for comparison)
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

    # Sort by magnitude of deviation
    flags.sort(key=lambda x: -abs(x['deviation_pct']))
    return flags[:10]


def _compute_broker_signals(lookback_days=60):
    """
    Detect suspicious patterns that might indicate broker behavior.
    Returns list of users with flags.
    """
    try:
        cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()
        r = (supabase.table('deals')
             .select('id, user_id, make, model, sale_price, created_at')
             .gte('created_at', cutoff)
             .execute())
        deals = r.data or []
    except Exception as e:
        app.logger.error(f"admin broker signals fetch failed: {e}")
        return []

    # Group deals by user
    by_user = defaultdict(list)
    for d in deals:
        if d.get('user_id'):
            by_user[d['user_id']].append(d)

    signals = []
    for uid, user_deals in by_user.items():
        total = len(user_deals)
        if total < 3:
            continue  # not enough data to be suspicious

        flags = []

        # Flag 1: Clustered same model (5+ deals of same make+model)
        model_counts = Counter((d['make'], d['model']) for d in user_deals)
        top_model, top_count = model_counts.most_common(1)[0] if model_counts else (None, 0)
        if top_count >= 5:
            flags.append(f"Clustered: {top_count} deals of {top_model[0]} {top_model[1]}")

        # Flag 2: Wildly varied price segments (budget + luxury in same 60d window)
        prices = [d.get('sale_price', 0) for d in user_deals if d.get('sale_price')]
        if prices:
            mn, mx = min(prices), max(prices)
            if mn > 0 and mx > 0 and mx / mn >= 10:
                flags.append(f"Mixed segments: ₹{mn:,} to ₹{mx:,}")

        # Flag 3: Hit weekly cap repeatedly (3 deals in multiple 7-day windows)
        # Simple check: if user submitted 6+ deals in 60 days, they're hitting the cap
        if total >= 6:
            flags.append(f"{total} deals in {lookback_days} days (cap-hitter)")

        if not flags:
            continue

        # Fetch user info
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

    # Sort by number of flags + deal count
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

    # Phase distribution counts
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

    # Top models by deal volume
    top_models = sorted(
        [mp for mp in model_phases if mp['deal_count'] > 0],
        key=lambda x: -x['deal_count']
    )[:20]

    # Upgrade queue
    upgrade_queue = _compute_upgrade_queue(model_phases)

    # Guardrail flags
    guardrail_flags = _compute_guardrail_flags(deals_180d, model_phases)

    # Broker signals
    broker_signals = _compute_broker_signals()

    # Data freshness: models with zero deals in last 90d
    try:
        cutoff_90d = (datetime.utcnow() - timedelta(days=90)).isoformat()
        r = (supabase.table('deals')
             .select('make, model')
             .eq('verified', True)
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
        data_version=BASE_PRICE_DATA_VERSION,
        last_updated=BASE_PRICE_LAST_UPDATED,
        now_display=datetime.utcnow().strftime('%d-%b-%Y %H:%M UTC'),
    )


@app.route('/admin/flag-user/<user_id>', methods=['POST'])
@login_required
@admin_required
def admin_flag_user(user_id):
    """Soft-flag a user as suspected broker. Logs a note in transactions; does NOT block."""
    admin = current_user()
    target = get_user_by_id(user_id)
    if not target:
        flash('User not found.', 'error')
        return redirect(url_for('admin_data_health'))

    try:
        supabase.table('transactions').insert({
            'user_id': user_id,
            'type': 'alert_cancelled',  # reuse existing type — no schema change needed
            'amount': 0,
            'balance_after': target.get('credits', 0),
            'description': f'[ADMIN FLAG] Suspected broker — flagged by {admin.get("email")}',
        }).execute()
        flash(f'User {target.get("email")} flagged for review. (Not blocked — this is a soft flag.)', 'success')
    except Exception as e:
        app.logger.error(f"admin_flag_user failed: {e}")
        flash('Could not flag user. Please try again.', 'error')

    return redirect(url_for('admin_data_health'))


# ---------- Misc ----------

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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
