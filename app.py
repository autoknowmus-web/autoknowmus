import os
import re
import json
import logging
import bcrypt
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash
from authlib.integrations.flask_client import OAuth
from supabase import create_client, Client

from car_data import (
    CAR_DATA, get_makes, get_models, get_variants, get_fuels,
    compute_base_valuation, compute_price_range, adjust_with_deals,
    get_base_price, CURRENT_YEAR
)

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-change-me')

logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

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

CONDITIONS = ['Excellent', 'Good', 'Fair']
OWNERS = ['1st Owner', '2nd Owner', '3rd Owner or more']
YEAR_START = 2011
YEAR_END = 2026
YEARS = list(range(YEAR_END, YEAR_START - 1, -1))
VALUATION_COST = 100
CREDIT_REQUEST_AMOUNT = 500

HIGH_DEMAND_BRANDS = {'Maruti Suzuki', 'Hyundai', 'Honda', 'Toyota', 'Tata', 'Kia', 'Mahindra'}
MEDIUM_DEMAND_BRANDS = {'Ford', 'Renault', 'Nissan', 'Volkswagen', 'Skoda', 'MG'}
LUXURY_BRANDS = {'Audi', 'BMW', 'Mercedes-Benz', 'Jaguar', 'Land Rover', 'Lexus', 'Volvo'}


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

def login_user_session(user: dict):
    session['user_id'] = user['id']
    session['user'] = {
        'name': user.get('name'),
        'email': user.get('email'),
        'credits': user.get('credits', 0)
    }
    session['credits'] = user.get('credits', 0)
    touch_last_login(user['id'])

def refresh_session_user(user: dict):
    session['user'] = {
        'name': user.get('name'),
        'email': user.get('email'),
        'credits': user.get('credits', 0)
    }
    session['credits'] = user.get('credits', 0)

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

def log_credit_transaction(user_id, type_, description, amount, balance_after):
    try:
        supabase.table('transactions').insert({
            'user_id': user_id,
            'type': type_,
            'description': description,
            'amount': amount,
            'balance_after': balance_after,
            'created_at': datetime.utcnow().isoformat(),
        }).execute()
    except Exception as e:
        app.logger.error(f"log_credit_transaction failed: {e}")

def fetch_similar_deals(make, model, year, fuel, window_years=2):
    try:
        min_year = max(year - window_years, YEAR_START)
        max_year = min(year + window_years, YEAR_END)
        r = (supabase.table('deals')
             .select('estimated_price')
             .eq('make', make)
             .eq('model', model)
             .eq('fuel', fuel)
             .gte('year', min_year)
             .lte('year', max_year)
             .limit(100)
             .execute())
        if r.data:
            return [d.get('estimated_price') for d in r.data if d.get('estimated_price')]
        return []
    except Exception as e:
        app.logger.warning(f"fetch_similar_deals failed: {e}")
        return []


def compute_demand(make, year):
    if make in HIGH_DEMAND_BRANDS:
        base = 75
    elif make in MEDIUM_DEMAND_BRANDS:
        base = 55
    elif make in LUXURY_BRANDS:
        base = 45
    else:
        base = 50
    age_penalty = max(0, (YEAR_END - year) * 1.5)
    return max(20, min(100, base - age_penalty))


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
    import math
    series = []
    for d in range(0, days + 1):
        frac = d / days if days else 0
        decay = 1.0 - 0.05 * (1 - math.exp(-2.5 * frac))
        price = int(round(current_price * decay))
        series.append({'day': d, 'price': price})
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
    try:
        r = (supabase.table('deals')
             .select('id', count='exact')
             .eq('make', make)
             .eq('model', model)
             .eq('verified', True)
             .execute())
        verified_count = r.count or 0
    except Exception as e:
        app.logger.warning(f"market_stats failed: {e}")
        verified_count = 0

    buyers_last_30d = max(5, verified_count * 3)
    avg = buyers_last_30d / 30.0
    lo = int(avg)
    hi = lo + 1
    return verified_count, buyers_last_30d, f"{lo}-{hi}"


# ========== ROUTES ==========

@app.route('/')
def index():
    user = current_user() if session.get('user_id') else None
    return render_template('index.html', user=user)


@app.route('/login', methods=['POST'])
def login():
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password') or ''
    
    if not email or not password:
        return render_template('login.html', error='Email and password required.')
    
    user = get_user_by_email(email)
    if not user:
        return render_template('login.html', error='User not found.')
    
    if not verify_password(password, user.get('password_hash')):
        return render_template('login.html', error='Incorrect password.')
    
    login_user_session(user)
    return redirect(url_for('role'))


@app.route('/signup', methods=['GET', 'POST'])
def signup():
    if request.method == 'GET':
        return render_template('signup.html')
    
    name = (request.form.get('name') or '').strip()
    email = (request.form.get('email') or '').strip().lower()
    password = request.form.get('password') or ''
    phone = (request.form.get('phone') or '').strip()
    whatsapp_phone = (request.form.get('whatsapp_phone') or '').strip()
    is_whatsapp = request.form.get('is_whatsapp') == 'on'
    
    if not name or not email or not password or not phone:
        return render_template('signup.html', error='All fields required.')
    
    if not EMAIL_RE.match(email):
        return render_template('signup.html', error='Invalid email format.')
    
    if not PHONE_RE.match(phone):
        return render_template('signup.html', error='Phone must be 10 digits.')
    
    if get_user_by_email(email):
        return render_template('signup.html', error='Email already registered.')
    
    pwd_hash = hash_password(password)
    user = create_user(
        name=name,
        email=email,
        password_hash=pwd_hash,
        phone=phone,
        whatsapp_phone=whatsapp_phone if not is_whatsapp else None,
        is_whatsapp=is_whatsapp,
        auth_method='manual',
        credits=500,
    )
    
    if not user:
        return render_template('signup.html', error='Signup failed. Please try again.')
    
    login_user_session(user)
    return redirect(url_for('complete_profile'))


@app.route('/google-login')
def google_login():
    redirect_uri = url_for('google_callback', _external=True)
    return google.authorize_redirect(redirect_uri)


@app.route('/google-callback')
def google_callback():
    try:
        token = google.authorize_access_token()
    except Exception as e:
        app.logger.error(f"Google callback error: {e}")
        return redirect(url_for('index'))
    
    user_info = token.get('userinfo')
    if not user_info:
        return redirect(url_for('index'))
    
    email = user_info.get('email', '').lower()
    name = user_info.get('name', 'Google User')
    google_id = user_info.get('sub')
    
    existing = get_user_by_email(email)
    if existing:
        login_user_session(existing)
        return redirect(url_for('role'))
    
    user = create_user(
        name=name,
        email=email,
        google_id=google_id,
        auth_method='google',
        credits=500,
    )
    
    if user:
        login_user_session(user)
        return redirect(url_for('complete_profile'))
    
    return redirect(url_for('index'))


@app.route('/complete-profile', methods=['GET', 'POST'])
@login_required
def complete_profile():
    user = current_user()
    
    if request.method == 'GET':
        return render_template('complete_profile.html', user=user, form=user or {})
    
    phone = (request.form.get('phone') or '').strip()
    whatsapp_phone = (request.form.get('whatsapp_phone') or '').strip()
    is_whatsapp = request.form.get('is_whatsapp') == 'on'
    
    if not phone:
        return render_template('complete_profile.html', user=user, error='Phone number required.')
    
    if not PHONE_RE.match(phone):
        return render_template('complete_profile.html', user=user, error='Phone must be 10 digits.')
    
    wp_phone = whatsapp_phone if not is_whatsapp else None
    update_user(user['id'], {
        'phone': phone,
        'whatsapp_phone': wp_phone,
        'is_whatsapp': is_whatsapp,
    })
    
    user = get_user_by_id(user['id'])
    if user:
        refresh_session_user(user)
    
    return redirect(url_for('role'))


@app.route('/role')
@login_required
def role():
    user = current_user()
    return render_template(
        'role.html',
        user=user,
        page_title='Choose Your Role',
        message='🎉 Welcome! 500 bonus credits added for 5 free searches!'
    )


@app.route('/seller', methods=['GET', 'POST'])
@login_required
def seller():
    user = current_user()
    if not user:
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

    for key in ('make', 'fuel', 'model', 'variant', 'year', 'owner', 'mileage', 'condition'):
        if not form_data[key]:
            return render_form(form_data, error='Please fill in all fields.')

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

    current_credits = user.get('credits', 0) or 0
    if current_credits < VALUATION_COST:
        return render_form(form_data,
                           error=f'Insufficient credits. You need {VALUATION_COST} credits to run a valuation.',
                           show_credit_request=True)

    estimated = compute_base_valuation(
        make=form_data['make'], model=form_data['model'], variant=form_data['variant'],
        fuel=form_data['fuel'], year=year_int, mileage=mileage_int,
        condition=form_data['condition'], owner=form_data['owner'],
    )
    if estimated is None:
        return render_form(form_data, error='Could not compute a price for this combination. Please check inputs.')

    similar_prices = fetch_similar_deals(
        make=form_data['make'], model=form_data['model'],
        year=year_int, fuel=form_data['fuel']
    )
    adjusted, confidence = adjust_with_deals(estimated, similar_prices)
    price_low, price_high = compute_price_range(adjusted)

    try:
        val_payload = {
            'user_id': user['id'],
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


@app.route('/seller-dashboard/<int:valuation_id>')
@login_required
def seller_dashboard(valuation_id):
    user = current_user()
    
    try:
        r = supabase.table('valuations').select('*').eq('id', valuation_id).eq('user_id', user['id']).limit(1).execute()
        valuation_row = r.data[0] if r.data else None
    except Exception as e:
        app.logger.error(f"Valuation lookup failed: {e}")
        valuation_row = None
    
    if not valuation_row:
        return render_template('placeholder.html', user=user, page_title='Not Found',
                               message='Valuation not found or access denied.'), 404
    
    deprecation_series = compute_depreciation_series(valuation_row['estimated_price'], days=90)
    demand = compute_demand(valuation_row['make'], valuation_row['year'])
    buyer_distribution = compute_buyer_distribution(
        valuation_row['price_low'],
        valuation_row['price_high'],
        valuation_row.get('confidence', 70)
    )
    
    demand_label = 'HIGH' if demand > 70 else 'MEDIUM' if demand > 40 else 'LOW'
    days_to_sell = compute_days_to_sell(demand_label, valuation_row['estimated_price'])
    
    market_stats = get_market_stats(valuation_row['make'], valuation_row['model'])
    
    dashboard = {
        'valuation_id': valuation_id,
        'make': valuation_row.get('make'),
        'model': valuation_row.get('model'),
        'variant': valuation_row.get('variant'),
        'year': valuation_row.get('year'),
        'fuel': valuation_row.get('fuel'),
        'mileage': valuation_row.get('mileage'),
        'condition': valuation_row.get('condition'),
        'owner': valuation_row.get('owner'),
        'estimated_price': valuation_row.get('estimated_price'),
        'price_low': valuation_row.get('price_low'),
        'price_high': valuation_row.get('price_high'),
        'confidence': valuation_row.get('confidence', 70),
        'demand': demand,
        'days_to_sell': days_to_sell,
        'deprecation_series': deprecation_series,
        'buyer_distribution': buyer_distribution,
        'market_stats': market_stats,
    }
    
    return render_template('dashboard.html', user=user, data=dashboard)


@app.route('/request-credits', methods=['POST'])
@login_required
def request_credits():
    user = current_user()
    
    form_data = {}
    if request.referrer and '/seller' in request.referrer:
        form_data = {
            'make': request.args.get('make', ''),
            'fuel': request.args.get('fuel', ''),
            'model': request.args.get('model', ''),
            'variant': request.args.get('variant', ''),
            'year': request.args.get('year', ''),
            'owner': request.args.get('owner', ''),
            'mileage': request.args.get('mileage', ''),
            'condition': request.args.get('condition', ''),
        }
    
    new_balance = (user.get('credits') or 0) + CREDIT_REQUEST_AMOUNT
    try:
        update_user(user['id'], {'credits': new_balance})
        log_credit_transaction(
            user_id=user['id'],
            type_='credit_top_up',
            description=f'Auto-approved top-up ({CREDIT_REQUEST_AMOUNT} credits)',
            amount=CREDIT_REQUEST_AMOUNT,
            balance_after=new_balance,
        )
        session['credits'] = new_balance
        session['user']['credits'] = new_balance
    except Exception as e:
        app.logger.error(f"Credit top-up failed: {e}")
    
    if request.referrer and '/seller' in request.referrer:
        kwargs = {k: v for k, v in form_data.items() if v}
        return redirect(url_for('seller', **kwargs))

    ref = request.referrer or url_for('seller')
    return redirect(ref)


# ---------- BUYER ROUTE (FIXED) ----------

@app.route('/buyer', methods=['GET', 'POST'])
@login_required
def buyer():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    def render_form(form_data, error='', show_credit_request=False):
        return render_template(
            'buyer.html',
            form=form_data,
            error=error,
            show_credit_request=show_credit_request,
            makes=get_makes(),
            years=YEARS,
            conditions=CONDITIONS,
            car_data_json=json.dumps(CAR_DATA),
        )

    if request.method == 'GET':
        prefill = {
            'make':        request.args.get('make', ''),
            'fuel':        request.args.get('fuel', ''),
            'model':       request.args.get('model', ''),
            'variant':     request.args.get('variant', ''),
            'year':        request.args.get('year', ''),
            'condition':   request.args.get('condition', ''),
            'asking_price': request.args.get('asking_price', ''),
        }
        return render_form(prefill)

    # POST: Process buyer search
    form_data = {
        'make':        (request.form.get('make') or '').strip(),
        'fuel':        (request.form.get('fuel') or '').strip(),
        'model':       (request.form.get('model') or '').strip(),
        'variant':     (request.form.get('variant') or '').strip(),
        'year':        (request.form.get('year') or '').strip(),
        'condition':   (request.form.get('condition') or '').strip(),
        'asking_price': (request.form.get('asking_price') or '').strip(),
    }

    # Validate required fields
    required = ['make', 'fuel', 'model', 'year', 'condition']
    for key in required:
        if not form_data[key]:
            return render_form(form_data, error='Please fill in all required fields.')

    # Validate car data
    if form_data['make'] not in CAR_DATA:
        return render_form(form_data, error='Invalid Make selected.')
    if form_data['model'] not in CAR_DATA[form_data['make']]:
        return render_form(form_data, error='Invalid Model selected.')
    if form_data['fuel'] not in CAR_DATA[form_data['make']][form_data['model']]['fuels']:
        return render_form(form_data, error='Selected Fuel is not available for this Model.')
    if form_data['condition'] not in CONDITIONS:
        return render_form(form_data, error='Invalid Condition selected.')

    # Validate year
    try:
        year_int = int(form_data['year'])
        if year_int < YEAR_START or year_int > YEAR_END:
            raise ValueError
    except (ValueError, TypeError):
        return render_form(form_data, error='Invalid Year.')

    # If asking price is provided, validate and clean it
    asking_price = None
    if form_data['asking_price']:
        try:
            # Remove commas from asking price
            clean_price = form_data['asking_price'].replace(',', '')
            asking_price = int(clean_price)
            if asking_price < 100000 or asking_price > 50000000:
                raise ValueError
        except (ValueError, TypeError):
            return render_form(form_data, error='Asking Price must be numeric and between ₹1,00,000 and ₹5,00,00,000.')

    # Check credits (100 per search)
    current_credits = user.get('credits', 0) or 0
    if current_credits < VALUATION_COST:
        return render_form(form_data,
                           error=f'Insufficient credits. You need {VALUATION_COST} credits to run a search.',
                           show_credit_request=True)

    # Compute market intelligence
    try:
        # Compute base valuation - fixed to handle empty variant
        base_price = compute_base_valuation(
            make=form_data['make'],
            model=form_data['model'],
            variant=form_data.get('variant') or '',
            fuel=form_data['fuel'],
            year=year_int,
            mileage=50000,
            condition=form_data['condition'],
            owner='2nd Owner',
        )
        
        if base_price is None:
            return render_form(form_data, error='Could not compute a price for this combination. Please check inputs.')
        
        # Fetch similar deals and adjust
        similar = fetch_similar_deals(
            make=form_data['make'],
            model=form_data['model'],
            year=year_int,
            fuel=form_data['fuel']
        )
        
        adjusted, confidence = adjust_with_deals(base_price, similar)
        price_low, price_high = compute_price_range(adjusted)
        demand = compute_demand(form_data['make'], year_int)
        
        market_stats = get_market_stats(form_data['make'], form_data['model'])

        # Deduct credits (100 per search)
        new_balance = current_credits - VALUATION_COST
        try:
            update_user(user['id'], {'credits': new_balance})
            log_credit_transaction(
                user_id=user['id'],
                type_='buyer_search',
                description=f"Buyer Search: {form_data['year']} {form_data['make']} {form_data['model']}",
                amount=-VALUATION_COST,
                balance_after=new_balance,
            )
            session['credits'] = new_balance
            session['user']['credits'] = new_balance
        except Exception as e:
            app.logger.error(f"Credit deduction failed: {e}")

        # Build dashboard data
        deprecation_series = compute_depreciation_series(adjusted, days=365)
        buyer_distribution = compute_buyer_distribution(price_low, price_high, confidence)
        demand_label = 'HIGH' if demand > 70 else 'MEDIUM' if demand > 40 else 'LOW'
        days_to_sell = compute_days_to_sell(demand_label, adjusted)

        dashboard_data = {
            'make': form_data['make'],
            'model': form_data['model'],
            'variant': form_data.get('variant') or '',
            'year': year_int,
            'fuel': form_data['fuel'],
            'condition': form_data['condition'],
            'asking_price': asking_price,
            'estimated_price': adjusted,
            'price_low': price_low,
            'price_high': price_high,
            'confidence': confidence,
            'demand': demand,
            'days_to_sell': days_to_sell,
            'deprecation_series': deprecation_series,
            'buyer_distribution': buyer_distribution,
            'market_stats': market_stats,
        }

        return render_template('buyer_dashboard.html', user=user, data=dashboard_data)

    except Exception as e:
        app.logger.error(f"Buyer search failed: {e}")
        app.logger.error(f"Error traceback: {repr(e)}")
        return render_form(form_data, error=f'Could not fetch market data. Please try again. Error: {str(e)}')


@app.route('/submit-deal')
@login_required
def submit_deal():
    user = current_user()
    return render_template('placeholder.html', user=user, page_title='Record Transaction',
                           message='Deal submission form is coming in Stage 3B.')

@app.route('/credit-history')
@login_required
def credit_history():
    user = current_user()
    return render_template('placeholder.html', user=user, page_title='Credit History',
                           message='Credit history is coming in Stage 3C.')

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
