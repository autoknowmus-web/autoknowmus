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
    compute_base_valuation, compute_price_range, adjust_with_deals
)

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-change-me')

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ---------- Jinja filters ----------
@app.template_filter('firstname')
def firstname_filter(full_name):
    if not full_name:
        return ''
    return str(full_name).split(' ')[0]

# ---------- Supabase ----------
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_SECRET_KEY = os.environ.get('SUPABASE_SECRET_KEY')
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SECRET_KEY)

# ---------- Google OAuth ----------
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# ========== HELPERS ==========

EMAIL_RE = re.compile(r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
PHONE_RE = re.compile(r'^\d{10}$')

CONDITIONS = ['Excellent', 'Good', 'Fair']
OWNERS = ['1st Owner', '2nd Owner', '3rd Owner or more']
YEAR_START = 2011
YEAR_END = 2026
YEARS = list(range(YEAR_END, YEAR_START - 1, -1))  # newest first
VALUATION_COST = 100


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
    """Record a credit movement in the transactions table."""
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

def fetch_similar_deals(make, model, year, fuel, window_years=2):
    """Query verified deals for same Make+Model, similar year and same fuel.
    Returns a list of sale_price integers."""
    try:
        year_low = int(year) - window_years
        year_high = int(year) + window_years
        r = (supabase.table('deals')
             .select('sale_price')
             .eq('make', make)
             .eq('model', model)
             .eq('fuel', fuel)
             .eq('verified', True)
             .gte('year', year_low)
             .lte('year', year_high)
             .execute())
        return [row['sale_price'] for row in (r.data or []) if row.get('sale_price')]
    except Exception as e:
        app.logger.warning(f"fetch_similar_deals failed: {e}")
        return []

# ========== ROUTES ==========

@app.route('/')
def index():
    if session.get('user_id'):
        return redirect(url_for('role'))
    prefill_email = request.args.get('email', '')
    error = request.args.get('error', '')
    return render_template('index.html', prefill_email=prefill_email, error=error)

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
    user = current_user()
    if not user:
        return redirect(url_for('index'))
    if not user.get('phone'):
        return redirect(url_for('complete_profile'))
    first_name = firstname_filter(user.get('name'))
    show_welcome = session.pop('show_welcome', False)
    return render_template('role.html', user=user, first_name=first_name, show_welcome=show_welcome)

# ========== SELLER FLOW (Stage 3A) ==========

@app.route('/seller', methods=['GET', 'POST'])
@login_required
def seller():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    def render_form(form_data, error=''):
        return render_template(
            'seller.html',
            form=form_data,
            error=error,
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

    # ----- POST -----
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
        return render_form(form_data, error=f'Insufficient credits. You need {VALUATION_COST} credits to run a valuation.')

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
        val = r.data[0] if r.data else None
    except Exception as e:
        app.logger.error(f"Load valuation failed: {e}")
        val = None

    if not val:
        return render_template('placeholder.html', user=user, page_title='Valuation Not Found',
                               message='We could not find that valuation.')

    msg = (f"Valuation #{val['id']} computed:<br>"
           f"<strong>{val['year']} {val['make']} {val['model']} {val['variant']}</strong> "
           f"({val['fuel']}, {val['mileage']} km, {val['condition']}, {val['owner']})<br><br>"
           f"Estimated: ₹ {val['estimated_price']:,}<br>"
           f"Range: ₹ {val['price_low']:,} — ₹ {val['price_high']:,}<br><br>"
           f"<em>Full dashboard ships in Stage 3A Part 2.</em>")
    return render_template('placeholder.html', user=user, page_title='Valuation Result', message=msg)


# ---------- Stage 3 stubs (Buyer + Submit Deal + Credit History — ship in 3B/3C) ----------

@app.route('/buyer')
@login_required
def buyer():
    user = current_user()
    return render_template('placeholder.html', user=user, page_title='Buyer Dashboard',
                           message='Buyer dashboard is coming in Stage 3B.')

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

# ---------- DB health check (remove before public launch) ----------
@app.route('/db-test')
def db_test():
    try:
        r = supabase.table('users').select('id').limit(1).execute()
        return f"✅ Supabase Connected! Users table reachable. Sample rows: {len(r.data)}"
    except Exception as e:
        return f"❌ DB error: {e}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
