import os
import re
import bcrypt
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash, jsonify
from authlib.integrations.flask_client import OAuth
from supabase import create_client, Client

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-change-me')

# ---------- Supabase ----------
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_SECRET_KEY = os.environ.get('SUPABASE_SECRET_KEY')  # legacy service_role JWT
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

def get_user_by_id(user_id: str):
    try:
        r = supabase.table('users').select('*').eq('id', user_id).limit(1).execute()
        return r.data[0] if r.data else None
    except Exception as e:
        app.logger.error(f"get_user_by_id error: {e}")
        return None

def create_user(name, email, password_hash=None, phone=None, whatsapp=None, google_id=None, credits=500):
    payload = {
        'name': name.strip(),
        'email': email.lower().strip(),
        'password_hash': password_hash,
        'phone': phone,
        'whatsapp': whatsapp,
        'google_id': google_id,
        'credits': credits,
        'created_at': datetime.utcnow().isoformat()
    }
    r = supabase.table('users').insert(payload).execute()
    return r.data[0] if r.data else None

def update_user(user_id: str, fields: dict):
    r = supabase.table('users').update(fields).eq('id', user_id).execute()
    return r.data[0] if r.data else None

def login_user_session(user: dict):
    session['user_id'] = user['id']
    session['user'] = {
        'name': user['name'],
        'email': user['email'],
        'credits': user.get('credits', 0)
    }

def current_user():
    """Fetch fresh user row from DB on every call — keeps credits in sync."""
    uid = session.get('user_id')
    if not uid:
        return None
    u = get_user_by_id(uid)
    if u:
        # Refresh cached session display
        session['user'] = {'name': u['name'], 'email': u['email'], 'credits': u.get('credits', 0)}
    return u

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('user_id'):
            # Session-migration path: if old session has 'user' dict but no user_id,
            # silently create a DB row and log them in.
            legacy = session.get('user')
            if legacy and legacy.get('email'):
                existing = get_user_by_email(legacy['email'])
                if existing:
                    login_user_session(existing)
                    return f(*args, **kwargs)
                migrated = create_user(
                    name=legacy.get('name', 'User'),
                    email=legacy['email'],
                    credits=legacy.get('credits', 500)
                )
                if migrated:
                    login_user_session(migrated)
                    return f(*args, **kwargs)
            return redirect(url_for('index'))
        return f(*args, **kwargs)
    return decorated

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
        # This email was registered via Google OAuth
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

    # POST
    form = {
        'name': (request.form.get('name') or '').strip(),
        'email': (request.form.get('email') or '').strip().lower(),
        'password': request.form.get('password') or '',
        'phone': (request.form.get('phone') or '').strip(),
        'wa_same': request.form.get('wa_same') == 'on',
        'whatsapp': (request.form.get('whatsapp') or '').strip(),
    }

    # Validation
    if not form['name'] or len(form['name']) < 2:
        return render_template('signup.html', form=form, error='Please enter your full name.')
    if not EMAIL_RE.match(form['email']):
        return render_template('signup.html', form=form, error='Please enter a valid email address.')
    if len(form['password']) < 8:
        return render_template('signup.html', form=form, error='Password must be at least 8 characters.')
    if not PHONE_RE.match(form['phone']):
        return render_template('signup.html', form=form, error='Please enter a valid 10-digit phone number.')

    # Duplicate check
    existing = get_user_by_email(form['email'])
    if existing:
        flash('An account with that email already exists. Please log in.', 'info')
        return redirect(url_for('index', email=form['email'], error='Account exists. Please log in.'))

    # WhatsApp logic
    whatsapp_final = form['phone'] if form['wa_same'] else form['whatsapp']
    if not form['wa_same'] and whatsapp_final and not PHONE_RE.match(whatsapp_final):
        return render_template('signup.html', form=form, error='Please enter a valid 10-digit WhatsApp number.')
    if not whatsapp_final:
        whatsapp_final = form['phone']

    # Create user
    try:
        new_user = create_user(
            name=form['name'],
            email=form['email'],
            password_hash=hash_password(form['password']),
            phone=form['phone'],
            whatsapp=whatsapp_final,
            credits=500
        )
    except Exception as e:
        app.logger.error(f"Signup insert failed: {e}")
        return render_template('signup.html', form=form, error='Something went wrong. Please try again.')

    if not new_user:
        return render_template('signup.html', form=form, error='Could not create account. Please try again.')

    login_user_session(new_user)
    flash(f"🎉 Welcome {form['name'].split(' ')[0]}! (500 Credits) added to get you started.", 'welcome')
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
        # Link google_id if missing
        if not existing.get('google_id'):
            update_user(existing['id'], {'google_id': google_id})
        login_user_session(existing)
        # If profile incomplete, send to /complete-profile
        if not existing.get('phone'):
            return redirect(url_for('complete_profile'))
        return redirect(url_for('role'))

    # New Google user — create minimal row, then force profile completion
    new_user = create_user(
        name=name,
        email=email,
        google_id=google_id,
        credits=500
    )
    if not new_user:
        return redirect(url_for('index', error='Could not create account. Please try again.'))

    login_user_session(new_user)
    return redirect(url_for('complete_profile'))

@app.route('/complete-profile', methods=['GET', 'POST'])
@login_required
def complete_profile():
    user = current_user()
    if not user:
        return redirect(url_for('index'))

    # Already complete? skip
    if user.get('phone'):
        return redirect(url_for('role'))

    if request.method == 'GET':
        return render_template('complete_profile.html', user=user, error='')

    phone = (request.form.get('phone') or '').strip()
    wa_same = request.form.get('wa_same') == 'on'
    whatsapp = (request.form.get('whatsapp') or '').strip()

    if not PHONE_RE.match(phone):
        return render_template('complete_profile.html', user=user, error='Please enter a valid 10-digit phone number.')

    whatsapp_final = phone if wa_same else whatsapp
    if not wa_same and whatsapp_final and not PHONE_RE.match(whatsapp_final):
        return render_template('complete_profile.html', user=user, error='Please enter a valid 10-digit WhatsApp number.')
    if not whatsapp_final:
        whatsapp_final = phone

    update_user(user['id'], {'phone': phone, 'whatsapp': whatsapp_final})
    flash(f"🎉 Welcome {user['name'].split(' ')[0]}! (500 Credits) added to get you started.", 'welcome')
    return redirect(url_for('role'))

@app.route('/role')
@login_required
def role():
    user = current_user()
    if not user:
        return redirect(url_for('index'))
    if not user.get('phone'):
        return redirect(url_for('complete_profile'))
    first_name = user['name'].split(' ')[0] if user.get('name') else 'there'
    return render_template('role.html', user=user, first_name=first_name)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

# ---------- DB health check (leave for now; remove before public launch) ----------
@app.route('/db-test')
def db_test():
    try:
        r = supabase.table('users').select('id').limit(1).execute()
        return f"✅ Supabase Connected! Users table reachable. Rows sample: {len(r.data)}"
    except Exception as e:
        return f"❌ DB error: {e}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)), debug=False)
