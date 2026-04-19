import os
import re
import logging
import bcrypt
from datetime import datetime
from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session, flash
from authlib.integrations.flask_client import OAuth
from supabase import create_client, Client

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-change-me')

# ---------- Logging ----------
logging.basicConfig(level=logging.INFO)
app.logger.setLevel(logging.INFO)

# ---------- Jinja filters ----------
@app.template_filter('firstname')
def firstname_filter(full_name):
    """Extract first name from a full name string. Safe on None/empty."""
    if not full_name:
        return ''
    return str(full_name).split(' ')[0]

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
    """Store user info in session. layout.html uses session.user.name, session.user.email,
    and session.credits — so we mirror credits at both levels for safety."""
    session['user_id'] = user['id']
    session['user'] = {
        'name': user.get('name'),
        'email': user.get('email'),
        'credits': user.get('credits', 0)
    }
    session['credits'] = user.get('credits', 0)
    touch_last_login(user['id'])

def refresh_session_user(user: dict):
    """Sync session state after any DB change that affects credits/profile."""
    session['user'] = {
        'name': user.get('name'),
        'email': user.get('email'),
        'credits': user.get('credits', 0)
    }
    session['credits'] = user.get('credits', 0)

def current_user():
    """Fetch fresh user row from DB on every call — keeps credits in sync."""
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
            # Session-migration path: if old session has 'user' dict but no user_id,
            # silently create/link a DB row and log them in.
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
        return render_t
