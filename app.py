import os
from flask import Flask, render_template, request, session, redirect, url_for, flash, get_flashed_messages
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "your-secret-key-change-this-in-production-12345")
app.config['SESSION_COOKIE_SECURE'] = True
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# =====================
# AUTHENTICATION ROUTES
# =====================

@app.route('/')
def index():
    """Landing page - login screen"""
    if session.get('user_name'):
        return redirect(url_for('role'))
    return render_template('index.html')

@app.route('/login/google')
def login_google():
    """Initiate Google OAuth"""
    redirect_uri = url_for('auth', _external=True, _scheme='https')
    return google.authorize_redirect(redirect_uri)

@app.route('/auth')
def auth():
    """Google OAuth callback - initialize credits and redirect to role selection"""
    try:
        token = google.authorize_access_token()
        user = token.get('userinfo')
        if user:
            session['user_name'] = user.get('name') or user.get('email').split('@')[0]
            session['credits'] = 500
            session['city'] = 'Bangalore'
            session['credit_transactions'] = [
                {'type': 'bonus', 'amount': 500, 'description': 'Login Bonus - 5 Free Searches', 'date': 'Today'},
            ]
            session.modified = True
            flash('🎉 Welcome! 500 bonus credits added for 5 free searches!', 'success')
        return redirect(url_for('role'))
    except Exception as e:
        print(f"Auth error: {e}")
        # If CSRF error, clear session and retry
        session.clear()
        flash('Session expired. Please login again.', 'danger')
