import os
from flask import Flask, render_template, request, session, redirect, url_for
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = os.urandom(24)

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get("GOOGLE_CLIENT_ID"), # This reads from the screen you're on!
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

@app.route('/auth')
def auth():
    token = google.authorize_access_token()
    user = token.get('userinfo')
    if user:
        session['user_name'] = user['name']
        session['email'] = user['email']
        # [FIX] Grant 300 Bonus Credits on very first login
        if 'has_claimed_bonus' not in session:
            session['credits'] = 300
            session['has_claimed_bonus'] = True
    return redirect(url_for('role'))
