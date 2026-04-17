import os
from flask import Flask, render_template, request, session, redirect, url_for, flash
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = os.urandom(24)

oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

@app.route('/')
def index():
    # [FIX] Credits show 0 before login
    if 'user_name' not in session:
        session['credits'] = 0
    return render_template('index.html')

@app.route('/login/google')
def login():
    return google.authorize_redirect(url_for('auth', _external=True))

@app.route('/auth')
def auth():
    token = google.authorize_access_token()
    user = token.get('userinfo')
    if user:
        # [FIX] Safety check: use email prefix if name is missing (fixes 500 error)
        session['user_name'] = user.get('name') or user.get('email').split('@')[0]
        session['credits'] = 500 
        flash("Success! 500 Bonus Credits added to your account.")
    return redirect(url_for('role'))

@app.route('/role', methods=['GET', 'POST'])
def role():
    if request.method == 'POST':
        session['user_name'] = request.form.get('name')
        session['credits'] = 500
        flash("Success! 500 Bonus Credits added to your account.")
    return render_template('role.html', user_name=session.get('user_name'), credits=session.get('credits'))

@app.route('/seller')
def seller():
    # [FIX] No welcome message on this screen
    return render_template('seller.html', credits=session.get('credits', 0))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
