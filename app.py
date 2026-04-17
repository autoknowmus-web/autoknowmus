import os
from flask import Flask, render_template, request, session, redirect, url_for, flash
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = os.urandom(24)

# [FIX] Actual Google OAuth Configuration
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
    return render_template('index.html')

@app.route('/login/google')
def login():
    # [FIX] This route is now explicitly called by the button
    return google.authorize_redirect(url_for('auth', _external=True))

@app.route('/auth')
def auth():
    token = google.authorize_access_token()
    user = token.get('userinfo')
    if user:
        session['user_name'] = user['name']
        session['credits'] = 500 # [FIX] Synchronized bonus
        flash("Success! 500 Bonus Credits added to your account.")
    return redirect(url_for('role'))

@app.route('/role', methods=['GET', 'POST'])
def role():
    if request.method == 'POST':
        session['user_name'] = request.form.get('name')
        session['credits'] = 500
        flash("Success! 500 Bonus Credits added to your account.")
    
    name = session.get('user_name', 'User')
    return render_template('role.html', user_name=name, credits=session.get('credits', 500))

if __name__ == '__main__':
    app.run(debug=True)
