import os
from flask import Flask, render_template, request, session, redirect, url_for, flash, get_flashed_messages
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Google OAuth Setup
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
    if 'credits' not in session:
        session['credits'] = 0
    return render_template('index.html')

@app.route('/login/google')
def login():
    return google.authorize_redirect(url_for('auth', _external=True))

@app.route('/auth')
def auth():
    get_flashed_messages() # Clear old messages
    try:
        token = google.authorize_access_token()
        user = token.get('userinfo')
        if user:
            # Use name if available, otherwise email prefix
            session['user_name'] = user.get('name') or user.get('email').split('@')[0]
            session['credits'] = 500 
            session.modified = True
            flash("Success! 500 Bonus Credits added to your account.")
        return redirect(url_for('role'))
    except Exception:
        return redirect(url_for('index'))

@app.route('/role', methods=['GET', 'POST'])
def role():
    if request.method == 'POST':
        get_flashed_messages()
        session['user_name'] = request.form.get('name')
        session['credits'] = 500
        session.modified = True
        flash("Success! 500 Bonus Credits added to your account.")
    
    if 'user_name' not in session:
        return redirect(url_for('index'))
        
    return render_template('role.html', user_name=session.get('user_name'), credits=session.get('credits'))

@app.route('/seller')
def seller():
    if 'user_name' not in session:
        return redirect(url_for('index'))
    return render_template('seller.html')

@app.route('/buyer')
def buyer():
    if 'user_name' not in session:
        return redirect(url_for('index'))
    return render_template('buyer.html')

@app.route('/generate_report', methods=['POST'])
def generate_report():
    get_flashed_messages()
    current_credits = session.get('credits', 0)
    
    if current_credits >= 100:
        session['credits'] = current_credits - 100
        session.modified = True 
        
        session['last_search'] = {
            'make': request.form.get('make'),
            'model': request.form.get('model'),
            'variant': request.form.get('variant'),
            'year': request.form.get('year'),
            'city': "Bangalore",
            'fuel': request.form.get('fuel'),
            'condition': request.form.get('condition'),
            'mileage': request.form.get('mileage'),
            'owners': request.form.get('owners')
        }
        return redirect(url_for('dashboard'))
    else:
        flash("Insufficient credits!")
        return redirect(url_for('role'))

@app.route('/dashboard')
def dashboard():
    if 'user_name' not in session:
        return redirect(url_for('index'))
    search_data = session.get('last_search', {})
    return render_template('dashboard.html', data=search_data)

@app.route('/submit_deal')
def submit_deal():
    if 'user_name' not in session:
        return redirect(url_for('index'))
    return render_template('submit_deal.html')

@app.route('/process_deal', methods=['POST'])
def process_deal():
    get_flashed_messages()
    session['credits'] = session.get('credits', 0) + 200
    session.modified = True
    flash("Success! 200 Credits rewarded for your contribution.")
    return redirect(url_for('role'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
