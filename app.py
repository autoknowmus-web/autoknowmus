import os
from flask import Flask, render_template, request, session, redirect, url_for, flash
from authlib.integrations.flask_client import OAuth

app = Flask(__name__)
app.secret_key = os.urandom(24)

# [FIX] Actual Google OAuth Trigger
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get("GOOGLE_CLIENT_ID"),
    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET"),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# [FIX] Bangalore Only per instructions
CITIES = ["Bangalore"]
BRANDS = sorted(["Audi", "BMW", "Honda", "Hyundai", "Kia", "Mahindra", "Maruti Suzuki", "Mercedes-Benz", "MG Motors", "Skoda", "Tata Motors", "Toyota", "Volkswagen"])
CONDITIONS = ["Excellent (showroom like)", "Average (normal wear)", "Fair (needs some repair)"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login/google')
def login():
    return google.authorize_redirect(url_for('auth', _external=True))

@app.route('/auth')
def auth():
    token = google.authorize_access_token()
    user = token.get('userinfo')
    if user:
        session['user_name'] = user['name']
        session['credits'] = 500 # [FIX] Sync to 500
        flash("Success! 500 Bonus Credits added to your account.")
    return redirect(url_for('role'))

@app.route('/role', methods=['GET', 'POST'])
def role():
    name = session.get('user_name', request.form.get('name', 'Rajeev Thakur'))
    if 'credits' not in session: 
        session['credits'] = 500
        flash("Success! 500 Bonus Credits added to your account.")
    return render_template('role.html', user_name=name, credits=session['credits'])

@app.route('/seller')
def seller():
    return render_template('seller.html', years=list(range(2026, 2010, -1)), brands=BRANDS, cities=CITIES, conditions=CONDITIONS, credits=session.get('credits', 500))

@app.route('/dashboard', methods=['POST'])
def dashboard():
    # [FIX] Mandatory Mileage check
    mileage = request.form.get('mileage')
    if not mileage or int(mileage) <= 0:
        flash("Please enter mileage to proceed.")
        return redirect(url_for('seller'))
        
    if session.get('credits', 0) >= 100:
        session['credits'] -= 100
    
    make = request.form.get('make')
    base = 1450000 if make == "Toyota" else 1100000
    # [FIX] Logarithmic depreciation with 12 points (15-day intervals)
    forecast = [int(base * (0.991**i)) for i in range(12)]
    return render_template('dashboard.html', base=base, forecast=forecast, credits=session.get('credits'))

@app.route('/buyer')
def buyer():
    return render_template('buyer.html', brands=BRANDS, cities=CITIES, conditions=CONDITIONS, credits=session.get('credits', 500))

if __name__ == '__main__':
    app.run(debug=True)
