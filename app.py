from flask import Flask, render_template, request, session, redirect, url_for
from authlib.integrations.flask_client import OAuth
import os

app = Flask(__name__)
app.secret_key = 'autoknowmus_master_key'

# Google Login Config (You will need to add your Client ID/Secret in Render environment variables)
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id="YOUR_GOOGLE_CLIENT_ID",
    client_secret="YOUR_GOOGLE_CLIENT_SECRET",
    access_token_url='https://accounts.google.com/o/oauth2/token',
    access_token_params=None,
    authorize_url='https://accounts.google.com/o/oauth2/auth',
    authorize_params=None,
    api_base_url='https://www.googleapis.com/oauth2/v1/',
    client_kwargs={'scope': 'openid email profile'},
)

CITIES = sorted(["Ahmedabad", "Bangalore", "Chandigarh", "Chennai", "Delhi", "Gurgaon", "Hyderabad", "Jaipur", "Kochi", "Kolkata", "Mumbai", "Noida", "Pune", "Lucknow", "Indore"])
BRANDS = sorted(["Audi", "BMW", "Honda", "Hyundai", "Kia", "Mahindra", "Maruti Suzuki", "Mercedes-Benz", "MG Motors", "Skoda", "Tata Motors", "Toyota", "Volkswagen"])
CONDITIONS = ["Excellent (showroom like)", "Average (normal wear)", "Fair (needs some repair)"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login/google')
def login_google():
    return google.authorize_redirect(url_for('auth', _external=True))

@app.route('/auth')
def auth():
    token = google.authorize_access_token()
    user = google.get('userinfo').json()
    session['user'] = user
    session['credits'] = 300 # Initial Bonus
    return redirect(url_for('role'))

@app.route('/role', methods=['GET', 'POST'])
def role():
    user_name = session.get('user', {}).get('name', request.form.get('name', 'Rajeev Thakur'))
    session['credits'] = session.get('credits', 300)
    return render_template('role.html', user_name=user_name, credits=session['credits'])

@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years, brands=BRANDS, cities=CITIES, conditions=CONDITIONS, credits=session.get('credits', 0))

@app.route('/dashboard', methods=['POST'])
def dashboard():
    if session.get('credits', 0) >= 100:
        session['credits'] -= 100
        can_view = True
    else:
        can_view = False
    
    make = request.form.get('make', 'Toyota')
    base = 1450000 if make == "Toyota" else 1100000
    forecast = [int(base * (0.993**i)) for i in range(12)] # 15-day intervals
    return render_template('dashboard.html', base=base, forecast=forecast, can_view=can_view, credits=session.get('credits', 0))

@app.route('/buyer')
def buyer():
    return render_template('buyer.html', brands=BRANDS, cities=CITIES, conditions=CONDITIONS, credits=session.get('credits', 0))

if __name__ == '__main__':
    app.run(debug=True)
