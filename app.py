from flask import Flask, render_template, redirect, url_for, session, request, flash
from authlib.integrations.flask_client import OAuth
from datetime import datetime
import os
import traceback

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-me')

# ========== GOOGLE OAUTH ==========
oauth = OAuth(app)
google = oauth.register(
    name='google',
    client_id=os.environ.get('GOOGLE_CLIENT_ID'),
    client_secret=os.environ.get('GOOGLE_CLIENT_SECRET'),
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    client_kwargs={'scope': 'openid email profile'}
)

# ========== MASTER DATA ==========
CAR_BRANDS = sorted([
    'Audi', 'BMW', 'Ford', 'Honda', 'Hyundai', 'Jaguar', 'Kia',
    'Land Rover', 'Lexus', 'Mahindra', 'Maruti Suzuki', 'Mercedes-Benz',
    'MG', 'Nissan', 'Renault', 'Skoda', 'Tata', 'Toyota', 'Volkswagen', 'Volvo'
])

MODELS_BY_BRAND = {
    'Audi': sorted(['A3', 'A4', 'A6', 'Q3', 'Q5', 'Q7']),
    'BMW': sorted(['3 Series', '5 Series', 'X1', 'X3', 'X5', 'X7']),
    'Ford': sorted(['EcoSport', 'Endeavour', 'Figo', 'Freestyle']),
    'Honda': sorted(['Amaze', 'City', 'Civic', 'Elevate', 'Jazz', 'WR-V']),
    'Hyundai': sorted(['Alcazar', 'Aura', 'Creta', 'Exter', 'Grand i10 Nios', 'i20', 'Kona Electric', 'Tucson', 'Venue', 'Verna']),
    'Jaguar': sorted(['F-Pace', 'XE', 'XF']),
    'Kia': sorted(['Carens', 'Carnival', 'EV6', 'Seltos', 'Sonet']),
    'Land Rover': sorted(['Defender', 'Discovery', 'Range Rover', 'Range Rover Evoque', 'Range Rover Sport']),
    'Lexus': sorted(['ES', 'LX', 'NX', 'RX']),
    'Mahindra': sorted(['Bolero', 'Scorpio', 'Scorpio N', 'Thar', 'XUV300', 'XUV400', 'XUV700', 'XUV3XO']),
    'Maruti Suzuki': sorted(['Alto K10', 'Baleno', 'Brezza', 'Celerio', 'Dzire', 'Eeco', 'Ertiga', 'Fronx', 'Grand Vitara', 'Ignis', 'Jimny', 'S-Presso', 'Swift', 'Wagon R', 'XL6']),
    'Mercedes-Benz': sorted(['A-Class', 'C-Class', 'E-Class', 'GLA', 'GLC', 'GLE', 'S-Class']),
    'MG': sorted(['Astor', 'Gloster', 'Hector', 'Hector Plus', 'ZS EV']),
    'Nissan': sorted(['Kicks', 'Magnite']),
    'Renault': sorted(['Kiger', 'Kwid', 'Triber']),
    'Skoda': sorted(['Kushaq', 'Octavia', 'Slavia', 'Superb']),
    'Tata': sorted(['Altroz', 'Harrier', 'Nexon', 'Nexon EV', 'Punch', 'Safari', 'Tiago', 'Tigor']),
    'Toyota': sorted(['Camry', 'Fortuner', 'Glanza', 'Hilux', 'Hyryder', 'Innova Crysta', 'Innova Hycross', 'Vellfire']),
    'Volkswagen': sorted(['Taigun', 'Tiguan', 'Virtus']),
    'Volvo': sorted(['XC40', 'XC60', 'XC90'])
}

FUEL_TYPES = ['Petrol', 'Diesel', 'CNG', 'HEV', 'PHEV', 'BEV']

CONDITIONS = [
    'Excellent (Showroom like)',
    'Good (Minor wear)',
    'Fair (Visible wear)'
]

OWNERS = ['1st Owner', '2nd Owner', '3rd Owner or more']

BUYER_TYPES = sorted(['Dealer', 'Direct Buyer', 'Exchange at Showroom'])

YEARS = list(range(2026, 2010, -1))

# ========== HELPERS ==========
def require_login():
    if 'user' not in session:
        return redirect(url_for('index'))
    return None

def ensure_session_defaults():
    """Make sure every session has the keys templates rely on."""
    if 'user' in session:
        session.setdefault('credits', 500)
        session.setdefault('transactions', [])
        session.setdefault('last_valuation', None)
        session.setdefault('welcome_shown', True)
        # Guarantee user dict has 'name' key
        if not isinstance(session.get('user'), dict):
            session['user'] = {'name': 'User', 'email': ''}
        else:
            session['user'].setdefault('name', 'User')
            session['user'].setdefault('email', '')
        session.modified = True

def first_name(full_name):
    """Return first name safely. Never raises."""
    try:
        if not full_name or not isinstance(full_name, str):
            return 'there'
        parts = full_name.strip().split()
        return parts[0] if parts else 'there'
    except Exception:
        return 'there'

# Jinja filter — bulletproof version
@app.template_filter('firstname')
def firstname_filter(s):
    return first_name(s)

# Global template context — makes {{ first_name }} always available
@app.context_processor
def inject_globals():
    user_name = ''
    try:
        if session.get('user') and isinstance(session['user'], dict):
            user_name = session['user'].get('name', '')
    except Exception:
        user_name = ''
    return {
        'current_first_name': first_name(user_name) if user_name else ''
    }

# ========== ROUTES ==========
@app.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('role'))
    return render_template('index.html')

@app.route('/login')
def login():
    redirect_uri = url_for('auth', _external=True)
    return google.authorize_redirect(redirect_uri)

@app.route('/auth')
def auth():
    try:
        token = google.authorize_access_token()
        user_info = token.get('userinfo')
        if not user_info:
            user_info = google.parse_id_token(token)
        session['user'] = {
            'name': user_info.get('name', 'User'),
            'email': user_info.get('email', '')
        }
        if 'credits' not in session:
            session['credits'] = 500
            session['transactions'] = [{
                'date': datetime.now().strftime('%d-%b-%Y'),
                'type': 'Signup Bonus',
                'description': 'Welcome gift - 5 free searches',
                'amount': 500,
                'balance': 500
            }]
            session['welcome_shown'] = False
        session.modified = True
        return redirect(url_for('role'))
    except Exception as e:
        print(f'[AUTH ERROR] {str(e)}')
        traceback.print_exc()
        flash(f'Login error: {str(e)}', 'danger')
        return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/role')
def role():
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()
    show_welcome = not session.get('welcome_shown', True)
    session['welcome_shown'] = True
    session.modified = True
    return render_template('role.html', show_welcome=show_welcome)

@app.route('/seller')
def seller():
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()
    return render_template('seller.html',
                           brands=CAR_BRANDS,
                           fuels=FUEL_TYPES,
                           conditions=CONDITIONS,
                           owners=OWNERS,
                           years=YEARS)

@app.route('/buyer')
def buyer():
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()
    return render_template('buyer.html',
                           brands=CAR_BRANDS,
                           fuels=FUEL_TYPES,
                           years=YEARS)

@app.route('/valuate', methods=['POST'])
def valuate():
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()

    if session.get('credits', 0) < 100:
        flash('Insufficient credits. Please submit a deal to earn credits.', 'warning')
        return redirect(url_for('seller'))

    car_data = {
        'make': request.form.get('make', 'N/A'),
        'model': request.form.get('model', 'N/A'),
        'variant': request.form.get('variant', 'N/A'),
        'fuel': request.form.get('fuel', 'N/A'),
        'year': request.form.get('year', 'N/A'),
        'mileage': request.form.get('mileage', '0'),
        'condition': request.form.get('condition', 'N/A'),
        'owner': request.form.get('owner', 'N/A'),
    }

    try:
        year = int(car_data['year'])
        mileage = int(car_data['mileage'])
        age = max(0, 2026 - year)
        base = 800000
        depreciated = base * (0.85 ** age)
        km_penalty = max(0, (mileage - 10000 * age) * 2)
        estimated = max(50000, int(depreciated - km_penalty))
    except (ValueError, TypeError):
        estimated = 500000

    car_data['estimated_price'] = estimated
    car_data['price_low'] = int(estimated * 0.92)
    car_data['price_high'] = int(estimated * 1.08)

    session['credits'] = session.get('credits', 500) - 100
    txns = session.get('transactions', [])
    txns.append({
        'date': datetime.now().strftime('%d-%b-%Y'),
        'type': 'Valuation',
        'description': f"{car_data['make']} {car_data['model']} ({car_data['year']})",
        'amount': -100,
        'balance': session['credits']
    })
    session['transactions'] = txns
    session['last_valuation'] = car_data
    session['dashboard_message'] = 'Valuation complete! 100 credits used.'
    session.modified = True

    return redirect(url_for('dashboard'))

@app.route('/dashboard')
def dashboard():
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()

    valuation = session.get('last_valuation')
    message = session.pop('dashboard_message', None)

    return render_template('dashboard.html',
                           valuation=valuation,
                           message=message)

@app.route('/submit_deal', methods=['GET', 'POST'])
def submit_deal():
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()

    if request.method == 'POST':
        make = request.form.get('make', 'Car')
        model = request.form.get('model', '')
        year = request.form.get('year', '')

        session['credits'] = session.get('credits', 0) + 100
        txns = session.get('transactions', [])
        txns.append({
            'date': datetime.now().strftime('%d-%b-%Y'),
            'type': 'Deal Submission',
            'description': f"{make} {model} ({year})",
            'amount': 100,
            'balance': session['credits']
        })
        session['transactions'] = txns
        session['dashboard_message'] = 'Deal submitted successfully! +100 Credits awarded!'
        session.modified = True
        return redirect(url_for('dashboard'))

    return render_template('submit_deal.html',
                           brands=CAR_BRANDS,
                           fuels=FUEL_TYPES,
                           conditions=CONDITIONS,
                           owners=OWNERS,
                           buyer_types=BUYER_TYPES,
                           years=YEARS,
                           today=datetime.now().strftime('%d-%b-%Y'))

@app.route('/credit_history')
def credit_history():
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()
    txns = list(reversed(session.get('transactions', [])))
    return render_template('credit_history.html',
                           transactions=txns,
                           balance=session.get('credits', 0))

# ========== ERROR HANDLERS with debug logging ==========
@app.errorhandler(404)
def not_found(e):
    try:
        return render_template('404.html'), 404
    except Exception as ex:
        print(f'[404 TEMPLATE ERROR] {str(ex)}')
        return '<h1>404 - Page Not Found</h1><a href="/">Go Home</a>', 404

@app.errorhandler(500)
def server_error(e):
    # Log the actual error to Render logs for debugging
    print(f'[500 ERROR] {str(e)}')
    traceback.print_exc()
    try:
        return render_template('500.html'), 500
    except Exception as ex:
        print(f'[500 TEMPLATE ERROR] {str(ex)}')
        return '<h1>500 - Server Error</h1><a href="/">Go Home</a>', 500

# Catch any unhandled exception in debug
@app.errorhandler(Exception)
def handle_exception(e):
    print(f'[UNHANDLED EXCEPTION] {type(e).__name__}: {str(e)}')
    traceback.print_exc()
    try:
        return render_template('500.html'), 500
    except Exception:
        return f'<h1>Server Error</h1><pre>{str(e)}</pre><a href="/">Go Home</a>', 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
