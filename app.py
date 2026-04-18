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

# ========== REAL PER-MODEL VARIANTS ==========
VARIANTS_BY_MODEL = {
    # Maruti Suzuki
    'Alto K10': ['LXi', 'VXi', 'VXi+', 'VXi AGS', 'VXi+ AGS'],
    'Baleno': ['Sigma', 'Delta', 'Zeta', 'Alpha'],
    'Brezza': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Celerio': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Dzire': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Eeco': ['5 Seater', '7 Seater', 'Cargo'],
    'Ertiga': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Fronx': ['Sigma', 'Delta', 'Delta+', 'Zeta', 'Alpha'],
    'Grand Vitara': ['Sigma', 'Delta', 'Delta+', 'Zeta', 'Alpha', 'Alpha+'],
    'Ignis': ['Sigma', 'Delta', 'Zeta', 'Alpha'],
    'Jimny': ['Zeta', 'Alpha'],
    'S-Presso': ['LXi', 'VXi', 'VXi+'],
    'Swift': ['LXi', 'VXi', 'ZXi', 'ZXi+'],
    'Wagon R': ['LXi', 'VXi', 'ZXi'],
    'XL6': ['Zeta', 'Alpha', 'Alpha+'],

    # Hyundai
    'Alcazar': ['Prestige', 'Platinum', 'Signature'],
    'Aura': ['E', 'S', 'SX', 'SX+', 'SX(O)'],
    'Creta': ['E', 'EX', 'S', 'S+', 'SX', 'SX(O)'],
    'Exter': ['EX', 'S', 'SX', 'SX(O)'],
    'Grand i10 Nios': ['Era', 'Magna', 'Sportz', 'Asta'],
    'i20': ['Magna', 'Sportz', 'Asta', 'Asta(O)'],
    'Kona Electric': ['Premium', 'Premium Dual Tone'],
    'Tucson': ['Platinum', 'Signature'],
    'Venue': ['E', 'S', 'S+', 'SX', 'SX(O)'],
    'Verna': ['EX', 'S', 'SX', 'SX(O)'],

    # Tata
    'Altroz': ['XE', 'XM+', 'XT', 'XZ', 'XZ+'],
    'Harrier': ['Smart', 'Pure', 'Adventure', 'Fearless', 'Fearless+'],
    'Nexon': ['Smart', 'Pure', 'Creative', 'Fearless', 'Fearless+'],
    'Nexon EV': ['Creative', 'Fearless', 'Empowered'],
    'Punch': ['Pure', 'Adventure', 'Accomplished', 'Creative'],
    'Safari': ['Smart', 'Pure', 'Adventure', 'Accomplished', 'Fearless+'],
    'Tiago': ['XE', 'XM', 'XT', 'XZ+'],
    'Tigor': ['XE', 'XM', 'XZ', 'XZ+'],

    # Mahindra
    'Bolero': ['B4', 'B6', 'B6(O)'],
    'Scorpio': ['S3', 'S5', 'S7', 'S9', 'S11'],
    'Scorpio N': ['Z2', 'Z4', 'Z6', 'Z8', 'Z8L'],
    'Thar': ['AX', 'AX(O)', 'LX'],
    'XUV300': ['W4', 'W6', 'W8', 'W8(O)'],
    'XUV400': ['EC', 'EL'],
    'XUV700': ['MX', 'AX3', 'AX5', 'AX7', 'AX7L'],
    'XUV3XO': ['MX1', 'MX2', 'MX3', 'AX5', 'AX7', 'AX7L'],

    # Toyota
    'Camry': ['Hybrid'],
    'Fortuner': ['2.7', '2.8 4x2', '2.8 4x4', 'Legender'],
    'Glanza': ['E', 'S', 'G', 'V'],
    'Hilux': ['Standard', 'High'],
    'Hyryder': ['E', 'S', 'G', 'V'],
    'Innova Crysta': ['GX', 'VX', 'ZX'],
    'Innova Hycross': ['GX', 'VX', 'ZX', 'ZX(O)'],
    'Vellfire': ['Executive Lounge'],

    # Honda
    'Amaze': ['E', 'S', 'VX', 'ZX'],
    'City': ['V', 'VX', 'ZX'],
    'Civic': ['V', 'VX', 'ZX'],
    'Elevate': ['SV', 'V', 'VX', 'ZX'],
    'Jazz': ['V', 'VX', 'ZX'],
    'WR-V': ['S', 'SV', 'VX'],

    # Kia
    'Carens': ['Premium', 'Prestige', 'Prestige+', 'Luxury', 'Luxury+'],
    'Carnival': ['Premium', 'Prestige', 'Limousine'],
    'EV6': ['GT Line'],
    'Seltos': ['HTE', 'HTK', 'HTK+', 'HTX', 'HTX+', 'GTX+'],
    'Sonet': ['HTE', 'HTK', 'HTK+', 'HTX', 'HTX+', 'GTX+'],

    # Volkswagen
    'Taigun': ['Comfortline', 'Highline', 'Topline', 'GT'],
    'Tiguan': ['Elegance'],
    'Virtus': ['Comfortline', 'Highline', 'Topline', 'GT'],

    # Skoda
    'Kushaq': ['Active', 'Ambition', 'Style'],
    'Octavia': ['Style', 'Laurin & Klement'],
    'Slavia': ['Active', 'Ambition', 'Style'],
    'Superb': ['Sportline', 'Laurin & Klement'],

    # Renault
    'Kiger': ['RXE', 'RXL', 'RXT', 'RXZ'],
    'Kwid': ['RXE', 'RXL', 'RXT', 'Climber'],
    'Triber': ['RXE', 'RXL', 'RXT', 'RXZ'],

    # Nissan
    'Kicks': ['XL', 'XV', 'XV Premium'],
    'Magnite': ['XE', 'XL', 'XV', 'XV Premium'],

    # Ford
    'EcoSport': ['Ambiente', 'Trend', 'Titanium', 'Titanium+', 'Sports'],
    'Endeavour': ['Trend', 'Titanium', 'Titanium+', 'Sport'],
    'Figo': ['Ambiente', 'Trend', 'Titanium', 'Titanium+'],
    'Freestyle': ['Ambiente', 'Trend', 'Titanium', 'Titanium+'],

    # MG
    'Astor': ['Style', 'Super', 'Smart', 'Sharp', 'Savvy'],
    'Gloster': ['Super', 'Smart', 'Sharp', 'Savvy'],
    'Hector': ['Style', 'Super', 'Smart', 'Sharp'],
    'Hector Plus': ['Style', 'Super', 'Smart', 'Sharp'],
    'ZS EV': ['Excite', 'Exclusive'],

    # Premium brands — simplified
    'A3': ['Premium', 'Premium Plus', 'Technology'],
    'A4': ['Premium', 'Premium Plus', 'Technology'],
    'A6': ['Premium', 'Premium Plus', 'Technology'],
    'Q3': ['Premium', 'Premium Plus', 'Technology'],
    'Q5': ['Premium', 'Premium Plus', 'Technology'],
    'Q7': ['Premium Plus', 'Technology'],
    '3 Series': ['330Li M Sport', '320d Luxury Line'],
    '5 Series': ['530i M Sport', '520d Luxury Line'],
    'X1': ['sDrive18i', 'sDrive20d'],
    'X3': ['xDrive20d', 'xDrive30i'],
    'X5': ['xDrive30d', 'xDrive40i'],
    'X7': ['xDrive40i', 'M50d'],
    'A-Class': ['A200', 'A200d'],
    'C-Class': ['C200', 'C220d', 'C300'],
    'E-Class': ['E200', 'E220d', 'E350d'],
    'GLA': ['GLA200', 'GLA220d'],
    'GLC': ['GLC200', 'GLC220d', 'GLC300'],
    'GLE': ['GLE300d', 'GLE450'],
    'S-Class': ['S450', 'S350d', 'Maybach'],
    'F-Pace': ['S', 'SE'],
    'XE': ['S'],
    'XF': ['Prestige'],
    'ES': ['300h Luxury'],
    'LX': ['500d'],
    'NX': ['350h Luxury'],
    'RX': ['350h Luxury'],
    'Defender': ['S', 'SE', 'HSE', 'X'],
    'Discovery': ['S', 'SE', 'HSE'],
    'Range Rover': ['SE', 'HSE', 'Autobiography'],
    'Range Rover Evoque': ['S', 'SE', 'HSE'],
    'Range Rover Sport': ['SE', 'HSE', 'Autobiography'],
    'XC40': ['Inscription', 'R-Design'],
    'XC60': ['Inscription', 'R-Design'],
    'XC90': ['Inscription', 'R-Design']
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
    if 'user' in session:
        session.setdefault('credits', 500)
        session.setdefault('transactions', [])
        session.setdefault('last_valuation', None)
        session.setdefault('welcome_shown', True)
        if not isinstance(session.get('user'), dict):
            session['user'] = {'name': 'User', 'email': ''}
        else:
            session['user'].setdefault('name', 'User')
            session['user'].setdefault('email', '')
        session.modified = True

def first_name(full_name):
    try:
        if not full_name or not isinstance(full_name, str):
            return 'there'
        parts = full_name.strip().split()
        return parts[0] if parts else 'there'
    except Exception:
        return 'there'

@app.template_filter('firstname')
def firstname_filter(s):
    return first_name(s)

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
    """Seller form — accepts query string to pre-fill (used when Back from dashboard)."""
    guard = require_login()
    if guard: return guard
    ensure_session_defaults()

    # Pre-fill from query string (supports ?make=...&fuel=...&year=... etc.)
    prefill = {
        'make': request.args.get('make', ''),
        'fuel': request.args.get('fuel', ''),
        'model': request.args.get('model', ''),
        'variant': request.args.get('variant', ''),
        'owner': request.args.get('owner', ''),
        'mileage': request.args.get('mileage', ''),
        'year': request.args.get('year', ''),
        'condition': request.args.get('condition', '')
    }

    return render_template('seller.html',
                           brands=CAR_BRANDS,
                           fuels=FUEL_TYPES,
                           conditions=CONDITIONS,
                           owners=OWNERS,
                           years=YEARS,
                           variants_by_model=VARIANTS_BY_MODEL,
                           prefill=prefill)

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

    # Valuation logic
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

    # Price range + bands (for dashboard UI)
    car_data['estimated_price'] = estimated
    car_data['price_low'] = int(estimated * 0.97)
    car_data['price_high'] = int(estimated * 1.03)

    # Dashboard metadata
    import random
    car_data['verified_txns'] = random.randint(28, 62)
    car_data['buyers_30d'] = random.randint(140, 260)
    car_data['avg_buyers_per_day'] = round(car_data['buyers_30d'] / 30, 1)
    car_data['demand'] = 'High' if car_data['buyers_30d'] > 200 else ('Medium' if car_data['buyers_30d'] > 150 else 'Low')
    car_data['confidence'] = min(95, 50 + car_data['verified_txns'])
    car_data['days_low'] = 12 if car_data['demand'] == 'High' else (18 if car_data['demand'] == 'Medium' else 25)
    car_data['days_high'] = car_data['days_low'] + 7

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
                           variants_by_model=VARIANTS_BY_MODEL,
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

# ========== ERROR HANDLERS ==========
@app.errorhandler(404)
def not_found(e):
    try:
        return render_template('404.html'), 404
    except Exception:
        return '<h1>404 - Page Not Found</h1><a href="/">Go Home</a>', 404

@app.errorhandler(500)
def server_error(e):
    print(f'[500 ERROR] {str(e)}')
    traceback.print_exc()
    try:
        return render_template('500.html'), 500
    except Exception:
        return '<h1>500 - Server Error</h1><a href="/">Go Home</a>', 500

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
