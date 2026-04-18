import os
from flask import Flask, render_template, request, session, redirect, url_for, flash, get_flashed_messages
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
            session['credits'] = 500  # ← CRITICAL: Initialize with 500 credits
            session['city'] = 'Bangalore'
            session.modified = True
        return redirect(url_for('role'))
    except Exception as e:
        print(f"Auth error: {e}")
        flash('Login failed. Please try again.', 'danger')
        return redirect(url_for('index'))

@app.route('/logout')
def logout():
    """Logout and clear session"""
    session.clear()
    return redirect(url_for('index'))

# =====================
# ROLE SELECTION
# =====================

@app.route('/role', methods=['GET', 'POST'])
def role():
    """Role selection page - Seller/Buyer/Dashboard"""
    if request.method == 'POST':
        # Handle manual login form submission
        name = request.form.get('name')
        email = request.form.get('email')
        mobile = request.form.get('mobile')
        
        if name and email and mobile:
            session['user_name'] = name
            session['credits'] = 500
            session['city'] = 'Bangalore'
            session.modified = True
            return redirect(url_for('role'))
    
    if not session.get('user_name'):
        return redirect(url_for('index'))
    
    return render_template('role.html', 
                         user_name=session.get('user_name'),
                         credits=session.get('credits', 0))

# =====================
# SELLER ROUTES
# =====================

@app.route('/seller')
def seller():
    """Seller valuation form - 4-4-1 grid layout"""
    if not session.get('user_name'):
        return redirect(url_for('index'))
    return render_template('seller.html',
                         user_name=session.get('user_name'),
                         credits=session.get('credits', 0))

@app.route('/generate_report', methods=['POST'])
def generate_report():
    """Deduct 100 credits for valuation and show dashboard with results"""
    if not session.get('user_name'):
        return redirect(url_for('index'))
    
    if session.get('credits', 0) < 100:
        flash('Insufficient credits! You need 100 credits for a valuation.', 'danger')
        return redirect(url_for('seller'))
    
    # Deduct credits
    session['credits'] -= 100
    session.modified = True
    
    # Store search parameters for dashboard display
    session['last_search'] = request.form.to_dict()
    
    return redirect(url_for('dashboard'))

# =====================
# BUYER ROUTES
# =====================

@app.route('/buyer')
def buyer():
    """Buyer search form - similar layout to seller"""
    if not session.get('user_name'):
        return redirect(url_for('index'))
    return render_template('buyer.html',
                         user_name=session.get('user_name'),
                         credits=session.get('credits', 0))

# =====================
# DASHBOARD & RESULTS
# =====================

@app.route('/dashboard')
def dashboard():
    """Show valuation results after search"""
    if not session.get('user_name'):
        return redirect(url_for('index'))
    
    last_search = session.get('last_search', {})
    
    return render_template('dashboard.html',
                         user_name=session.get('user_name'),
                         credits=session.get('credits', 0),
                         search_data=last_search)

# =====================
# DEAL SUBMISSION
# =====================

@app.route('/submit_deal')
def submit_deal():
    """Form to submit real deal data - awards 200 credits"""
    if not session.get('user_name'):
        return redirect(url_for('index'))
    return render_template('submit_deal.html',
                         user_name=session.get('user_name'),
                         credits=session.get('credits', 0))

@app.route('/submit_deal_post', methods=['POST'])
def submit_deal_post():
    """Process deal submission and add 200 credits"""
    if not session.get('user_name'):
        return redirect(url_for('index'))
    
    # Add 200 credits for submitting deal data
    session['credits'] += 200
    session.modified = True
    
    flash('Deal submitted successfully! +200 credits awarded!', 'success')
    return redirect(url_for('dashboard'))

# =====================
# ERROR HANDLERS
# =====================

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(error):
    return render_template('500.html'), 500

# =====================
# RUN APP
# =====================

if __name__ == '__main__':
    app.run(debug=False)
