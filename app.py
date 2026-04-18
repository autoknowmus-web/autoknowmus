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

@app.route('/')
def index():
    if session.get('user_name'):
        return redirect(url_for('role'))
    return render_template('index.html')

@app.route('/login/google')
def login_google():
    redirect_uri = url_for('auth', _external=True, _scheme='https')
    return google.authorize_redirect(redirect_uri)

@app.route('/auth')
def auth():
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
        session.clear()
        flash('Session expired. Please login again.', 'danger')
        return redirect(url_for('index'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/role', methods=['GET', 'POST'])
def role():
    if request.method == 'POST':
        name = request.form.get('name')
        email = request.form.get('email')
        mobile = request.form.get('mobile')
        if name and email and mobile:
            session['user_name'] = name
            session['credits'] = 500
            session['city'] = 'Bangalore'
            session['credit_transactions'] = [
                {'type': 'bonus', 'amount': 500, 'description': 'Login Bonus - 5 Free Searches', 'date': 'Today'},
            ]
            session.modified = True
            flash('🎉 Welcome! 500 bonus credits added for 5 free searches!', 'success')
            return redirect(url_for('role'))
    if not session.get('user_name'):
        return redirect(url_for('index'))
    return render_template('role.html', user_name=session.get('user_name'), credits=session.get('credits', 0))

@app.route('/seller')
def seller():
    if not session.get('user_name'):
        return redirect(url_for('index'))
    return render_template('seller.html', user_name=session.get('user_name'), credits=session.get('credits', 0))

@app.route('/generate_report', methods=['POST'])
def generate_report():
    if not session.get('user_name'):
        return redirect(url_for('index'))
    if session.get('credits', 0) < 100:
        flash('Insufficient credits! You need 100 credits for a valuation.', 'danger')
        return redirect(url_for('seller'))
    session['credits'] -= 100
    session.modified = True
    transactions = session.get('credit_transactions', [])
    transactions.append({'type': 'deducted', 'amount': 100, 'description': 'Car Valuation', 'date': 'Today'})
    session['credit_transactions'] = transactions
    session['last_search'] = request.form.to_dict()
    return redirect(url_for('dashboard'))

@app.route('/buyer')
def buyer():
    if not session.get('user_name'):
        return redirect(url_for('index'))
    return render_template('buyer.html', user_name=session.get('user_name'), credits=session.get('credits', 0))

@app.route('/dashboard')
def dashboard():
    if not session.get('user_name'):
        return redirect(url_for('index'))
    last_search = session.get('last_search', {})
    return render_template('dashboard.html', user_name=session.get('user_name'), credits=session.get('credits', 0), search_data=last_search)

@app.route('/credit_history')
def credit_history():
    if not session.get('user_name'):
        return redirect(url_for('index'))
    history = session.get('credit_transactions', [
        {'type': 'bonus', 'amount': 500, 'description': 'Login Bonus - 5 Free Searches', 'date': 'Today'},
    ])
    return render_template('credit_history.html', user_name=session.get('user_name'), credits=session.get('credits', 0), history=history)

@app.route('/submit_deal')
def submit_deal():
    if not session.get('user_name'):
        return redirect(url_for('index'))
    return render_template('submit_deal.html', user_name=session.get('user_name'), credits=session.get('credits', 0))

@app.route('/submit_deal_post', methods=['POST'])
def submit_deal_post():
    if not session.get('user_name'):
        return redirect(url_for('index'))
    session['credits'] += 100
    session.modified = True
    transactions = session.get('credit_transactions', [])
    transactions.append({'type': 'earned', 'amount': 100, 'description': 'Deal Submission Reward', 'date': 'Today'})
    session['credit_transactions'] = transactions
    flash('Deal submitted successfully! +100 credits awarded!', 'success')
    return redirect(url_for('dashboard'))

@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def server_error(error):
    return render_template('500.html'), 500

if __name__ == '__main__':
    app.run(debug=False)
