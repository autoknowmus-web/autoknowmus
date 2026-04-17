import os
from flask import Flask, render_template, request, session, redirect, url_for, flash
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
    # Credits start at 0 before login
    if 'credits' not in session:
        session['credits'] = 0
    return render_template('index.html')

@app.route('/login/google')
def login():
    return google.authorize_redirect(url_for('auth', _external=True))

@app.route('/auth')
def auth():
    try:
        token = google.authorize_access_token()
        user = token.get('userinfo')
        if user:
            # SAFETY: Use name if available, otherwise use email prefix
            session['user_name'] = user.get('name') or user.get('email').split('@')[0]
            session['credits'] = 500 
            flash("Success! 500 Bonus Credits added to your account.")
        return redirect(url_for('role'))
    except Exception as e:
        print(f"Auth Error: {e}")
        flash("Login failed. Please try again.")
        return redirect(url_for('index'))

@app.route('/role', methods=['GET', 'POST'])
def role():
    if request.method == 'POST':
        session['user_name'] = request.form.get('name')
        session['credits'] = 500
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

# [FIX] Complete Intelligence Generation Route
@app.route('/generate_report', methods=['POST'])
def generate_report():
    current_credits = session.get('credits', 0)
    
    if current_credits >= 100:
        session['credits'] = current_credits - 100
        # Logic for calculation will go here in the next step
        flash("Intelligence Report Generated! 100 Credits deducted.")
        # For now, we return to the role screen; later, this will go to a 'Result' page.
        return redirect(url_for('role'))
    else:
        flash("Insufficient credits! Please purchase more to generate intelligence.")
        return redirect(url_for('role'))

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':
    # Use environment port for Render, default to 5000 for local
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
