from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

# 1. Home Page (Dark Mode with Social Login & Manual Form)
@app.route('/')
def index():
    return render_template('index.html')

# 2. Role Selection (Buyer vs Seller)
@app.route('/role', methods=['GET', 'POST'])
def role():
    # If coming from the form on index.html, you can capture data here later
    return render_template('role.html')

# 3. Seller Page (Cascading Dropdowns)
@app.route('/seller')
def seller():
    return render_template('seller.html')

# 4. Dashboard (Price Simulator)
@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
