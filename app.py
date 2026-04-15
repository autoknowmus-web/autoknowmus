from flask import Flask, render_template, request
import random

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    user_name = request.form.get('name', 'Rajeev Thakur')
    return render_template('role.html', user_name=user_name)

@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years)

@app.route('/buyer')
def buyer():
    years = list(range(2026, 2010, -1))
    return render_template('buyer.html', years=years)

# SELLER DASHBOARD (Slider/Simulator)
@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    return render_template('dashboard.html')

# BUYER DASHBOARD (Intelligence/Forecast)
@app.route('/buyer_dashboard', methods=['GET', 'POST'])
def buyer_dashboard():
    # Capture search parameters
    make = request.form.get('make', 'Toyota')
    model = request.form.get('model', 'Fortuner')
    
    # Logic for Buyer Dashboard
    res = {
        'dealer_price': 1420000,
        'private_price': 1345000,
        'walkaway_price': 1475000,
        'deal_price': 1385000,
        'conf_score': 84,
        'demand': "High" if make in ['Toyota', 'Maruti Suzuki'] else "Moderate"
    }
    
    # 5-Year Forecast Logic
    forecast = [1210000, 1080000, 950000, 820000, 740000]
    
    return render_template('buyer_dashboard.html', res=res, forecast=forecast, make=make, model=model)

if __name__ == '__main__':
    app.run(debug=True)
