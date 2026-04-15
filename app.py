from flask import Flask, render_template, request
import random

app = Flask(__name__)

# --- CORE ROUTES ---

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    # Personalizes the journey by grabbing the name from the home page
    user_name = request.form.get('name', 'Rajeev Thakur')
    return render_template('role.html', user_name=user_name)

# --- SELLER JOURNEY ---

@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years)

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    # The Seller Slider/Simulator page
    return render_template('dashboard.html')

# --- BUYER JOURNEY ---

@app.route('/buyer')
def buyer():
    years = list(range(2026, 2010, -1))
    return render_template('buyer.html', years=years)

@app.route('/buyer_dashboard', methods=['GET', 'POST'])
def buyer_dashboard():
    # Capture form data from buyer.html
    make = request.form.get('make', 'Toyota')
    model = request.form.get('model', 'Fortuner')
    condition = request.form.get('condition', 'Good')
    
    # --- INTELLIGENCE LOGIC ---
    # Base values for Bangalore Market
    base_val = 1420000 if make == "Toyota" else 1150000
    if condition == "Excellent": base_val += 45000
    
    res = {
        'dealer_price': base_val,
        'private_price': int(base_val * 0.94),
        'walkaway_price': int(base_val * 1.05),
        'deal_price': int(base_val * 0.97),
        'conf_score': random.randint(82, 91),
        'demand': "High" if make in ['Toyota', 'Maruti Suzuki'] else "Moderate"
    }
    
    # 5-Year Depreciation Forecast (approx 12% drop per year)
    forecast = []
    current_f = res['private_price']
    for i in range(1, 6):
        current_f = int(current_f * 0.88)
        forecast.append(current_f)
    
    return render_template('buyer_dashboard.html', 
                           res=res, 
                           forecast=forecast, 
                           make=make, 
                           model=model, 
                           data={'model': model})

if __name__ == '__main__':
    app.run(debug=True)
