from flask import Flask, render_template, request
import random

app = Flask(__name__)

# --- ROUTES ---

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

@app.route('/buyer_dashboard', methods=['POST'])
def buyer_dashboard():
    # Capture buyer search data
    data = {
        'make': request.form.get('make'),
        'model': request.form.get('model'),
        'year': int(request.form.get('year', 2022)),
        'mileage': int(request.form.get('mileage', 40000)),
        'condition': request.form.get('condition', 'Good')
    }

    # --- INTELLIGENCE LOGIC ---
    # Base Price Logic (Simplified for MVP)
    base_price = 1200000 # Default for demo
    if data['make'] == 'Toyota': base_price = 1500000
    
    # Adjust for mileage & condition
    adjusted_base = base_price - (data['mileage'] * 2)
    if data['condition'] == 'Excellent': adjusted_base += 50000
    elif data['condition'] == 'Fair': adjusted_base -= 80000

    # Calculate specific metrics
    results = {
        'dealer_price': int(adjusted_base * 1.12), # 12% Dealer Markup
        'private_price': int(adjusted_base),
        'walkaway_price': int(adjusted_base * 1.08),
        'deal_price': int(adjusted_base * 0.96),
        'conf_score': random.randint(75, 92),
        'demand': "High" if data['make'] in ['Toyota', 'Maruti Suzuki'] else "Moderate"
    }

    # 5-Year Depreciation Forecast (approx 10-12% annually)
    forecast = []
    current_f = results['private_price']
    for i in range(1, 6):
        current_f = int(current_f * 0.88)
        forecast.append(current_f)

    return render_template('buyer_dashboard.html', data=data, res=results, forecast=forecast)

if __name__ == '__main__':
    app.run(debug=True)
