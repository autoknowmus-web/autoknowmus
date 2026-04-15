from flask import Flask, render_template, request
import random

app = Flask(__name__)

# Locked Industry Standards
BRANDS = ["Toyota", "Maruti Suzuki", "Hyundai", "Tata Motors", "Mahindra", "Kia", "Honda", "MG Motors", "Skoda", "Volkswagen"]
CITIES = ["Bangalore", "Hyderabad", "Delhi", "Mumbai", "Pune", "Chennai"]
CONDITIONS = ["Excellent (Showroom Like)", "Average (Normal Wear)", "Fair (Needs some repair)"]

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
    return render_template('seller.html', years=years, brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

@app.route('/buyer')
def buyer():
    years = list(range(2026, 2010, -1))
    return render_template('buyer.html', years=years, brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

@app.route('/buyer_dashboard', methods=['POST'])
def buyer_dashboard():
    # Capture Inputs
    mode = request.form.get('search_mode', 'discovery')
    asking = int(request.form.get('asking_price', 0) or 0)
    make = request.form.get('make', 'Toyota')
    model = request.form.get('model', 'Vehicle')
    
    # Pricing Intelligence Logic
    base = 1450000 if make == "Toyota" else 1150000
    res = {
        'low': int(base * 0.94),
        'high': int(base * 1.06),
        'likely': base,
        'walkaway': int(base * 1.12),
        'conf': random.randint(84, 92)
    }
    
    # STEEP DEPRECIATION DATA (Bezier-aligned)
    # 22% drop Year 1, followed by flattening curve
    forecast = [
        base,                      # Year 0
        int(base * 0.78),          # Year 1
        int(base * 0.68),          # Year 2
        int(base * 0.62),          # Year 3
        int(base * 0.58),          # Year 4
        int(base * 0.55)           # Year 5
    ]

    return render_template('buyer_dashboard.html', mode=mode, asking=asking, res=res, forecast=forecast, make=make, model=model)

@app.route('/dashboard', methods=['POST'])
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
