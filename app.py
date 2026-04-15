from flask import Flask, render_template, request
import random

app = Flask(__name__)

# Core Model Data
BRANDS = ["Toyota", "Maruti Suzuki", "Hyundai", "Tata Motors", "Mahindra", "Kia", "Honda", "MG Motors", "Skoda", "Volkswagen"]
CITIES = ["Bangalore", "Hyderabad", "Delhi", "Mumbai", "Pune", "Chennai"]

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
    return render_template('seller.html', years=years, brands=BRANDS, cities=CITIES)

@app.route('/buyer')
def buyer():
    years = list(range(2026, 2010, -1))
    return render_template('buyer.html', years=years, brands=BRANDS, cities=CITIES)

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    return render_template('dashboard.html')

@app.route('/buyer_dashboard', methods=['POST'])
def buyer_dashboard():
    mode = request.form.get('search_mode', 'discovery')
    asking_price = int(request.form.get('asking_price', 0) or 0)
    make = request.form.get('make', 'Toyota')
    model = request.form.get('model', 'Fortuner')
    
    # Range Logic
    base = 1450000 if make == "Toyota" else 1100000
    res = {
        'low': int(base * 0.94),
        'high': int(base * 1.06),
        'likely': base,
        'walkaway': int(base * 1.08),
        'conf': random.randint(82, 94),
        'demand': "High" if make in ["Toyota", "Maruti Suzuki"] else "Moderate"
    }
    
    # 5-Year Forecast for SVG Graph
    forecast = []
    curr = base
    for i in range(6):
        forecast.append(int(curr))
        curr *= 0.88

    return render_template('buyer_dashboard.html', mode=mode, asking=asking_price, res=res, forecast=forecast, make=make, model=model)

if __name__ == '__main__':
    app.run(debug=True)
