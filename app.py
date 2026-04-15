from flask import Flask, render_template, request
import random

app = Flask(__name__)

# Core Industry Data
BRANDS = ["Toyota", "Maruti Suzuki", "Hyundai", "Tata Motors", "Mahindra", "Kia", "Honda", "MG Motors", "Skoda", "Volkswagen"]
CITIES = ["Bangalore", "Hyderabad", "Delhi", "Mumbai", "Pune", "Chennai"]
CONDITIONS = ["Excellent (Showroom Like)", "Average (Normal Wear)", "Fair (Needs Cosmetic Work)"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    user_name = request.form.get('name', 'Rajeev Thakur')
    return render_template('role.html', user_name=user_name)

# SELLER JOURNEY
@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years, brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    return render_template('dashboard.html')

# BUYER JOURNEY
@app.route('/buyer')
def buyer():
    years = list(range(2026, 2010, -1))
    return render_template('buyer.html', years=years, brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

@app.route('/buyer_dashboard', methods=['POST'])
def buyer_dashboard():
    mode = request.form.get('search_mode', 'discovery')
    asking = int(request.form.get('asking_price', 0) or 0)
    make = request.form.get('make', 'Toyota')
    
    # Range Logic for Dashboard
    base = 1450000 if make == "Toyota" else 1100000
    res = {'low': int(base * 0.94), 'high': int(base * 1.06), 'likely': base, 'walkaway': int(base * 1.10)}
    
    forecast = [base]
    for i in range(5): forecast.append(int(forecast[-1] * 0.88))

    return render_template('buyer_dashboard.html', mode=mode, asking=asking, res=res, forecast=forecast, make=make)

if __name__ == '__main__':
    app.run(debug=True)
