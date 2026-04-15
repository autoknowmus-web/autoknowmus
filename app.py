from flask import Flask, render_template, request
import random

app = Flask(__name__)

CITIES = sorted(["Ahmedabad", "Bangalore", "Chandigarh", "Chennai", "Delhi", "Gurgaon", "Hyderabad", "Jaipur", "Kochi", "Kolkata", "Mumbai", "Noida", "Pune", "Lucknow", "Indore"])
BRANDS = sorted(["Audi", "BMW", "Honda", "Hyundai", "Kia", "Mahindra", "Maruti Suzuki", "Mercedes-Benz", "MG Motors", "Skoda", "Tata Motors", "Toyota", "Volkswagen"])
CONDITIONS = ["Excellent (showroom like)", "Average (normal wear)", "Fair (needs some repair)"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    user_name = request.form.get('name', 'User')
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
    data = request.form
    make = data.get('make', 'Toyota')
    model = data.get('model', 'Vehicle')
    mode = data.get('search_mode', 'discovery')
    asking = int(data.get('asking_price', 0) or 0)
    
    base = 1450000 if make == "Toyota" else 1100000
    res = {'low': int(base * 0.94), 'high': int(base * 1.06), 'likely': base, 'walkaway': int(base * 1.12)}
    forecast = [base, int(base*0.78), int(base*0.68), int(base*0.62), int(base*0.58), int(base*0.55)]
    
    return render_template('buyer_dashboard.html', res=res, forecast=forecast, make=make, model=model, mode=mode, asking=asking)

@app.route('/dashboard', methods=['POST'])
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
