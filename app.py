from flask import Flask, render_template, request
import random

app = Flask(__name__)

# Master Industry Data
CITIES = sorted(["Ahmedabad", "Bangalore", "Chennai", "Delhi", "Gurgaon", "Hyderabad", "Kolkata", "Mumbai", "Noida", "Pune", "Jaipur", "Lucknow", "Chandigarh", "Kochi"])
BRANDS = sorted(["Toyota", "Maruti Suzuki", "Hyundai", "Tata Motors", "Mahindra", "Kia", "Honda", "MG Motors", "Skoda", "Volkswagen", "BMW", "Mercedes-Benz", "Audi"])
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
    
    # Real-world Range Logic
    base = 1450000 if make == "Toyota" else 1100000
    res = {
        'low': int(base * 0.94), 'high': int(base * 1.06),
        'likely': base, 'walkaway': int(base * 1.10)
    }
    
    # Steep Logarithmic Depreciation
    forecast = [base, int(base*0.78), int(base*0.68), int(base*0.61), int(base*0.56), int(base*0.53)]
    
    return render_template('buyer_dashboard.html', res=res, forecast=forecast, make=make, model=model, mode=mode, asking=asking)

@app.route('/dashboard', methods=['POST'])
def dashboard():
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
