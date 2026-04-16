from flask import Flask, render_template, request
import random

app = Flask(__name__)

# [Iteration 50] Master Specification Data
CITIES = sorted(["Ahmedabad", "Bangalore", "Chandigarh", "Chennai", "Delhi", "Gurgaon", "Hyderabad", "Jaipur", "Kochi", "Kolkata", "Mumbai", "Noida", "Pune", "Lucknow", "Indore"])
BRANDS = sorted(["Audi", "BMW", "Honda", "Hyundai", "Kia", "Mahindra", "Maruti Suzuki", "Mercedes-Benz", "MG Motors", "Skoda", "Tata Motors", "Toyota", "Volkswagen"])
CONDITIONS = ["Excellent (showroom like)", "Average (normal wear)", "Fair (needs some repair)"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['POST'])
def role():
    user_name = request.form.get('name', 'Rajeev Thakur')
    return render_template('role.html', user_name=user_name)

@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years, brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

@app.route('/dashboard', methods=['POST'])
def dashboard():
    make = request.form.get('make', 'Toyota')
    model = request.form.get('model', 'Vehicle')
    base = 1450000 if make == "Toyota" else 1100000
    
    # [Requirement Fix] 6-Month Forecast (Every 15 days = 12 points)
    forecast_points = []
    current_val = base
    for i in range(12):
        current_val = int(current_val * 0.993) # Realistic 15-day drop
        forecast_points.append(current_val)
        
    return render_template('dashboard.html', base=base, forecast=forecast_points, model=model)

@app.route('/buyer')
def buyer():
    return render_template('buyer.html', brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

@app.route('/buyer_dashboard', methods=['POST'])
def buyer_dashboard():
    make = request.form.get('make', 'Toyota')
    model = request.form.get('model', 'Vehicle')
    asking = int(request.form.get('asking_price', 0) or 0)
    base = 1450000 if make == "Toyota" else 1100000
    res = {'low': int(base * 0.94), 'high': int(base * 1.06), 'likely': base, 'walkaway': int(base * 1.12)}
    forecast = [base, int(base*0.78), int(base*0.68), int(base*0.62), int(base*0.58), int(base*0.55)]
    return render_template('buyer_dashboard.html', res=res, forecast=forecast, model=model, asking=asking)

if __name__ == '__main__':
    app.run(debug=True)
