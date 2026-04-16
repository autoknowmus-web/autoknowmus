from flask import Flask, render_template, request
import random

app = Flask(__name__)

# Master Data - Iteration 51
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
    base = 1450000 if make == "Toyota" else 1100000
    
    # [FIX] 6-Month Forecast with 15-Day Intervals (12 Points)
    forecast_points = []
    current_val = base
    for i in range(12):
        current_val = int(current_val * 0.993) # ~0.7% drop every 15 days
        forecast_points.append(current_val)
        
    return render_template('dashboard.html', base=base, forecast=forecast_points)

@app.route('/buyer')
def buyer():
    return render_template('buyer.html', brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

if __name__ == '__main__':
    app.run(debug=True)
