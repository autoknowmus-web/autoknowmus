from flask import Flask, render_template, request
import datetime

app = Flask(__name__)

CITIES = sorted(["Ahmedabad", "Bangalore", "Chandigarh", "Chennai", "Delhi", "Gurgaon", "Hyderabad", "Jaipur", "Kochi", "Kolkata", "Mumbai", "Noida", "Pune", "Lucknow", "Indore"])
BRANDS = sorted(["Audi", "BMW", "Honda", "Hyundai", "Kia", "Mahindra", "Maruti Suzuki", "Mercedes-Benz", "MG Motors", "Skoda", "Tata Motors", "Toyota", "Volkswagen"])
# [Requirement Fix] Decapitalized descriptions, Capitalized Start
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
    
    # [Requirement Fix] 6-Month Forecast (Every 15 days = 12 points)
    # Steep drop initially, then 0.5% every 15 days
    forecast_15d = []
    current_val = base
    for i in range(12):
        current_val = int(current_val * 0.995)
        forecast_15d.append(current_val)
        
    return render_template('dashboard.html', base=base, forecast=forecast_15d)

@app.route('/buyer')
def buyer():
    return render_template('buyer.html', brands=BRANDS, cities=CITIES, conditions=CONDITIONS)

if __name__ == '__main__':
    app.run(debug=True)
