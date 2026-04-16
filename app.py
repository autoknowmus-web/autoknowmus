from flask import Flask, render_template, request, session
import random

app = Flask(__name__)
app.secret_key = 'autoknowmus_secret'

CITIES = sorted(["Ahmedabad", "Bangalore", "Chandigarh", "Chennai", "Delhi", "Gurgaon", "Hyderabad", "Jaipur", "Kochi", "Kolkata", "Mumbai", "Noida", "Pune", "Lucknow", "Indore"])
BRANDS = sorted(["Audi", "BMW", "Honda", "Hyundai", "Kia", "Mahindra", "Maruti Suzuki", "Mercedes-Benz", "MG Motors", "Skoda", "Tata Motors", "Toyota", "Volkswagen"])
CONDITIONS = ["Excellent (showroom like)", "Average (normal wear)", "Fair (needs some repair)"]

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['POST'])
def role():
    user_name = request.form.get('name', 'Rajeev Thakur')
    # [Requirement] Initial Bonus Credits on first login
    session['credits'] = 300  # Equal to 3 free searches (100 per search)
    session['user_name'] = user_name
    return render_template('role.html', user_name=user_name, credits=session['credits'])

@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years, brands=BRANDS, cities=CITIES, conditions=CONDITIONS, credits=session.get('credits', 0))

@app.route('/dashboard', methods=['POST'])
def dashboard():
    # [Requirement] Deduct credits for search
    current_credits = session.get('credits', 0)
    if current_credits >= 100:
        session['credits'] -= 100
        can_search = True
    else:
        can_search = False

    make = request.form.get('make', 'Toyota')
    base = 1450000 if make == "Toyota" else 1100000
    
    # 15-day interval depreciation
    forecast = [int(base * (0.993**i)) for i in range(12)]
        
    return render_template('dashboard.html', base=base, forecast=forecast, can_search=can_search, credits=session.get('credits', 0))

@app.route('/buyer')
def buyer():
    return render_template('buyer.html', brands=BRANDS, cities=CITIES, conditions=CONDITIONS, credits=session.get('credits', 0))

if __name__ == '__main__':
    app.run(debug=True)
