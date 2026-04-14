from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

stats = {
    "total_searches": "1,24,500+",
    "real_transactions": "8,432"
}

@app.route('/')
def index():
    return render_template('index.html', stats=stats)

# This route MUST be here to prevent the 404 error when clicking Google/Apple
@app.route('/login', methods=['GET', 'POST'])
def login():
    user_info = {"name": "Rajeev Thakur", "credits": 500}
    return render_template('role.html', stats=stats, user=user_info)

@app.route('/get_started', methods=['GET', 'POST'])
def get_started():
    # Captures name from email signup and moves to role selection
    name = request.form.get('name', 'User')
    user_info = {"name": name, "credits": 500}
    return render_template('role.html', stats=stats, user=user_info)

@app.route('/seller', methods=['GET', 'POST'])
def seller():
    name = request.args.get('name', 'Rajeev Thakur')
    user_info = {"name": name, "credits": 500}
    return render_template('seller.html', user=user_info)

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    car_data = {
        "make": request.form.get('make'),
        "model": request.form.get('model'),
        "year": request.form.get('year'),
        "variant": request.form.get('variant'),
        "fuel": request.form.get('fuel'),
        "owners": request.form.get('owners'),
        "condition": request.form.get('condition'),
        "mileage": request.form.get('mileage')
    }
    user_info = {"name": request.form.get('user_name', 'Rajeev Thakur'), "credits": 500}
    return render_template('dashboard.html', car=car_data, user=user_info)

@app.route('/submit_transaction', methods=['POST'])
def submit_transaction():
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)