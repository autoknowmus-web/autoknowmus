from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    # Capture name from home page to personalize the greeting
    user_name = request.form.get('name', 'Rajeev Thakur')
    return render_template('role.html', user_name=user_name)

@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years)

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    # This route now accepts POST from the seller form
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
