from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    user_name = request.form.get('name', 'Rajeev Thakur')
    return render_template('role.html', user_name=user_name)

@app.route('/seller')
def seller():
    # Ensuring the year range is correct for the Indian market
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years)

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    # Accepting POST ensures the form submission doesn't crash
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
