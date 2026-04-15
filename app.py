from flask import Flask, render_template, request

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/role', methods=['GET', 'POST'])
def role():
    # Capture the name from the home page form
    user_name = request.form.get('name', 'Rajeev Thakur')
    return render_template('role.html', user_name=user_name)

@app.route('/seller')
def seller():
    years = list(range(2026, 2010, -1))
    return render_template('seller.html', years=years)

@app.route('/dashboard', methods=['GET', 'POST'])
def dashboard():
    # Accepting POST fixes the 500/Internal Server Error
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True)
