from flask import Flask, render_template, request, redirect, url_for

app = Flask(__name__)

# Mock data to simulate active usage on landing page
stats = {
    "total_searches": "1,24,500+",
    "real_transactions": "8,432"
}

@app.route('/')
def index():
    return render_template('index.html', stats=stats)

@app.route('/get_insights', methods=['POST'])
def get_insights():
    # After user clicks a button, we redirect them to the seller page
    return redirect(url_for('seller'))

@app.route('/seller')
def seller():
    # The page where the user enters their car details
    return render_template('seller.html')

@app.route('/dashboard')
def dashboard():
    # The final results page
    return render_template('dashboard.html')

if __name__ == '__main__':
    app.run(debug=True, port=5000)